// Copyright 2020 CrowdStrike Holdings, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ingress

import (
	"context"
	"sort"
	"sync"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	_ core.Lifecycle = &Controller{}
)

// Controller represents the ingress controller
type Controller struct {
	config          ControllerConfig
	destTopicMap    map[string][]sourceInfo
	consumer        core.Consumer
	producer        core.Producer
	segmentStore    core.SegmentStore
	checkpointStore core.CheckpointStore
	nextWorkerID    int
	workers         map[workerKey]*worker
	controlChan     chan interface{}
}

type sourceInfo struct {
	source Source
	config SourceConfig
}

type workerKey struct {
	region    string
	topic     string
	partition uint32
}

type stopCommand chan struct{}

// Start will start the controller
func (c *Controller) Start() error {
	if err := utils.LifecycleStart(c.log(), c.segmentStore, c.checkpointStore, c.producer, c.consumer); err != nil {
		return err
	}

	// group sources by destination topic
	destTopicMap := map[string][]sourceInfo{}
	var destTopics []string

	for source, config := range c.config.Sources {
		topic := config.DestinationTopic

		if _, ok := destTopicMap[topic]; !ok {
			destTopics = append(destTopics, topic)
		}

		destTopicMap[topic] = append(destTopicMap[topic], sourceInfo{source: source, config: *config})
	}

	c.destTopicMap = destTopicMap
	sort.Strings(destTopics)

	if err := c.consumer.Subscribe(destTopics...); err != nil {
		return err
	}

	go c.consumerLoop()

	return nil
}

// Stop will stop the controller
func (c *Controller) Stop() {
	stopCmd := make(stopCommand)
	c.controlChan <- stopCmd
	<-stopCmd

	utils.LifecycleStop(c.log(), c.segmentStore, c.checkpointStore, c.producer, c.consumer)
}

func (c *Controller) consumerLoop() {
	handleEvent := func(event core.SegmentEvent) {
		if event.Segment.Region == c.config.LocalRegion {
			return
		}

		key := workerKey{
			region:    event.Segment.Region,
			topic:     event.Segment.Topic,
			partition: event.Segment.Partition,
		}

		worker := c.workers[key]
		if worker != nil {
			worker.eventsChan <- event
		}
	}

LOOP:
	for {
		select {
		case msg := <-c.controlChan:
			switch cmd := msg.(type) {
			case stopCommand:
				c.stopAllWorkers()
				close(cmd)
				break LOOP
			default:
				c.log().Warnf("Ignored unknown control message: %+v", msg)
			}
		case event := <-c.segmentStore.Events():
			handleEvent(event.SegmentEvent)
			close(event.Result)
		default:
			event := c.consumer.Poll()
			if event == nil {
				continue
			}

			switch e := event.(type) {
			case kafka.RevokedPartitions:
				c.stopWorkers(e.Partitions)

				if err := c.consumer.Unassign(); err != nil {
					c.log().Errorf("Consumer unassign error: %+v", err)
				}
			case kafka.AssignedPartitions:
				if err := c.consumer.Assign(e.Partitions); err != nil {
					c.log().Errorf("Consumer assign error: %+v", err)
					continue
				}

				if err := c.consumer.Pause(e.Partitions); err != nil {
					c.log().Errorf("Consumer pause error: %+v", err)
					continue
				}

				c.startWorkers(e.Partitions)
			case *kafka.Message:
				c.log().Warn("Unexpected message")
			case utils.DelayEvent:
				e.Delay()
			}
		}
	}
}

func (c *Controller) getNextWorkerID() int {
	result := c.nextWorkerID
	c.nextWorkerID++
	return result
}

func (c *Controller) startWorkers(topicPartition []kafka.TopicPartition) {
	for _, tp := range topicPartition {
		if !c.sanityChecks(tp) {
			continue
		}

		destTopic := *tp.Topic
		partition := uint32(tp.Partition)

		for _, s := range c.destTopicMap[destTopic] {
			srcRegion := s.source.Region
			srcTopic := s.source.Topic

			key := workerKey{
				region:    srcRegion,
				topic:     srcTopic,
				partition: partition,
			}

			if c.workers[key] != nil {
				c.log().Warnf("Worker for source region %s, topic %s, partition %d already running", srcRegion, srcTopic, partition)
				continue
			}

			ctx, ctxCancel := context.WithCancel(context.Background())

			worker := &worker{
				localRegion:     c.config.LocalRegion,
				config:          s.config,
				id:              c.getNextWorkerID(),
				srcRegion:       srcRegion,
				srcTopic:        srcTopic,
				destTopic:       destTopic,
				partition:       partition,
				producer:        c.producer,
				segmentStore:    c.segmentStore,
				checkpointStore: c.checkpointStore,
				context:         ctx,
				contextCancel:   ctxCancel,
				controlChan:     make(chan interface{}, 1),
				eventsChan:      make(chan core.SegmentEvent, s.config.WorkerChanSize),
				metrics:         newWorkerMetrics(srcRegion, srcTopic, partition),
			}

			c.workers[key] = worker
			go worker.controlLoop()
		}
	}
}

func (c *Controller) stopWorkers(topicPartition []kafka.TopicPartition) {
	var wg sync.WaitGroup
	var toRemove []workerKey

	for _, tp := range topicPartition {
		if !c.sanityChecks(tp) {
			continue
		}

		destTopic := *tp.Topic
		partition := uint32(tp.Partition)

		for _, s := range c.destTopicMap[destTopic] {
			srcRegion := s.source.Region
			srcTopic := s.source.Topic

			key := workerKey{
				region:    srcRegion,
				topic:     srcTopic,
				partition: partition,
			}

			wrk := c.workers[key]
			if wrk == nil {
				c.log().Warnf("Worker not found for source region %s, topic %s, partition %d", srcRegion, srcTopic, partition)
				continue
			}

			toRemove = append(toRemove, key)
			wg.Add(1)

			go func(w *worker) {
				w.stop()
				wg.Done()
			}(wrk)
		}
	}

	wg.Wait()
	for _, key := range toRemove {
		delete(c.workers, key)
	}
}

func (c *Controller) stopAllWorkers() {
	var wg sync.WaitGroup

	for _, wrk := range c.workers {
		wg.Add(1)
		go func(w *worker) {
			w.stop()
			wg.Done()
		}(wrk)
	}

	wg.Wait()
	c.workers = map[workerKey]*worker{}
}

func (c *Controller) sanityChecks(tp kafka.TopicPartition) bool {
	if tp.Topic == nil {
		c.log().Warn("Ignored topic partition with missing topic name")
		return false
	}

	if tp.Partition < 0 {
		c.log().Warnf("Ignored topic partition with invalid partition number %d", tp.Partition)
		return false
	}

	return true
}

func (c *Controller) log() core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type":        "ingress.Controller",
		"LocalRegion": c.config.LocalRegion,
	})
}
