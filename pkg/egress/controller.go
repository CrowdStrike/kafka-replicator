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

package egress

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

// Controller represents the egress controller
type Controller struct {
	config       ControllerConfig
	consumer     core.Consumer
	segmentStore core.SegmentStore
	nextWorkerID int
	workers      map[workerKey]*worker
	controlChan  chan interface{}
}

type workerKey struct {
	topic     string
	partition uint32
}

type stopCommand chan struct{}

// Start will start the controller
func (c *Controller) Start() error {
	if err := utils.LifecycleStart(c.log(), c.segmentStore, c.consumer); err != nil {
		return err
	}

	var topics []string
	for topic := range c.config.Topics {
		topics = append(topics, topic)
	}

	sort.Strings(topics)

	if err := c.consumer.Subscribe(topics...); err != nil {
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

	utils.LifecycleStop(c.log(), c.segmentStore, c.consumer)
}

func (c *Controller) consumerLoop() {
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
		default:
			event := c.consumer.Poll()
			if event == nil {
				continue
			}

			switch e := event.(type) {
			case *kafka.Message:
				if !c.sanityChecks(e.TopicPartition) {
					continue
				}

				key := workerKey{
					topic:     *e.TopicPartition.Topic,
					partition: uint32(e.TopicPartition.Partition),
				}

				worker := c.workers[key]
				if worker == nil {
					c.log().Warnf("Ignored message for unassigned topic %s, partition %d", key.topic, key.partition)
					continue
				}

				worker.messageChan <- e
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

				c.startWorkers(e.Partitions)
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

		topic := *tp.Topic
		key := workerKey{
			topic:     topic,
			partition: uint32(tp.Partition),
		}

		if c.workers[key] != nil {
			c.log().Warnf("Worker for source topic %s, partition %d already running", key.topic, key.partition)
			continue
		}

		tconfig := c.config.Topics[topic]
		ctx, ctxCancel := context.WithCancel(context.Background())

		worker := &worker{
			localRegion:   c.config.LocalRegion,
			config:        *tconfig,
			id:            c.getNextWorkerID(),
			topic:         key.topic,
			partition:     key.partition,
			consumer:      c.consumer,
			segmentStore:  c.segmentStore,
			context:       ctx,
			contextCancel: ctxCancel,
			messageChan:   make(chan *kafka.Message, tconfig.WorkerChanSize),
			controlChan:   make(chan interface{}, 1),
			metrics:       newWorkerMetrics(c.config.LocalRegion, key.topic, key.partition),
		}

		c.workers[key] = worker
		go worker.run()
	}
}

func (c *Controller) stopWorkers(topicPartition []kafka.TopicPartition) {
	var wg sync.WaitGroup
	toRemove := make([]workerKey, 0, len(topicPartition))

	for _, tp := range topicPartition {
		if !c.sanityChecks(tp) {
			continue
		}

		key := workerKey{
			topic:     *tp.Topic,
			partition: uint32(tp.Partition),
		}

		wrk := c.workers[key]
		if wrk == nil {
			c.log().Warnf("Worker not found for source topic %s, partition %d", key.topic, key.partition)
			continue
		}

		toRemove = append(toRemove, key)
		wg.Add(1)

		go func(w *worker) {
			w.stop()
			wg.Done()
		}(wrk)
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

	if c.config.Topics[*tp.Topic] == nil {
		c.log().Warnf("Ignored unknown topic %", *tp.Topic)
		return false
	}

	return true
}

func (c *Controller) log() core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type":        "egress.Controller",
		"LocalRegion": c.config.LocalRegion,
	})
}
