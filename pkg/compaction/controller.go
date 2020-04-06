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

package compaction

import (
	"context"
	"sort"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/robfig/cron"
)

var (
	_ core.Lifecycle = &Controller{}
)

// Controller represents the compaction controller.
type Controller struct {
	config       ControllerConfig
	consumer     core.Consumer
	segmentStore core.SegmentStore
	compactor    *Compactor
	controlChan  chan interface{}
	cron         *cron.Cron
}

type compactionKey struct {
	topic     string
	partition uint32
}

type compaction struct {
	request       compactRequest
	context       context.Context
	contextCancel context.CancelFunc
}

type stopCommand chan struct{}
type compactCommand struct{}

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

	if c.cron != nil {
		c.cron.Start()
	}

	return nil
}

// Stop will stop the controller
func (c *Controller) Stop() {
	if c.cron != nil {
		c.cron.Stop()
	}

	stopCmd := make(stopCommand)
	c.controlChan <- stopCmd
	<-stopCmd

	utils.LifecycleStop(c.log(), c.segmentStore, c.consumer)
}

// Compact starts the compaction for current assigned topic partitions that are not already running or scheduled to run.
func (c *Controller) Compact() {
	c.controlChan <- compactCommand{}
}

func (c *Controller) consumerLoop() {
	requestChan := make(chan compaction)
	resultChan := make(chan compaction, c.config.Parallelism)

	for i := 0; i < c.config.Parallelism; i++ {
		go c.compactLoop(requestChan, resultChan)
	}

	assignments := map[compactionKey]bool{}
	queued := map[compactionKey]*compaction{}
	running := map[compactionKey]*compaction{}

	getNextQueued := func() (compaction, chan compaction) {
		for _, compaction := range queued {
			return *compaction, requestChan
		}

		return compaction{}, nil
	}

LOOP:
	for {
		nextQueued, requestChanDisabled := getNextQueued()

		select {
		case msg := <-c.controlChan:
			switch cmd := msg.(type) {
			case stopCommand:
				for _, compaction := range running {
					compaction.stop()
				}

				for range running {
					<-resultChan
				}

				close(requestChan)
				close(cmd)
				break LOOP
			case compactCommand:
				for key := range assignments {
					if queued[key] != nil || running[key] != nil {
						continue
					}

					ctx, ctxCancel := context.WithCancel(context.Background())

					queued[key] = &compaction{
						request: compactRequest{
							region:    c.config.LocalRegion,
							topic:     key.topic,
							partition: key.partition,
							config:    *c.config.Topics[key.topic],
						},
						context:       ctx,
						contextCancel: ctxCancel,
					}

					c.log().Debugf("Queued compaction for topic=%s, partition=%d", key.topic, key.partition)
				}
			default:
				c.log().Warnf("Ignored unknown control message: %+v", msg)
			}
		case requestChanDisabled <- nextQueued:
			key := nextQueued.key()

			running[key] = &nextQueued
			delete(queued, key)
		case c := <-resultChan:
			delete(running, c.key())
		default:
			event := c.consumer.Poll()
			if event == nil {
				continue
			}

			switch e := event.(type) {
			case kafka.RevokedPartitions:
				for _, tp := range e.Partitions {
					if !c.sanityChecks(tp) {
						continue
					}

					key := c.getCompactionKey(tp)

					if compaction, ok := running[key]; ok {
						compaction.stop()
					}

					delete(queued, key)
					delete(assignments, key)
				}

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

				for _, tp := range e.Partitions {
					if !c.sanityChecks(tp) {
						continue
					}

					key := c.getCompactionKey(tp)
					assignments[key] = true
				}
			case *kafka.Message:
				c.log().Warn("Unexpected message")
			case utils.DelayEvent:
				e.Delay()
			}
		}
	}
}

func (c *Controller) compactLoop(requestChan <-chan compaction, resultChan chan<- compaction) {
	for r := range requestChan {
		c.compactor.compactAndLog(r.context, r.request)
		resultChan <- r
	}
}

func (c *Controller) getCompactionKey(tp kafka.TopicPartition) compactionKey {
	return compactionKey{
		topic:     *tp.Topic,
		partition: uint32(tp.Partition),
	}
}

func (c *Controller) onCronSchedule() {
	c.Compact()
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
		"Type":        "compaction.Controller",
		"LocalRegion": c.config.LocalRegion,
	})
}

func (c *compaction) key() compactionKey {
	return compactionKey{
		topic:     c.request.topic,
		partition: c.request.partition,
	}
}

func (c *compaction) stop() {
	if c.context.Err() == nil {
		c.contextCancel()
	}
}
