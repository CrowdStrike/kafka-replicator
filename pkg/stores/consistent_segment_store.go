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

package stores

import (
	"context"
	"sync"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/emirpasic/gods/trees/binaryheap"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

var (
	// DefaultEventsRetentionCheckInterval is the default interval when background expiration task is running.
	DefaultEventsRetentionCheckInterval = time.Minute

	// DefaultSegmentEventsChanSize is the default size of events buffered channel.
	DefaultSegmentEventsChanSize = 100

	cssStartTimeTimer             = core.DefaultMetrics.GetTimer("css.start.time")
	cssMessagesConsumedMeter      = core.DefaultMetrics.GetMeter("css.messages.consumed")
	cssListSegmentsTimeTimer      = core.DefaultMetrics.GetTimer("css.list-segments.time")
	cssListSegmentsStaleReadTimer = core.DefaultMetrics.GetTimer("css.list-segments.stale-read")

	_ core.SegmentStore       = &ConsistentSegmentStore{}
	_ core.SegmentEventSource = &ConsistentSegmentStore{}
	_ core.Lifecycle          = &ConsistentSegmentStore{}
	_ core.Factory            = &ConsistentSegmentStoreConfig{}
)

// ConsistentSegmentStoreConfig is the ConsistentSegmentStore configuration.
type ConsistentSegmentStoreConfig struct {
	// PubSub is used to:
	//   - distribute the incoming events to all running instances,
	//   - temporarily store events for group rebalance scenarios, and
	//   - to address problems that can arise due to AWS S3 eventual consistency model.
	PubSub core.Factory `required:"true"`

	// SegmentStore is the underlying segment store implementation.
	SegmentStore core.Factory `required:"true"`

	// SegmentEventSource is the source of segment events for underlying segment store.
	SegmentEventSource core.Factory `required:"true"`

	// Kafka topic name for storing segment events.
	//
	// The topic cleanup policy needs to be set to 'delete' with appropriate
	// retention time set to discard old segment events.
	//
	// Field value is required.
	Topic string `required:"true"`

	// The duration each segment event is tracked.
	//
	// Should match the configured Kafka topic retention time.
	//
	// Field value is required.
	EventsRetention time.Duration `min:"10m"`

	// The interval when background expiration task is running to remove old event from memory.
	//
	// Default value is set via DefaultEventsRetentionCheckInterval variable.
	EventsRetentionCheckInterval time.Duration `min:"1ms" max:"10m"`

	// Size of events buffered channel.
	//
	// Default value is set via DefaultSegmentEventsChanSize variable.
	EventsChanSize int `min:"1"`
}

// ConsistentSegmentStore is the decorator that handles segment events to provide a consistent view of underlying store state.
type ConsistentSegmentStore struct {
	config        ConsistentSegmentStoreConfig
	pubsub        core.PubSub
	innerStore    core.SegmentStore
	eventSource   core.SegmentEventSource
	stopChan      chan struct{}
	eventsChan    chan core.SegmentEventRequest
	mutex         sync.RWMutex
	lastEvents    map[core.Segment]*core.SegmentEvent
	orderedEvents *binaryheap.Heap
}

// NewConsistentSegmentStore returns a new ConsistentSegmentStore instance
func NewConsistentSegmentStore(config ConsistentSegmentStoreConfig) (*ConsistentSegmentStore, error) {
	utils.SetDefaultDuration(&config.EventsRetentionCheckInterval, DefaultEventsRetentionCheckInterval)
	utils.SetDefaultInt(&config.EventsChanSize, DefaultSegmentEventsChanSize)

	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		return nil, errors.Errorf("NewCheckpointStore: invalid config %s", errMessage)
	}

	instances, err := utils.CallFactory(
		utils.FactoryType{Factory: config.PubSub, Type: (*core.PubSub)(nil)},
		utils.FactoryType{Factory: config.SegmentStore, Type: (*core.SegmentStore)(nil)},
		utils.FactoryType{Factory: config.SegmentEventSource, Type: (*core.SegmentEventSource)(nil)})
	if err != nil {
		return nil, err
	}

	result := &ConsistentSegmentStore{
		config:      config,
		pubsub:      instances[0].(core.PubSub),
		innerStore:  instances[1].(core.SegmentStore),
		eventSource: instances[2].(core.SegmentEventSource),
		stopChan:    make(chan struct{}),
		eventsChan:  make(chan core.SegmentEventRequest, config.EventsChanSize),
		lastEvents:  map[core.Segment]*core.SegmentEvent{},
	}

	result.orderedEvents = binaryheap.NewWith(result.segmentEventComparator)
	return result, nil
}

// Get creates the corresponding instance
func (c ConsistentSegmentStoreConfig) Get() (interface{}, error) {
	return NewConsistentSegmentStore(c)
}

// Start will start updating store state
func (r *ConsistentSegmentStore) Start() error {
	if err := utils.LifecycleStart(r.log(), r.innerStore, r.pubsub, r.eventSource); err != nil {
		return err
	}

	if err := r.pubsub.Subscribe(r.config.Topic); err != nil {
		return err
	}

	startupDone := make(chan struct{})
	startTime := time.Now()

	go r.consumerLoop(startupDone)

	<-startupDone
	cssStartTimeTimer.UpdateSince(startTime)

	go r.removeExpiredLoop()

	return nil
}

// Stop will stop updating store state
func (r *ConsistentSegmentStore) Stop() {
	close(r.stopChan)

	utils.LifecycleStop(r.log(), r.innerStore, r.pubsub, r.eventSource)
}

// Create will create a new segment file
func (r *ConsistentSegmentStore) Create(ctx context.Context) (core.SegmentWriter, error) {
	return r.innerStore.Create(ctx)
}

// Open will open the segment for reading
func (r *ConsistentSegmentStore) Open(ctx context.Context, segment core.Segment) (core.SegmentReader, error) {
	return r.innerStore.Open(ctx, segment)
}

// ListSegments returns the requested segments
func (r *ConsistentSegmentStore) ListSegments(ctx context.Context, region, topic string, partition uint32) (map[core.Segment]core.SegmentInfo, error) {
	startTime := time.Now()
	defer cssListSegmentsTimeTimer.UpdateSince(startTime)

	result, err := r.innerStore.ListSegments(ctx, region, topic, partition)
	if err != nil {
		return nil, err
	}

	r.mutex.RLock()

	// to avoid linear scan could use an in-memory index as a space–time tradeoff, but it would add more complexity
	// current approach is good enough as the method is not part of main execution path
	for segment, event := range r.lastEvents {
		if segment.Partition != partition || segment.Region != region || segment.Topic != topic {
			continue
		}

		switch event.Type {
		case core.SegmentEvent_CREATED:
			if _, ok := result[segment]; !ok {
				result[segment] = core.SegmentInfo{
					Segment:   segment,
					Timestamp: event.Timestamp,
					Size:      event.SegmentSize,
				}

				cssListSegmentsStaleReadTimer.UpdateSince(event.Timestamp)
			}
		case core.SegmentEvent_REMOVED:
			if info, ok := result[segment]; ok && info.Timestamp.Before(event.Timestamp) {
				delete(result, segment)
				cssListSegmentsStaleReadTimer.UpdateSince(event.Timestamp)
			}
		}
	}

	r.mutex.RUnlock()
	return result, nil
}

// Events returns the segment event channel
func (r *ConsistentSegmentStore) Events() <-chan core.SegmentEventRequest {
	return r.eventsChan
}

// Delete removes the provided segment
func (r *ConsistentSegmentStore) Delete(ctx context.Context, segment core.Segment) error {
	return r.innerStore.Delete(ctx, segment)
}

func (r *ConsistentSegmentStore) consumerLoop(startupDone chan struct{}) {
	initialized := false

	sendEvent := func(event core.SegmentEventRequest) bool {
		select {
		case r.eventsChan <- event:
			return true
		case <-r.stopChan:
			return false
		}
	}

LOOP:
	for {
		select {
		case <-r.stopChan:
			break LOOP
		case event := <-r.eventSource.Events():
			if !r.updateState(&event.SegmentEvent) {
				close(event.Result)
				continue
			}

			value, err := r.marshalSegmentEvent(event.SegmentEvent)
			if err != nil {
				event.Result <- err
				continue
			}

			if err := r.pubsub.Publish(r.config.Topic, nil, value); err != nil {
				event.Result <- err
				continue
			}

			if !initialized {
				close(event.Result)
				continue
			}

			if !sendEvent(event) {
				close(event.Result)
				break LOOP
			}
		case event := <-r.pubsub.Events():
			switch e := event.(type) {
			case *kafka.Message:
				cssMessagesConsumedMeter.Mark(1)

				if !r.sanityChecks(e.TopicPartition) {
					continue
				}

				event, err := r.unmarshalSegmentEvent(e.Value)
				if err != nil {
					r.log().Errorf("Parse segment event failed at partition %d, offset %d. Error: %+v", e.TopicPartition.Partition, e.TopicPartition.Offset, err)
					continue
				}

				if !r.updateState(event) {
					continue
				}

				if !initialized {
					continue
				}

				resultChan := make(chan error)
				request := core.SegmentEventRequest{
					SegmentEvent: *event,
					Result:       resultChan,
				}

				if !sendEvent(request) {
					break LOOP
				}

				if err := <-resultChan; err != nil {
					r.log().Errorf("Unexpected segment event error: %+v", err)
				}
			case core.TopicEOF:
				close(startupDone)
				initialized = true
			default:
				r.log().Warnf("Ignored unknown consumer event: %+v", event)
			}
		}
	}

	// if stopped before initialization is done
	if !initialized {
		close(startupDone)
	}
}

func (r *ConsistentSegmentStore) removeExpiredLoop() {
LOOP:
	for {
		select {
		case <-r.stopChan:
			break LOOP
		case <-time.After(r.config.EventsRetentionCheckInterval):
			r.removeExpired()
		}
	}
}

func (r *ConsistentSegmentStore) removeExpired() {
	r.mutex.Lock()

	oldest := r.getOldestTimestamp()
	for {
		eventI, ok := r.orderedEvents.Peek()
		if !ok {
			break
		}

		event := eventI.(*core.SegmentEvent)
		if !event.Timestamp.Before(oldest) {
			break
		}

		r.orderedEvents.Pop()

		lastEvent, ok := r.lastEvents[event.Segment]
		if ok && event.Timestamp.Equal(lastEvent.Timestamp) {
			delete(r.lastEvents, event.Segment)
		}
	}

	r.mutex.Unlock()
}

func (r *ConsistentSegmentStore) updateState(event *core.SegmentEvent) bool {
	oldest := r.getOldestTimestamp()
	if event.Timestamp.Before(oldest) {
		return false
	}

	r.mutex.Lock()

	lastEvent, ok := r.lastEvents[event.Segment]
	if ok && !event.Timestamp.After(lastEvent.Timestamp) {
		r.mutex.Unlock()
		return false
	}

	r.lastEvents[event.Segment] = event
	r.orderedEvents.Push(event)

	r.mutex.Unlock()
	return true
}

func (r *ConsistentSegmentStore) getOldestTimestamp() time.Time {
	return time.Now().UTC().Add(-r.config.EventsRetention)
}

func (r *ConsistentSegmentStore) segmentEventComparator(a, b interface{}) int {
	s1 := a.(*core.SegmentEvent)
	s2 := b.(*core.SegmentEvent)

	if s1.Timestamp.Before(s2.Timestamp) {
		return -1
	} else if s1.Timestamp.Equal(s2.Timestamp) {
		return 0
	}

	return 1
}

func (r *ConsistentSegmentStore) sanityChecks(tp kafka.TopicPartition) bool {
	if tp.Topic == nil {
		r.log().Warn("Ignored message with missing topic")
		return false
	}

	if *tp.Topic != r.config.Topic {
		r.log().Warnf("Ignored message for unknown topic %s", *tp.Topic)
		return false
	}

	return true
}

func (r *ConsistentSegmentStore) marshalSegmentEvent(event core.SegmentEvent) ([]byte, error) {
	bytes, err := proto.Marshal(&event)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal event")
	}

	return bytes, nil
}

func (r *ConsistentSegmentStore) unmarshalSegmentEvent(bytes []byte) (*core.SegmentEvent, error) {
	var result core.SegmentEvent
	if err := proto.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (r *ConsistentSegmentStore) log() core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type": "ConsistentSegmentStore",
	})
}
