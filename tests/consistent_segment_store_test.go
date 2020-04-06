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

package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/stores"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ConsistentSegmentStore tests", func() {
	var (
		ctx                          = context.Background()
		eventsRetention              = time.Hour
		eventsTopic                  = "segment_events_topic"
		region                       = "test_region"
		topic                        = "test_topic"
		partition1                   = uint32(1)
		partition2                   = uint32(2)
		eventsRetentionCheckInterval = waitDuration
		mockPubSub                   *MockPubSub
		mockInnerStore               *MockSegmentStore
		mockSegmentEventSource       *MockSegmentEventSource
		store                        *stores.ConsistentSegmentStore
	)

	BeforeEach(func() {
		mockPubSub = NewMockPubSub(mockController)
		mockInnerStore = NewMockSegmentStore(mockController)
		mockSegmentEventSource = NewMockSegmentEventSource(mockController)

		s, err := stores.NewConsistentSegmentStore(stores.ConsistentSegmentStoreConfig{
			PubSub:             core.Self(mockPubSub),
			SegmentStore:       core.Self(mockInnerStore),
			SegmentEventSource: core.Self(mockSegmentEventSource),

			Topic:                        eventsTopic,
			EventsRetention:              eventsRetention,
			EventsRetentionCheckInterval: eventsRetentionCheckInterval,
		})

		Expect(err).To(BeNil())
		store = s
	})

	expectEvents := func(events ...interface{}) {
		var calls []*gomock.Call

		addEvents := func(segmentEvent *core.SegmentEventRequest, kafkaEvent *kafka.Event) {
			var segmentEventChan chan core.SegmentEventRequest
			var kafkaEventChan chan kafka.Event

			if segmentEvent != nil {
				segmentEventChan = make(chan core.SegmentEventRequest, 1)
				segmentEventChan <- *segmentEvent
			}

			if kafkaEvent != nil {
				kafkaEventChan = make(chan kafka.Event, 1)
				kafkaEventChan <- *kafkaEvent
			}

			calls = append(calls, mockSegmentEventSource.EXPECT().Events().Return(segmentEventChan))
			calls = append(calls, mockPubSub.EXPECT().Events().Return(kafkaEventChan))
		}

		for _, event := range events {
			switch e := event.(type) {
			case core.SegmentEventRequest:
				addEvents(&e, nil)
			case core.SegmentEvent:
				request := newSegmentEventRequest(e)
				addEvents(&request, nil)
			case kafka.Event:
				addEvents(nil, &e)
			default:
				Fail(fmt.Sprintf("unknown event type %T", e))
			}
		}

		gomock.InOrder(calls...)
		expectPubSubForever(mockPubSub, lastCall(calls))
		expectSegmentEventsForever(mockSegmentEventSource.EXPECT(), lastCall(calls))
	}

	Context("When Subscribe is successful", func() {
		startStore := func() {
			mockPubSub.EXPECT().Subscribe(eventsTopic)
			err := store.Start()
			Expect(err).To(BeNil())
		}

		stopStore := func(delay time.Duration) {
			<-time.After(delay)
			store.Stop()
		}

		runStore := func() {
			startStore()
			stopStore(stopWaitDuration)
		}

		newSegmentEventMessage := func(partition int32, offset kafka.Offset, event core.SegmentEvent) *kafka.Message {
			value, err := proto.Marshal(&event)
			Expect(err).To(BeNil())

			return &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &eventsTopic,
					Partition: partition,
					Offset:    offset,
				},
				Value: value,
			}
		}

		Context("When ListSegments is called", func() {
			var (
				now               = time.Now().UTC()
				segment1          = core.Segment{Region: region, Topic: topic, Partition: partition1, StartOffset: 0, EndOffset: 1} // created
				segment2          = core.Segment{Region: region, Topic: topic, Partition: partition1, StartOffset: 2, EndOffset: 3} // removed: events in order
				segment3          = core.Segment{Region: region, Topic: topic, Partition: partition1, StartOffset: 4, EndOffset: 5} // removed: events out of order
				segment4          = core.Segment{Region: region, Topic: topic, Partition: partition1, StartOffset: 6, EndOffset: 7} // created: duplicate events
				segment5          = core.Segment{Region: region, Topic: topic, Partition: partition1, StartOffset: 8, EndOffset: 9} // removed: duplicate events
				segment6          = core.Segment{Region: region, Topic: topic, Partition: partition2, StartOffset: 0, EndOffset: 1} // other partition
				segment1Timestamp = now.Add(100)
				segment2Timestamp = now.Add(201)
				segment4Timestamp = now.Add(402)
			)

			BeforeEach(func() {
				expectEvents(
					newSegmentEventMessage(1, 0, core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: segment1Timestamp, Segment: segment1}),
					newSegmentEventMessage(1, 1, core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: now.Add(200), Segment: segment2}),
					newSegmentEventMessage(1, 2, core.SegmentEvent{Type: core.SegmentEvent_REMOVED, Timestamp: segment2Timestamp, Segment: segment2}),
					newSegmentEventMessage(1, 3, core.SegmentEvent{Type: core.SegmentEvent_REMOVED, Timestamp: now.Add(301), Segment: segment3}),
					newSegmentEventMessage(1, 4, core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: now.Add(300), Segment: segment3}),
					newSegmentEventMessage(1, 5, core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: now.Add(400), Segment: segment4}),
					newSegmentEventMessage(1, 6, core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: now.Add(401), Segment: segment4}),
					newSegmentEventMessage(1, 6, core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: segment4Timestamp, Segment: segment4}),
					newSegmentEventMessage(1, 7, core.SegmentEvent{Type: core.SegmentEvent_REMOVED, Timestamp: now.Add(500), Segment: segment5}),
					newSegmentEventMessage(1, 8, core.SegmentEvent{Type: core.SegmentEvent_REMOVED, Timestamp: now.Add(501), Segment: segment5}),
					newSegmentEventMessage(1, 9, core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: now.Add(600), Segment: segment6}),
					newTopicEOF(eventsTopic))

				startStore()
			})

			AfterEach(func() {
				stopStore(stopWaitDuration)
			})

			Context("When underlying store is missing segments", func() {
				It("Should add the missing segments to result", func() {
					mockInnerStore.EXPECT().ListSegments(ctx, region, topic, partition1).Return(map[core.Segment]core.SegmentInfo{}, nil)

					segments, err := store.ListSegments(ctx, region, topic, partition1)
					Expect(err).To(BeNil())
					Expect(len(segments)).To(Equal(2))
					Expect(segments[segment1].Timestamp).To(Equal(segment1Timestamp))
					Expect(segments[segment4].Timestamp).To(Equal(segment4Timestamp))
				})
			})

			Context("When underlying store contains deleted segment with older timestamp", func() {
				It("Should remove segment from result", func() {
					mockInnerStore.EXPECT().ListSegments(ctx, region, topic, partition1).Return(map[core.Segment]core.SegmentInfo{
						segment1: {Timestamp: segment1Timestamp},
						segment2: {Timestamp: segment2Timestamp.Add(-1000)},
						segment4: {Timestamp: segment4Timestamp},
					}, nil)

					segments, err := store.ListSegments(ctx, region, topic, partition1)
					Expect(err).To(BeNil())
					Expect(len(segments)).To(Equal(2))
					Expect(segments[segment1].Timestamp).To(Equal(segment1Timestamp))
					Expect(segments[segment4].Timestamp).To(Equal(segment4Timestamp))
				})
			})

			Context("When underlying store contains deleted segment with newer timestamp (undelete/recreate scenario)", func() {
				It("Should add the missing segments to result", func() {
					expectedSegment2Timestamp := segment2Timestamp.Add(1000)

					mockInnerStore.EXPECT().ListSegments(ctx, region, topic, partition1).Return(map[core.Segment]core.SegmentInfo{
						segment1: {Timestamp: segment1Timestamp},
						segment2: {Timestamp: expectedSegment2Timestamp},
						segment4: {Timestamp: segment4Timestamp},
					}, nil)

					segments, err := store.ListSegments(ctx, region, topic, partition1)
					Expect(err).To(BeNil())
					Expect(len(segments)).To(Equal(3))
					Expect(segments[segment1].Timestamp).To(Equal(segment1Timestamp))
					Expect(segments[segment2].Timestamp).To(Equal(expectedSegment2Timestamp))
					Expect(segments[segment4].Timestamp).To(Equal(segment4Timestamp))
				})
			})

			Context("When underlying store call fails", func() {
				It("Should return error", func() {
					mockInnerStore.EXPECT().ListSegments(ctx, region, topic, partition1).Return(nil, errors.New("error"))

					segments, err := store.ListSegments(ctx, region, topic, partition1)
					Expect(err).NotTo(BeNil())
					Expect(segments).To(BeNil())
				})
			})
		})

		Context("When receives from segment event source", func() {
			var (
				now               = time.Now().UTC()
				segment1          = core.Segment{Region: region, Topic: topic, Partition: partition1, StartOffset: 0, EndOffset: 1} // created
				segment2          = core.Segment{Region: region, Topic: topic, Partition: partition2, StartOffset: 0, EndOffset: 1} // removed
				segment1Timestamp = now.Add(100)
				segment2Timestamp = now.Add(200)
				baseEvents        []interface{}
			)

			BeforeEach(func() {
				baseEvents = []interface{}{
					newSegmentEventMessage(1, 0, core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: segment1Timestamp, Segment: segment1}),
					newSegmentEventMessage(1, 1, core.SegmentEvent{Type: core.SegmentEvent_REMOVED, Timestamp: segment2Timestamp, Segment: segment2}),
					newTopicEOF(eventsTopic),
				}
			})

			Context("When event timestamp is before current timestamp", func() {
				It("Should skip the event", func() {
					events := append(baseEvents,
						core.SegmentEvent{
							Type:      core.SegmentEvent_CREATED,
							Timestamp: segment1Timestamp.Add(-1),
							Segment:   segment1,
						},
						core.SegmentEvent{
							Type:      core.SegmentEvent_REMOVED,
							Timestamp: segment1Timestamp.Add(-1),
							Segment:   segment1,
						},
						core.SegmentEvent{
							Type:      core.SegmentEvent_CREATED,
							Timestamp: segment2Timestamp.Add(-1),
							Segment:   segment2,
						},
						core.SegmentEvent{
							Type:      core.SegmentEvent_REMOVED,
							Timestamp: segment2Timestamp.Add(-1),
							Segment:   segment2,
						})

					expectEvents(events...)
					runStore()

					Expect(store.Events()).ShouldNot(Receive())
				})
			})

			Context("When event timestamp is after current timestamp: created then created (duplicate event)", func() {
				It("Should update state and persist event", func() {
					newTimestamp := segment1Timestamp.Add(1)
					request := newSegmentEventRequest(core.SegmentEvent{
						Type:      core.SegmentEvent_CREATED,
						Timestamp: newTimestamp,
						Segment:   segment1,
					})

					mockPubSub.EXPECT().Publish(eventsTopic, nil, matchSegmentEventBytes(request.SegmentEvent))
					mockInnerStore.EXPECT().ListSegments(ctx, region, topic, partition1).Return(map[core.Segment]core.SegmentInfo{}, nil)

					expectEvents(append(baseEvents, request)...)
					runStore()

					Expect(store.Events()).Should(Receive(Equal(request)))

					segments, err := store.ListSegments(ctx, region, topic, partition1)
					Expect(err).To(BeNil())
					Expect(len(segments)).To(Equal(1))
					Expect(segments[segment1].Timestamp).To(Equal(newTimestamp))
				})
			})

			Context("When event timestamp is after current timestamp: created then removed", func() {
				It("Should update state and persist event", func() {
					newTimestamp := segment1Timestamp.Add(1)
					request := newSegmentEventRequest(core.SegmentEvent{
						Type:      core.SegmentEvent_REMOVED,
						Timestamp: newTimestamp,
						Segment:   segment1,
					})

					mockPubSub.EXPECT().Publish(eventsTopic, nil, matchSegmentEventBytes(request.SegmentEvent))
					mockInnerStore.EXPECT().ListSegments(ctx, region, topic, partition1).Return(map[core.Segment]core.SegmentInfo{}, nil)

					expectEvents(append(baseEvents, request)...)
					runStore()

					Expect(store.Events()).Should(Receive(Equal(request)))

					segments, err := store.ListSegments(ctx, region, topic, partition1)
					Expect(err).To(BeNil())
					Expect(len(segments)).To(Equal(0))
				})
			})

			Context("When event timestamp is after current timestamp: removed then created (undelete/recreate scenario)", func() {
				It("Should update state and persist event", func() {
					newTimestamp := segment2Timestamp.Add(1)
					request := newSegmentEventRequest(core.SegmentEvent{
						Type:      core.SegmentEvent_CREATED,
						Timestamp: newTimestamp,
						Segment:   segment2,
					})

					mockPubSub.EXPECT().Publish(eventsTopic, nil, matchSegmentEventBytes(request.SegmentEvent))
					mockInnerStore.EXPECT().ListSegments(ctx, region, topic, partition2).Return(map[core.Segment]core.SegmentInfo{}, nil)

					expectEvents(append(baseEvents, request)...)
					runStore()

					Expect(store.Events()).Should(Receive(Equal(request)))

					segments, err := store.ListSegments(ctx, region, topic, partition2)
					Expect(err).To(BeNil())
					Expect(len(segments)).To(Equal(1))
					Expect(segments[segment2].Timestamp).To(Equal(newTimestamp))
				})
			})

			Context("When event timestamp is after current timestamp: removed then removed (duplicate event)", func() {
				It("Should update state and persist event", func() {
					newTimestamp := segment2Timestamp.Add(1)
					request := newSegmentEventRequest(core.SegmentEvent{
						Type:      core.SegmentEvent_REMOVED,
						Timestamp: newTimestamp,
						Segment:   segment2,
					})

					mockPubSub.EXPECT().Publish(eventsTopic, nil, matchSegmentEventBytes(request.SegmentEvent))
					mockInnerStore.EXPECT().ListSegments(ctx, region, topic, partition2).Return(map[core.Segment]core.SegmentInfo{}, nil)

					expectEvents(append(baseEvents, request)...)
					runStore()

					Expect(store.Events()).Should(Receive(Equal(request)))

					segments, err := store.ListSegments(ctx, region, topic, partition2)
					Expect(err).To(BeNil())
					Expect(len(segments)).To(Equal(0))
				})
			})

			Context("When Publishr call fails", func() {
				It("Should not propagate event and return the error to event source", func() {
					newTimestamp := segment1Timestamp.Add(1)

					resultChan := make(chan error, 1)
					request := core.SegmentEventRequest{
						SegmentEvent: core.SegmentEvent{
							Type:      core.SegmentEvent_CREATED,
							Timestamp: newTimestamp,
							Segment:   segment1,
						},
						Result: resultChan,
					}

					err := errors.New("error")

					mockPubSub.EXPECT().Publish(eventsTopic, nil, matchSegmentEventBytes(request.SegmentEvent)).Return(err)
					mockInnerStore.EXPECT().ListSegments(ctx, region, topic, partition1).Return(map[core.Segment]core.SegmentInfo{}, nil)

					expectEvents(append(baseEvents, request)...)
					runStore()

					Expect(store.Events()).ShouldNot(Receive())
					Expect(resultChan).Should(Receive(Equal(err)))

					segments, err := store.ListSegments(ctx, region, topic, partition1)
					Expect(err).To(BeNil())
					Expect(len(segments)).To(Equal(1))
					Expect(segments[segment1].Timestamp).To(Equal(newTimestamp))
				})
			})
		})

		Context("When event retention expires", func() {
			var (
				segment1          = core.Segment{Region: region, Topic: topic, Partition: partition1, StartOffset: 0, EndOffset: 1} // expired
				segment2          = core.Segment{Region: region, Topic: topic, Partition: partition1, StartOffset: 2, EndOffset: 3} // expires soon
				segment3          = core.Segment{Region: region, Topic: topic, Partition: partition1, StartOffset: 4, EndOffset: 5} // expires later
				segment1Timestamp time.Time
				segment2Timestamp time.Time
				segment3Timestamp time.Time
				baseEvents        []interface{}
			)

			BeforeEach(func() {
				now := time.Now().UTC()
				segment1Timestamp = now.Add(-eventsRetention)
				segment2Timestamp = now.Add(-eventsRetention + waitDuration)
				segment3Timestamp = now

				baseEvents = []interface{}{
					newSegmentEventMessage(1, 0, core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: segment1Timestamp, Segment: segment1}),
					newSegmentEventMessage(1, 1, core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: segment2Timestamp, Segment: segment2}),
					newSegmentEventMessage(1, 2, core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: segment3Timestamp, Segment: segment3}),
					newTopicEOF(eventsTopic)}
			})

			Context("When ListSegments is called", func() {
				It("Should not return the expired segments", func() {
					mockInnerStore.EXPECT().ListSegments(ctx, region, topic, partition1).Return(map[core.Segment]core.SegmentInfo{}, nil)
					mockInnerStore.EXPECT().ListSegments(ctx, region, topic, partition1).Return(map[core.Segment]core.SegmentInfo{}, nil)

					expectEvents(baseEvents...)
					startStore()

					segments, err := store.ListSegments(ctx, region, topic, partition1)
					Expect(err).To(BeNil())
					Expect(len(segments)).To(Equal(2))
					Expect(segments[segment2].Timestamp).To(Equal(segment2Timestamp))
					Expect(segments[segment3].Timestamp).To(Equal(segment3Timestamp))

					<-time.After(2 * eventsRetentionCheckInterval) // wait for segment2 to expire
					stopStore(stopWaitDuration)

					segments, err = store.ListSegments(ctx, region, topic, partition1)
					Expect(err).To(BeNil())
					Expect(len(segments)).To(Equal(1))
					Expect(segments[segment3].Timestamp).To(Equal(segment3Timestamp))
				})
			})

			Context("When expired event is received", func() {
				It("Should skip the expired event", func() {
					event := core.SegmentEvent{
						Type:      core.SegmentEvent_CREATED,
						Timestamp: segment1Timestamp.Add(-eventsRetention),
						Segment:   segment1,
					}

					expectEvents(append(baseEvents, event)...)
					runStore()

					Expect(store.Events()).ShouldNot(Receive())
				})
			})
		})

		Context("When Stop is called before partition EOF event", func() {
			It("Should stop immediately", func() {
				expectEvents()

				var wg sync.WaitGroup
				wg.Add(1)

				go func() {
					startStore()
					wg.Done()
				}()

				stopStore(stopWaitDuration)
				wg.Wait()
			})
		})
	})

	Context("When Subscribe fails", func() {
		It("Start should return error", func() {
			mockPubSub.EXPECT().Subscribe(eventsTopic).Return(errors.New("error"))
			err := store.Start()
			Expect(err).NotTo(BeNil())
		})
	})
})
