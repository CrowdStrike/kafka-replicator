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
	"fmt"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/ingress"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

var _ = Describe("Ingress tests", func() {
	var (
		localRegion            = "local_region"
		region                 = "test_region"
		topic1                 = "test_topic1"
		topic2                 = "test_topic2"
		destinationTopicFormat = "ingress_%s_test"
		workerInitDelay        = waitDuration / 10
		firstSegmentDelay      = time.Hour
		lateSegmentRetries     = 5
		lostSegmentTimeout     = 5 * waitDuration
		batchSize              = 10
		mockConsumer           *MockConsumer
		mockProducer           *MockProducer
		mockSegmentStore       *MockSegmentStore
		mockCheckpointStore    *MockCheckpointStore
		controller             *ingress.Controller
	)

	destinationTopic := func(sourceTopic string) string {
		return fmt.Sprintf(destinationTopicFormat, sourceTopic)
	}

	getSourceConfig := func(topic string) *ingress.SourceConfig {
		return &ingress.SourceConfig{
			DestinationTopic:       destinationTopic(topic),
			WorkerChanSize:         10,
			WorkerStopTimeout:      waitDuration / 2,
			WorkerInitDelay:        time.Millisecond,
			FirstSegmentDelay:      firstSegmentDelay,
			LateSegmentRetries:     lateSegmentRetries,
			LostSegmentTimeout:     lostSegmentTimeout,
			BatchSize:              batchSize,
			SegmentStoreRetrier:    newRetrier(),
			ProducerRetrier:        newRetrier(),
			CheckpointStoreRetrier: newRetrier(),
		}
	}

	BeforeEach(func() {
		mockConsumer = NewMockConsumer(mockController)
		mockProducer = NewMockProducer(mockController)
		mockSegmentStore = NewMockSegmentStore(mockController)
		mockCheckpointStore = NewMockCheckpointStore(mockController)

		c, err := ingress.New(ingress.ControllerConfig{
			Consumer:        core.Self(mockConsumer),
			Producer:        core.Self(mockProducer),
			SegmentStore:    core.Self(mockSegmentStore),
			CheckpointStore: core.Self(mockCheckpointStore),

			LocalRegion: localRegion,
			Sources: map[ingress.Source]*ingress.SourceConfig{
				{Region: region, Topic: topic1}: getSourceConfig(topic1),
				{Region: region, Topic: topic2}: getSourceConfig(topic2),
			},
		})

		Expect(err).To(BeNil())
		controller = c
	})

	expectEvents := func(events ...interface{}) {
		expectEvents(mockConsumer, mockSegmentStore.EXPECT(), events...)
	}

	expectProcessedSegments := func(segments ...core.Segment) {
		var calls []*gomock.Call

		for _, segment := range segments {
			messages := []core.Message{newMessage(segment.EndOffset, 1000)}
			reader := NewMockSegmentReader(mockController)
			checkpoint := core.Checkpoint{
				Region:    segment.Region,
				Topic:     segment.Topic,
				Partition: segment.Partition,
				Offset:    segment.EndOffset,
			}

			calls = append(calls,
				mockSegmentStore.EXPECT().Open(gomock.Any(), segment).Return(reader, nil),
				reader.EXPECT().Read(gomock.Any(), batchSize).Return(messages, nil),
				mockProducer.EXPECT().ProduceMessages(destinationTopic(segment.Topic), segment.Partition, messages),
				mockCheckpointStore.EXPECT().Save(matchCheckpoint(checkpoint)),
				reader.EXPECT().Close(gomock.Any()))
		}

		gomock.InOrder(calls...)
	}

	Context("When Subscribe is successful", func() {
		startController := func() {
			mockConsumer.EXPECT().Subscribe(destinationTopic(topic1), destinationTopic(topic2))
			err := controller.Start()
			Expect(err).To(BeNil())
		}

		stopController := func(delay time.Duration) {
			<-time.After(delay)
			controller.Stop()
		}

		runController := func() {
			startController()
			stopController(stopWaitDuration)
		}

		Context("When receives AssignedPartitions event", func() {
			It("Should start and initialize workers", func() {
				partition1 := uint32(1)
				partition2 := uint32(2)

				partitions := []kafka.TopicPartition{
					newTopicPartition(destinationTopic(topic1), partition1, 0),
					newTopicPartition(destinationTopic(topic2), partition2, 0),
				}

				expectEvents(newAssignedPartitions(partitions...))

				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)
				mockCheckpointStore.EXPECT().Load(region, topic1, partition1)
				mockCheckpointStore.EXPECT().Load(region, topic2, partition2)
				mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition1)
				mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic2, partition2)

				runController()
			})
		})

		Context("When receives RevokedPartitions event", func() {
			It("Should stop workers and discard events", func() {
				partition := uint32(1)
				segment := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 1, EndOffset: 2}

				partitions := []kafka.TopicPartition{
					newTopicPartition(destinationTopic(topic1), partition, 0),
				}

				expectEvents(
					newAssignedPartitions(partitions...),
					newDelay(workerInitDelay),
					newRevokedPartitions(partitions...),
					core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: time.Now(), Segment: segment})

				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)
				mockConsumer.EXPECT().Unassign()
				mockCheckpointStore.EXPECT().Load(region, topic1, partition).Return(&core.Checkpoint{Offset: 10})
				mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition)

				runController()
			})
		})

		Context("When processing first segment", func() {
			partition := uint32(1)
			partitions := []kafka.TopicPartition{
				newTopicPartition(destinationTopic(topic1), partition, 0),
			}

			BeforeEach(func() {
				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)
				mockCheckpointStore.EXPECT().Load(region, topic1, partition)
			})

			Context("When ListSegments result is empty", func() {
				BeforeEach(func() {
					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition)
				})

				Context("When first segment is earlier than FirstSegmentDelay", func() {
					It("Should wait", func() {
						segment := core.Segment{Region: region, Topic: topic1, Partition: partition}
						timestamp := time.Now()

						expectEvents(
							newAssignedPartitions(partitions...),
							newDelay(workerInitDelay),
							core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: timestamp, Segment: segment})

						runController()
					})
				})

				Context("When FirstSegmentDelay elapses", func() {
					It("Should process segments in order", func() {
						segment1 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 1, EndOffset: 1} // older: arrives 2nd
						segment2 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 2, EndOffset: 2} // newer: arrives 1st
						now := time.Now()
						timestamp1 := now.Add(-firstSegmentDelay + waitDuration + 1)
						timestamp2 := now.Add(-firstSegmentDelay + waitDuration + 2)

						expectEvents(
							newAssignedPartitions(partitions...),
							newDelay(workerInitDelay),
							core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: timestamp2, Segment: segment2},
							core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: timestamp1, Segment: segment1})

						expectProcessedSegments(segment1, segment2)

						runController()
					})
				})

				Context("When older segment than FirstSegmentDelay arrives", func() {
					It("Should process segment", func() {
						segment1 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 1, EndOffset: 2} // older: arrives 2nd
						segment2 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 3, EndOffset: 4} // newer: arrives 1st
						now := time.Now()
						timestamp1 := now.Add(-2 * firstSegmentDelay)
						timestamp2 := now

						expectEvents(
							newAssignedPartitions(partitions...),
							newDelay(workerInitDelay),
							core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: timestamp2, Segment: segment2},
							core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: timestamp1, Segment: segment1})

						expectProcessedSegments(segment1, segment2)

						runController()
					})
				})
			})

			Context("When ListSegments result is not empty", func() {
				Context("While first segment is earlier than FirstSegmentDelay", func() {
					It("Should wait", func() {
						segment := core.Segment{Region: region, Topic: topic1, Partition: partition}
						timestamp := time.Now()

						mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition).Return(map[core.Segment]core.SegmentInfo{
							segment: {Segment: segment, Timestamp: timestamp},
						}, nil)

						expectEvents(newAssignedPartitions(partitions...))
						runController()
					})
				})

				Context("When FirstSegmentDelay elapses", func() {
					It("Should process segments in order", func() {
						segment1 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 1, EndOffset: 2} // oldest: arrives 2nd
						segment2 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 3, EndOffset: 4} // older: arrives 1st
						segment3 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 5, EndOffset: 6} // newest: part of ListSegments result
						now := time.Now()
						timestamp1 := now.Add(-firstSegmentDelay + waitDuration + 1)
						timestamp2 := now.Add(-firstSegmentDelay + waitDuration + 2)
						timestamp3 := now.Add(-firstSegmentDelay + waitDuration + 3)

						mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition).Return(map[core.Segment]core.SegmentInfo{
							segment3: {Segment: segment3, Timestamp: timestamp3},
						}, nil)

						expectEvents(
							newAssignedPartitions(partitions...),
							newDelay(workerInitDelay),
							core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: timestamp2, Segment: segment2},
							core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: timestamp1, Segment: segment1})

						expectProcessedSegments(segment1, segment2, segment3)

						runController()
					})
				})

				Context("When older segment than FirstSegmentDelay arrives", func() {
					It("Should process segment", func() {
						segment1 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 1, EndOffset: 2} // older: arrives as segment event
						segment2 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 3, EndOffset: 4} // newer: part of ListSegments result
						now := time.Now()
						timestamp1 := now.Add(-2 * firstSegmentDelay)
						timestamp2 := now

						mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition).Return(map[core.Segment]core.SegmentInfo{
							segment2: {Segment: segment2, Timestamp: timestamp2},
						}, nil)

						expectEvents(
							newAssignedPartitions(partitions...),
							newDelay(workerInitDelay),
							core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: timestamp1, Segment: segment1})

						expectProcessedSegments(segment1, segment2)

						runController()
					})
				})
			})
		})

		Context("When processing next segment", func() {
			partition := uint32(1)
			checkpointOffset := uint64(200)
			processedSegment1 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 1, EndOffset: 100}
			processedSegment2 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 101, EndOffset: 200}
			nextSegment := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 201, EndOffset: 300}
			futureSegment1 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 301, EndOffset: 400}
			futureSegment2 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 401, EndOffset: 500}

			partitions := []kafka.TopicPartition{
				newTopicPartition(destinationTopic(topic1), partition, 0),
			}

			BeforeEach(func() {
				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)
				mockCheckpointStore.EXPECT().Load(region, topic1, partition).Return(&core.Checkpoint{Offset: checkpointOffset})
			})

			Context("When ListSegments result contains the next segment", func() {
				It("Should process the segment immediately", func() {
					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition).Return(map[core.Segment]core.SegmentInfo{
						nextSegment:    {Segment: nextSegment},
						futureSegment1: {Segment: futureSegment1},
						futureSegment2: {Segment: futureSegment2},
					}, nil)

					expectEvents(newAssignedPartitions(partitions...))
					expectProcessedSegments(nextSegment, futureSegment1, futureSegment2)

					runController()
				})
			})

			Context("When next segment is late", func() {
				It("Should wait for next segment event", func() {
					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition).
						MinTimes(1).
						MaxTimes(lateSegmentRetries+1).
						Return(map[core.Segment]core.SegmentInfo{
							futureSegment1: {Segment: futureSegment1},
						}, nil)

					expectEvents(
						newAssignedPartitions(partitions...),
						newDelay(workerInitDelay),
						core.SegmentEvent{Type: core.SegmentEvent_CREATED, Segment: nextSegment})

					expectProcessedSegments(nextSegment, futureSegment1)

					runController()
				})

				It("Should reload segments up to LateSegmentRetries times", func() {
					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition).Times(lateSegmentRetries).
						Return(map[core.Segment]core.SegmentInfo{
							futureSegment1: {Segment: futureSegment1},
						}, nil)

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition).
						Return(map[core.Segment]core.SegmentInfo{
							nextSegment:    {Segment: nextSegment},
							futureSegment1: {Segment: futureSegment1},
						}, nil)

					expectEvents(newAssignedPartitions(partitions...))
					expectProcessedSegments(nextSegment, futureSegment1)

					startController()
					stopController(2 * lostSegmentTimeout)
				})
			})

			Context("When next segment does not arrive", func() {
				It("Should declare segment lost and skip to next", func() {
					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition).Times(lateSegmentRetries+1).
						Return(map[core.Segment]core.SegmentInfo{
							futureSegment1: {Segment: futureSegment1},
						}, nil)

					expectEvents(newAssignedPartitions(partitions...))
					expectProcessedSegments(futureSegment1)

					startController()
					stopController(2 * lostSegmentTimeout)
				})
			})

			Context("When duplicate events arrive for the same segment", func() {
				It("Should process the segment only once", func() {
					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition)

					expectEvents(
						newAssignedPartitions(partitions...),
						newDelay(workerInitDelay),
						core.SegmentEvent{Type: core.SegmentEvent_CREATED, Segment: futureSegment1},
						core.SegmentEvent{Type: core.SegmentEvent_CREATED, Segment: nextSegment},
						core.SegmentEvent{Type: core.SegmentEvent_CREATED, Segment: futureSegment1},
						core.SegmentEvent{Type: core.SegmentEvent_CREATED, Segment: nextSegment})

					expectProcessedSegments(nextSegment, futureSegment1)

					runController()
				})
			})

			Context("When segment was already processed", func() {
				It("Should skip processed segments", func() {
					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition).
						Return(map[core.Segment]core.SegmentInfo{
							processedSegment1: {Segment: processedSegment1},
							processedSegment2: {Segment: processedSegment2},
						}, nil)

					expectEvents(
						newAssignedPartitions(partitions...),
						newDelay(workerInitDelay),
						core.SegmentEvent{Type: core.SegmentEvent_CREATED, Segment: nextSegment})

					expectProcessedSegments(nextSegment)

					runController()
				})

				It("Should skip old events", func() {
					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition)

					expectEvents(
						newAssignedPartitions(partitions...),
						newDelay(workerInitDelay),
						core.SegmentEvent{Type: core.SegmentEvent_CREATED, Segment: processedSegment1},
						core.SegmentEvent{Type: core.SegmentEvent_CREATED, Segment: processedSegment2},
						core.SegmentEvent{Type: core.SegmentEvent_CREATED, Segment: nextSegment})

					expectProcessedSegments(nextSegment)

					runController()
				})
			})

			Context("When segments are overlapping", func() {
				segment1 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 200, EndOffset: 203}
				segment2 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 202, EndOffset: 204}
				segment3 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 203, EndOffset: 205}
				segment4 := core.Segment{Region: region, Topic: topic1, Partition: partition, StartOffset: 203, EndOffset: 206}

				It("Should skip duplicate and already processed messages", func() {
					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition).
						Return(map[core.Segment]core.SegmentInfo{
							segment1: {Segment: segment1},
							segment2: {Segment: segment2},
							segment3: {Segment: segment3},
							segment4: {Segment: segment4},
						}, nil)

					expectEvents(newAssignedPartitions(partitions...))

					message0 := newMessage(200, 1000) // already processed
					message1 := newMessage(201, 1001)
					message2 := newMessage(202, 1002)
					message3 := newMessage(203, 1003)
					message4 := newMessage(204, 1004)
					message5 := newMessage(205, 1005)
					message6 := newMessage(206, 1006)

					segment1Checkpoint := core.Checkpoint{Region: region, Topic: topic1, Partition: partition, Offset: 203}
					segment2Checkpoint := core.Checkpoint{Region: region, Topic: topic1, Partition: partition, Offset: 204}
					segment4Checkpoint := core.Checkpoint{Region: region, Topic: topic1, Partition: partition, Offset: 206}

					reader := NewMockSegmentReader(mockController)

					gomock.InOrder(
						mockSegmentStore.EXPECT().Open(gomock.Any(), segment1).Return(reader, nil),
						reader.EXPECT().Read(gomock.Any(), batchSize).Return([]core.Message{message0, message1, message2, message3}, nil),
						mockProducer.EXPECT().ProduceMessages(destinationTopic(topic1), partition, []core.Message{message1, message2, message3}),
						mockCheckpointStore.EXPECT().Save(matchCheckpoint(segment1Checkpoint)),
						reader.EXPECT().Close(gomock.Any()),

						mockSegmentStore.EXPECT().Open(gomock.Any(), segment2).Return(reader, nil),
						reader.EXPECT().Read(gomock.Any(), batchSize).Return([]core.Message{message2, message3, message4}, nil),
						mockProducer.EXPECT().ProduceMessages(destinationTopic(topic1), partition, []core.Message{message4}),
						mockCheckpointStore.EXPECT().Save(matchCheckpoint(segment2Checkpoint)),
						reader.EXPECT().Close(gomock.Any()),

						mockSegmentStore.EXPECT().Open(gomock.Any(), segment4).Return(reader, nil),
						reader.EXPECT().Read(gomock.Any(), batchSize).Return([]core.Message{message3, message4, message5, message6}, nil),
						mockProducer.EXPECT().ProduceMessages(destinationTopic(topic1), partition, []core.Message{message5, message6}),
						mockCheckpointStore.EXPECT().Save(matchCheckpoint(segment4Checkpoint)),
						reader.EXPECT().Close(gomock.Any()),
					)

					runController()
				})
			})

			Context("When processing segment generates errors", func() {
				BeforeEach(func() {
					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition).Return(map[core.Segment]core.SegmentInfo{
						nextSegment: {Segment: nextSegment},
					}, nil)
				})

				Context("When SegmentStore.Open fails", func() {
					Context("Intermittently", func() {
						It("Should retry until successful", func() {
							mockSegmentStore.EXPECT().Open(gomock.Any(), nextSegment).Times(5).Return(nil, errors.New("error"))

							expectEvents(newAssignedPartitions(partitions...))
							expectProcessedSegments(nextSegment)

							runController()
						})
					})

					Context("Indefinitely", func() {
						It("Should retry forever", func() {
							mockSegmentStore.EXPECT().Open(gomock.Any(), nextSegment).AnyTimes().Return(nil, errors.New("error"))

							expectEvents(newAssignedPartitions(partitions...))

							runController()
						})
					})
				})

				Context("When SegmentReader.Read fails", func() {
					Context("Intermittently", func() {
						It("Should reopen segment and retry until successful", func() {
							reader := NewMockSegmentReader(mockController)
							mockSegmentStore.EXPECT().Open(gomock.Any(), nextSegment).Times(5).Return(reader, nil)
							reader.EXPECT().Read(gomock.Any(), batchSize).Times(5).Return(nil, errors.New("error"))
							reader.EXPECT().Close(gomock.Any()).Times(5)

							expectEvents(newAssignedPartitions(partitions...))
							expectProcessedSegments(nextSegment)

							runController()
						})

						It("Should skip already processed messaged", func() {
							message1 := newMessage(201, 1001)
							message2 := newMessage(202, 1002)
							message3 := newMessage(300, 1003)

							checkpoint1 := core.Checkpoint{Region: region, Topic: topic1, Partition: partition, Offset: 202}
							checkpoint2 := core.Checkpoint{Region: region, Topic: topic1, Partition: partition, Offset: 300}

							reader := NewMockSegmentReader(mockController)

							gomock.InOrder(
								mockSegmentStore.EXPECT().Open(gomock.Any(), nextSegment).Return(reader, nil),
								reader.EXPECT().Read(gomock.Any(), batchSize).Return([]core.Message{message1, message2}, nil),
								mockProducer.EXPECT().ProduceMessages(destinationTopic(topic1), partition, []core.Message{message1, message2}),
								mockCheckpointStore.EXPECT().Save(matchCheckpoint(checkpoint1)),

								reader.EXPECT().Read(gomock.Any(), batchSize).Return(nil, errors.New("error")),
								reader.EXPECT().Close(gomock.Any()),

								mockSegmentStore.EXPECT().Open(gomock.Any(), nextSegment).Return(reader, nil),
								reader.EXPECT().Read(gomock.Any(), batchSize).Return([]core.Message{message1, message2, message3}, nil),
								mockProducer.EXPECT().ProduceMessages(destinationTopic(topic1), partition, []core.Message{message3}),
								mockCheckpointStore.EXPECT().Save(matchCheckpoint(checkpoint2)),
								reader.EXPECT().Close(gomock.Any()),
							)

							expectEvents(newAssignedPartitions(partitions...))

							runController()
						})
					})

					Context("Indefinitely", func() {
						It("Should reopen segment and retry forever", func() {
							reader := NewMockSegmentReader(mockController)
							mockSegmentStore.EXPECT().Open(gomock.Any(), nextSegment).AnyTimes().Return(reader, nil)
							reader.EXPECT().Read(gomock.Any(), batchSize).AnyTimes().Return(nil, errors.New("error"))
							reader.EXPECT().Close(gomock.Any()).AnyTimes()

							expectEvents(newAssignedPartitions(partitions...))

							runController()
						})
					})
				})

				Context("When Producer.ProduceMessage fails", func() {
					Context("Intermittently", func() {
						It("Should retry until successful", func() {
							messages := []core.Message{newMessage(nextSegment.EndOffset, 1000)}
							reader := NewMockSegmentReader(mockController)

							mockSegmentStore.EXPECT().Open(gomock.Any(), nextSegment).Return(reader, nil)
							reader.EXPECT().Read(gomock.Any(), batchSize).Return(messages, nil)
							gomock.InOrder(
								mockProducer.EXPECT().ProduceMessages(destinationTopic(topic1), partition, messages).Times(5).Return(errors.Errorf("error")),
								mockProducer.EXPECT().ProduceMessages(destinationTopic(topic1), partition, messages))
							mockCheckpointStore.EXPECT().Save(gomock.Any())
							reader.EXPECT().Close(gomock.Any())

							expectEvents(newAssignedPartitions(partitions...))

							runController()
						})
					})

					Context("Indefinitely", func() {
						It("Should retry forever", func() {
							messages := []core.Message{newMessage(nextSegment.EndOffset, 1000)}
							reader := NewMockSegmentReader(mockController)

							mockSegmentStore.EXPECT().Open(gomock.Any(), nextSegment).Return(reader, nil)
							reader.EXPECT().Read(gomock.Any(), batchSize).Return(messages, nil)
							mockProducer.EXPECT().ProduceMessages(destinationTopic(topic1), partition, messages).AnyTimes().Return(errors.Errorf("error"))
							reader.EXPECT().Close(gomock.Any())

							expectEvents(newAssignedPartitions(partitions...))

							runController()
							<-time.After(waitDuration) // wait for reader.Close()
						})
					})
				})

				Context("When CheckpointStore.Save fails", func() {
					Context("Intermittently", func() {
						It("Should retry until successful", func() {
							messages := []core.Message{newMessage(nextSegment.EndOffset, 1000)}
							reader := NewMockSegmentReader(mockController)

							mockSegmentStore.EXPECT().Open(gomock.Any(), nextSegment).Return(reader, nil)
							reader.EXPECT().Read(gomock.Any(), batchSize).Return(messages, nil)
							mockProducer.EXPECT().ProduceMessages(destinationTopic(topic1), partition, messages)
							gomock.InOrder(
								mockCheckpointStore.EXPECT().Save(gomock.Any()).Times(5).Return(errors.Errorf("error")),
								mockCheckpointStore.EXPECT().Save(gomock.Any()))
							reader.EXPECT().Close(gomock.Any())

							expectEvents(newAssignedPartitions(partitions...))

							runController()
						})
					})

					Context("Indefinitely", func() {
						It("Should retry forever", func() {
							messages := []core.Message{newMessage(nextSegment.EndOffset, 1000)}
							reader := NewMockSegmentReader(mockController)

							mockSegmentStore.EXPECT().Open(gomock.Any(), nextSegment).Return(reader, nil)
							reader.EXPECT().Read(gomock.Any(), batchSize).Return(messages, nil)
							mockProducer.EXPECT().ProduceMessages(destinationTopic(topic1), partition, messages)
							mockCheckpointStore.EXPECT().Save(gomock.Any()).AnyTimes().Return(errors.Errorf("error"))
							reader.EXPECT().Close(gomock.Any())

							expectEvents(newAssignedPartitions(partitions...))

							runController()
							<-time.After(waitDuration) // wait for reader.Close()
						})
					})
				})
			})
		})

		Context("When receives segment event for unassigned topic partition", func() {
			It("Should ignore the event", func() {
				partition := uint32(1)
				segment := core.Segment{Region: region, Topic: topic1, Partition: 999}

				partitions := []kafka.TopicPartition{
					newTopicPartition(destinationTopic(topic1), partition, 0),
				}

				expectEvents(
					newAssignedPartitions(partitions...),
					core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: time.Now(), Segment: segment})

				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)
				mockCheckpointStore.EXPECT().Load(region, topic1, partition)
				mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition)

				runController()
			})
		})

		Context("When receives segment event for local region", func() {
			It("Should ignore the event", func() {
				partition := uint32(1)
				segment := core.Segment{Region: localRegion, Topic: topic1, Partition: partition}

				partitions := []kafka.TopicPartition{
					newTopicPartition(destinationTopic(topic1), partition, 0),
				}

				expectEvents(
					newAssignedPartitions(partitions...),
					core.SegmentEvent{Type: core.SegmentEvent_CREATED, Timestamp: time.Now(), Segment: segment})

				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)
				mockCheckpointStore.EXPECT().Load(region, topic1, partition)
				mockSegmentStore.EXPECT().ListSegments(gomock.Any(), region, topic1, partition)

				runController()
			})
		})
	})

	Context("When Subscribe fails", func() {
		It("Start should return error", func() {
			mockConsumer.EXPECT().Subscribe(destinationTopic(topic1), destinationTopic(topic2)).Return(errors.New("error"))
			err := controller.Start()
			Expect(err).NotTo(BeNil())
		})
	})
})
