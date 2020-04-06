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
	"bytes"
	"errors"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/egress"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Egress tests", func() {
	var (
		localRegion      = "local_region"
		topic1           = "test_topic1"
		topic2           = "test_topic2"
		maxSegmentAge    = 20 * waitDuration
		mockConsumer     *MockConsumer
		mockSegmentStore *MockSegmentStore
		controller       *egress.Controller
	)

	BeforeEach(func() {
		mockConsumer = NewMockConsumer(mockController)
		mockSegmentStore = NewMockSegmentStore(mockController)

		topicConfig := &egress.TopicConfig{
			WorkerChanSize:      10,
			WorkerStopTimeout:   waitDuration / 2,
			MaxSegmentMessages:  3,
			MaxSegmentSize:      1000,
			MaxSegmentAge:       maxSegmentAge,
			ConsumerRetrier:     newRetrier(),
			SegmentStoreRetrier: newRetrier(),
		}

		c, err := egress.New(egress.ControllerConfig{
			Consumer:     core.Self(mockConsumer),
			SegmentStore: core.Self(mockSegmentStore),

			LocalRegion: localRegion,
			Topics: map[string]*egress.TopicConfig{
				topic1: topicConfig,
				topic2: topicConfig,
			},
		})

		Expect(err).To(BeNil())
		controller = c
	})

	expectEvents := func(events ...interface{}) {
		expectEvents(mockConsumer, nil, events...)
	}

	Context("When Subscribe is successful", func() {
		startController := func() {
			mockConsumer.EXPECT().Subscribe(topic1, topic2)
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
			It("Should start workers and wait for Message events", func() {
				partitions := []kafka.TopicPartition{
					newTopicPartition(topic1, 0, 0),
					newTopicPartition(topic2, 0, 1),
				}

				expectEvents(newAssignedPartitions(partitions...))
				mockConsumer.EXPECT().Assign(partitions)

				runController()
			})

			It("Should dispatch Messages to correct workers", func() {
				partition1 := uint32(1)
				partition2 := uint32(2)

				partitions := []kafka.TopicPartition{
					newTopicPartition(topic1, partition1, 0),
					newTopicPartition(topic1, partition2, 0),
					newTopicPartition(topic2, partition1, 0),
				}

				sameWorkerMatcher := func() *predicateMatcher {
					// testing trick: worker ID is conveyed using message Offset
					workerID := uint64(0)

					return predicate(func(v interface{}) bool {
						msg := v.(core.Message)
						actual := msg.Offset / 1000
						if workerID == 0 {
							workerID = actual
							return true
						}

						return actual == workerID
					})
				}

				expectEvents(
					newAssignedPartitions(partitions...),
					newKafkaMessage(topic1, partition1, 1001, 1000),
					newKafkaMessage(topic2, partition1, 3001, 3000),
					newKafkaMessage(topic1, partition2, 2001, 2000),
					newKafkaMessage(topic1, partition1, 1002, 1000),
					newKafkaMessage(topic1, partition2, 2002, 2000),
					newKafkaMessage(topic2, partition1, 3002, 3000),
					newKafkaMessage(topic2, partition1, 3003, 3000),
					newKafkaMessage(topic1, partition2, 2003, 2000),
					newKafkaMessage(topic1, partition1, 1003, 1000))

				mockConsumer.EXPECT().Assign(partitions)

				writer1 := NewMockSegmentWriter(mockController)
				writer1.EXPECT().Write(gomock.Any(), sameWorkerMatcher()).Times(3)
				writer1.EXPECT().Close(gomock.Any(), gomock.Any())

				writer2 := NewMockSegmentWriter(mockController)
				writer2.EXPECT().Write(gomock.Any(), sameWorkerMatcher()).Times(3)
				writer2.EXPECT().Close(gomock.Any(), gomock.Any())

				writer3 := NewMockSegmentWriter(mockController)
				writer3.EXPECT().Write(gomock.Any(), sameWorkerMatcher()).Times(3)
				writer3.EXPECT().Close(gomock.Any(), gomock.Any())

				mockSegmentStore.EXPECT().Create(gomock.Any()).Return(writer1, nil)
				mockSegmentStore.EXPECT().Create(gomock.Any()).Return(writer2, nil)
				mockSegmentStore.EXPECT().Create(gomock.Any()).Return(writer3, nil)
				mockConsumer.EXPECT().Commit(gomock.Any(), gomock.Any(), gomock.Any()).Times(3)

				runController()
			})
		})

		Context("When receives RevokedPartitions event", func() {
			It("Should stop all workers and discard messages", func() {
				partitions := []kafka.TopicPartition{
					newTopicPartition(topic1, 0, 0),
					newTopicPartition(topic2, 0, 1),
				}

				expectEvents(
					newAssignedPartitions(partitions...),
					newRevokedPartitions(partitions...),
					newKafkaMessage(topic1, 0, 100, 1000))

				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Unassign()

				runController()
			})
		})

		Context("When receives Message events and is stopped before segment is full", func() {
			It("Should discard incomplete segment", func() {
				partition := uint32(107)

				topicPartitions := []kafka.TopicPartition{
					newTopicPartition(topic1, partition, 0),
				}

				expectEvents(
					newAssignedPartitions(topicPartitions...),
					newKafkaMessage(topic1, partition, 100, 1000))

				segmentWriter := NewMockSegmentWriter(mockController)
				segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000)))
				segmentWriter.EXPECT().Abort(gomock.Any())

				mockConsumer.EXPECT().Assign(topicPartitions)
				mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter, nil)

				runController()
			})
		})

		Context("When receives enough Message events to complete the segment", func() {
			Context("When MaxSegmentMessages is reached", func() {
				It("Should successfully complete the segment", func() {
					partition := uint32(107)

					topicPartitions := []kafka.TopicPartition{
						newTopicPartition(topic1, partition, 0),
					}

					firstMessage := newKafkaMessage(topic1, partition, 100, 1000)
					lastMessage := newKafkaMessage(topic1, partition, 102, 1002)

					metadata := core.SegmentMetadata{
						Region:       localRegion,
						Topic:        topic1,
						Partition:    partition,
						Level:        core.LevelStreaming,
						StartOffset:  100,
						EndOffset:    102,
						MessageCount: 3,
					}

					expectEvents(
						newAssignedPartitions(topicPartitions...),
						firstMessage,
						newKafkaMessage(topic1, partition, 101, 1001),
						lastMessage)

					segmentWriter := NewMockSegmentWriter(mockController)
					gomock.InOrder(
						segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000))),
						segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(101, 1001))),
						segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(102, 1002))),
						segmentWriter.EXPECT().Close(gomock.Any(), matchSegmentMetadata(metadata)))

					mockConsumer.EXPECT().Assign(topicPartitions)
					mockConsumer.EXPECT().Commit(topic1, partition, kafka.Offset(102))
					mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter, nil)

					runController()
				})
			})

			Context("When MaxSegmentSize is reached", func() {
				It("Should successfully complete the segment", func() {
					partition := uint32(107)

					topicPartitions := []kafka.TopicPartition{
						newTopicPartition(topic1, partition, 0),
					}

					firstMessage := newKafkaMessage(topic1, partition, 100, 1000)

					bigKafkaMessage := newKafkaMessage(topic1, partition, 101, 1001)
					bigKafkaMessage.Value = bytes.Repeat([]byte("data_0123456789"), 100)

					bigMessage := newMessage(101, 1001)
					bigMessage.Value = bigKafkaMessage.Value

					metadata := core.SegmentMetadata{
						Region:       localRegion,
						Topic:        topic1,
						Partition:    partition,
						Level:        core.LevelStreaming,
						StartOffset:  100,
						EndOffset:    101,
						MessageCount: 2,
					}

					expectEvents(
						newAssignedPartitions(topicPartitions...),
						firstMessage,
						bigKafkaMessage)

					segmentWriter := NewMockSegmentWriter(mockController)
					gomock.InOrder(
						segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000))),
						segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(bigMessage)),
						segmentWriter.EXPECT().Close(gomock.Any(), matchSegmentMetadata(metadata)))

					mockConsumer.EXPECT().Assign(topicPartitions)
					mockConsumer.EXPECT().Commit(topic1, partition, kafka.Offset(101))
					mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter, nil)

					runController()
				})
			})

			Context("When MaxSegmentAge is reached", func() {
				It("Should successfully complete the segment", func() {
					partition := uint32(107)

					topicPartitions := []kafka.TopicPartition{
						newTopicPartition(topic1, partition, 0),
					}

					message := newKafkaMessage(topic1, partition, 100, 1000)

					metadata := core.SegmentMetadata{
						Region:       localRegion,
						Topic:        topic1,
						Partition:    partition,
						Level:        core.LevelStreaming,
						StartOffset:  100,
						EndOffset:    100,
						MessageCount: 1,
					}

					expectEvents(
						newAssignedPartitions(topicPartitions...),
						message)

					segmentWriter := NewMockSegmentWriter(mockController)
					gomock.InOrder(
						segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000))),
						segmentWriter.EXPECT().Close(gomock.Any(), matchSegmentMetadata(metadata)))

					mockConsumer.EXPECT().Assign(topicPartitions)
					mockConsumer.EXPECT().Commit(topic1, partition, kafka.Offset(100))
					mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter, nil)

					startController()
					stopController(2 * maxSegmentAge)
				})
			})

			Context("When SegmentWriter.Write fails", func() {
				It("Should rewind the segment and seek to first message offset", func() {
					partition := uint32(107)

					topicPartitions := []kafka.TopicPartition{
						newTopicPartition(topic1, partition, 0),
					}

					expectEvents(
						newAssignedPartitions(topicPartitions...),
						newKafkaMessage(topic1, partition, 100, 1000),
						newKafkaMessage(topic1, partition, 101, 1001),
						newKafkaMessage(topic1, partition, 102, 1002), // will be skipped
						newKafkaMessage(topic1, partition, 103, 1003), // will be skipped
						newKafkaMessage(topic1, partition, 100, 1000),
						newKafkaMessage(topic1, partition, 101, 1001),
						newKafkaMessage(topic1, partition, 102, 1002))

					segmentWriter1 := NewMockSegmentWriter(mockController)
					gomock.InOrder(
						segmentWriter1.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000))),
						segmentWriter1.EXPECT().Write(gomock.Any(), matchMessage(newMessage(101, 1001))).Return(errors.New("error")),
						segmentWriter1.EXPECT().Abort(gomock.Any()))

					segmentWriter2 := NewMockSegmentWriter(mockController)
					gomock.InOrder(
						segmentWriter2.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000))),
						segmentWriter2.EXPECT().Write(gomock.Any(), matchMessage(newMessage(101, 1001))),
						segmentWriter2.EXPECT().Write(gomock.Any(), matchMessage(newMessage(102, 1002))),
						segmentWriter2.EXPECT().Close(gomock.Any(), gomock.Any()))

					mockConsumer.EXPECT().Assign(topicPartitions)
					mockConsumer.EXPECT().Seek(topic1, partition, kafka.Offset(100))
					mockConsumer.EXPECT().Commit(topic1, partition, kafka.Offset(102))

					gomock.InOrder(
						mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter1, nil),
						mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter2, nil))

					runController()
				})
			})

			Context("When SegmentWriter.Close fails", func() {
				It("Should rewind the segment and seek to first message offset", func() {
					partition := uint32(107)

					topicPartitions := []kafka.TopicPartition{
						newTopicPartition(topic1, partition, 0),
					}

					expectEvents(
						newAssignedPartitions(topicPartitions...),
						newKafkaMessage(topic1, partition, 100, 1000),
						newKafkaMessage(topic1, partition, 101, 1001),
						newKafkaMessage(topic1, partition, 102, 1002),
						newKafkaMessage(topic1, partition, 103, 1003), // will be skipped
						newKafkaMessage(topic1, partition, 100, 1000),
						newKafkaMessage(topic1, partition, 101, 1001),
						newKafkaMessage(topic1, partition, 102, 1002))

					segmentWriter1 := NewMockSegmentWriter(mockController)
					gomock.InOrder(
						segmentWriter1.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000))),
						segmentWriter1.EXPECT().Write(gomock.Any(), matchMessage(newMessage(101, 1001))),
						segmentWriter1.EXPECT().Write(gomock.Any(), matchMessage(newMessage(102, 1002))),
						segmentWriter1.EXPECT().Close(gomock.Any(), gomock.Any()).Return(errors.New("error")))

					segmentWriter2 := NewMockSegmentWriter(mockController)
					gomock.InOrder(
						segmentWriter2.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000))),
						segmentWriter2.EXPECT().Write(gomock.Any(), matchMessage(newMessage(101, 1001))),
						segmentWriter2.EXPECT().Write(gomock.Any(), matchMessage(newMessage(102, 1002))),
						segmentWriter2.EXPECT().Close(gomock.Any(), gomock.Any()))

					mockConsumer.EXPECT().Assign(topicPartitions)
					mockConsumer.EXPECT().Seek(topic1, partition, kafka.Offset(100))
					mockConsumer.EXPECT().Commit(topic1, partition, kafka.Offset(102))

					gomock.InOrder(
						mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter1, nil),
						mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter2, nil))

					runController()
				})
			})

			Context("When KafkaConsumer.Commit fails", func() {
				Context("Intermittently", func() {
					It("Should retry until successful then advance message processing", func() {
						partition := uint32(107)

						topicPartitions := []kafka.TopicPartition{
							newTopicPartition(topic1, partition, 0),
						}

						expectEvents(
							newAssignedPartitions(topicPartitions...),
							newKafkaMessage(topic1, partition, 100, 1000),
							newKafkaMessage(topic1, partition, 101, 1001),
							newKafkaMessage(topic1, partition, 102, 1002))

						segmentWriter := NewMockSegmentWriter(mockController)
						gomock.InOrder(
							segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000))),
							segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(101, 1001))),
							segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(102, 1002))),
							segmentWriter.EXPECT().Close(gomock.Any(), gomock.Any()))

						mockConsumer.EXPECT().Assign(topicPartitions)

						gomock.InOrder(
							mockConsumer.EXPECT().Commit(topic1, partition, kafka.Offset(102)).Times(5).Return(errors.New("error")),
							mockConsumer.EXPECT().Commit(topic1, partition, kafka.Offset(102)))

						mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter, nil)

						runController()
					})
				})

				Context("Indefinitely", func() {
					It("Should retry forever without further message processing", func() {
						partition := uint32(107)

						topicPartitions := []kafka.TopicPartition{
							newTopicPartition(topic1, partition, 0),
						}

						expectEvents(
							newAssignedPartitions(topicPartitions...),
							newKafkaMessage(topic1, partition, 100, 1000),
							newKafkaMessage(topic1, partition, 101, 1001),
							newKafkaMessage(topic1, partition, 102, 1002),
							newKafkaMessage(topic1, partition, 103, 1003))

						segmentWriter := NewMockSegmentWriter(mockController)
						gomock.InOrder(
							segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000))),
							segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(101, 1001))),
							segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(102, 1002))),
							segmentWriter.EXPECT().Close(gomock.Any(), gomock.Any()))

						mockConsumer.EXPECT().Assign(topicPartitions)
						mockConsumer.EXPECT().Commit(topic1, partition, kafka.Offset(102)).AnyTimes().Return(errors.New("error"))
						mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter, nil)

						runController()
					})
				})
			})
		})

		Context("When SegmentStore.Create fails", func() {
			Context("Intermittently", func() {
				It("Should retry until successful then advance message processing", func() {
					partition := uint32(107)

					topicPartitions := []kafka.TopicPartition{
						newTopicPartition(topic1, partition, 0),
					}

					expectEvents(
						newAssignedPartitions(topicPartitions...),
						newKafkaMessage(topic1, partition, 100, 1000))

					segmentWriter := NewMockSegmentWriter(mockController)
					gomock.InOrder(
						segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000))),
						segmentWriter.EXPECT().Abort(gomock.Any()))

					mockConsumer.EXPECT().Assign(topicPartitions)

					gomock.InOrder(
						mockSegmentStore.EXPECT().Create(gomock.Any()).Times(5).Return(nil, errors.New("error")),
						mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter, nil))

					runController()
				})
			})

			Context("Indefinitely", func() {
				It("Should retry forever without further message processing", func() {
					partition := uint32(107)

					topicPartitions := []kafka.TopicPartition{
						newTopicPartition(topic1, partition, 0),
					}

					expectEvents(
						newAssignedPartitions(topicPartitions...),
						newKafkaMessage(topic1, partition, 100, 1000))

					mockConsumer.EXPECT().Assign(topicPartitions)
					mockSegmentStore.EXPECT().Create(gomock.Any()).AnyTimes().Return(nil, errors.New("error"))

					runController()
				})
			})
		})

		Context("When KafkaConsumer.Seek fails", func() {
			Context("Intermittently", func() {
				It("Should retry until successful then advance message processing", func() {
					partition := uint32(107)

					topicPartitions := []kafka.TopicPartition{
						newTopicPartition(topic1, partition, 0),
					}

					expectEvents(
						newAssignedPartitions(topicPartitions...),
						newKafkaMessage(topic1, partition, 100, 1000),
						newKafkaMessage(topic1, partition, 100, 1000))

					segmentWriter1 := NewMockSegmentWriter(mockController)
					gomock.InOrder(
						segmentWriter1.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000))).Return(errors.New("error")),
						segmentWriter1.EXPECT().Abort(gomock.Any()))

					segmentWriter2 := NewMockSegmentWriter(mockController)
					gomock.InOrder(
						segmentWriter2.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000))),
						segmentWriter2.EXPECT().Abort(gomock.Any()))

					mockConsumer.EXPECT().Assign(topicPartitions)
					gomock.InOrder(
						mockConsumer.EXPECT().Seek(topic1, partition, kafka.Offset(100)).Times(5).Return(errors.New("error")),
						mockConsumer.EXPECT().Seek(topic1, partition, kafka.Offset(100)))

					gomock.InOrder(
						mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter1, nil),
						mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter2, nil))

					runController()
				})
			})

			Context("Indefinitely", func() {
				It("Should retry forever without further message processing", func() {
					partition := uint32(107)

					topicPartitions := []kafka.TopicPartition{
						newTopicPartition(topic1, partition, 0),
					}

					expectEvents(
						newAssignedPartitions(topicPartitions...),
						newKafkaMessage(topic1, partition, 100, 1000))

					segmentWriter := NewMockSegmentWriter(mockController)
					gomock.InOrder(
						segmentWriter.EXPECT().Write(gomock.Any(), matchMessage(newMessage(100, 1000))).Return(errors.New("error")),
						segmentWriter.EXPECT().Abort(gomock.Any()))

					mockConsumer.EXPECT().Assign(topicPartitions)
					mockConsumer.EXPECT().Seek(topic1, partition, kafka.Offset(100)).AnyTimes().Return(errors.New("error"))
					mockSegmentStore.EXPECT().Create(gomock.Any()).Return(segmentWriter, nil)

					runController()
				})
			})
		})

		Context("When receives Message event for unassigned topic partition", func() {
			It("Should ignore the event", func() {
				topicPartitions := []kafka.TopicPartition{
					newTopicPartition(topic1, 107, 0),
				}

				expectEvents(
					newAssignedPartitions(topicPartitions...),
					newKafkaMessage(topic1, 999, 0, 0))

				mockConsumer.EXPECT().Assign(topicPartitions)

				runController()
			})
		})

		Context("When receives Message event for unknown topic partition", func() {
			It("Should ignore the event", func() {
				topicPartitions := []kafka.TopicPartition{
					newTopicPartition(topic1, 107, 0),
				}

				expectEvents(
					newAssignedPartitions(topicPartitions...),
					newKafkaMessage("unknown", 999, 0, 0))

				mockConsumer.EXPECT().Assign(topicPartitions)

				runController()
			})
		})
	})

	Context("When Subscribe fails", func() {
		It("Start should return error", func() {
			mockConsumer.EXPECT().Subscribe(topic1, topic2).Return(errors.New("error"))
			err := controller.Start()
			Expect(err).NotTo(BeNil())
		})
	})
})
