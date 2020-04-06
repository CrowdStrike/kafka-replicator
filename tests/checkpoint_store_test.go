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
	"errors"
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

var _ = Describe("CheckpointStore tests", func() {
	var (
		localRegion     = "local_region"
		checkpointTopic = "checkpoint_topic"
		region          = "test_region"
		topic           = "test_topic"
		partition1      = uint32(1)
		partition2      = uint32(2)
		mockPubSub      *MockPubSub
		store           *stores.CheckpointStore
	)

	BeforeEach(func() {
		mockPubSub = NewMockPubSub(mockController)

		s, err := stores.NewCheckpointStore(stores.CheckpointStoreConfig{
			PubSub: core.Self(mockPubSub),

			LocalRegion: localRegion,
			Topic:       checkpointTopic,
		})

		Expect(err).To(BeNil())
		store = s
	})

	expectEvents := func(events ...kafka.Event) {
		var calls []*gomock.Call
		for _, event := range events {
			eventsChan := make(chan kafka.Event, 1)
			eventsChan <- event

			calls = append(calls, mockPubSub.EXPECT().Events().Return(eventsChan))
		}

		gomock.InOrder(calls...)
		expectPubSubForever(mockPubSub, lastCall(calls))
	}

	Context("When Subscribe is successful", func() {
		startStore := func() {
			mockPubSub.EXPECT().Subscribe(checkpointTopic)
			err := store.Start()
			Expect(err).To(BeNil())
		}

		stopStore := func(delay time.Duration) {
			<-time.After(delay)
			store.Stop()
		}

		newCheckpointMessage := func(partition int32, offset kafka.Offset, checkpoint core.Checkpoint) *kafka.Message {
			value, err := proto.Marshal(&checkpoint)
			Expect(err).To(BeNil())

			return &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &checkpointTopic,
					Partition: partition,
					Offset:    offset,
				},
				Value: value,
			}
		}

		Context("When Load is called", func() {
			var (
				expectedOffset1 = uint64(1020)
				expectedOffset2 = uint64(2030)
			)

			BeforeEach(func() {
				expectEvents(
					newCheckpointMessage(1, 0, core.Checkpoint{Region: region, Topic: topic, Partition: partition1, Offset: 1010}),
					newCheckpointMessage(1, 1, core.Checkpoint{Region: region, Topic: topic, Partition: partition1, Offset: expectedOffset1}),
					newCheckpointMessage(1, 2, core.Checkpoint{Region: region, Topic: topic, Partition: partition1, Offset: expectedOffset1 - 1}),
					newCheckpointMessage(1, 3, core.Checkpoint{Region: region, Topic: topic, Partition: partition2, Offset: 2010}),
					newCheckpointMessage(1, 4, core.Checkpoint{Region: region, Topic: topic, Partition: partition2, Offset: 2020}),
					newCheckpointMessage(1, 5, core.Checkpoint{Region: region, Topic: topic, Partition: partition2, Offset: expectedOffset2}),
					newTopicEOF(checkpointTopic))

				startStore()
			})

			AfterEach(func() {
				stopStore(stopWaitDuration)
			})

			Context("For local region", func() {
				It("Should return false", func() {
					checkpoint := store.Load(localRegion, topic, partition1)
					Expect(checkpoint).To(BeNil())
				})
			})

			Context("For unknown region", func() {
				It("Should return false", func() {
					checkpoint := store.Load("unknown", topic, partition1)
					Expect(checkpoint).To(BeNil())
				})
			})

			Context("For unknown topic", func() {
				It("Should return false", func() {
					checkpoint := store.Load(region, "unknown", partition1)
					Expect(checkpoint).To(BeNil())
				})
			})

			Context("For unknown partition", func() {
				It("Should return false", func() {
					checkpoint := store.Load(region, topic, 999)
					Expect(checkpoint).To(BeNil())
				})
			})

			Context("For known (region, topic, partition) values", func() {
				It("Should return true and stored offset", func() {
					checkpoint := store.Load(region, topic, partition1)
					Expect(checkpoint).NotTo(BeNil())
					Expect(checkpoint.Offset).To(Equal(expectedOffset1))

					checkpoint = store.Load(region, topic, partition2)
					Expect(checkpoint).NotTo(BeNil())
					Expect(checkpoint.Offset).To(Equal(expectedOffset2))
				})
			})
		})

		Context("When Save is called", func() {
			var (
				expectedOffset = uint64(2020)
			)

			BeforeEach(func() {
				expectEvents(
					newCheckpointMessage(1, 0, core.Checkpoint{Region: region, Topic: topic, Partition: partition1, Offset: 1010}),
					newTopicEOF(checkpointTopic))

				startStore()
			})

			AfterEach(func() {
				stopStore(stopWaitDuration)
			})

			Context("For local region", func() {
				It("Should return error", func() {
					err := store.Save(core.Checkpoint{Region: localRegion})
					Expect(err).NotTo(BeNil())
				})
			})

			Context("For new region", func() {
				It("Should successfully store the offset", func() {
					mockPubSub.EXPECT().Publish(checkpointTopic, gomock.Any(), matchCheckpointBytes(core.Checkpoint{
						Region:    "new",
						Topic:     topic,
						Partition: partition1,
						Offset:    expectedOffset,
					}))

					err := store.Save(core.Checkpoint{
						Region:    "new",
						Topic:     topic,
						Partition: partition1,
						Offset:    expectedOffset,
					})
					Expect(err).To(BeNil())

					checkpoint := store.Load("new", topic, partition1)
					Expect(checkpoint).NotTo(BeNil())
					Expect(checkpoint.Offset).To(Equal(expectedOffset))
				})
			})

			Context("For new topic", func() {
				It("Should successfully store the offset", func() {
					mockPubSub.EXPECT().Publish(checkpointTopic, gomock.Any(), matchCheckpointBytes(core.Checkpoint{
						Region:    region,
						Topic:     "new",
						Partition: partition1,
						Offset:    expectedOffset,
					}))

					err := store.Save(core.Checkpoint{
						Region:    region,
						Topic:     "new",
						Partition: partition1,
						Offset:    expectedOffset,
					})
					Expect(err).To(BeNil())

					checkpoint := store.Load(region, "new", partition1)
					Expect(checkpoint).NotTo(BeNil())
					Expect(checkpoint.Offset).To(Equal(expectedOffset))
				})
			})

			Context("For new partition", func() {
				It("Should successfully store the offset", func() {
					mockPubSub.EXPECT().Publish(checkpointTopic, gomock.Any(), matchCheckpointBytes(core.Checkpoint{
						Region:    region,
						Topic:     topic,
						Partition: 999,
						Offset:    expectedOffset,
					}))

					err := store.Save(core.Checkpoint{
						Region:    region,
						Topic:     topic,
						Partition: 999,
						Offset:    expectedOffset,
					})
					Expect(err).To(BeNil())

					checkpoint := store.Load(region, topic, 999)
					Expect(checkpoint).NotTo(BeNil())
					Expect(checkpoint.Offset).To(Equal(expectedOffset))
				})
			})

			Context("For already stored offset with greater value", func() {
				It("Should update the offset to new value", func() {
					mockPubSub.EXPECT().Publish(checkpointTopic, gomock.Any(), matchCheckpointBytes(core.Checkpoint{
						Region:    region,
						Topic:     topic,
						Partition: partition1,
						Offset:    expectedOffset,
					}))

					err := store.Save(core.Checkpoint{
						Region:    region,
						Topic:     topic,
						Partition: partition1,
						Offset:    expectedOffset,
					})
					Expect(err).To(BeNil())

					checkpoint := store.Load(region, topic, partition1)
					Expect(checkpoint).NotTo(BeNil())
					Expect(checkpoint.Offset).To(Equal(expectedOffset))
				})
			})

			Context("For already stored offset with lower value", func() {
				It("Should not update the offset", func() {
					oldCheckpoint := store.Load(region, topic, partition1)
					Expect(oldCheckpoint).NotTo(BeNil())

					err := store.Save(core.Checkpoint{
						Region:    region,
						Topic:     topic,
						Partition: partition1,
						Offset:    oldCheckpoint.Offset - 1,
					})
					Expect(err).To(BeNil())

					newCheckpoint := store.Load(region, topic, partition1)
					Expect(newCheckpoint).NotTo(BeNil())
					Expect(newCheckpoint.Offset).To(Equal(oldCheckpoint.Offset))
				})
			})

			Context("For already stored offset with same value", func() {
				It("Should not update the offset", func() {
					oldCheckpoint := store.Load(region, topic, partition1)
					Expect(oldCheckpoint).NotTo(BeNil())

					err := store.Save(core.Checkpoint{
						Region:    region,
						Topic:     topic,
						Partition: partition1,
						Offset:    oldCheckpoint.Offset,
					})
					Expect(err).To(BeNil())

					newCheckpoint := store.Load(region, topic, partition1)
					Expect(newCheckpoint).NotTo(BeNil())
					Expect(newCheckpoint.Offset).To(Equal(oldCheckpoint.Offset))
				})
			})

			Context("For same (region, topic, partition) multiple times", func() {
				It("Should produce using the use same message key (required for log compacted Kafka topic)", func() {
					key1 := same()
					key2 := same()

					gomock.InOrder(
						mockPubSub.EXPECT().Publish(checkpointTopic, key1, gomock.Any()).Times(1),
						mockPubSub.EXPECT().Publish(checkpointTopic, key2, gomock.Any()).Times(1),
						mockPubSub.EXPECT().Publish(checkpointTopic, key1, gomock.Any()).Times(2),
						mockPubSub.EXPECT().Publish(checkpointTopic, key2, gomock.Any()).Times(1))

					Expect(store.Save(core.Checkpoint{Region: region, Topic: topic, Partition: partition1, Offset: 1000000})).To(BeNil())
					Expect(store.Save(core.Checkpoint{Region: region, Topic: topic, Partition: partition2, Offset: 1000000})).To(BeNil())
					Expect(store.Save(core.Checkpoint{Region: region, Topic: topic, Partition: partition1, Offset: 2000000})).To(BeNil())
					Expect(store.Save(core.Checkpoint{Region: region, Topic: topic, Partition: partition1, Offset: 3000000})).To(BeNil())
					Expect(store.Save(core.Checkpoint{Region: region, Topic: topic, Partition: partition2, Offset: 2000000})).To(BeNil())
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
			mockPubSub.EXPECT().Subscribe(checkpointTopic).Return(errors.New("error"))
			err := store.Start()
			Expect(err).NotTo(BeNil())
		})
	})
})
