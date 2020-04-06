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
	"sync"
	"sync/atomic"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/compaction"
	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Compaction tests", func() {
	var (
		localRegion      = "local_region"
		topic1           = "test_topic1"
		topic2           = "test_topic2"
		eventDelay       = 2 * waitDuration
		parallelism      = 8
		minLevel         = uint32(1)
		maxLevel         = uint32(3)
		minSegmentAge    = time.Hour
		minSegmentCount  = 3
		maxSegmentCount  = 5
		minSegmentSize   = uint64(1000)
		maxSegmentSize   = uint64(5000)
		batchSize        = 10
		mockConsumer     *MockConsumer
		mockSegmentStore *MockSegmentStore
		controller       *compaction.Controller
	)

	BeforeEach(func() {
		mockConsumer = NewMockConsumer(mockController)
		mockSegmentStore = NewMockSegmentStore(mockController)

		topicConfig := &compaction.Config{
			MinLevel:        minLevel,
			MaxLevel:        maxLevel,
			MinSegmentAge:   minSegmentAge,
			MinSegmentCount: minSegmentCount,
			MinSegmentSize:  minSegmentSize,
			MaxSegmentCount: maxSegmentCount,
			MaxSegmentSize:  maxSegmentSize,
			BatchSize:       batchSize,
			Delete:          true,
		}

		c, err := compaction.New(compaction.ControllerConfig{
			Consumer:     core.Self(mockConsumer),
			SegmentStore: core.Self(mockSegmentStore),

			LocalRegion: localRegion,
			Topics: map[string]*compaction.Config{
				topic1: topicConfig,
				topic2: topicConfig,
			},
			Parallelism: parallelism,
		})

		Expect(err).To(BeNil())
		controller = c
	})

	expectEvents := func(events ...interface{}) {
		expectEvents(mockConsumer, nil, events...)
	}

	expectCopiedSegment := func(after *gomock.Call, writer *MockSegmentWriter, segment core.Segment) *gomock.Call {
		message := newMessage(segment.EndOffset, 1000)
		reader := NewMockSegmentReader(mockController)

		calls := []*gomock.Call{
			mockSegmentStore.EXPECT().Open(gomock.Any(), segment).Return(reader, nil),
			reader.EXPECT().Read(gomock.Any(), batchSize).Return([]core.Message{message}, nil),
			writer.EXPECT().Write(gomock.Any(), message),
			reader.EXPECT().Close(gomock.Any())}

		if after != nil {
			calls[0].After(after)
		}

		gomock.InOrder(calls...)
		return lastCall(calls)
	}

	expectCopiedSegments := func(after *gomock.Call, writer *MockSegmentWriter, segments []core.Segment) *gomock.Call {
		for _, segment := range segments {
			after = expectCopiedSegment(after, writer, segment)
		}

		return after
	}

	expectDeletedSegments := func(segments []core.Segment) {
		for _, segment := range segments {
			mockSegmentStore.EXPECT().Delete(gomock.Any(), segment)
		}
	}

	expectCompaction := func(copied []core.Segment, deleted []core.Segment) {
		writer := NewMockSegmentWriter(mockController)
		mockSegmentStore.EXPECT().Create(gomock.Any()).Return(writer, nil)

		metadata := core.SegmentMetadata{
			Region:       copied[0].Region,
			Topic:        copied[0].Topic,
			Partition:    copied[0].Partition,
			Level:        lastSegment(copied).Level + 1,
			StartOffset:  copied[0].StartOffset,
			EndOffset:    lastSegment(copied).EndOffset,
			MessageCount: uint64(len(copied)),
		}

		writer.EXPECT().Close(gomock.Any(), matchSegmentMetadata(metadata))

		expectCopiedSegments(nil, writer, copied)
		expectDeletedSegments(deleted)
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

			<-time.After(waitDuration)
			controller.Compact()

			stopController(stopWaitDuration)
		}

		Context("When receives AssignedPartitions event", func() {
			It("Should store partition assignments and start compaction", func() {
				partition0 := uint32(0)
				partition1 := uint32(1)

				partitions := []kafka.TopicPartition{
					newTopicPartition(topic1, partition0, 0),
					newTopicPartition(topic1, partition1, 0),
					newTopicPartition(topic2, partition0, 0),
				}

				expectEvents(newAssignedPartitions(partitions...))

				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)

				mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, topic1, partition0).Return(nil, errors.New("skip"))
				mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, topic1, partition1).Return(nil, errors.New("skip"))
				mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, topic2, partition0).Return(nil, errors.New("skip"))

				runController()
			})

			It("Should limit maximum simultaneous compactions", func() {
				var partitions []kafka.TopicPartition
				for i := 0; i < 2*parallelism; i++ {
					partitions = append(partitions, newTopicPartition(topic1, uint32(i), 0))
				}

				expectEvents(newAssignedPartitions(partitions...))

				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)

				var mutex sync.Mutex
				running := 0
				maxRunning := 0

				mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, topic1, gomock.Any()).
					Times(len(partitions)).
					DoAndReturn(func(ctx context.Context, region, topic string, partition uint32) (map[core.Segment]core.SegmentInfo, error) {
						mutex.Lock()
						running++
						if running > maxRunning {
							maxRunning = running
						}
						mutex.Unlock()

						<-time.After(waitDuration)

						mutex.Lock()
						running--
						mutex.Unlock()
						return nil, errors.New("skip")
					})

				runController()
				Expect(maxRunning <= parallelism).To(BeTrue())
			})
		})

		Context("When receives RevokedPartitions event", func() {
			It("Should drop partition assignments and stop running compactions", func() {
				partitions := []kafka.TopicPartition{
					newTopicPartition(topic1, 0, 0),
					newTopicPartition(topic1, 1, 0),
					newTopicPartition(topic2, 0, 0),
				}

				expectEvents(
					newAssignedPartitions(partitions...),
					newDelay(eventDelay),
					newRevokedPartitions(partitions...))

				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)
				mockConsumer.EXPECT().Unassign()

				stopped := int32(0)
				mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).
					Times(len(partitions)).
					DoAndReturn(func(ctx context.Context, region, topic string, partition uint32) (map[core.Segment]core.SegmentInfo, error) {
						<-ctx.Done()
						atomic.AddInt32(&stopped, 1)
						return nil, errors.New("skip")
					})

				runController()
				Expect(int(stopped)).To(Equal(len(partitions)))
			})
		})

		Context("When Compact is invoked while compaction is running", func() {
			It("Should not start parallel compaction", func() {
				partitions := []kafka.TopicPartition{
					newTopicPartition(topic1, 0, 0),
				}

				expectEvents(newAssignedPartitions(partitions...))

				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)

				ran := int32(0)
				mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).
					Times(len(partitions)).
					DoAndReturn(func(ctx context.Context, region, topic string, partition uint32) (map[core.Segment]core.SegmentInfo, error) {
						<-time.After(waitDuration)
						atomic.AddInt32(&ran, 1)
						return nil, errors.New("skip")
					})

				startController()

				<-time.After(waitDuration)
				controller.Compact()
				controller.Compact() // second call should result in noop

				stopController(stopWaitDuration)

				Expect(int(ran)).To(Equal(len(partitions)))
			})
		})

		Context("When Compact is invoked without assigned partitions", func() {
			It("Should take no action", func() {
				partitions := []kafka.TopicPartition{}

				expectEvents(newAssignedPartitions(partitions...))

				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)

				runController()
			})
		})

		Context("When Compact is invoked right after RevokedPartitions event", func() {
			It("Should take no action", func() {
				partitions := []kafka.TopicPartition{
					newTopicPartition(topic1, 0, 0),
				}

				expectEvents(
					newAssignedPartitions(partitions...),
					newRevokedPartitions(partitions...))

				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)
				mockConsumer.EXPECT().Unassign()

				runController()
			})
		})

		Context("When SegmentStore.ListSegments is successful", func() {
			topic := topic1
			partition := uint32(107)

			BeforeEach(func() {
				partitions := []kafka.TopicPartition{
					newTopicPartition(topic, partition, 0),
				}

				expectEvents(newAssignedPartitions(partitions...))

				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)
			})

			newSegment := func(level uint32, offset uint64) core.Segment {
				return core.Segment{Region: localRegion, Topic: topic, Partition: partition, Level: level, StartOffset: offset, EndOffset: offset}
			}

			Context("When compaction is successful", func() {
				It("Should write all messages and segment metadata, then delete compacted segments", func() {
					count := uint64(maxSegmentCount)
					size := (maxSegmentSize / uint64(maxSegmentCount)) + 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}

					writer := NewMockSegmentWriter(mockController)
					mockSegmentStore.EXPECT().Create(gomock.Any()).Return(writer, nil)

					var writeCalls []*gomock.Call
					nextOffset := uint64(100)
					for i := uint64(0); i < count; i++ {
						startOffset := nextOffset
						endOffset := nextOffset + i
						segment := core.Segment{Region: localRegion, Topic: topic, Partition: partition, Level: minLevel, StartOffset: startOffset, EndOffset: endOffset}
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
						mockSegmentStore.EXPECT().Delete(gomock.Any(), segment)

						var messages []core.Message
						for offset := startOffset; offset <= endOffset; offset++ {
							messages = append(messages, newMessage(offset, 1000))
						}

						reader := NewMockSegmentReader(mockController)
						mockSegmentStore.EXPECT().Open(gomock.Any(), segment).Return(reader, nil)
						reader.EXPECT().Read(gomock.Any(), batchSize).Return(messages, nil)
						reader.EXPECT().Close(gomock.Any())

						for _, message := range messages {
							writeCalls = append(writeCalls, writer.EXPECT().Write(gomock.Any(), message))
						}

						nextOffset = endOffset + 1
					}

					gomock.InOrder(writeCalls...)

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)

					metadata := core.SegmentMetadata{
						Region:       localRegion,
						Topic:        topic,
						Partition:    partition,
						Level:        minLevel + 1,
						StartOffset:  100,
						EndOffset:    nextOffset - 1,
						MessageCount: uint64(len(writeCalls)),
					}

					writer.EXPECT().Close(gomock.Any(), matchSegmentMetadata(metadata))

					runController()
				})
			})

			Context("When segment contains multiple message batches", func() {
				It("Should read all batches until end offset is reached", func() {
					count := uint64(minSegmentCount)
					size := (minSegmentSize / count) + 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}
					var segments []core.Segment

					for i := uint64(0); i < count-1; i++ {
						segment := newSegment(minLevel, i)
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
						segments = append(segments, segment)
					}

					nextOffset := lastSegment(segments).EndOffset + 1
					var batches [][]core.Message

					for i := 0; i < 3; i++ {
						var batch []core.Message
						for j := 0; j < batchSize; j++ {
							batch = append(batch, newMessage(nextOffset, 1000))
							nextOffset++
						}

						batches = append(batches, batch)
					}

					bigSegment := newSegment(minLevel, 0)
					bigSegment.StartOffset = batches[0][0].Offset
					bigSegment.EndOffset = nextOffset - 1
					listSegmentsResult[bigSegment] = core.SegmentInfo{Segment: bigSegment, Size: size}

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)

					writer := NewMockSegmentWriter(mockController)
					writer.EXPECT().Close(gomock.Any(), gomock.Any())
					mockSegmentStore.EXPECT().Create(gomock.Any()).Return(writer, nil)

					after := expectCopiedSegments(nil, writer, segments)

					// read bigSegment
					reader := NewMockSegmentReader(mockController)
					calls := []*gomock.Call{mockSegmentStore.EXPECT().Open(gomock.Any(), bigSegment).Return(reader, nil).After(after)}

					for _, batch := range batches {
						calls := append(calls, reader.EXPECT().Read(gomock.Any(), batchSize).Return(batch, nil))

						for _, message := range batch {
							calls = append(calls, writer.EXPECT().Write(gomock.Any(), message))
						}
					}

					calls = append(calls, reader.EXPECT().Close(gomock.Any()))
					gomock.InOrder(calls...)

					expectDeletedSegments(append(segments, bigSegment))

					runController()
				})
			})

			Context("When segment offset range contains gaps", func() {
				Context("With gap between compacted segments", func() {
					It("Should skip compaction", func() {
						count := uint64(minSegmentCount)
						size := (minSegmentSize / count) + 1
						listSegmentsResult := map[core.Segment]core.SegmentInfo{}

						for i := uint64(0); i < count; i++ {
							offset := i

							// gap between last two segments
							if i == count-1 {
								offset++
							}

							segment := newSegment(minLevel, offset)
							listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
						}

						mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)

						runController()
					})
				})

				Context("With gap between previously-compacted segments and current segments", func() {
					It("Should skip compaction", func() {
						count := uint64(minSegmentCount)
						size := (minSegmentSize / count) + 1
						listSegmentsResult := map[core.Segment]core.SegmentInfo{}

						prevCount := uint64(5)
						offset := uint64(0)

						// previously-compacted segments at level above MaxLevel
						for ; offset < prevCount; offset++ {
							segment := newSegment(maxLevel+1, offset)
							listSegmentsResult[segment] = core.SegmentInfo{Segment: segment}
						}

						// insert offset gap
						offset++

						for ; offset < count+prevCount; offset++ {
							segment := newSegment(maxLevel, offset)
							listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
						}

						mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)

						runController()
					})
				})
			})

			Context("When segments contain overlapping ranges", func() {
				Context("With previously-compacted segment", func() {
					It("Should skip duplicate messages", func() {
						count := uint64(minSegmentCount)
						size := (minSegmentSize / count) + 1
						listSegmentsResult := map[core.Segment]core.SegmentInfo{}

						// previously-compacted segment
						compactedEndOffset := uint64(10)
						compacted := core.Segment{Region: localRegion, Topic: topic, Partition: partition, Level: maxLevel + 1, StartOffset: 0, EndOffset: compactedEndOffset}
						listSegmentsResult[compacted] = core.SegmentInfo{Segment: compacted}

						overlapping := core.Segment{Region: localRegion, Topic: topic, Partition: partition, Level: maxLevel, StartOffset: compactedEndOffset - 2, EndOffset: compactedEndOffset + 2}
						listSegmentsResult[overlapping] = core.SegmentInfo{Segment: overlapping, Size: size}

						segments := []core.Segment{overlapping}
						for i := uint64(0); i < count-1; i++ {
							segment := newSegment(maxLevel, compactedEndOffset+i+3)
							listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
							segments = append(segments, segment)
						}

						mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)

						writer := NewMockSegmentWriter(mockController)
						writer.EXPECT().Close(gomock.Any(), gomock.Any())
						mockSegmentStore.EXPECT().Create(gomock.Any()).Return(writer, nil)

						messages := []core.Message{
							newMessage(compactedEndOffset-2, 1000), // skipped
							newMessage(compactedEndOffset-1, 1000), // skipped
							newMessage(compactedEndOffset, 1000),   // skipped
							newMessage(compactedEndOffset+1, 1000),
							newMessage(compactedEndOffset+2, 1000),
						}
						reader := NewMockSegmentReader(mockController)

						calls := []*gomock.Call{
							mockSegmentStore.EXPECT().Open(gomock.Any(), overlapping).Return(reader, nil),
							reader.EXPECT().Read(gomock.Any(), batchSize).Return(messages, nil),
							writer.EXPECT().Write(gomock.Any(), messages[3]),
							writer.EXPECT().Write(gomock.Any(), messages[4]),
							reader.EXPECT().Close(gomock.Any())}

						gomock.InOrder(calls...)

						expectCopiedSegments(lastCall(calls), writer, segments[1:])
						expectDeletedSegments(segments)

						runController()
					})
				})

				Context("With partial overlap within compacted segments", func() {
					It("Should skip duplicate messages", func() {
						count := uint64(minSegmentCount)
						size := (minSegmentSize / count) + 1
						listSegmentsResult := map[core.Segment]core.SegmentInfo{}

						overlapping1 := core.Segment{Region: localRegion, Topic: topic, Partition: partition, Level: maxLevel, StartOffset: 0, EndOffset: 3}
						listSegmentsResult[overlapping1] = core.SegmentInfo{Segment: overlapping1, Size: size}

						overlapping2 := core.Segment{Region: localRegion, Topic: topic, Partition: partition, Level: maxLevel, StartOffset: 2, EndOffset: 5}
						listSegmentsResult[overlapping2] = core.SegmentInfo{Segment: overlapping2, Size: size}

						segments := []core.Segment{overlapping1, overlapping2}
						for i := uint64(0); i < count-2; i++ {
							segment := newSegment(maxLevel, i+6)
							listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
							segments = append(segments, segment)
						}

						mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)

						writer := NewMockSegmentWriter(mockController)
						writer.EXPECT().Close(gomock.Any(), gomock.Any())
						mockSegmentStore.EXPECT().Create(gomock.Any()).Return(writer, nil)

						messages := []core.Message{
							newMessage(2, 1000), // skipped
							newMessage(3, 1000), // skipped
							newMessage(4, 1000),
							newMessage(5, 1000),
						}
						reader := NewMockSegmentReader(mockController)

						calls := []*gomock.Call{
							expectCopiedSegment(nil, writer, overlapping1),
							mockSegmentStore.EXPECT().Open(gomock.Any(), overlapping2).Return(reader, nil),
							reader.EXPECT().Read(gomock.Any(), batchSize).Return(messages, nil),
							writer.EXPECT().Write(gomock.Any(), messages[2]),
							writer.EXPECT().Write(gomock.Any(), messages[3]),
							reader.EXPECT().Close(gomock.Any())}

						gomock.InOrder(calls...)

						expectCopiedSegments(lastCall(calls), writer, segments[2:])
						expectDeletedSegments(segments)

						runController()
					})
				})

				Context("With complete overlap within compacted segments", func() {
					It("Should skip segment containing all duplicate messages", func() {
						count := uint64(minSegmentCount)
						size := (minSegmentSize / count) + 1
						listSegmentsResult := map[core.Segment]core.SegmentInfo{}

						overlapping1 := core.Segment{Region: localRegion, Topic: topic, Partition: partition, Level: maxLevel, StartOffset: 0, EndOffset: 3}
						listSegmentsResult[overlapping1] = core.SegmentInfo{Segment: overlapping1, Size: size}

						// will be entirely skipped
						overlapping2 := core.Segment{Region: localRegion, Topic: topic, Partition: partition, Level: maxLevel, StartOffset: 0, EndOffset: 2}
						listSegmentsResult[overlapping2] = core.SegmentInfo{Segment: overlapping2, Size: size}

						segments := []core.Segment{overlapping1}
						for i := uint64(0); i < count-1; i++ {
							segment := newSegment(maxLevel, i+4)
							listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
							segments = append(segments, segment)
						}

						mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)
						expectCompaction(segments, append(segments, overlapping2))

						runController()
					})
				})
			})

			Context("When segment was previously-compacted (previous cleanup failed scanario)", func() {
				It("Should only copy segments that were not compacted before and delete previously-compacted segments", func() {
					count := uint64(minSegmentCount)
					size := (minSegmentSize / count) + 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}
					var copied []core.Segment
					var deleted []core.Segment
					prevEndOffset := uint64(5)

					// previously-compacted segment
					segment := core.Segment{Region: localRegion, Topic: topic, Partition: partition, Level: maxLevel + 1, StartOffset: 0, EndOffset: prevEndOffset}
					listSegmentsResult[segment] = core.SegmentInfo{Segment: segment}

					for i := uint64(0); i < count+prevEndOffset+1; i++ {
						segment := newSegment(maxLevel, i)
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}

						deleted = append(deleted, segment)
						if i > prevEndOffset {
							copied = append(copied, segment)
						}
					}

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)
					expectCompaction(copied, deleted)

					runController()
				})
			})

			Context("When segment level is less than MinLevel", func() {
				It("Should skip the segment", func() {
					count := uint64(minSegmentCount)
					size := (minSegmentSize / count) + 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}
					var compacted []core.Segment

					for i := uint64(0); i < count+5; i++ {
						level := minLevel - 1
						if i < count {
							level = minLevel
						}

						segment := newSegment(level, i)
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}

						if i < count {
							compacted = append(compacted, segment)
						}
					}

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)
					expectCompaction(compacted, compacted)

					runController()
				})
			})

			Context("When segment level is greater than MaxLevel", func() {
				It("Should skip the segment", func() {
					count := uint64(minSegmentCount)
					size := (minSegmentSize / count) + 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}
					var compacted []core.Segment

					prevCount := uint64(5)
					offset := uint64(0)

					for ; offset < prevCount; offset++ {
						segment := newSegment(maxLevel+1, offset)
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment}
					}

					// segments at lower levels have greater offsets
					for ; offset < count+prevCount; offset++ {
						segment := newSegment(maxLevel, offset)
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
						compacted = append(compacted, segment)
					}

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)
					expectCompaction(compacted, compacted)

					runController()
				})
			})

			Context("When segment is newer than MinSegmentAge", func() {
				It("Should skip the segment", func() {
					count := uint64(minSegmentCount)
					size := (minSegmentSize / count) + 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}
					var compacted []core.Segment

					for i := uint64(0); i < count+5; i++ {
						segment := newSegment(minLevel, i)

						timestamp := time.Now()
						if i < count {
							timestamp = timestamp.Add(-2 * minSegmentAge)
							compacted = append(compacted, segment)
						}

						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Timestamp: timestamp, Size: size}
					}

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)
					expectCompaction(compacted, compacted)

					runController()
				})
			})

			Context("When segment count is less than MinSegmentCount", func() {
				It("Should skip the compaction", func() {
					count := uint64(minSegmentCount) - 1
					size := (minSegmentSize / count) + 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}

					for i := uint64(0); i < count; i++ {
						segment := newSegment(minLevel, i)
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
					}

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)

					runController()
				})
			})

			Context("When segments total size is less than MinSegmentSize", func() {
				It("Should skip the compaction", func() {
					count := uint64(minSegmentCount)
					size := (minSegmentSize / count) - 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}

					for i := uint64(0); i < count; i++ {
						segment := newSegment(minLevel, i)
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
					}

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)

					runController()
				})
			})

			Context("When segment count is greater than MaxSegmentCount", func() {
				It("Should compact only MaxSegmentCount segments", func() {
					count := uint64(maxSegmentCount)
					size := (minSegmentSize / count) + 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}
					var compacted []core.Segment

					for i := uint64(0); i < count+5; i++ {
						segment := newSegment(minLevel, i)
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}

						if i < count {
							compacted = append(compacted, segment)
						}
					}

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)
					expectCompaction(compacted, compacted)

					runController()
				})
			})

			Context("When total segment size is greater than MaxSegmentSize", func() {
				It("Should compact up to MaxSegmentSize", func() {
					count := uint64(minSegmentCount)
					size := (maxSegmentSize / count) + 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}
					var compacted []core.Segment

					for i := uint64(0); i < count+5; i++ {
						segment := newSegment(minLevel, i)
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}

						if i < count {
							compacted = append(compacted, segment)
						}
					}

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)
					expectCompaction(compacted, compacted)

					runController()
				})
			})

			Context("When SegmentStore.Create fails", func() {
				It("Should stop the compaction", func() {
					count := uint64(minSegmentCount)
					size := (maxSegmentSize / count) + 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}

					for i := uint64(0); i < count; i++ {
						segment := newSegment(minLevel, i)
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
					}

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)
					mockSegmentStore.EXPECT().Create(gomock.Any()).Return(nil, errors.New("error"))

					runController()
				})
			})

			Context("When SegmentStore.Open fails", func() {
				It("Should abort the writer and stop the compaction", func() {
					count := uint64(minSegmentCount)
					size := (maxSegmentSize / count) + 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}
					var compacted []core.Segment

					for i := uint64(0); i < count; i++ {
						segment := newSegment(minLevel, i)
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
						compacted = append(compacted, segment)
					}

					writer := NewMockSegmentWriter(mockController)
					writer.EXPECT().Abort(gomock.Any())

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)
					mockSegmentStore.EXPECT().Create(gomock.Any()).Return(writer, nil)
					mockSegmentStore.EXPECT().Open(gomock.Any(), compacted[0]).Return(nil, errors.New("error"))

					runController()
				})
			})

			Context("When SegmentReader.Read fails", func() {
				It("Should abort the writer and stop the compaction", func() {
					count := uint64(minSegmentCount)
					size := (maxSegmentSize / count) + 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}
					var compacted []core.Segment

					for i := uint64(0); i < count; i++ {
						segment := newSegment(minLevel, i)
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
						compacted = append(compacted, segment)
					}

					writer := NewMockSegmentWriter(mockController)
					writer.EXPECT().Abort(gomock.Any())

					reader := NewMockSegmentReader(mockController)
					reader.EXPECT().Read(gomock.Any(), gomock.Any()).Return(nil, errors.New("error"))
					reader.EXPECT().Close(gomock.Any())

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)
					mockSegmentStore.EXPECT().Create(gomock.Any()).Return(writer, nil)
					mockSegmentStore.EXPECT().Open(gomock.Any(), compacted[0]).Return(reader, nil)

					runController()
				})
			})

			Context("When SegmentWriter.Close fails", func() {
				It("Should abort the compaction and not delete the segments", func() {
					count := uint64(minSegmentCount)
					size := (maxSegmentSize / count) + 1
					listSegmentsResult := map[core.Segment]core.SegmentInfo{}
					var compacted []core.Segment

					for i := uint64(0); i < count; i++ {
						segment := newSegment(minLevel, i)
						listSegmentsResult[segment] = core.SegmentInfo{Segment: segment, Size: size}
						compacted = append(compacted, segment)
					}

					mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(listSegmentsResult, nil)
					writer := NewMockSegmentWriter(mockController)
					mockSegmentStore.EXPECT().Create(gomock.Any()).Return(writer, nil)

					expectCopiedSegments(nil, writer, compacted)
					writer.EXPECT().Close(gomock.Any(), gomock.Any()).Return(errors.New("error"))

					runController()
				})
			})
		})

		Context("When SegmentStore.ListSegments fails", func() {
			It("Should stop the compaction", func() {
				partitions := []kafka.TopicPartition{
					newTopicPartition(topic1, 0, 0),
				}

				expectEvents(newAssignedPartitions(partitions...))

				mockConsumer.EXPECT().Assign(partitions)
				mockConsumer.EXPECT().Pause(partitions)
				mockSegmentStore.EXPECT().ListSegments(gomock.Any(), localRegion, gomock.Any(), gomock.Any()).Return(nil, errors.New("error"))

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
