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
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	minKafkaOffset = kafka.Offset(0)
)

type worker struct {
	localRegion   string
	config        TopicConfig
	id            int
	topic         string
	partition     uint32
	consumer      core.Consumer
	segmentStore  core.SegmentStore
	context       context.Context
	contextCancel context.CancelFunc
	messageChan   chan *kafka.Message
	controlChan   chan interface{}
	metrics       *workerMetrics
}

func (w *worker) run() {
	w.log().Debug("Running")

	startOffset := kafka.OffsetInvalid
	endOffset := kafka.OffsetInvalid
	var writer core.SegmentWriter
	var timer *time.Timer
	sizeBytes := uint64(0)
	messageCount := 0

	isFull := func() bool {
		return sizeBytes >= w.config.MaxSegmentSize || messageCount == w.config.MaxSegmentMessages
	}

	timerChan := func() <-chan time.Time {
		if timer == nil {
			return nil
		}

		return timer.C
	}

	reset := func() {
		endOffset = kafka.OffsetInvalid
		writer = nil
		timer.Stop()
		timer = nil
		sizeBytes = 0
		messageCount = 0
	}

	rewindSegment := func(abort bool) bool {
		if abort {
			writer.Abort(w.context)
		}

		if !w.seekPartition(startOffset) {
			return false
		}

		w.metrics.partition.segmentsRewinded.Mark(1)
		w.metrics.partition.messagesRewinded.Mark(int64(endOffset-startOffset) + 1)
		reset()
		return true
	}

	completeSegment := func() bool {
		metadata := core.SegmentMetadata{
			Region:           w.localRegion,
			Topic:            w.topic,
			Partition:        w.partition,
			Level:            core.LevelStreaming,
			StartOffset:      uint64(startOffset),
			EndOffset:        uint64(endOffset),
			MessageCount:     uint64(messageCount),
			CreatedTimestamp: time.Now().UTC(),
		}

		err := writer.Close(w.context, metadata)
		if err != nil {
			w.log().Errorf("Writer close failed with error: %+v", err)
			return false
		}

		if !w.commitOffset(endOffset) {
			return false
		}

		w.metrics.partition.segmentsWritten.Mark(1)
		w.log().Infof("Segment written successfully for range [%d, %d]", startOffset, endOffset)

		startOffset = kafka.OffsetInvalid
		reset()
		return true
	}

LOOP:
	for {
		select {
		case msg := <-w.controlChan:
			switch cmd := msg.(type) {
			case stopCommand:
				if writer != nil {
					if messageCount > 0 {
						w.log().Infof("Discarded partial segment with %d messages during shutdown", messageCount)
					}
					writer.Abort(w.context)
				}

				w.metrics.unregister()
				close(cmd)
				break LOOP
			default:
				w.log().Warnf("Ignored unknown control message: %+v", msg)
			}
		case <-timerChan():
			if !completeSegment() && !rewindSegment(false) {
				break LOOP
			}
		case msg := <-w.messageChan:
			w.metrics.partition.messagesConsumed.Mark(1)

			if !w.sanityChecks(msg.TopicPartition, endOffset) {
				continue
			}

			if writer == nil {
				// skip buffered messages after rewind
				if startOffset != kafka.OffsetInvalid && msg.TopicPartition.Offset != startOffset {
					continue
				}

				if !w.createSegmentWriter(&writer) {
					break LOOP
				}

				startOffset = msg.TopicPartition.Offset
				timer = time.NewTimer(w.config.MaxSegmentAge)
			}

			message := w.createMessage(msg)
			if err := writer.Write(w.context, message); err != nil {
				w.log().Errorf("Segment write failed with error: %+v", err)

				if !rewindSegment(true) {
					break LOOP
				}
				continue
			}

			w.metrics.partition.messagesWritten.Mark(1)

			endOffset = msg.TopicPartition.Offset
			sizeBytes += message.Size()
			messageCount++

			if isFull() && !completeSegment() && !rewindSegment(false) {
				break LOOP
			}
		}
	}

	w.log().Debug("Stopped")
}

func (w *worker) stop() {
	w.log().Debug("Stopping")

	stopCmd := make(stopCommand)
	w.controlChan <- stopCmd

	select {
	case <-time.After(w.config.WorkerStopTimeout):
		w.log().Warn("Failed to stop within the allotted time")
		w.contextCancel()
	case <-stopCmd:
	}
}

func (w *worker) createSegmentWriter(writer *core.SegmentWriter) bool {
	return w.config.SegmentStoreRetrier.Forever(w.context, func() error {
		tmp, err := w.segmentStore.Create(w.context)
		if err != nil {
			w.log().Errorf("Failed to create segment writer. Error: %+v", err)
			return err
		}

		*writer = tmp
		return nil
	})
}

func (w *worker) commitOffset(offset kafka.Offset) bool {
	w.log().Debugf("Committing offset %d", offset)

	return w.config.ConsumerRetrier.Forever(w.context, func() error {
		err := w.consumer.Commit(w.topic, w.partition, offset)
		if err != nil {
			w.log().Errorf("Failed to commit offset %d. Error: %+v", offset, err)
			return err
		}

		w.log().Debugf("Committed offset %d", offset)
		return nil
	})
}

func (w *worker) seekPartition(offset kafka.Offset) bool {
	w.log().Debugf("Seeking to offset %d", offset)

	return w.config.ConsumerRetrier.Forever(w.context, func() error {
		err := w.consumer.Seek(w.topic, w.partition, offset)
		if err != nil {
			w.log().Errorf("Failed to seek to offset %d. Error: %+v", offset, err)
			return err
		}

		w.log().Debugf("Seek to offset %d successful", offset)
		return nil
	})
}

func (w *worker) createMessage(msg *kafka.Message) core.Message {
	result := core.Message{
		Key:    msg.Key,
		Value:  msg.Value,
		Offset: uint64(msg.TopicPartition.Offset),
	}

	if msg.TimestampType != kafka.TimestampNotAvailable {
		result.Timestamp = msg.Timestamp
	}

	if len(msg.Headers) > 0 {
		result.Headers = make([]*core.Message_Header, len(msg.Headers))
		for i, header := range msg.Headers {
			result.Headers[i] = &core.Message_Header{
				Key:   header.Key,
				Value: header.Value,
			}
		}
	}

	return result
}

func (w *worker) sanityChecks(tp kafka.TopicPartition, endOffset kafka.Offset) bool {
	if *tp.Topic != w.topic || uint32(tp.Partition) != w.partition {
		w.log().Errorf("Invalid message: bad dispatch for messsage in topic %s, partition %d", *tp.Topic, tp.Partition)
		return false
	}

	if tp.Offset < minKafkaOffset {
		w.log().Errorf("Invalid message: bad offset %d", tp.Offset)
		return false
	}

	if tp.Offset <= endOffset {
		w.log().Errorf("Invalid message: bad offset %d needs to be strictly greater than %d", tp.Offset, endOffset)
		return false
	}

	return true
}

func (w *worker) log() core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type":        "egress.worker",
		"LocalRegion": w.localRegion,
		"ID":          w.id,
		"Topic":       w.topic,
		"Partition":   w.partition,
	})
}
