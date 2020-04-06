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
	"sync"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/pkg/errors"
)

const (
	maxDeleteParallelism = 16
)

var (
	runningMeter         = core.DefaultMetrics.GetMeter("compaction.running")
	successMeter         = core.DefaultMetrics.GetMeter("compaction.success")
	skippedMeter         = core.DefaultMetrics.GetMeter("compaction.skipped")
	stoppedMeter         = core.DefaultMetrics.GetMeter("compaction.stopped")
	errorsMeter          = core.DefaultMetrics.GetMeter("compaction.errors")
	durationTimer        = core.DefaultMetrics.GetTimer("compaction.duration")
	segmentsReadMeter    = core.DefaultMetrics.GetMeter("compaction.segments.read")
	segmentsWrittenMeter = core.DefaultMetrics.GetMeter("compaction.segments.written")

	// ErrSkipped indicates that compaction was skipped.
	ErrSkipped = errors.New("compaction skipped")
)

// Compactor encapsulates segment compaction logic.
type Compactor struct {
	segmentStore core.SegmentStore
}

// NewCompactor creates a new Compactor instance
func NewCompactor(store core.SegmentStore) (*Compactor, error) {
	if store == nil {
		return nil, errors.New("invalid parameter")
	}

	return &Compactor{
		segmentStore: store,
	}, nil
}

type compactRequest struct {
	region    string
	topic     string
	partition uint32
	config    Config
}

// CompactPartition performs the segment compaction for a single partition.
func (r *Compactor) CompactPartition(ctx context.Context, region, topic string, partition uint32, config Config) error {
	if len(region) == 0 || len(topic) == 0 {
		return errors.New("CompactPartition: invalid parameters")
	}

	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		return errors.Errorf("CompactPartition: invalid config %s", errMessage)
	}

	return r.compactAndLog(ctx, compactRequest{
		region:    region,
		topic:     topic,
		partition: partition,
		config:    config,
	})
}

func (r *Compactor) compactAndLog(ctx context.Context, c compactRequest) error {
	startTime := time.Now()
	log := r.logRequest(c)

	log.Info("Starting compaction")
	runningMeter.Mark(1)

	err := r.compact(ctx, c)

	switch err {
	case nil:
		successMeter.Mark(1)
		durationTimer.UpdateSince(startTime)
		log.Info("Compaction completed successfully")
	case ErrSkipped:
		skippedMeter.Mark(1)
	case context.Canceled:
		stoppedMeter.Mark(1)
		log.Info("Compaction stopped")
	default:
		errorsMeter.Mark(1)
		log.Errorf("Compaction error: %+v", err)
	}

	runningMeter.Mark(-1)
	return err
}

func (r *Compactor) compact(ctx context.Context, c compactRequest) error {
	segments, startOffset, err := r.getSegments(ctx, c)
	if err != nil {
		return err
	}

	rangeStart, rangeEnd := r.getOffsetRange(segments)
	r.logRequest(c).Infof("Processing segments in range [%d, %d]", rangeStart, rangeEnd)

	writer, err := r.segmentStore.Create(ctx)
	if err != nil {
		return err
	}

	messageCount, err := r.copySegments(ctx, writer, segments, startOffset, c.config.BatchSize)
	if err != nil {
		writer.Abort(ctx)
		return err
	}

	maxLevel := uint32(0)
	for _, segment := range segments {
		if segment.Level > maxLevel {
			maxLevel = segment.Level
		}
	}

	metadata := core.SegmentMetadata{
		Region:           c.region,
		Topic:            c.topic,
		Partition:        c.partition,
		Level:            maxLevel + 1,
		StartOffset:      startOffset,
		EndOffset:        segments[len(segments)-1].EndOffset,
		MessageCount:     messageCount,
		CreatedTimestamp: time.Now().UTC(),
	}

	if err := writer.Close(ctx, metadata); err != nil {
		return err
	}

	segmentsWrittenMeter.Mark(1)

	if c.config.Delete {
		r.deleteSegments(ctx, segments)
	}

	return nil
}

func (r *Compactor) getSegments(ctx context.Context, c compactRequest) ([]core.Segment, uint64, error) {
	segments, err := r.segmentStore.ListSegments(ctx, c.region, c.topic, c.partition)
	if err != nil {
		return nil, 0, err
	}

	heap := utils.NewSegmentHeap()
	maxTimestamp := time.Now().Add(-c.config.MinSegmentAge)
	prevEndOffset := uint64(0)
	prevEndOffsetFound := false

	for _, segment := range segments {
		switch {
		case segment.Level < c.config.MinLevel:
			// skip
		case segment.Level > c.config.MaxLevel:
			prevEndOffsetFound = true

			if segment.EndOffset > prevEndOffset {
				prevEndOffset = segment.EndOffset
			}
		default:
			if !segment.Timestamp.After(maxTimestamp) {
				heap.Push(segment)
			}
		}
	}

	if heap.Empty() {
		r.logRequest(c).Debug("Skipped: no segments")
		return nil, 0, ErrSkipped
	}

	var startOffset uint64
	if prevEndOffsetFound {
		startOffset = prevEndOffset + 1
	} else {
		startOffset = heap.Peek().StartOffset
	}

	nextOffset := startOffset
	var result []core.Segment
	count := 0
	totalSize := uint64(0)

	for !heap.Empty() {
		segment := heap.Pop()
		result = append(result, segment.Segment)

		// check if segment was compacted previously
		if segment.EndOffset < nextOffset {
			continue
		}

		if !segment.HasOffset(nextOffset) {
			return nil, 0, errors.Errorf("missing message range: [%d, %d]", nextOffset, segment.StartOffset-1)
		}

		count++
		totalSize += segment.Size
		nextOffset = segment.EndOffset + 1

		if count == c.config.MaxSegmentCount || totalSize >= c.config.MaxSegmentSize {
			break
		}
	}

	if count < c.config.MinSegmentCount {
		r.logRequest(c).Debugf("Skipped: segment count %d is less than threshold", count)
		return nil, 0, ErrSkipped
	}

	if totalSize < c.config.MinSegmentSize {
		r.logRequest(c).Debugf("Skipped: segment size %d is less than threshold", totalSize)
		return nil, 0, ErrSkipped
	}

	return result, startOffset, nil
}

func (r *Compactor) copySegments(ctx context.Context, writer core.SegmentWriter, segments []core.Segment, startOffset uint64, batchSize int) (uint64, error) {
	nextOffset := startOffset
	messageCount := uint64(0)

	copyMessages := func(segment core.Segment) error {
		reader, err := r.segmentStore.Open(ctx, segment)
		if err != nil {
			return err
		}
		defer reader.Close(ctx)

		for {
			// check if compaction was stopped
			if err := ctx.Err(); err != nil {
				return err
			}

			messages, err := reader.Read(ctx, batchSize)
			if err != nil {
				return err
			}

			// sanity check
			if len(messages) == 0 {
				return errors.New("segment read result is empty but EndOffset was not reached")
			}

			for _, message := range messages {
				if message.Offset < nextOffset {
					continue
				}

				if err := writer.Write(ctx, message); err != nil {
					return err
				}

				messageCount++
			}

			if messages[len(messages)-1].Offset == segment.EndOffset {
				break
			}
		}

		return nil
	}

	for _, segment := range segments {
		// skip segments before next offset caused by overlapping segments OR delete errors in previous compaction
		if segment.EndOffset < nextOffset {
			continue
		}

		if err := copyMessages(segment); err != nil {
			if !utils.IsCanceledError(err) {
				err = errors.Wrapf(err, "copy failed for segment Level=%d, StartOffset=%d, EndOffset=%d",
					segment.Level, segment.StartOffset, segment.EndOffset)
			}

			return 0, err
		}

		nextOffset = segment.EndOffset + 1
		segmentsReadMeter.Mark(1)
	}

	return messageCount, nil
}

func (r *Compactor) deleteSegments(ctx context.Context, segments []core.Segment) {
	var wg sync.WaitGroup
	wg.Add(len(segments))

	delete := func(requestChan chan int) {
		for i := range requestChan {
			segment := segments[i]

			if err := r.segmentStore.Delete(ctx, segment); err != nil {
				// error is not considered critical
				// will try again on next compaction
				r.logSegment(segment).Warnf("Failed to delete segment. Error: %+v", err)
			} else {
				r.logSegment(segment).Infof("Segment was deleted")
			}

			wg.Done()
		}
	}

	requestChan := make(chan int)
	for i := 0; i < len(segments) && i < maxDeleteParallelism; i++ {
		go delete(requestChan)
	}

LOOP:
	for i := range segments {
		select {
		case requestChan <- i:
		case <-ctx.Done():
			wg.Add(-len(segments) + i)
			break LOOP
		}
	}

	wg.Wait()
	close(requestChan)
}

func (r *Compactor) getOffsetRange(segments []core.Segment) (uint64, uint64) {
	end := segments[0].EndOffset
	for i := 1; i < len(segments); i++ {
		if segments[i].EndOffset > end {
			end = segments[i].EndOffset
		}
	}

	return segments[0].StartOffset, end
}

func (r *Compactor) log(region, topic string, partition uint32) core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type":      "Compactor",
		"Region":    region,
		"Topic":     topic,
		"Partition": partition,
	})
}

func (r *Compactor) logRequest(c compactRequest) core.Logger {
	return r.log(c.region, c.topic, c.partition)
}

func (r *Compactor) logSegment(s core.Segment) core.Logger {
	return r.log(s.Region, s.Topic, s.Partition).WithFields(core.Fields{
		"Level":       s.Level,
		"StartOffset": s.StartOffset,
		"EndOffset":   s.EndOffset,
	})
}
