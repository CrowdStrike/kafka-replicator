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

package ingress

import (
	"context"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
)

type worker struct {
	localRegion     string
	config          SourceConfig
	id              int
	srcRegion       string
	srcTopic        string
	destTopic       string
	partition       uint32
	producer        core.Producer
	segmentStore    core.SegmentStore
	checkpointStore core.CheckpointStore
	context         context.Context
	contextCancel   context.CancelFunc
	controlChan     chan interface{}
	eventsChan      chan core.SegmentEvent
	metrics         *workerMetrics
	timer           *time.Timer // used for two purposes: 1. wait for first segment 2. late segment retry backoff
}

type segmentHeap struct {
	*utils.SegmentHeap
	metrics *workerMetrics
}

type workerInitRequest struct {
}

type workerInitResult struct {
	checkpoint *core.Checkpoint
	heap       *segmentHeap
}

type workerProcessRequest struct {
	segment    core.Segment
	nextOffset uint64
}

type workerProcessResult struct {
	checkpoint *core.Checkpoint
}

type workerReloadRequest struct {
	checkpoint *core.Checkpoint
}

type workerReloadResponse struct {
	heap *segmentHeap
}

// controlLoop MUST not block with all I/O and CPU intensive operations sent to workLoop below
func (w *worker) controlLoop() {
	w.log().Debug("Running")

	stopChan := make(chan struct{})
	requestChan := make(chan interface{})
	resultChan := make(chan interface{})
	waitingResult := false

	sendRequest := func(request interface{}) {
		if waitingResult {
			w.log().Panic("Invalid state: concurrent workLoop requests detected")
		}

		requestChan <- request
		waitingResult = true
	}

	go w.workLoop(requestChan, resultChan, stopChan)

	var checkpoint *core.Checkpoint
	var heap *segmentHeap
	lateSegmentRetry := 0

	getFirst := func() (*core.Segment, uint64) {
		tmp := heap.Peek().Timestamp.Add(w.config.FirstSegmentDelay)
		now := time.Now()
		if now.Before(tmp) {
			w.setTimer(tmp.Sub(now))
			return nil, 0
		}

		segment := heap.Pop().Segment
		return &segment, segment.StartOffset
	}

	getNext := func(reloaded bool) (*core.Segment, uint64) {
		nextOffset := checkpoint.Offset + 1

		// skip segments before next offset caused by duplicate events OR overlapping segment scenarios
		for {
			segment := heap.Peek()
			if segment == nil {
				return nil, 0
			}

			if segment.EndOffset >= nextOffset {
				break
			}

			heap.Pop()
		}

		if !heap.Peek().HasOffset(nextOffset) {
			if lateSegmentRetry == 0 {
				w.metrics.partition.segmentsLate.Mark(1)
			} else if !reloaded {
				// wait for reload response
				return nil, 0
			}

			if lateSegmentRetry < len(w.config.lateSegmentBackoff) {
				w.setTimer(w.config.lateSegmentBackoff[lateSegmentRetry])
				lateSegmentRetry++
				return nil, 0
			}

			// declare the segment lost and skip to next
			w.metrics.partition.messagesLost.Mark(int64(heap.Peek().StartOffset - nextOffset))
			w.metrics.partition.segmentsLost.Mark(1) // there could be more than one segment lost, but no way of knowing
			w.log().Warnf("Lost message range: [%d, %d]", nextOffset, heap.Peek().StartOffset-1)
		}

		if lateSegmentRetry > 0 {
			w.metrics.partition.segmentsLate.Mark(-1)
		}

		lateSegmentRetry = 0
		segment := heap.Pop().Segment
		return &segment, nextOffset
	}

	process := func(reloaded bool) {
		if waitingResult || heap.Empty() {
			return
		}

		var segment *core.Segment
		var nextOffset uint64

		if checkpoint == nil {
			segment, nextOffset = getFirst()
		} else {
			segment, nextOffset = getNext(reloaded)
		}

		if segment == nil {
			return
		}

		w.clearTimer()
		sendRequest(workerProcessRequest{
			segment:    *segment,
			nextOffset: nextOffset,
		})
	}

	sendRequest(workerInitRequest{})

LOOP:
	for {
		select {
		case msg := <-w.controlChan:
			switch cmd := msg.(type) {
			case stopCommand:
				close(stopChan)
				close(requestChan)

				if waitingResult {
					result := <-resultChan
					if r, ok := (result).(workerProcessResult); ok && r.checkpoint != nil {
						checkpoint = r.checkpoint
					}
				}

				if checkpoint != nil {
					w.log().Infof("Stopped at checkpoint %d", checkpoint.Offset)
				} else {
					w.log().Info("Stopped")
				}

				w.metrics.unregister()
				close(cmd)
				break LOOP
			default:
				w.log().Warnf("Ignored unknown control message: %+v", msg)
			}
		case event := <-w.eventsChan:
			// drop all events during init phase
			if heap == nil {
				continue
			}

			if event.Type == core.SegmentEvent_REMOVED {
				if checkpoint == nil || checkpoint.Offset < event.Segment.EndOffset {
					w.logSegment(event.Segment).Warn("Received removed event for unprocessed range")
				}
				continue
			}

			heap.Push(core.SegmentInfo{
				Segment:   event.Segment,
				Timestamp: event.Timestamp,
				Size:      event.SegmentSize,
			})

			process(false)
		case <-w.getTimerChan():
			if checkpoint == nil {
				process(false)
			} else {
				sendRequest(workerReloadRequest{
					checkpoint: checkpoint,
				})
			}
		case result := <-resultChan:
			waitingResult = false

			switch r := result.(type) {
			case workerInitResult:
				checkpoint = r.checkpoint
				heap = r.heap
				w.metrics.partition.segmentsWaiting.Mark(int64(heap.Size()))

				if checkpoint != nil {
					w.log().Infof("Resuming from checkpoint %d", checkpoint.Offset)
					w.metrics.partition.checkpoint.Update(int64(checkpoint.Offset))
				} else {
					w.log().Info("Checkpoint not found")
					w.metrics.partition.checkpoint.Update(0)
				}

				process(false)
			case workerReloadResponse:
				w.metrics.partition.segmentsWaiting.Mark(int64(-heap.Size()))
				heap = r.heap
				w.metrics.partition.segmentsWaiting.Mark(int64(heap.Size()))

				process(true)
			case workerProcessResult:
				if r.checkpoint != nil {
					checkpoint = r.checkpoint
				}

				process(false)
			default:
				w.log().Warnf("Ignored unknown workLoop result: %+v", result)
			}
		}
	}
}

func (w *worker) getTimerChan() <-chan time.Time {
	if w.timer == nil {
		return nil
	}

	return w.timer.C
}

func (w *worker) setTimer(d time.Duration) {
	if w.timer != nil {
		w.timer.Stop()
	}

	w.timer = time.NewTimer(d)
}

func (w *worker) clearTimer() {
	if w.timer == nil {
		return
	}

	w.timer.Stop()
	w.timer = nil
}

// workLoop performs the heavy lifting requests received from controlLoop method.
// For each request received it MUST emit a corresponding response even during shutdown procedure.
func (w *worker) workLoop(requestChan <-chan interface{}, resultChan chan<- interface{}, stopChan chan struct{}) {
	for request := range requestChan {
		switch r := request.(type) {
		case workerInitRequest:
			resultChan <- w.handleInitRequest(stopChan, r)
		case workerReloadRequest:
			resultChan <- w.handleReloadRequest(r)
		case workerProcessRequest:
			resultChan <- w.handleProcessRequest(stopChan, r)
		default:
			w.log().Panicf("Unknown request type %T", request)
		}
	}
}

func (w *worker) handleInitRequest(stopChan chan struct{}, request workerInitRequest) interface{} {
	delay := core.AddDefaultJitter(w.config.WorkerInitDelay)

	select {
	case <-stopChan:
		return nil
	case <-time.After(delay):
		// continue
	}

	checkpoint := w.checkpointStore.Load(w.srcRegion, w.srcTopic, w.partition)

	heap, ok := w.loadSegments(checkpoint)
	if !ok {
		return nil
	}

	return workerInitResult{
		checkpoint: checkpoint,
		heap:       heap,
	}
}

func (w *worker) handleReloadRequest(request workerReloadRequest) interface{} {
	heap, ok := w.loadSegments(request.checkpoint)
	if !ok {
		return nil
	}

	return workerReloadResponse{
		heap: heap,
	}
}

func (w *worker) handleProcessRequest(stopChan chan struct{}, request workerProcessRequest) interface{} {
	segment := request.segment
	w.logSegment(segment).Debug("Segment processing started")

	var checkpoint *core.Checkpoint
	if !w.config.SegmentStoreRetrier.Forever(w.context, func() error {
		nextOffset := request.nextOffset
		if checkpoint != nil {
			nextOffset = checkpoint.Offset + 1
		}

		reader, err := w.segmentStore.Open(w.context, segment)
		if err != nil {
			w.logSegment(segment).Errorf("Failed to open segment. Error: %+v", err)
			return err
		}
		defer reader.Close(w.context)

		for {
			select {
			case <-stopChan:
				return nil
			default:
				// continue
			}

			messages, err := reader.Read(w.context, w.config.BatchSize)
			if err != nil {
				w.logSegment(segment).Errorf("Failed to read messages. Error: %+v", err)
				return err
			}

			// sanity check
			if len(messages) == 0 {
				w.logSegment(segment).Warn("Segment read result is empty but EndOffset was not reached")
				return nil
			}

			for len(messages) > 0 && messages[0].Offset < nextOffset {
				messages = messages[1:]
			}

			if len(messages) == 0 {
				continue
			}

			if !w.produceMessages(segment, messages) {
				return nil
			}

			lastMessage := messages[len(messages)-1]
			checkpoint = &core.Checkpoint{
				Region:    w.srcRegion,
				Topic:     w.srcTopic,
				Partition: w.partition,
				Offset:    lastMessage.Offset,
				Timestamp: time.Now().UTC(),
			}

			if !w.saveCheckpoint(segment, *checkpoint) {
				return nil
			}

			if lastMessage.Offset == segment.EndOffset {
				w.metrics.partition.segmentsRead.Mark(1)
				w.logSegment(segment).Infof("Segment processed successfully")
				return nil
			}
		}
	}) {
		return nil
	}

	return workerProcessResult{
		checkpoint: checkpoint,
	}
}

func (w *worker) produceMessages(segment core.Segment, messages []core.Message) bool {
	return w.config.ProducerRetrier.Forever(w.context, func() error {
		err := w.producer.ProduceMessages(w.destTopic, w.partition, messages...)
		if err != nil {
			w.logSegment(segment).Errorf("Failed to produce messages. Error: %+v", err)
			return err
		}

		minTimestamp := messages[0].Timestamp
		for _, message := range messages {
			if message.Timestamp.Before(minTimestamp) {
				minTimestamp = message.Timestamp
			}
		}

		w.metrics.updateLag(minTimestamp)
		w.metrics.partition.messagesProduced.Mark(int64(len(messages)))
		return nil
	})
}

func (w *worker) saveCheckpoint(segment core.Segment, checkpoint core.Checkpoint) bool {
	w.log().Debugf("Saving checkpoint %d", checkpoint.Offset)

	return w.config.CheckpointStoreRetrier.Forever(w.context, func() error {
		err := w.checkpointStore.Save(checkpoint)
		if err != nil {
			w.logSegment(segment).Errorf("Failed to save checkpoint at offset %d. Error: %+v", checkpoint.Offset, err)
			return err
		}

		w.log().Debugf("Saved checkpoint %d", checkpoint.Offset)
		w.metrics.partition.checkpoint.Update(int64(checkpoint.Offset))
		return nil
	})
}

func (w *worker) loadSegments(checkpoint *core.Checkpoint) (*segmentHeap, bool) {
	var segments map[core.Segment]core.SegmentInfo
	if !w.config.SegmentStoreRetrier.Forever(w.context, func() error {
		s, err := w.segmentStore.ListSegments(w.context, w.srcRegion, w.srcTopic, w.partition)
		if err != nil {
			w.log().Errorf("Failed to get segments. Error: %+v", err)
			return err
		}

		segments = s
		return nil
	}) {
		return nil, false
	}

	heap := utils.NewSegmentHeap()
	for _, segment := range segments {
		// skip segments before last checkpoint
		if checkpoint != nil && segment.EndOffset <= checkpoint.Offset {
			continue
		}

		heap.Push(segment)
	}

	return &segmentHeap{
		SegmentHeap: heap,
		metrics:     w.metrics,
	}, true
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

func (w *worker) log() core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type":             "ingress.worker",
		"LocalRegion":      w.localRegion,
		"ID":               w.id,
		"SourceRegion":     w.srcRegion,
		"SourceTopic":      w.srcTopic,
		"DestinationTopic": w.destTopic,
		"Partition":        w.partition,
	})
}

func (w *worker) logSegment(segment core.Segment) core.Logger {
	return w.log().WithFields(core.Fields{
		"Level":       segment.Level,
		"StartOffset": segment.StartOffset,
		"EndOffset":   segment.EndOffset,
	})
}

func (h *segmentHeap) Push(segment core.SegmentInfo) {
	h.SegmentHeap.Push(segment)
	h.metrics.partition.segmentsWaiting.Mark(1)
}

func (h *segmentHeap) Pop() core.SegmentInfo {
	h.metrics.partition.segmentsWaiting.Mark(-1)
	return h.SegmentHeap.Pop()
}
