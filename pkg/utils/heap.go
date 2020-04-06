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

package utils

import (
	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/emirpasic/gods/trees/binaryheap"
)

// SegmentHeap is the heap of segments ordered by offset
type SegmentHeap binaryheap.Heap

// NewSegmentHeap returns a new SegmentHeap instance
func NewSegmentHeap() *SegmentHeap {
	heap := binaryheap.NewWith(segmentComparator)
	return (*SegmentHeap)(heap)
}

// Push adds a segment to the heap
func (h *SegmentHeap) Push(segment core.SegmentInfo) {
	h.convert().Push(segment)
}

// Peek returns top segment without removing it, or nil if heap is empty.
func (h *SegmentHeap) Peek() *core.SegmentInfo {
	tmp, ok := h.convert().Peek()
	if !ok {
		return nil
	}

	segment := tmp.(core.SegmentInfo)
	return &segment
}

// Pop removes top segment and returns it. It panics if the heap is empty.
func (h *SegmentHeap) Pop() core.SegmentInfo {
	tmp, ok := h.convert().Pop()
	if !ok {
		panic("segment heap is empty")
	}

	return tmp.(core.SegmentInfo)
}

// Size returns number of segments in the heap.
func (h *SegmentHeap) Size() int {
	return h.convert().Size()
}

// Empty returns true if heap does not contain any segments.
func (h *SegmentHeap) Empty() bool {
	return h.convert().Empty()
}

func (h *SegmentHeap) convert() *binaryheap.Heap {
	return (*binaryheap.Heap)(h)
}

func segmentComparator(a, b interface{}) int {
	s1 := a.(core.SegmentInfo)
	s2 := b.(core.SegmentInfo)

	switch {
	case s1.StartOffset < s2.StartOffset:
		return -1
	case s1.StartOffset > s2.StartOffset:
		return 1
	default:
		// sort longer segment first
		switch {
		case s1.EndOffset > s2.EndOffset:
			return -1
		case s1.EndOffset < s2.EndOffset:
			return 1
		default:
			return 0
		}
	}
}
