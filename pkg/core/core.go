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

package core

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka" // leaky abstraction that works for performance reasons
)

//go:generate mockgen -source=core.go -package=tests -destination=../../tests/mocks.go
//go:generate protoc -I ./ -I ../../../../../ --gogo_out=. messages.proto

const (
	// LevelStreaming is the compaction level used for live/streaming segments.
	LevelStreaming uint32 = 0
)

// Lifecycle is the interface defining methods for instance lifecycle control.
type Lifecycle interface {
	Start() error
	Stop()
}

// Factory is used to supply the required component dependency.
type Factory interface {
	Get() (interface{}, error)
}

// Breaker implements a simple circuit breaker interface.
type Breaker interface {
	Mark()
}

// Retrier will execute the provided work function until success
// Returns true if the work was executed successfully
type Retrier interface {
	Forever(ctx context.Context, work func() error) bool
}

// SegmentStore represents the segment storage.
type SegmentStore interface {
	SegmentEventSource
	Create(ctx context.Context) (SegmentWriter, error)
	Open(ctx context.Context, segment Segment) (SegmentReader, error)
	ListSegments(ctx context.Context, region, topic string, partition uint32) (map[Segment]SegmentInfo, error)
	Delete(ctx context.Context, segment Segment) error
}

// SegmentEventSource represents source for segment events.
type SegmentEventSource interface {
	Events() <-chan SegmentEventRequest
}

// SegmentWriter represents the segment writer operations.
type SegmentWriter interface {
	Write(ctx context.Context, message Message) error
	Close(ctx context.Context, metadata SegmentMetadata) error
	Abort(ctx context.Context)
}

// SegmentReader represents the segment reader operations.
type SegmentReader interface {
	Read(ctx context.Context, count int) ([]Message, error)
	Metadata() SegmentMetadata
	Close(ctx context.Context)
}

// SegmentFormat represents the segment data serialization format.
type SegmentFormat interface {
	NewWriter(ctx context.Context, path string) (SegmentWriter, error)
	NewReader(ctx context.Context, path string) (SegmentReader, error)
}

// SegmentInfo provides contextual information for the corresponding segment.
type SegmentInfo struct {
	Segment
	Timestamp time.Time
	Size      uint64
}

// SegmentEventRequest is used to implement the chain-of-responsibility design pattern for handling segment events.
type SegmentEventRequest struct {
	SegmentEvent
	Result chan<- error
}

// Consumer represents the consumer operations.
type Consumer interface {
	Subscribe(topics ...string) error
	Poll() kafka.Event
	Assign(partitions []kafka.TopicPartition) error
	Unassign() error
	Pause(partitions []kafka.TopicPartition) error
	Seek(topic string, partition uint32, offset kafka.Offset) error
	Commit(topic string, partition uint32, offset kafka.Offset) error
}

// Producer represents the producer operations.
type Producer interface {
	Produce(topic string, key, value []byte) error
	ProduceMessages(topic string, partition uint32, messages ...Message) error
}

// PubSub represents the publish-subscribe operations.
type PubSub interface {
	Subscribe(topic string) error
	Events() <-chan kafka.Event
	Publish(topic string, key, value []byte) error
}

// TopicEOF signals that PubSub consumer reached end of topic
type TopicEOF string

// CheckpointStore represents the checkpoint storage used by ingress controller.
type CheckpointStore interface {
	Save(checkpoint Checkpoint) error
	Load(region, topic string, partition uint32) *Checkpoint
}

// Size returns the message raw bytes size.
func (r *Message) Size() uint64 {
	result := uint64(16)
	result += uint64(len(r.Key))
	result += uint64(len(r.Value))

	for _, h := range r.Headers {
		result += uint64(len(h.Key))
		result += uint64(len(h.Value))
	}

	return result
}

// HasOffset returns true if the provided offset is contained by the segment.
func (s *Segment) HasOffset(offset uint64) bool {
	return offset >= s.StartOffset && offset <= s.EndOffset
}

func (t TopicEOF) String() string {
	return fmt.Sprintf("Topic %s EOF", string(t))
}

type self func() (interface{}, error)

var _ Factory = Self(nil)

// Self represents the Factory that returns the instance when invoked.
func Self(instance interface{}) Factory {
	return self(func() (interface{}, error) {
		return instance, nil
	})
}

func (s self) Get() (interface{}, error) {
	return s()
}
