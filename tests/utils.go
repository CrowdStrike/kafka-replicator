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
	"reflect"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/onsi/ginkgo"
)

const (
	waitDuration     = 5 * time.Millisecond
	stopWaitDuration = 10 * waitDuration
)

type eventSourceMockRecorder interface {
	Events() *gomock.Call
}

func lastCall(calls []*gomock.Call) *gomock.Call {
	if len(calls) == 0 {
		return nil
	}

	return calls[len(calls)-1]
}

func lastSegment(segments []core.Segment) core.Segment {
	if len(segments) == 0 {
		ginkgo.Fail("empty segment slice")
	}

	return segments[len(segments)-1]
}

func expectEvents(consumer *MockConsumer, eventSource eventSourceMockRecorder, events ...interface{}) {
	var calls []*gomock.Call

	addSegmentEventRequest := func(request core.SegmentEventRequest) {
		if eventSource == nil {
			ginkgo.Fail("unexpected SegmentEvent event")
		}

		eventsChan := make(chan core.SegmentEventRequest, 1)
		eventsChan <- request

		calls = append(calls, eventSource.Events().Return(eventsChan))
	}

	for _, event := range events {
		switch e := event.(type) {
		case core.SegmentEventRequest:
			addSegmentEventRequest(e)
		case core.SegmentEvent:
			request := newSegmentEventRequest(e)
			addSegmentEventRequest(request)
		case kafka.Event:
			if eventSource != nil {
				calls = append(calls, eventSource.Events().Return(nil))
			}

			calls = append(calls, consumer.EXPECT().Poll().Return(event))
		default:
			ginkgo.Fail(fmt.Sprintf("unknown event type %T", e))
		}
	}

	gomock.InOrder(calls...)
	expectPollForever(consumer, lastCall(calls))

	if eventSource != nil {
		expectSegmentEventsForever(eventSource, lastCall(calls))
	}
}

func expectPollForever(mock *MockConsumer, after *gomock.Call) {
	// Will return an infinite sequence of nil events after the expected call
	call := mock.EXPECT().Poll().AnyTimes().
		DoAndReturn(func() kafka.Event {
			<-time.After(waitDuration)
			return nil
		})

	if after != nil {
		call.After(after)
	}
}

func expectSegmentEventsForever(eventSource eventSourceMockRecorder, after *gomock.Call) {
	call := eventSource.Events().AnyTimes().Return(nil)

	if after != nil {
		call.After(after)
	}
}

func expectPubSubForever(mock *MockPubSub, after *gomock.Call) {
	call := mock.EXPECT().Events().AnyTimes().Return(nil)

	if after != nil {
		call.After(after)
	}
}

func newKafkaMessage(topic string, partition uint32, offset kafka.Offset, seed int) *kafka.Message {
	result := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: int32(partition),
			Offset:    offset,
		},
		Key:       []byte(fmt.Sprintf("key_%d", seed)),
		Value:     []byte(fmt.Sprintf("value_QnVpbHQgYnkgQm9nZGFuLUNpcHJpYW4gUnVzdSAoZ2l0aHViLmNvbS9iY3J1c3UpIGluIDIwMTkg_%d", seed)),
		Timestamp: time.Unix(0, int64(1553000000000+seed)),
		Headers: []kafka.Header{
			{
				Key:   fmt.Sprintf("header_key_%d", seed),
				Value: []byte(fmt.Sprintf("header_value_IGFuZCB0aGUgd29yayB3YXMgZnVuIDop_%d", seed)),
			},
		},
	}

	if seed%2 == 0 {
		result.TimestampType = kafka.TimestampCreateTime
	} else {
		result.TimestampType = kafka.TimestampLogAppendTime
	}

	return result
}

func newAssignedPartitions(partitions ...kafka.TopicPartition) kafka.AssignedPartitions {
	return kafka.AssignedPartitions{
		Partitions: partitions,
	}
}

func newRevokedPartitions(partitions ...kafka.TopicPartition) kafka.RevokedPartitions {
	return kafka.RevokedPartitions{
		Partitions: partitions,
	}
}

func newTopicPartition(topic string, partition uint32, offset kafka.Offset) kafka.TopicPartition {
	return kafka.TopicPartition{
		Topic:     &topic,
		Partition: int32(partition),
		Offset:    offset,
	}
}

func newPartitionEOF(topic string, partition uint32, offset kafka.Offset) kafka.PartitionEOF {
	return kafka.PartitionEOF{
		Topic:     &topic,
		Partition: int32(partition),
		Offset:    offset,
	}
}

func newTopicEOF(topic string) core.TopicEOF {
	return core.TopicEOF(topic)
}

func newDelay(d time.Duration) utils.DelayEvent {
	return utils.DelayEvent(d)
}

// newMessage returns the corresponding segment message for the message in newKafkaMessage method
func newMessage(offset uint64, seed int) core.Message {
	return core.Message{
		Key:       []byte(fmt.Sprintf("key_%d", seed)),
		Value:     []byte(fmt.Sprintf("value_QnVpbHQgYnkgQm9nZGFuLUNpcHJpYW4gUnVzdSAoZ2l0aHViLmNvbS9iY3J1c3UpIGluIDIwMTkg_%d", seed)),
		Offset:    offset,
		Timestamp: time.Unix(0, int64(1553000000000+seed)),
		Headers: []*core.Message_Header{
			{
				Key:   fmt.Sprintf("header_key_%d", seed),
				Value: []byte(fmt.Sprintf("header_value_IGFuZCB0aGUgd29yayB3YXMgZnVuIDop_%d", seed)),
			},
		},
	}
}

func newSegmentEventRequest(event core.SegmentEvent) core.SegmentEventRequest {
	return core.SegmentEventRequest{
		SegmentEvent: event,
		Result:       make(chan error),
	}
}

func newRetrier() core.Retrier {
	return core.NewExponentialRetrier(time.Microsecond, time.Microsecond, 0.1)
}

type messageMatcher struct {
	message core.Message
}

func matchMessage(message core.Message) *messageMatcher {
	return &messageMatcher{message: message}
}

func (m *messageMatcher) Matches(actual interface{}) bool {
	switch x := actual.(type) {
	case core.Message:
		return reflect.DeepEqual(x, m.message)
	default:
		return false
	}
}

func (m *messageMatcher) String() string {
	return "message matcher"
}

type segmentMetadataMatcher struct {
	metadata core.SegmentMetadata
}

func matchSegmentMetadata(metadata core.SegmentMetadata) *segmentMetadataMatcher {
	return &segmentMetadataMatcher{metadata: metadata}
}

func (m *segmentMetadataMatcher) Matches(bytes interface{}) bool {
	switch x := bytes.(type) {
	case core.SegmentMetadata:
		return x.Region == m.metadata.Region &&
			x.Topic == m.metadata.Topic &&
			x.Partition == m.metadata.Partition &&
			x.Level == m.metadata.Level &&
			x.StartOffset == m.metadata.StartOffset &&
			x.EndOffset == m.metadata.EndOffset &&
			x.MessageCount == m.metadata.MessageCount &&
			!x.CreatedTimestamp.IsZero()
	default:
		return false
	}
}

func (m *segmentMetadataMatcher) String() string {
	return "segment metadata matcher"
}

type checkpointMatcher struct {
	checkpoint core.Checkpoint
}

func matchCheckpoint(checkpoint core.Checkpoint) *checkpointMatcher {
	return &checkpointMatcher{checkpoint: checkpoint}
}

func (m *checkpointMatcher) Matches(bytes interface{}) bool {
	switch x := bytes.(type) {
	case core.Checkpoint:
		return x.Region == m.checkpoint.Region &&
			x.Topic == m.checkpoint.Topic &&
			x.Partition == m.checkpoint.Partition &&
			x.Offset == m.checkpoint.Offset &&
			!x.Timestamp.IsZero()
	default:
		return false
	}
}

func (m *checkpointMatcher) String() string {
	return "checkpoint matcher"
}

type checkpointBytesMatcher struct {
	checkpoint core.Checkpoint
}

func matchCheckpointBytes(checkpoint core.Checkpoint) *checkpointBytesMatcher {
	return &checkpointBytesMatcher{checkpoint: checkpoint}
}

func (m *checkpointBytesMatcher) Matches(bytes interface{}) bool {
	switch x := bytes.(type) {
	case []byte:
		var actual core.Checkpoint
		if err := proto.Unmarshal(x, &actual); err != nil {
			return false
		}

		return reflect.DeepEqual(actual, m.checkpoint)
	default:
		return false
	}
}

func (m *checkpointBytesMatcher) String() string {
	return "checkpoint bytes matcher"
}

type segmentEventBytesMatcher struct {
	event core.SegmentEvent
}

func matchSegmentEventBytes(event core.SegmentEvent) *segmentEventBytesMatcher {
	return &segmentEventBytesMatcher{event: event}
}

func (m *segmentEventBytesMatcher) Matches(bytes interface{}) bool {
	switch x := bytes.(type) {
	case []byte:
		var actual core.SegmentEvent
		if err := proto.Unmarshal(x, &actual); err != nil {
			return false
		}

		return actual == m.event
	default:
		return false
	}
}

func (m *segmentEventBytesMatcher) String() string {
	return "segment event bytes matcher"
}

// sameMatcher matches all values to first seen value
type sameMatcher struct {
	firstValue interface{}
}

func same() *sameMatcher {
	return &sameMatcher{}
}

func (m *sameMatcher) Matches(value interface{}) bool {
	if m.firstValue == nil {
		m.firstValue = value
		return true
	}

	return reflect.DeepEqual(m.firstValue, value)
}

func (m *sameMatcher) String() string {
	return "repeat matcher"
}

type predicateMatcher struct {
	predicate func(interface{}) bool
}

func predicate(p func(interface{}) bool) *predicateMatcher {
	return &predicateMatcher{predicate: p}
}

func (m *predicateMatcher) Matches(value interface{}) bool {
	return m.predicate(value)
}

func (m *predicateMatcher) String() string {
	return "action matcher"
}
