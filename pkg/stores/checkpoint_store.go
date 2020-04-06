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

package stores

import (
	"fmt"
	"sync"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

var (
	checkpointStartTimeTimer        = core.DefaultMetrics.GetTimer("checkpoint.start.time")
	checkpointSaveCallsMeter        = core.DefaultMetrics.GetMeter("checkpoint.save.calls")
	checkpointMessagesConsumedMeter = core.DefaultMetrics.GetMeter("checkpoint.messages.consumed")

	_ core.CheckpointStore = &CheckpointStore{}
	_ core.Lifecycle       = &CheckpointStore{}
	_ core.Factory         = &CheckpointStoreConfig{}
)

// CheckpointStoreConfig is the checkpoint store configuration
type CheckpointStoreConfig struct {
	// PubSub is used:
	//   - store ingress checkpoints, and
	//   - distribute the checkpoints to all running instances.
	PubSub core.Factory `required:"true"`

	// Unique name that identifies the local region/data center/cloud.
	//
	// Field value is required.
	LocalRegion string `required:"true"`

	// Kafka topic name for ingress checkpoints.
	//
	// The topic cleanup policy needs to be set to 'compacted' to retain
	// only the last checkpoint for each <region, topic, partition> tuple.
	// This avoids unecessary disk space consumption and improved startup time.
	//
	// Field value is required.
	Topic string `required:"true"`
}

// CheckpointStore represents the checkpoint store used by ingress controller
type CheckpointStore struct {
	config   CheckpointStoreConfig
	pubsub   core.PubSub
	stopChan chan struct{}
	mutex    sync.RWMutex
	state    map[checkpointKey]*core.Checkpoint
}

type checkpointKey struct {
	Region    string
	Topic     string
	Partition uint32
}

// NewCheckpointStore returns a new CheckpointStore instance
func NewCheckpointStore(config CheckpointStoreConfig) (*CheckpointStore, error) {
	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		return nil, errors.Errorf("NewCheckpointStore: invalid config %s", errMessage)
	}

	instances, err := utils.CallFactory(
		utils.FactoryType{Factory: config.PubSub, Type: (*core.PubSub)(nil)})
	if err != nil {
		return nil, err
	}

	return &CheckpointStore{
		config:   config,
		pubsub:   instances[0].(core.PubSub),
		stopChan: make(chan struct{}),
		state:    map[checkpointKey]*core.Checkpoint{},
	}, nil
}

// Get creates the corresponding instance
func (c CheckpointStoreConfig) Get() (interface{}, error) {
	return NewCheckpointStore(c)
}

// Start will start updating store state
func (r *CheckpointStore) Start() error {
	if err := utils.LifecycleStart(r.log(), r.pubsub); err != nil {
		return err
	}

	if err := r.pubsub.Subscribe(r.config.Topic); err != nil {
		return err
	}

	startupDone := make(chan struct{})
	startTime := time.Now()

	go r.consumerLoop(startupDone)

	<-startupDone
	checkpointStartTimeTimer.UpdateSince(startTime)

	return nil
}

// Stop will stop updating store state
func (r *CheckpointStore) Stop() {
	close(r.stopChan)

	utils.LifecycleStop(r.log(), r.pubsub)
}

// Load returns the last saved checkpoint
func (r *CheckpointStore) Load(region, topic string, partition uint32) *core.Checkpoint {
	if region == r.config.LocalRegion {
		return nil
	}

	r.mutex.RLock()

	key := checkpointKey{
		Region:    region,
		Topic:     topic,
		Partition: partition,
	}

	result := r.state[key]

	r.mutex.RUnlock()
	return result
}

// Save persists the checkpoint
func (r *CheckpointStore) Save(checkpoint core.Checkpoint) error {
	checkpointSaveCallsMeter.Mark(1)

	if checkpoint.Region == r.config.LocalRegion {
		return errors.New("cannot save checkpoint for local region")
	}

	if !r.updateState(checkpoint) {
		return nil
	}

	key := r.formatMessageKey(checkpoint)
	value, err := r.marshalCheckpoint(checkpoint)
	if err != nil {
		return err
	}

	return r.pubsub.Publish(r.config.Topic, key, value)
}

func (r *CheckpointStore) consumerLoop(startupDone chan struct{}) {
	initialized := false

LOOP:
	for {
		select {
		case <-r.stopChan:
			break LOOP
		case event := <-r.pubsub.Events():
			switch e := event.(type) {
			case *kafka.Message:
				checkpointMessagesConsumedMeter.Mark(1)

				if !r.sanityChecks(e.TopicPartition) {
					continue
				}

				// handle log-compacted topic tombstones
				if len(e.Value) == 0 {
					continue
				}

				checkpoint, err := r.unmarshalCheckpoint(e.Value)
				if err != nil {
					r.log().Errorf("Parse checkpoint failed at partition %d, offset %d. Error: %+v", e.TopicPartition.Partition, e.TopicPartition.Offset, err)
					continue
				}

				if checkpoint.Region == r.config.LocalRegion {
					r.log().Errorf("Received checkpoint for local region at partition %d, offset %d", e.TopicPartition.Partition, e.TopicPartition.Offset)
					continue
				}

				r.updateState(*checkpoint)
			case core.TopicEOF:
				close(startupDone)
				initialized = true
			default:
				r.log().Warnf("Ignored unknown consumer event: %+v", event)
			}
		}
	}

	// if stopped before initialization is done
	if !initialized {
		close(startupDone)
	}
}

func (r *CheckpointStore) updateState(next core.Checkpoint) bool {
	r.mutex.Lock()

	key := checkpointKey{
		Region:    next.Region,
		Topic:     next.Topic,
		Partition: next.Partition,
	}

	current := r.state[key]
	if current != nil && current.Offset >= next.Offset {
		r.mutex.Unlock()
		return false
	}

	r.state[key] = &next
	r.mutex.Unlock()
	return true
}

func (r *CheckpointStore) sanityChecks(tp kafka.TopicPartition) bool {
	if tp.Topic == nil {
		r.log().Warn("Ignored message with missing topic")
		return false
	}

	if *tp.Topic != r.config.Topic {
		r.log().Warnf("Ignored message for unknown topic %s", *tp.Topic)
		return false
	}

	return true
}

func (r *CheckpointStore) formatMessageKey(c core.Checkpoint) []byte {
	return []byte(fmt.Sprintf("%s:%s:%d", c.Region, c.Topic, c.Partition))
}

func (r *CheckpointStore) marshalCheckpoint(checkpoint core.Checkpoint) ([]byte, error) {
	bytes, err := proto.Marshal(&checkpoint)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal checkpoint")
	}

	return bytes, nil
}

func (r *CheckpointStore) unmarshalCheckpoint(bytes []byte) (*core.Checkpoint, error) {
	var result core.Checkpoint
	if err := proto.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

func (r *CheckpointStore) log() core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type":        "CheckpointStore",
		"LocalRegion": r.config.LocalRegion,
	})
}
