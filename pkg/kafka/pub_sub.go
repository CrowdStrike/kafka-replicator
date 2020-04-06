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

package kafka

import (
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

var (
	// DefaultPubSubEventsChanSize is the default size of events buffered channel.
	DefaultPubSubEventsChanSize = 100

	_ core.PubSub    = &PubSub{}
	_ core.Lifecycle = &PubSub{}
	_ core.Factory   = &PubSubConfig{}
)

// PubSubConfig is the PubSub configuration
type PubSubConfig struct {
	// The list of Kafka brokers.
	//
	// Field value is required.
	Brokers []string `required:"true"`

	// Prefix used to generate an unique consumer group identifier.
	//
	// Field value is required.
	ConsumerGroupPrefix string `required:"true"`

	// Size of events buffered channel.
	//
	// Default value is set via DefaultPubSubEventsChanSize variable.
	EventsChanSize int `min:"1"`

	// The maximum interval consumer Poll call can block
	//
	// Default value is set via DefaultPollInterval variable.
	PollInterval time.Duration `required:"false" min:"0ms"`

	// The timeout duration for producer Flush call.
	//
	// Default value is set via DefaultFlushTimeout variable.
	FlushTimeout time.Duration `required:"false" min:"0ms"`

	// Allows further configuration of underlying Kafka consumer.
	ConsumerAdditionalConfig kafka.ConfigMap

	// Allows further configuration of underlying Kafka producer.
	ProducerAdditionalConfig kafka.ConfigMap

	// Breaker enables tracking error rate.
	//
	// Default value is set via DefaultKafkaBreaker variable.
	Breaker core.Breaker
}

// PubSub implements the publish–subscribe messaging pattern using a Kafka producer and consumer pair.
type PubSub struct {
	config     PubSubConfig
	consumer   *Consumer
	producer   *Producer
	eventsChan chan kafka.Event
	stopChan   chan struct{}
}

// NewPubSub creates a new PubSub instance
func NewPubSub(config PubSubConfig) (*PubSub, error) {
	utils.SetDefaultInt(&config.EventsChanSize, DefaultPubSubEventsChanSize)
	utils.SetDefaultDuration(&config.PollInterval, DefaultPollInterval)
	utils.SetDefaultDuration(&config.FlushTimeout, DefaultFlushTimeout)
	utils.SetDefaultBreaker(&config.Breaker, core.DefaultKafkaBreaker)

	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		return nil, errors.Errorf("NewPubSub: invalid config %s", errMessage)
	}

	consumerAdditionalConfig := cloneConfigMap(config.ConsumerAdditionalConfig)
	consumerAdditionalConfig["enable.partition.eof"] = true

	consumer, err := NewConsumer(ConsumerConfig{
		Brokers:          config.Brokers,
		ConsumerGroup:    uniqueKafkaConsumerGroup(config.ConsumerGroupPrefix),
		PollInterval:     config.PollInterval,
		AdditionalConfig: consumerAdditionalConfig,
		Breaker:          config.Breaker,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "NewPubSub: failed to create Kafka consumer")
	}

	producer, err := NewProducer(ProducerConfig{
		Brokers:          config.Brokers,
		FlushTimeout:     config.FlushTimeout,
		AdditionalConfig: config.ProducerAdditionalConfig,
		Breaker:          config.Breaker,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "NewPubSub: failed to create Kafka producer")
	}

	return &PubSub{
		config:     config,
		consumer:   consumer,
		producer:   producer,
		eventsChan: make(chan kafka.Event, config.EventsChanSize),
		stopChan:   make(chan struct{}),
	}, nil
}

// Get creates the corresponding instance
func (c PubSubConfig) Get() (interface{}, error) {
	return NewPubSub(c)
}

// Start will start the consumer and producer
func (p *PubSub) Start() error {
	if err := utils.LifecycleStart(p.log(), p.producer, p.consumer); err != nil {
		return err
	}

	go p.consumerLoop()
	return nil
}

// Stop will stop the consumer and producer
func (p *PubSub) Stop() {
	close(p.stopChan)
	utils.LifecycleStop(p.log(), p.producer, p.consumer)
}

// Subscribe subscribes to the provided topic
func (p *PubSub) Subscribe(topic string) error {
	return p.consumer.Subscribe(topic)
}

// Publish will send a new messsage to specified topic
func (p *PubSub) Publish(topic string, key, value []byte) error {
	return p.producer.Produce(topic, key, value)
}

// Events is the channel of incoming events
func (p *PubSub) Events() <-chan kafka.Event {
	return p.eventsChan
}

func (p *PubSub) consumerLoop() {
	lastOffsets := map[int32]kafka.Offset{}
	initialized := false
	waitPartitionEOF := map[int32]bool{}

	sendEvent := func(event kafka.Event) bool {
		select {
		case p.eventsChan <- event:
			return true
		case <-p.stopChan:
			return false
		}
	}

LOOP:
	for {
		select {
		case <-p.stopChan:
			break LOOP
		default:
			event := p.consumer.Poll()
			if event == nil {
				continue
			}

			switch e := event.(type) {
			case *kafka.Message:
				lastOffsets[e.TopicPartition.Partition] = e.TopicPartition.Offset

				if !sendEvent(event) {
					break LOOP
				}
			case kafka.AssignedPartitions:
				assignments := make([]kafka.TopicPartition, len(e.Partitions))

				for i, partition := range e.Partitions {
					if lastOffset, ok := lastOffsets[partition.Partition]; ok {
						partition.Offset = lastOffset
					} else {
						partition.Offset = kafka.OffsetBeginning
					}

					assignments[i] = partition

					if !initialized {
						waitPartitionEOF[partition.Partition] = true
					}
				}

				if err := p.consumer.Assign(assignments); err != nil {
					p.log().Errorf("Consumer assign error: %+v", err)
					continue
				}
			case kafka.RevokedPartitions:
				if err := p.consumer.Unassign(); err != nil {
					p.log().Errorf("Consumer unassign error: %+v", err)
				}
			case kafka.PartitionEOF:
				if initialized {
					continue
				}

				delete(waitPartitionEOF, e.Partition)

				if len(waitPartitionEOF) == 0 {
					initialized = true

					eofEvent := core.TopicEOF(*e.Topic)
					if !sendEvent(eofEvent) {
						break LOOP
					}
				}
			}
		}
	}
}

func (p *PubSub) log() core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type": "kafka.PubSub",
	})
}
