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
	"strings"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

var (
	// DefaultPartitionAssignmentStrategy is the default partition assignment strategy used by group members.
	DefaultPartitionAssignmentStrategy = "roundrobin,range"

	// DefaultClientRack is the default client rack identifier. Usually set to region name.
	DefaultClientRack = ""

	// DefaultPollInterval is the default maximum interval Poll call can block.
	DefaultPollInterval = 100 * time.Millisecond

	kafkaConsumerErrorsMeter = core.DefaultMetrics.GetMeter("kafka.consumer.errors")

	_ core.Consumer  = &Consumer{}
	_ core.Lifecycle = &Consumer{}
	_ core.Factory   = &ConsumerConfig{}
)

// ConsumerConfig is the Kafka consumer configuration
type ConsumerConfig struct {
	// The list of Kafka brokers.
	//
	// Field value is required.
	Brokers []string `required:"true"`

	// Client group identifier. All clients sharing the same id belong to the same group.
	//
	// Field value is required.
	ConsumerGroup string `required:"true"`

	// The partition assignment strategy used by group members.
	//
	// Default value is set via DefaultPartitionAssignmentStrategy variable.
	PartitionAssignmentStrategy string

	// The maximum interval Poll call can block
	//
	// Default value is set via DefaultPollInterval variable.
	PollInterval time.Duration `required:"false" min:"0ms"`

	// A rack identifier for the client.
	//
	// Default value is set via DefaultClientRack variable.
	ClientRack string

	// Allows further configuration of underlying Kafka consumer.
	//
	// For advanced consumer configuration options, check librdkafka docs:
	// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	AdditionalConfig kafka.ConfigMap

	// Breaker enables tracking consumer error rate.
	//
	// Default value is set via DefaultKafkaBreaker variable.
	Breaker core.Breaker
}

// Consumer is the Kafka consumer
type Consumer struct {
	config         ConsumerConfig
	pollIntervalMs int
	consumer       *kafka.Consumer
}

// NewConsumer creates a new Consumer instance
func NewConsumer(config ConsumerConfig) (*Consumer, error) {
	utils.SetDefaultString(&config.PartitionAssignmentStrategy, DefaultPartitionAssignmentStrategy)
	utils.SetDefaultDuration(&config.PollInterval, DefaultPollInterval)
	utils.SetDefaultString(&config.ClientRack, DefaultClientRack)
	utils.SetDefaultBreaker(&config.Breaker, core.DefaultKafkaBreaker)

	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		return nil, errors.Errorf("NewConsumer: invalid config %s", errMessage)
	}

	return &Consumer{
		config:         config,
		pollIntervalMs: int(config.PollInterval / time.Millisecond),
	}, nil
}

// Get creates and returns the corresponding instance
func (c ConsumerConfig) Get() (interface{}, error) {
	return NewConsumer(c)
}

// Start will start the consumer
func (c *Consumer) Start() error {
	configMap := kafka.ConfigMap{
		"go.application.rebalance.enable": true,
		"go.events.channel.enable":        false,
		"auto.offset.reset":               "earliest",
		"enable.auto.commit":              false,
		"enable.auto.offset.store":        false,
		"session.timeout.ms":              30000,
		"log.connection.close":            false,
	}

	for key, value := range c.config.AdditionalConfig {
		configMap[key] = value
	}

	configMap["bootstrap.servers"] = strings.Join(c.config.Brokers, ",")
	configMap["group.id"] = c.config.ConsumerGroup
	configMap["partition.assignment.strategy"] = c.config.PartitionAssignmentStrategy

	if len(c.config.ClientRack) > 0 {
		configMap["client.rack"] = c.config.ClientRack
	}

	consumer, err := kafka.NewConsumer(&configMap)
	if err != nil {
		return errors.Wrapf(err, "Consumer: failed to create Kafka consumer")
	}

	c.consumer = consumer
	return nil
}

// Stop will stop the consumer
func (c *Consumer) Stop() {
	err := c.consumer.Close()

	c.handleError(err)

	if err != nil {
		c.log().Warnf("Close error: %+v", err)
	}
}

// Subscribe subscribes to the provided list of topics
func (c *Consumer) Subscribe(topics ...string) error {
	err := c.consumer.SubscribeTopics(topics, nil)

	c.handleError(err)
	return err
}

// Poll the consumer for messages or events
func (c *Consumer) Poll() kafka.Event {
	event := c.consumer.Poll(c.pollIntervalMs)
	if event == nil {
		return nil
	}

	switch e := event.(type) {
	case kafka.Error:
		// https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#threads-and-callbacks
		// "These errors are usually of an informational nature, i.e., failure to connect to a broker, and the application usually does not need to take any action.
		// An application typically does not have to perform any action when an error is raised through the error callback, the client will automatically
		// try to recover from all errors, given that the client and cluster is correctly configured."
		c.log().Infof("Poll error: %+v", e)
	case *kafka.Message, kafka.AssignedPartitions, kafka.RevokedPartitions, kafka.PartitionEOF, kafka.OffsetsCommitted:
		// pass
	default:
		c.log().Warnf("Unexpected event type: %T", event)
	}

	return event
}

// Assign the set of partitions to consume
func (c *Consumer) Assign(partitions []kafka.TopicPartition) error {
	err := c.consumer.Assign(partitions)

	c.handleError(err)
	return err
}

// Unassign the current set of partitions to consume
func (c *Consumer) Unassign() error {
	err := c.consumer.Unassign()

	c.handleError(err)
	return err
}

// Pause consumption for the provided list of partitions
func (c *Consumer) Pause(partitions []kafka.TopicPartition) error {
	err := c.consumer.Pause(partitions)

	c.handleError(err)
	return err
}

// Seek seeks the given topic partition
func (c *Consumer) Seek(topic string, partition uint32, offset kafka.Offset) error {
	err := c.consumer.Seek(kafka.TopicPartition{
		Topic:     &topic,
		Partition: int32(partition),
		Offset:    offset,
	}, 0)

	c.handleError(err)
	return err
}

// Commit commits the offset for the provided topic partition
func (c *Consumer) Commit(topic string, partition uint32, offset kafka.Offset) error {
	_, err := c.consumer.CommitOffsets([]kafka.TopicPartition{
		{
			Topic:     &topic,
			Partition: int32(partition),
			Offset:    offset + 1,
		}})

	c.handleError(err)
	return err
}

func (c *Consumer) handleError(err error) {
	if err == nil {
		return
	}

	kafkaConsumerErrorsMeter.Mark(1)
	c.config.Breaker.Mark()
}

func (c *Consumer) log() core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type": "kafka.Consumer",
	})
}
