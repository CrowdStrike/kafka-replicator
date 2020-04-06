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
	"sync"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

var (
	// DefaultFlushTimeout is the default timeout duration for producer Flush call.
	DefaultFlushTimeout = 5 * time.Second

	kafkaProducerErrorsMeter = core.DefaultMetrics.GetMeter("kafka.producer.errors")

	_ core.Producer  = &Producer{}
	_ core.Lifecycle = &Producer{}
	_ core.Factory   = &ProducerConfig{}
)

// ProducerConfig is the Kafka producer configuration
type ProducerConfig struct {
	// The list of Kafka brokers.
	//
	// Field value is required.
	Brokers []string `required:"true"`

	// Maximum number of messages in one borker request.
	//
	// Default value is set by underlying Kafka producer.
	BatchSize int

	// Delay in milliseconds to wait for messages in the producer queue to accumulate before
	// constructing message batches to transmit to brokers. A higher value allows larger and
	// more effective (less overhead, improved compression) batches of messages to accumulate
	// at the expense of increased message delivery latency.
	//
	// Default value is set by underlying Kafka producer.
	BatchDelay time.Duration

	// The timeout duration for producer Flush call.
	//
	// Default value is set via DefaultFlushTimeout variable.
	FlushTimeout time.Duration `required:"false" min:"0ms"`

	// Allows further configuration of underlying Kafka producer.
	//
	// For advanced producer configuration options, check librdkafka docs:
	// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	AdditionalConfig kafka.ConfigMap

	// Breaker enables tracking producer error rate.
	//
	// Default value is set via DefaultKafkaBreaker variable.
	Breaker core.Breaker
}

// Producer is the Kafka producer
type Producer struct {
	config         ProducerConfig
	flushTimeoutMs int
	producer       *kafka.Producer
}

type completionHandler func(err error)

// NewProducer creates a new Producer instance
func NewProducer(config ProducerConfig) (*Producer, error) {
	utils.SetDefaultDuration(&config.FlushTimeout, DefaultFlushTimeout)
	utils.SetDefaultBreaker(&config.Breaker, core.DefaultKafkaBreaker)

	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		return nil, errors.Errorf("NewProducer: invalid config %s", errMessage)
	}

	return &Producer{
		config:         config,
		flushTimeoutMs: int(config.FlushTimeout / time.Millisecond),
	}, nil
}

// Get creates the corresponding instance
func (c ProducerConfig) Get() (interface{}, error) {
	return NewProducer(c)
}

// Start will start the producer
func (p *Producer) Start() error {
	configMap := kafka.ConfigMap{
		"partitioner":          "consistent_random",
		"enable.idempotence":   true,
		"log.connection.close": false,
	}

	for key, value := range p.config.AdditionalConfig {
		configMap[key] = value
	}

	configMap["bootstrap.servers"] = strings.Join(p.config.Brokers, ",")

	if p.config.BatchSize > 0 {
		configMap["batch.num.messages"] = p.config.BatchSize
	}

	if p.config.BatchDelay > 0 {
		configMap["queue.buffering.max.ms"] = int(p.config.BatchDelay / time.Millisecond)
	}

	producer, err := kafka.NewProducer(&configMap)
	if err != nil {
		return errors.Wrapf(err, "Producer: failed to create Kafka producer")
	}

	p.producer = producer
	go p.handleDeliveryCompletion()
	return nil
}

// Stop will stop the producer
func (p *Producer) Stop() {
	p.producer.Flush(p.flushTimeoutMs)
	p.producer.Close()
}

// Produce will send a new messsage to specified topic
func (p *Producer) Produce(topic string, key, value []byte) error {
	return p.produceSync(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   key,
		Value: value,
	})
}

// ProduceMessages will write the messsages to Kafka
func (p *Producer) ProduceMessages(topic string, partition uint32, messages ...core.Message) error {
	stopChan := make(chan struct{})

	wg := &sync.WaitGroup{}
	wg.Add(1)

	var firstErr error
	handler := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
			close(stopChan)
		}

		wg.Done()
	}

	go func() {
		defer wg.Done()

		for _, message := range messages {
			msg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: int32(partition),
				},
				Key:    message.Key,
				Value:  message.Value,
				Opaque: completionHandler(handler),
			}

			if len(message.Headers) > 0 {
				msg.Headers = make([]kafka.Header, len(message.Headers))
				for i, header := range message.Headers {
					msg.Headers[i] = kafka.Header{
						Key:   header.Key,
						Value: header.Value,
					}
				}
			}

			if !message.Timestamp.IsZero() {
				msg.TimestampType = kafka.TimestampCreateTime
				msg.Timestamp = message.Timestamp
			}

			select {
			case p.producer.ProduceChannel() <- msg:
				wg.Add(1)
			case <-stopChan:
				return
			}
		}
	}()

	wg.Wait()

	if firstErr != nil {
		p.markError()
		return errors.Wrapf(firstErr, "Producer: produce messages failed on topic %s", topic)
	}

	return nil
}

func (p *Producer) produceSync(msg *kafka.Message) error {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	var produceErr error
	handler := func(err error) {
		produceErr = err
		wg.Done()
	}

	msg.Opaque = completionHandler(handler)

	if err := p.producer.Produce(msg, nil); err != nil {
		p.markError()
		return errors.Wrapf(err, "Producer: produce returned error on topic %s", *msg.TopicPartition.Topic)
	}

	wg.Wait()
	if produceErr != nil {
		p.markError()
		return errors.Wrapf(produceErr, "Producer: produce failed on topic %s", *msg.TopicPartition.Topic)
	}

	return nil
}

func (p *Producer) handleDeliveryCompletion() {
	for msg := range p.producer.Events() {
		switch e := msg.(type) {
		case *kafka.Message:
			handler, ok := e.Opaque.(completionHandler)
			if !ok {
				continue
			}

			handler(e.TopicPartition.Error)
		default:
			// skip
		}
	}
}

func (p *Producer) markError() {
	kafkaProducerErrorsMeter.Mark(1)
	p.config.Breaker.Mark()
}
