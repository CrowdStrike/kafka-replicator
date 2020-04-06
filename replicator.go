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

package replicator

import (
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/compaction"
	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/egress"
	"github.com/CrowdStrike/kafka-replicator/pkg/ingress"
	"github.com/CrowdStrike/kafka-replicator/pkg/kafka"
	"github.com/CrowdStrike/kafka-replicator/pkg/stores"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/pkg/errors"
)

var (
	// DefaultEgressKafkaConsumerGroup is the default Kafka group name used by egress controller.
	DefaultEgressKafkaConsumerGroup = "replicator.egress"

	// DefaultIngressKafkaConsumerGroup is the default Kafka group name used by ingress controller.
	DefaultIngressKafkaConsumerGroup = "replicator.ingress"

	// DefaultCompactionKafkaConsumerGroup is the default Kafka group name used by compaction controller.
	DefaultCompactionKafkaConsumerGroup = "replicator.compaction"
)

// EgressConfig represents the basic egresss configuration.
type EgressConfig struct {
	// Unique name that identifies the local region/data center/cloud.
	//
	// Field value is required.
	LocalRegion string `required:"true"`

	// The AWS config object used to create AWS service clients.
	//
	// Field value is required.
	AWSConfig *aws.Config `required:"true"`

	// The AWS session object used to create AWS service clients.
	//
	// Field value is required.
	AWSSession *session.Session `required:"true"`

	// Bucket name to store segments.
	//
	// Field value is required.
	S3Bucket string `required:"true"`

	// Key prefix for segment objects.
	//
	// Default value is set via DefaultS3KeyPrefix variable.
	S3KeyPrefix string

	// The segment file format used to write segments.
	SegmentFormat core.SegmentFormat `required:"true"`

	// The list of Kafka brokers.
	//
	// Field value is required.
	KafkaBrokers []string `required:"true"`

	// Source Kafka topic names that will be consumed and written to segment store.
	//
	// Will use DefaultTopicConfig if topic config was not set.
	//
	// Field value is required.
	KafkaTopics map[string]*egress.TopicConfig `required:"true"`

	// Kafka consumer group identifier.
	//
	// Default value is set via DefaultEgressKafkaConsumerGroup variable.
	KafkaConsumerGroup string
}

// NewEgress creates a basic egress controller.
func NewEgress(config EgressConfig) (*egress.Controller, error) {
	utils.SetDefaultString(&config.KafkaConsumerGroup, DefaultEgressKafkaConsumerGroup)

	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		return nil, errors.Errorf("Invalid egress config: %s", errMessage)
	}

	return egress.New(egress.ControllerConfig{
		Consumer: kafka.ConsumerConfig{
			Brokers:       config.KafkaBrokers,
			ConsumerGroup: config.KafkaConsumerGroup,
		},
		SegmentStore: stores.S3SegmentStoreConfig{
			SegmentFormat: core.Self(config.SegmentFormat),
			AWSSession:    config.AWSSession,
			AWSConfig:     config.AWSConfig,
			Bucket:        config.S3Bucket,
			KeyPrefix:     config.S3KeyPrefix,
		},
		LocalRegion: config.LocalRegion,
		Topics:      config.KafkaTopics,
	})
}

// IngressConfig represents the basic ingress configuration.
type IngressConfig struct {
	// Unique name that identifies the local region/data center/cloud.
	//
	// Field value is required.
	LocalRegion string `required:"true"`

	// List of sources to ingress from.
	//
	// Will use DefaultSourceConfig if source config was not set.
	//
	// Field value is required.
	Sources map[ingress.Source]*ingress.SourceConfig `required:"true"`

	// The AWS session object used to create AWS service clients.
	//
	// Field value is required.
	AWSSession *session.Session `required:"true"`

	// The AWS config object used to create AWS service clients.
	//
	// Field value is required.
	AWSConfig *aws.Config `required:"true"`

	// Bucket name to store segments.
	//
	// Field value is required.
	S3Bucket string `required:"true"`

	// Key prefix for segment objects.
	//
	// Default value is set via DefaultS3KeyPrefix variable.
	S3KeyPrefix string

	// The segment file format used to read segments.
	SegmentFormat core.SegmentFormat `required:"true"`

	// AWS SQS queue name where AWS S3 notification events are published.
	//
	// The implementation expects that both Created and Removed event types to be enabled
	// for keys storing segments (i.e. the keys with DataKeyPrefix).
	//
	// Check the AWS S3 documentation for instructions on how to enable event notifications:
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
	//
	// Field value is required.
	SQSQueueName string `required:"true"`

	// The list of Kafka brokers.
	//
	// Field value is required.
	KafkaBrokers []string `required:"true"`

	// Kafka consumer group identifier.
	//
	// Used for main Kafka consumer, the checkpoint store and consistent
	// segment store PubSubs consumers.
	//
	// Default value is set via DefaultIngressKafkaConsumerGroup variable.
	KafkaConsumerGroup string

	// Kafka topic name for used by consistent segment store PubSub client.
	//
	// The topic cleanup policy needs to be set to 'delete' with appropriate
	// retention time set to discard old segment events.
	KafkaSegmentEventsTopic string `required:"true"`

	// The duration each segment event is tracked.
	//
	// Should match the configured Kafka topic retention time.
	//
	// Field value is required.
	KafkaSegmentEventsRetention time.Duration `required:"true"`

	// Kafka topic name for used by checkpoint store PubSub client.
	//
	// The topic cleanup policy needs to be set to 'compacted' to retain
	// only the last checkpoint for each <region, topic, partition> tuple.
	// This avoids unnecessary disk space consumption and improved startup time.
	KafkaCheckpointTopic string `required:"true"`

	// Kafka producer message batch size.
	//
	// Higher values improve throughput while lower values improve latency.
	//
	// Default value is set by underlying Kafka producer.
	KafkaProducerBatchSize int

	// Kafka producer message batch delay.
	//
	// Higher values improve throughput while lower values improve latency.
	//
	// Default value is set by underlying Kafka producer.
	KafkaProducerBatchDelay time.Duration
}

// NewIngress creates a basic ingress controller.
func NewIngress(config IngressConfig) (*ingress.Controller, error) {
	utils.SetDefaultString(&config.KafkaConsumerGroup, DefaultIngressKafkaConsumerGroup)

	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		return nil, errors.Errorf("Invalid ingress config: %s", errMessage)
	}

	return ingress.New(ingress.ControllerConfig{
		Consumer: kafka.ConsumerConfig{
			Brokers:       config.KafkaBrokers,
			ConsumerGroup: config.KafkaConsumerGroup,
		},
		Producer: kafka.ProducerConfig{
			Brokers:    config.KafkaBrokers,
			BatchSize:  config.KafkaProducerBatchSize,
			BatchDelay: config.KafkaProducerBatchDelay,
		},
		SegmentStore: stores.ConsistentSegmentStoreConfig{
			PubSub: kafka.PubSubConfig{
				Brokers:             config.KafkaBrokers,
				ConsumerGroupPrefix: config.KafkaConsumerGroup + ".segmentevents",
			},
			SegmentStore: stores.S3SegmentStoreConfig{
				SegmentFormat: core.Self(config.SegmentFormat),
				AWSSession:    config.AWSSession,
				AWSConfig:     config.AWSConfig,
				Bucket:        config.S3Bucket,
				KeyPrefix:     config.S3KeyPrefix,
			},
			SegmentEventSource: stores.SQSSegmentEventSourceConfig{
				AWSSession:  config.AWSSession,
				AWSConfig:   config.AWSConfig,
				QueueName:   config.SQSQueueName,
				S3KeyPrefix: config.S3KeyPrefix,
			},
			Topic:           config.KafkaSegmentEventsTopic,
			EventsRetention: config.KafkaSegmentEventsRetention,
		},
		CheckpointStore: stores.CheckpointStoreConfig{
			PubSub: kafka.PubSubConfig{
				Brokers:             config.KafkaBrokers,
				ConsumerGroupPrefix: config.KafkaConsumerGroup + ".checkpoint",
			},
			LocalRegion: config.LocalRegion,
			Topic:       config.KafkaCheckpointTopic,
		},
		LocalRegion: config.LocalRegion,
		Sources:     config.Sources,
	})
}

// CompactionConfig represents the compaction configuration.
type CompactionConfig struct {
	// Unique name that identifies the local region/data center/cloud.
	//
	// Field value is required.
	LocalRegion string `required:"true"`

	// The AWS session object used to create AWS service clients.
	//
	// Field value is required.
	AWSSession *session.Session `required:"true"`

	// The AWS config object used to create AWS service clients.
	//
	// Field value is required.
	AWSConfig *aws.Config `required:"true"`

	// Bucket name to store segments.
	//
	// Field value is required.
	S3Bucket string `required:"true"`

	// Key prefix for segment objects.
	//
	// Default value is set via DefaultS3KeyPrefix variable.
	S3KeyPrefix string

	// The segment file format used to write segments.
	SegmentFormat core.SegmentFormat `required:"true"`

	// The list of Kafka brokers.
	//
	// Field value is required.
	KafkaBrokers []string `required:"true"`

	// Source Kafka topic names that will be compacted.
	//
	// Will use DefaultConfig if topic config was not set.
	//
	// Field value is required.
	Topics map[string]*compaction.Config `required:"true"`

	// Kafka consumer group identifier.
	//
	// Default value is set via DefaultCompactionKafkaConsumerGroup variable.
	KafkaConsumerGroup string

	// Cron expression that determines compaction schedule.
	//
	// If not set, automatic compaction will not be executed and
	// is required to call Compact method to trigger the operation.
	CronSchedule string

	// Time zone location used for cron schedule
	//
	// Default value is UTC.
	CronLocation *time.Location

	// Maximum number of compactions running simultaneously.
	Parallelism int `min:"1"`
}

// NewCompaction creates a basic compaction controller.
func NewCompaction(config CompactionConfig) (*compaction.Controller, error) {
	utils.SetDefaultString(&config.KafkaConsumerGroup, DefaultCompactionKafkaConsumerGroup)

	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		return nil, errors.Errorf("Invalid compaction config: %s", errMessage)
	}

	return compaction.New(compaction.ControllerConfig{
		Consumer: kafka.ConsumerConfig{
			Brokers:       config.KafkaBrokers,
			ConsumerGroup: config.KafkaConsumerGroup,
		},
		SegmentStore: stores.S3SegmentStoreConfig{
			SegmentFormat: core.Self(config.SegmentFormat),
			AWSSession:    config.AWSSession,
			AWSConfig:     config.AWSConfig,
			Bucket:        config.S3Bucket,
			KeyPrefix:     config.S3KeyPrefix,
		},
		LocalRegion:  config.LocalRegion,
		Topics:       config.Topics,
		CronSchedule: config.CronSchedule,
		CronLocation: config.CronLocation,
		Parallelism:  config.Parallelism,
	})
}
