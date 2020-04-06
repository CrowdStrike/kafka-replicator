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
	"fmt"
	"strings"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/pkg/errors"
)

const (
	destinationTopicSuffix = ".ingress"
)

// DefaultSourceConfig is the default ingress source configuration.
var DefaultSourceConfig = SourceConfig{
	LostSegmentTimeout: 24 * time.Hour,
	LateSegmentRetries: 16,
	FirstSegmentDelay:  30 * time.Minute,
	BatchSize:          10000,
	WorkerInitDelay:    10 * time.Second,
	WorkerChanSize:     100,
	WorkerStopTimeout:  10 * time.Second,
}

// ControllerConfig represents the ingress controller configuration
type ControllerConfig struct {
	// Consumer is used to provide group membership functionality used to distribute
	// work across multiple instances. It ensures that only one instance is allowed to
	// process a certain source topic partition at any given moment.
	Consumer core.Factory `required:"true"`

	// Producer is used for writing messages to destination topic.
	Producer core.Factory `required:"true"`

	// SegmentStore provides access to segment events and contents.
	SegmentStore core.Factory `required:"true"`

	// CheckpointStore is used for offset tracking.
	CheckpointStore core.Factory `required:"true"`

	// Unique name that identifies the local region/data center/cloud.
	//
	// Field value is required.
	LocalRegion string `required:"true"`

	// List of sources to ingress from.
	//
	// Will use DefaultSourceConfig if source config was not set.
	//
	// Field value is required.
	Sources map[Source]*SourceConfig `required:"true"`
}

// Source represents the ingress source.
type Source struct {
	// Region name.
	//
	// Field value is required.
	Region string `required:"true"`

	// Kafka topic name.
	//
	// Field value is required.
	Topic string `required:"true"`
}

// SourceConfig represents the ingress source configuration.
type SourceConfig struct {
	// Topic name where segment messages will be produced.
	//
	// Default value appends '.ingress' suffix to source topic name.
	DestinationTopic string

	// Maximum time to wait for a late segment before it is declared lost and skipped.
	//
	// WARNING: has the potential to break the at-least-once delivery promise.
	//
	// The ingress worker detects missing segments and will reload bucket state up to
	// LateSegmentRetries times using exponential backoff until LostSegmentTimeout is reached.
	//
	// Possible root causes for this scenario:
	//   - problems with cross-region AWS S3 bucket sync operation.
	//	 - problems related to AWS S3 eventual consistency model.
	//   - dropped AWS S3 notification event en-route from S3 -> SQS -> SQSSegmentEventSource -> ConsistentSegmentStore.
	//
	// The default value is currently set to 24h which should give enough time to
	// operations/SRE team to react and fix the problem.
	LostSegmentTimeout time.Duration `min:"1ms"`

	// Number of retry attempts for a late segment before it is declared lost and skipped.
	//
	// Uses exponential backoff.
	// The first computed backoff interval should be at least 1s.
	LateSegmentRetries int `min:"1" max:"50"`

	// Avoids issues related to S3 eventual consistency model.
	//
	// A higher value results in smaller chance of missing first segment.
	FirstSegmentDelay time.Duration `min:"1ms"`

	// Number of segment messages to read/produce in each request.
	//
	// A higher value usually results in better throughput.
	// A checkpoint is performed after each successful batch.
	BatchSize int `min:"1"`

	// Allows last checkpoint to propagate and avoids thundering herd effects during Kafka group rebalance.
	WorkerInitDelay time.Duration `min:"1ms"`

	// Size of ingress worker buffered channel.
	WorkerChanSize int `min:"1"`

	// Ingress worker shutdown grace period.
	WorkerStopTimeout time.Duration `min:"1ms"`

	// Retrier instance used for Producer operations
	ProducerRetrier core.Retrier

	// Retrier instance used for SegmentStore operations
	SegmentStoreRetrier core.Retrier

	// Retrier instance used for CheckpointStore operations
	CheckpointStoreRetrier core.Retrier

	lateSegmentBackoff []time.Duration // computed value
}

// New returns a new ingress controller instance
func New(config ControllerConfig) (*Controller, error) {
	config, err := validateConfig(config)
	if err != nil {
		return nil, err
	}

	instances, err := utils.CallFactory(
		utils.FactoryType{Factory: config.Consumer, Type: (*core.Consumer)(nil)},
		utils.FactoryType{Factory: config.Producer, Type: (*core.Producer)(nil)},
		utils.FactoryType{Factory: config.SegmentStore, Type: (*core.SegmentStore)(nil)},
		utils.FactoryType{Factory: config.CheckpointStore, Type: (*core.CheckpointStore)(nil)})
	if err != nil {
		return nil, err
	}

	return &Controller{
		config:          config,
		consumer:        instances[0].(core.Consumer),
		producer:        instances[1].(core.Producer),
		segmentStore:    instances[2].(core.SegmentStore),
		checkpointStore: instances[3].(core.CheckpointStore),
		nextWorkerID:    1,
		workers:         map[workerKey]*worker{},
		controlChan:     make(chan interface{}, 1),
	}, nil
}

func validateConfig(config ControllerConfig) (ControllerConfig, error) {
	var errMessages []string
	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		errMessages = append(errMessages, errMessage)
	}

	if len(config.LocalRegion) > 0 {
		for source := range config.Sources {
			if source.Region == config.LocalRegion {
				errMessages = append(errMessages, "cannot ingress from local")
			}
		}
	}

	addSourceError := func(source Source, message string) {
		errMessages = append(errMessages, fmt.Sprintf("Source (%s, %s): %s", source.Region, source.Topic, message))
	}

	sources := map[Source]*SourceConfig{}
	for source, cfg := range config.Sources {
		if errMessage := utils.Validate(source); len(errMessage) > 0 {
			addSourceError(source, errMessage)
			continue
		}

		sconfig := DefaultSourceConfig
		if cfg != nil {
			sconfig = *cfg

			utils.SetDefaultInt(&sconfig.BatchSize, DefaultSourceConfig.BatchSize)
			utils.SetDefaultDuration(&sconfig.WorkerInitDelay, DefaultSourceConfig.WorkerInitDelay)
			utils.SetDefaultInt(&sconfig.WorkerChanSize, DefaultSourceConfig.WorkerChanSize)
			utils.SetDefaultDuration(&sconfig.WorkerStopTimeout, DefaultSourceConfig.WorkerStopTimeout)
		}

		utils.SetDefaultRetrier(&sconfig.ProducerRetrier, core.DefaultKafkaRetrier)
		utils.SetDefaultRetrier(&sconfig.SegmentStoreRetrier, core.DefaultS3Retrier)
		utils.SetDefaultRetrier(&sconfig.CheckpointStoreRetrier, core.DefaultKafkaRetrier)

		if len(sconfig.DestinationTopic) == 0 {
			sconfig.DestinationTopic = source.Topic + destinationTopicSuffix
		}

		if errMessage := utils.Validate(sconfig); len(errMessage) > 0 {
			addSourceError(source, errMessage)
			continue
		}

		lateSegmentBackoff, err := utils.ExponentialBackoff(sconfig.LateSegmentRetries, sconfig.LostSegmentTimeout)
		if err != nil {
			addSourceError(source, "(LateSegmentRetries, LostSegmentTimeout) pair is invalid")
			continue
		}

		sconfig.lateSegmentBackoff = lateSegmentBackoff
		sources[source] = &sconfig
	}

	if len(errMessages) > 0 {
		return ControllerConfig{}, errors.Errorf("Invalid ingress config: %s", strings.Join(errMessages, "; "))
	}

	config.Sources = sources
	return config, nil
}
