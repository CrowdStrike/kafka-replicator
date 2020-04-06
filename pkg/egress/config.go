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

package egress

import (
	"fmt"
	"strings"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/pkg/errors"
)

// DefaultTopicConfig is the default egress topic configuration.
var DefaultTopicConfig = TopicConfig{
	MaxSegmentMessages: 1000000,
	MaxSegmentSize:     100 * 1024 * 1024, // 100 MiB
	MaxSegmentAge:      5 * time.Minute,
	WorkerChanSize:     10000,
	WorkerStopTimeout:  10 * time.Second,
}

// ControllerConfig represents the egress controller configuration
type ControllerConfig struct {
	// Consumer is used to consume topic messages.
	Consumer core.Factory `required:"true"`

	// SegmentStore is used to write segments.
	SegmentStore core.Factory `required:"true"`

	// Unique name that identifies the local region/data center/cloud.
	//
	// Field value is required.
	LocalRegion string `required:"true"`

	// Source Kafka topic names that will be consumed and written to segment store.
	//
	// Will use DefaultTopicConfig if topic config was not set.
	//
	// Field value is required.
	Topics map[string]*TopicConfig `required:"true"`
}

// TopicConfig represents the egress topic configuration.
type TopicConfig struct {
	// Maximum number of messages written to a segment.
	MaxSegmentMessages int `min:"2"`

	// Maximum byte size of messages contained in a segment.
	//
	// The value is the raw Kafka message size before compression
	// and other possible encoding performed by the formatter.
	MaxSegmentSize uint64 `min:"1"`

	// Maximum duration from segment creation to completion.
	//
	// A higher value results in fewer and larger segments being created
	// at the expense of increased message delivery latency.
	MaxSegmentAge time.Duration `min:"1ms"`

	// Size of egress worker buffered channel.
	WorkerChanSize int `min:"1"`

	// Egress worker shutdown grace period.
	WorkerStopTimeout time.Duration `min:"1ms"`

	// Retrier instance used for Consumer operations
	ConsumerRetrier core.Retrier

	// Retrier instance used for SegmentStore operations
	SegmentStoreRetrier core.Retrier
}

// New returns a new egress controller instance
func New(config ControllerConfig) (*Controller, error) {
	config, err := validateConfig(config)
	if err != nil {
		return nil, err
	}

	instances, err := utils.CallFactory(
		utils.FactoryType{Factory: config.Consumer, Type: (*core.Consumer)(nil)},
		utils.FactoryType{Factory: config.SegmentStore, Type: (*core.SegmentStore)(nil)})
	if err != nil {
		return nil, err
	}

	return &Controller{
		config:       config,
		consumer:     instances[0].(core.Consumer),
		segmentStore: instances[1].(core.SegmentStore),
		nextWorkerID: 1,
		workers:      map[workerKey]*worker{},
		controlChan:  make(chan interface{}, 1),
	}, nil
}

func validateConfig(config ControllerConfig) (ControllerConfig, error) {
	var errMessages []string
	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		errMessages = append(errMessages, errMessage)
	}

	addTopicError := func(topic, message string) {
		errMessages = append(errMessages, fmt.Sprintf("Topic %s: %s", topic, message))
	}

	topics := map[string]*TopicConfig{}
	for topic, cfg := range config.Topics {
		if len(topic) == 0 {
			errMessages = append(errMessages, "Empty topic name")
			continue
		}

		tconfig := DefaultTopicConfig
		if cfg != nil {
			tconfig = *cfg

			utils.SetDefaultInt(&tconfig.WorkerChanSize, DefaultTopicConfig.WorkerChanSize)
			utils.SetDefaultDuration(&tconfig.WorkerStopTimeout, DefaultTopicConfig.WorkerStopTimeout)
		}

		utils.SetDefaultRetrier(&tconfig.ConsumerRetrier, core.DefaultKafkaRetrier)
		utils.SetDefaultRetrier(&tconfig.SegmentStoreRetrier, core.DefaultS3Retrier)

		if errMessage := utils.Validate(tconfig); len(errMessage) > 0 {
			addTopicError(topic, errMessage)
			continue
		}

		topics[topic] = &tconfig
	}

	if len(errMessages) > 0 {
		return ControllerConfig{}, errors.Errorf("Invalid egress config: %s", strings.Join(errMessages, "; "))
	}

	config.Topics = topics
	return config, nil
}
