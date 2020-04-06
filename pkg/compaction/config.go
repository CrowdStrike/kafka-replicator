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

package compaction

import (
	"fmt"
	"strings"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/pkg/errors"
	"github.com/robfig/cron"
)

// DefaultConfig is the default compaction configuration.
var DefaultConfig = Config{
	MinLevel:        core.LevelStreaming,
	MaxLevel:        core.LevelStreaming,
	MinSegmentAge:   time.Hour,
	MinSegmentCount: 10,
	MinSegmentSize:  1 * 1024 * 1024 * 1024, // 1 GiB
	MaxSegmentCount: 10000,
	MaxSegmentSize:  4 * 1024 * 1024 * 1024, // 4 GiB
	BatchSize:       10000,
	Delete:          true,
}

// ControllerConfig represents the compaction controller configuration
type ControllerConfig struct {
	// Consumer is used to provide group membership functionality used to distribute
	// work across multiple instances. It ensures that only one instance is allowed to
	// process a certain source topic partition at any given moment.
	Consumer core.Factory `required:"true"`

	// SegmentStore provides access to segment contents.
	SegmentStore core.Factory `required:"true"`

	// Unique name that identifies the local region/data center/cloud.
	//
	// Field value is required.
	LocalRegion string `required:"true"`

	// Source Kafka topic names that will be compacted.
	//
	// Will use DefaultConfig if topic config was not set.
	//
	// Field value is required.
	Topics map[string]*Config `required:"true"`

	// Cron expression that determines compaction schedule.
	//
	// If not set, automatic compaction will not be executed and
	// is required to call Compact method to trigger the operation.
	CronSchedule string

	// Time zone location used for cron schedule
	//
	// Default is the system's local time zone.
	CronLocation *time.Location

	// Maximum number of compactions running simultaneously.
	Parallelism int `min:"1"`
}

// Config represents the compaction configuration.
type Config struct {
	// Minimum level of segments to include in compaction.
	MinLevel uint32

	// Maximum level of segments to include in compaction.
	MaxLevel uint32

	// Minimum age a segment must be in order to be considered for compaction.
	//
	// A higher value increases the chance segment was replicated to all
	// destinations and combats issues related to S3 eventual consistency model.
	MinSegmentAge time.Duration `required:"false" min:"0ms"`

	// Minimum number of segments required for compaction to run.
	MinSegmentCount int `min:"2"`

	// Minimum byte size of segments required for compaction to run.
	MinSegmentSize uint64 `min:"1"`

	// Maximum number of segments compacted in one run.
	MaxSegmentCount int `min:"2"`

	// Maximum byte size of segments compacted in one run.
	MaxSegmentSize uint64 `min:"1"`

	// Number of segment messages to read/write in each request
	//
	// A higher value usually results in better throughput.
	BatchSize int `min:"1"`

	// Allows to disable the removal of compacted segments.
	//
	// In normal operation, it does not make sense to keep around the compacted
	// segments, but it can be useful during troubleshooting.
	Delete bool
}

// New returns a new compaction controller instance
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

	segmentStore := instances[1].(core.SegmentStore)

	result := &Controller{
		config:       config,
		consumer:     instances[0].(core.Consumer),
		segmentStore: segmentStore,
		compactor:    &Compactor{segmentStore},
		controlChan:  make(chan interface{}, 1),
	}

	if len(config.CronSchedule) > 0 {
		location := time.Local
		if config.CronLocation != nil {
			location = config.CronLocation
		}

		result.cron = cron.NewWithLocation(location)
		if err := result.cron.AddFunc(config.CronSchedule, result.onCronSchedule); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func validateConfig(config ControllerConfig) (ControllerConfig, error) {
	var errMessages []string
	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		errMessages = append(errMessages, errMessage)
	}

	if len(config.CronSchedule) > 0 {
		if _, err := cron.Parse(config.CronSchedule); err != nil {
			errMessages = append(errMessages, "CronSchedule: invalid value")
		}
	}

	addTopicError := func(topic, message string) {
		errMessages = append(errMessages, fmt.Sprintf("Topic %s: %s", topic, message))
	}

	topics := map[string]*Config{}
	for topic, cfg := range config.Topics {
		if len(topic) == 0 {
			errMessages = append(errMessages, "Empty topic name")
			continue
		}

		tconfig := DefaultConfig
		if cfg != nil {
			tconfig = *cfg
		}

		if errMessage := utils.Validate(tconfig); len(errMessage) > 0 {
			addTopicError(topic, errMessage)
			continue
		}

		if tconfig.MaxSegmentCount < tconfig.MinSegmentCount {
			addTopicError(topic, "(MinSegmentCount, MaxSegmentCount) pair is invalid")
			continue
		}

		if tconfig.MaxSegmentSize < tconfig.MinSegmentSize {
			addTopicError(topic, "(MinSegmentSize, MaxSegmentSize) pair is invalid")
			continue
		}

		topics[topic] = &tconfig
	}

	if len(errMessages) > 0 {
		return ControllerConfig{}, errors.Errorf("Invalid compaction config: %s", strings.Join(errMessages, "; "))
	}

	config.Topics = topics
	return config, nil
}
