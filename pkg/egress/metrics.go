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

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
)

type partitionMetrics struct {
	messagesConsumed core.Meter
	messagesWritten  core.Meter
	messagesRewinded core.Meter
	segmentsWritten  core.Meter
	segmentsRewinded core.Meter
	names            []string
}

type workerMetrics struct {
	partition *partitionMetrics
}

func newWorkerMetrics(region, topic string, partition uint32) *workerMetrics {
	region = utils.FormatMetricName(region)
	topic = utils.FormatMetricName(topic)

	return &workerMetrics{
		partition: newPartitionMetrics(region, topic, partition),
	}
}

func newPartitionMetrics(region, topic string, partition uint32) *partitionMetrics {
	var names []string
	getName := func(pattern string) string {
		name := fmt.Sprintf(pattern, region, topic, partition)
		names = append(names, name)
		return name
	}

	return &partitionMetrics{
		messagesConsumed: core.DefaultMetrics.GetMeter(getName("egress.%s.%s.%d.messages.consumed")),
		messagesWritten:  core.DefaultMetrics.GetMeter(getName("egress.%s.%s.%d.messages.written")),
		messagesRewinded: core.DefaultMetrics.GetMeter(getName("egress.%s.%s.%d.messages.rewinded")),
		segmentsWritten:  core.DefaultMetrics.GetMeter(getName("egress.%s.%s.%d.segments.written")),
		segmentsRewinded: core.DefaultMetrics.GetMeter(getName("egress.%s.%s.%d.segments.rewinded")),
		names:            names,
	}
}

func (m *workerMetrics) unregister() {
	for _, name := range m.partition.names {
		core.DefaultMetrics.Remove(name)
	}
}
