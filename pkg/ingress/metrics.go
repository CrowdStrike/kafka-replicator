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
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
)

type regionMetrics struct {
	lag core.Timer
}

type topicMetrics struct {
	lag core.Timer
}

type partitionMetrics struct {
	segmentsWaiting  core.Meter
	segmentsLate     core.Meter
	segmentsLost     core.Meter
	segmentsRead     core.Meter
	messagesLost     core.Meter
	messagesProduced core.Meter
	lag              core.Timer
	checkpoint       core.Gauge
	names            []string
}

type workerMetrics struct {
	region    *regionMetrics
	topic     *topicMetrics
	partition *partitionMetrics
}

func newWorkerMetrics(region, topic string, partition uint32) *workerMetrics {
	region = utils.FormatMetricName(region)
	topic = utils.FormatMetricName(topic)

	return &workerMetrics{
		region: &regionMetrics{
			lag: core.DefaultMetrics.GetTimer(fmt.Sprintf("ingress.%s.lag", region)),
		},
		topic: &topicMetrics{
			lag: core.DefaultMetrics.GetTimer(fmt.Sprintf("ingress.%s.%s.lag", region, topic)),
		},
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
		segmentsWaiting:  core.DefaultMetrics.GetMeter(getName("ingress.%s.%s.%d.segments.waiting")),
		segmentsLate:     core.DefaultMetrics.GetMeter(getName("ingress.%s.%s.%d.segments.late")),
		segmentsLost:     core.DefaultMetrics.GetMeter(getName("ingress.%s.%s.%d.segments.lost")),
		segmentsRead:     core.DefaultMetrics.GetMeter(getName("ingress.%s.%s.%d.segments.read")),
		messagesLost:     core.DefaultMetrics.GetMeter(getName("ingress.%s.%s.%d.messages.lost")),
		messagesProduced: core.DefaultMetrics.GetMeter(getName("ingress.%s.%s.%d.messages.produced")),
		checkpoint:       core.DefaultMetrics.GetGauge(getName("ingress.%s.%s.%d.checkpoint")),
		lag:              core.DefaultMetrics.GetTimer(getName("ingress.%s.%s.%d.lag")),
		names:            names,
	}
}

func (m *workerMetrics) updateLag(since time.Time) {
	m.region.lag.UpdateSince(since)
	m.topic.lag.UpdateSince(since)
	m.partition.lag.UpdateSince(since)
}

func (m *workerMetrics) unregister() {
	// region and topic metrics are not unregistered as they are shared by workers
	for _, name := range m.partition.names {
		core.DefaultMetrics.Remove(name)
	}
}
