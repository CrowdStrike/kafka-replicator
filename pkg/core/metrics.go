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

package core

import (
	"time"

	"github.com/rcrowley/go-metrics"
)

// DefaultMetrics is the metrics instance used by the replicator
var DefaultMetrics = NewMetrics(metrics.DefaultRegistry)

// Metrics is the provider for various metric types
type Metrics interface {
	GetMeter(name string) Meter
	GetTimer(name string) Timer
	GetGauge(name string) Gauge
	Remove(name string)
}

// Meter is the interface for a meter metric
type Meter interface {
	Mark(int64)
}

// Timer is the interface for a timer metric
type Timer interface {
	UpdateSince(time.Time)
}

// Gauge is the interface for a gauge metric
type Gauge interface {
	Update(int64)
}

type defaultMetrics struct {
	registry metrics.Registry
}

// NewMetrics returns a new metrics instance that uses 'github.com/rcrowley/go-metrics' under the covers
func NewMetrics(registry metrics.Registry) Metrics {
	return &defaultMetrics{registry: registry}
}

func (m *defaultMetrics) GetMeter(name string) Meter {
	return metrics.GetOrRegisterMeter(name, m.registry)
}

func (m *defaultMetrics) GetTimer(name string) Timer {
	return metrics.GetOrRegisterTimer(name, m.registry)
}

func (m *defaultMetrics) GetGauge(name string) Gauge {
	return metrics.GetOrRegisterGauge(name, m.registry)
}

func (m *defaultMetrics) Remove(name string) {
	m.registry.Unregister(name)
}

type nilMetrics struct {
	meter *nilMeter
	timer *nilTimer
	gauge *nilGauge
}

type nilMeter struct{}
type nilTimer struct{}
type nilGauge struct{}

// NewNullMetrics returns the metrics instance used to disable all metrics collection
func NewNullMetrics() Metrics {
	return &nilMetrics{
		meter: &nilMeter{},
		timer: &nilTimer{},
		gauge: &nilGauge{},
	}
}

func (m *nilMetrics) GetMeter(name string) Meter { return m.meter }
func (m *nilMetrics) GetTimer(name string) Timer { return m.timer }
func (m *nilMetrics) GetGauge(name string) Gauge { return m.gauge }
func (m *nilMetrics) Remove(name string)         {}
func (m *nilMeter) Mark(int64)                   {}
func (t *nilTimer) UpdateSince(time.Time)        {}
func (t *nilGauge) Update(int64)                 {}
