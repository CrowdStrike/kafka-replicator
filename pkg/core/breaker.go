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

	"golang.org/x/time/rate"
)

var (
	// DefaultKafkaBreaker is the default breaker used to track Kafka errors.
	DefaultKafkaBreaker Breaker = NewThresholdBreaker(100, time.Minute, getDefaultAction("Kafka"))

	// DefaultS3Breaker is the default breaker used to track AWS S3 errors.
	DefaultS3Breaker Breaker = NewThresholdBreaker(10, time.Minute, getDefaultAction("S3"))

	// DefaultSQSBreaker is the default breaker used to track AWS S3 errors.
	DefaultSQSBreaker Breaker = NewThresholdBreaker(20, time.Minute, getDefaultAction("SQS"))
)

// ThresholdBreaker implements a simple circuit breaker that triggers when the error rate reaches the threshold.
type ThresholdBreaker struct {
	limiter *rate.Limiter
	action  func()
}

// NewThresholdBreaker returns a new ThresholdBreaker instance that allows errors up to threshold inside the interval.
// When the threshold is reached, the provided action will be invoked.
func NewThresholdBreaker(threshold int, interval time.Duration, action func()) *ThresholdBreaker {
	perSecond := rate.Limit(float64(threshold) / interval.Seconds())

	return &ThresholdBreaker{
		limiter: rate.NewLimiter(perSecond, threshold),
		action:  action,
	}
}

// Mark increments the internal error counter.
func (b *ThresholdBreaker) Mark() {
	if b.limiter.Allow() {
		return
	}

	b.action()
}

func getDefaultAction(label string) func() {
	return func() {
		DefaultLogger.Warnf("% breaker was triggered", label)
	}
}
