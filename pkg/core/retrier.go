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
	"context"
	"math/rand"
	"time"
)

var (
	// DefaultJitter is the default jitter percentage
	DefaultJitter = 0.25

	// DefaultKafkaRetrier is the default retrier used for Kafka operations
	DefaultKafkaRetrier Retrier = NewExponentialRetrier(100*time.Millisecond, 1*time.Second, DefaultJitter)

	// DefaultS3Retrier is the default retrier used for AWS S3 operations
	DefaultS3Retrier Retrier = NewExponentialRetrier(200*time.Millisecond, 5*time.Second, DefaultJitter)
)

// ExponentialRetrier implements the retry with exponential backoff resiliency pattern
type ExponentialRetrier struct {
	min    time.Duration
	max    time.Duration
	jitter float64
}

// NewExponentialRetrier returns a new ExponentialRetrier instance
func NewExponentialRetrier(min, max time.Duration, jitter float64) *ExponentialRetrier {
	if min > max {
		panic("invalid min/max arguments")
	}

	return &ExponentialRetrier{
		min:    min,
		max:    max,
		jitter: jitter,
	}
}

// Forever will execute the provided work function until success
// Returns true if the work was executed successfully
func (r ExponentialRetrier) Forever(ctx context.Context, work func() error) bool {
	if err := ctx.Err(); err != nil {
		return false
	}

	exp := time.Duration(1)
	for {
		err := work()
		if err == nil {
			return true
		}

		if err := ctx.Err(); err != nil {
			return false
		}

		delay := r.min * exp
		if delay > r.max {
			delay = r.max
		} else {
			exp *= 2
		}

		delay = AddJitter(delay, r.jitter)

		select {
		case <-ctx.Done():
			return false
		case <-time.After(delay):
			// continue
		}
	}
}

// AddJitter adds random jitter in the range (-jitter, +jitter) to provided duration
func AddJitter(d time.Duration, jitter float64) time.Duration {
	return time.Duration(((rand.Float64() * 2) - 1) * jitter * float64(d))
}

// AddDefaultJitter adds default jitter to provided duration
func AddDefaultJitter(d time.Duration) time.Duration {
	return AddJitter(d, DefaultJitter)
}
