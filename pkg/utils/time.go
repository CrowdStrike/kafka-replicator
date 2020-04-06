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

package utils

import (
	"errors"
	"time"
)

// ExponentialBackoff computes the exponential backoff intervals summing to provided total duration
func ExponentialBackoff(retries int, total time.Duration) ([]time.Duration, error) {
	count := (uint64(1) << uint64(retries)) - 1
	next := total / time.Duration(count)
	if next <= 0 {
		return nil, errors.New("invalid parameters")
	}

	sum := time.Duration(0)
	result := make([]time.Duration, retries)
	for i := 0; i < retries-1; i++ {
		result[i] = next
		sum += next
		next *= 2
	}

	result[retries-1] = total - sum
	return result, nil
}
