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
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/gofrs/uuid"
)

// DelayEvent is a helper Kafka event used during tests
type DelayEvent time.Duration

// IsCanceledError returns true if the provided value is a cancellation error
func IsCanceledError(err error) bool {
	if err == nil {
		return false
	}

	if err == context.Canceled {
		return true
	}

	awsErr, ok := err.(awserr.Error)
	if !ok {
		return false
	}

	return awsErr.Code() == request.CanceledErrorCode
}

// FormatMetricName returns the metric name with reserved chars removed
func FormatMetricName(str string) string {
	return strings.ReplaceAll(str, ".", "-")
}

// NewUUID returns a randomly generated UUID
func NewUUID() uuid.UUID {
	return uuid.Must(uuid.NewV4())
}

// GetLocalIPv4 will return this hosts IPv4 address.
// It will not return the loopback address and it will panic if no address is found.
func GetLocalIPv4() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		core.DefaultLogger.Panicf("Failed to fetch net interfaces. Error: %+v", err)
	}

	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			ip := ipnet.IP.To4()
			if len(ip) > 0 {
				return ipnet.IP.String()
			}
		}
	}

	core.DefaultLogger.Panic("Host IPv4 address could not be determined.")
	panic("unreachable")
}

// SetDefaultString sets the default value for provided location
func SetDefaultString(dest *string, defaultValue string) {
	if len(*dest) == 0 {
		*dest = defaultValue
	}
}

// SetDefaultInt sets the default value for provided location
func SetDefaultInt(dest *int, defaultValue int) {
	if *dest == 0 {
		*dest = defaultValue
	}
}

// SetDefaultUint64 sets the default value for provided location
func SetDefaultUint64(dest *uint64, defaultValue uint64) {
	if *dest == 0 {
		*dest = defaultValue
	}
}

// SetDefaultDuration sets the default value for provided location
func SetDefaultDuration(dest *time.Duration, defaultValue time.Duration) {
	if *dest == 0 {
		*dest = defaultValue
	}
}

// SetDefaultBreaker sets the default value for provided location
func SetDefaultBreaker(dest *core.Breaker, defaultValue core.Breaker) {
	if *dest == nil {
		*dest = defaultValue
	}
}

// SetDefaultRetrier sets the default value for provided location
func SetDefaultRetrier(dest *core.Retrier, defaultValue core.Retrier) {
	if *dest == nil {
		*dest = defaultValue
	}
}

func (d DelayEvent) String() string {
	return fmt.Sprintf("DelayEvent: %s", time.Duration(d))
}

// Delay blocks for the specified duration
func (d DelayEvent) Delay() {
	<-time.After(time.Duration(d))
}
