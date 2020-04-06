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
	"reflect"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/pkg/errors"
)

// LifecycleStart executes startup procedure
func LifecycleStart(logger core.Logger, instances ...interface{}) error {
	for i, instance := range instances {
		lifecycle, ok := instance.(core.Lifecycle)
		if !ok {
			continue
		}

		logger.Debugf("Starting %T", instance)

		if err := lifecycle.Start(); err != nil {
			logger.Debugf("Start failed %T", instance)

			// rollback started instances so far
			LifecycleStop(logger, instances[:i]...)

			return errors.Wrapf(err, "%T failed to start", instance)
		}

		logger.Debugf("Started %T", instance)
	}

	return nil
}

// LifecycleStop executes shutdown procedure
func LifecycleStop(logger core.Logger, instances ...interface{}) {
	for i := len(instances) - 1; i >= 0; i-- {
		instance := instances[i]
		lifecycle, ok := instance.(core.Lifecycle)
		if !ok {
			continue
		}

		logger.Debugf("Stopping %T", instance)

		lifecycle.Stop()

		logger.Debugf("Stopped %T", instance)
	}
}

// FactoryType could have been avoided if Go had generics
type FactoryType struct {
	Factory core.Factory
	Type    interface{}
}

// CallFactory invokes the list of factories and returns the obtained instances
func CallFactory(items ...FactoryType) ([]interface{}, error) {
	result := make([]interface{}, len(items))
	for i, item := range items {
		expectedType := reflect.TypeOf(item.Type).Elem()

		if item.Factory == nil {
			return nil, errors.Errorf("factory %T is nil", expectedType)
		}

		instance, err := item.Factory.Get()
		if err != nil {
			return nil, errors.Wrapf(err, "factory %T error", expectedType)
		}

		actualType := reflect.TypeOf(instance)
		if !actualType.Implements(expectedType) {
			return nil, errors.Errorf("invalid type %T provided instead of %T", actualType, expectedType)
		}

		result[i] = instance
	}

	return result, nil
}
