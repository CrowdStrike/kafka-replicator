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
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
)

const (
	invalidParam   = "[bug] invalid param"
	notImplemented = "[bug] not implemented"
)

var (
	required   = validator{key: "required", fn: requiredValidator}
	validators = []validator{
		required,
		{key: "min", fn: minValidator},
		{key: "max", fn: maxValidator},
	}
)

type validator struct {
	key string
	fn  validatorFn
}

type validatorFn func(v reflect.Value, params string) string

type evalContext struct {
	validator validator
	param     string
}

// Validate performs tag-based validation for the provided instance.
func Validate(instance interface{}) string {
	val := unwrap(reflect.ValueOf(instance))
	typ := val.Type()

	if typ.Kind() != reflect.Struct {
		core.DefaultLogger.Panic("The provided instance is not a struct")
	}

	var result []string

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		fieldTag := field.Tag
		if len(fieldTag) == 0 {
			continue
		}

		fieldVal := val.Field(i)
		fieldKind := fieldVal.Kind()
		if !fieldVal.CanInterface() || fieldKind == reflect.Invalid {
			continue
		}

		var toEval []evalContext
		var requiredCtx *evalContext

		for _, v := range validators {
			if param, found := fieldTag.Lookup(v.key); found {
				ctx := evalContext{validator: v, param: param}

				if v.key == required.key {
					requiredCtx = &ctx
				} else {
					toEval = append(toEval, ctx)
				}
			}
		}

		if len(toEval) == 0 && requiredCtx == nil {
			continue
		}

		if requiredCtx == nil {
			requiredCtx = &evalContext{validator: required, param: "true"}
		}

		var errors []string
		eval := func(ctx evalContext) bool {
			if err := ctx.validator.fn(fieldVal, ctx.param); len(err) > 0 {
				errors = append(errors, err)
				return false
			}
			return true
		}

		if eval(*requiredCtx) {
			for _, ctx := range toEval {
				eval(ctx)
			}
		}

		if len(errors) > 0 {
			result = append(result, fmt.Sprintf("%s: %s", field.Name, strings.Join(errors, ", ")))
		}
	}

	return strings.Join(result, "; ")
}

func requiredValidator(v reflect.Value, param string) string {
	if param != "true" {
		return ""
	}

	v = unwrap(v)

	switch {
	case canLen(v) && v.Len() == 0:
		return "is empty"
	case isNumeric(v) && isZero(v):
		return "is zero"
	case canIsNil(v) && v.IsNil():
		return "is nil"
	case v.Kind() == reflect.Struct && isZero(v):
		return "not set"
	}

	return ""
}

func minValidator(v reflect.Value, param string) string {
	v = unwrap(v)

	switch n := v.Interface().(type) {
	case int:
		if max, err := strconv.ParseInt(param, 10, 0); err != nil {
			return invalidParam
		} else if n < int(max) {
			return fmt.Sprintf("is less than %d", max)
		}
	case uint64:
		if max, err := strconv.ParseUint(param, 10, 64); err != nil {
			return invalidParam
		} else if n < uint64(max) {
			return fmt.Sprintf("is less than %d", max)
		}
	case time.Duration:
		if max, err := time.ParseDuration(param); err != nil {
			return invalidParam
		} else if n < time.Duration(max) {
			return fmt.Sprintf("is less than %s", max)
		}
	default:
		return notImplemented
	}

	return ""
}

func maxValidator(v reflect.Value, param string) string {
	v = unwrap(v)

	switch n := v.Interface().(type) {
	case int:
		if max, err := strconv.ParseInt(param, 10, 0); err != nil {
			return invalidParam
		} else if n > int(max) {
			return fmt.Sprintf("is greater than %d", max)
		}
	case time.Duration:
		if max, err := time.ParseDuration(param); err != nil {
			return invalidParam
		} else if n > time.Duration(max) {
			return fmt.Sprintf("is greater than %s", max)
		}
	default:
		return notImplemented
	}

	return ""
}

func canIsNil(v reflect.Value) bool {
	k := v.Kind()
	return k == reflect.Chan || k == reflect.Func || k == reflect.Map || k == reflect.Ptr ||
		k == reflect.UnsafePointer || k == reflect.Interface || k == reflect.Slice
}

func canLen(v reflect.Value) bool {
	k := v.Kind()
	return k == reflect.Array || k == reflect.Map || k == reflect.Slice || k == reflect.String
}

func isNumeric(v reflect.Value) bool {
	k := v.Kind()
	return k == reflect.Int || k == reflect.Int8 || k == reflect.Int16 || k == reflect.Int32 || k == reflect.Int64 ||
		k == reflect.Uint || k == reflect.Uint8 || k == reflect.Uint16 || k == reflect.Uint32 || k == reflect.Uint64 ||
		k == reflect.Float32 || k == reflect.Float64 || k == reflect.Complex64 || k == reflect.Complex128
}

func isZero(v reflect.Value) bool {
	zero := reflect.Zero(v.Type()).Interface()
	return reflect.DeepEqual(v.Interface(), zero)
}

func unwrap(v reflect.Value) reflect.Value {
	k := v.Kind()
	if (k == reflect.Ptr || k == reflect.Interface) && !v.IsNil() {
		return v.Elem()
	}

	return v
}
