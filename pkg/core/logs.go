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
	"fmt"

	"github.com/sirupsen/logrus"
)

// DefaultLogger is the logger used by the replicator
var DefaultLogger = NewNullLogger()

// Fields type is used with WithFields method
type Fields map[string]interface{}

// Logger provides basic logging functionality
type Logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Panic(args ...interface{})
	Panicf(format string, args ...interface{})
	WithFields(fields Fields) Logger
}

type defaultLogger struct {
	logger logrus.FieldLogger
}

// NewLogger returns a new logger instance that uses 'github.com/sirupsen/logrus' under the covers
func NewLogger(logger *logrus.Logger) Logger {
	return &defaultLogger{logger: logger}
}

func (l *defaultLogger) Debug(args ...interface{})                 { l.logger.Debug(args...) }
func (l *defaultLogger) Debugf(format string, args ...interface{}) { l.logger.Debugf(format, args...) }
func (l *defaultLogger) Info(args ...interface{})                  { l.logger.Info(args...) }
func (l *defaultLogger) Infof(format string, args ...interface{})  { l.logger.Infof(format, args...) }
func (l *defaultLogger) Warn(args ...interface{})                  { l.logger.Warn(args...) }
func (l *defaultLogger) Warnf(format string, args ...interface{})  { l.logger.Warnf(format, args...) }
func (l *defaultLogger) Error(args ...interface{})                 { l.logger.Error(args...) }
func (l *defaultLogger) Errorf(format string, args ...interface{}) { l.logger.Errorf(format, args...) }
func (l *defaultLogger) Panic(args ...interface{})                 { l.logger.Panic(args...) }
func (l *defaultLogger) Panicf(format string, args ...interface{}) { l.logger.Panicf(format, args...) }
func (l *defaultLogger) WithFields(fields Fields) Logger {
	entry := l.logger.WithFields(logrus.Fields(fields))
	return &defaultLogger{logger: entry}
}

type nilLogger struct{}

// NewNullLogger returns the logger instance used to disable all logging
func NewNullLogger() Logger {
	return &nilLogger{}
}

func (l *nilLogger) Debug(args ...interface{})                 {}
func (l *nilLogger) Debugf(format string, args ...interface{}) {}
func (l *nilLogger) Info(args ...interface{})                  {}
func (l *nilLogger) Infof(format string, args ...interface{})  {}
func (l *nilLogger) Warn(args ...interface{})                  {}
func (l *nilLogger) Warnf(format string, args ...interface{})  {}
func (l *nilLogger) Error(args ...interface{})                 {}
func (l *nilLogger) Errorf(format string, args ...interface{}) {}
func (l *nilLogger) Panic(args ...interface{})                 { panic(fmt.Sprint(args...)) }
func (l *nilLogger) Panicf(format string, args ...interface{}) { panic(fmt.Sprintf(format, args...)) }
func (l *nilLogger) WithFields(fields Fields) Logger           { return l }
