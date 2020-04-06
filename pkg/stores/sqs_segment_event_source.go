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

package stores

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
)

const (
	s3EventSource            = "aws:s3"
	s3EventNameCreatedPrefix = "ObjectCreated:"
	s3EventNameRemovedPrefix = "ObjectRemoved:"
	s3EventTimeFormat        = time.RFC3339Nano
	minSQSReceiveWaitTime    = time.Second
)

var (
	// DefaultSQSReceiveMaxNumberOfMessages is the default maximum number of messages to return in each receive messages call.
	DefaultSQSReceiveMaxNumberOfMessages = 10

	// DefaultSQSReceiveWaitTime is default duration for which the receive call waits for a message to arrive in the queue before returning.
	DefaultSQSReceiveWaitTime = 20 * time.Second

	// DefaultSQSMessageVisibilityTimeout is the default duration that the received messages are hidden from subsequent retrieve requests.
	DefaultSQSMessageVisibilityTimeout = 5 * time.Second

	// DefaultSQSMessageMaxRetryCount is the default maximum number of retries for a failed message before it is dropped.
	DefaultSQSMessageMaxRetryCount = 3

	// DefaultSQSEventsChanSize is the default size of events buffered channel.
	DefaultSQSEventsChanSize = 100

	sqsClientErrorsMeter     = core.DefaultMetrics.GetMeter("sqs.client.errors")
	sqsMessagesReceivedMeter = core.DefaultMetrics.GetMeter("sqs.messages.received")
	sqsMessagesSuccessMeter  = core.DefaultMetrics.GetMeter("sqs.messages.success")
	sqsMessagesFailedMeter   = core.DefaultMetrics.GetMeter("sqs.messages.failed")
	sqsMessagesInvalidMeter  = core.DefaultMetrics.GetMeter("sqs.messages.invalid")
	sqsMessagesLagTimer      = core.DefaultMetrics.GetTimer("sqs.messages.lag")

	_ core.SegmentEventSource = &SQSSegmentEventSource{}
	_ core.Lifecycle          = &SQSSegmentEventSource{}
	_ core.Factory            = &SQSSegmentEventSourceConfig{}
)

// SQSSegmentEventSourceConfig is the configuration for AWS SQS segment event source
type SQSSegmentEventSourceConfig struct {
	// The AWS config object used to create the AWS SQS client.
	//
	// Field value is required.
	AWSConfig *aws.Config `required:"true"`

	// The AWS session object used to create the AWS SQS client.
	//
	// Field value is required.
	AWSSession *session.Session `required:"true"`

	// AWS SQS queue name where AWS S3 notification events are published.
	//
	// The implementation expects that both Created and Removed event types to be enabled
	// for keys storing segments (i.e. the keys with DataKeyPrefix).
	//
	// Check the AWS S3 documentation for instructions on how to enable event notifications:
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html
	//
	// Field value is required.
	QueueName string `required:"true"`

	// Key prefix for segment notification events.
	//
	// Default value is set via DefaultS3KeyPrefix variable.
	S3KeyPrefix string

	// The maximum number of messages to return in each receive messages call.
	//
	// Default value is set via DefaultSQSReceiveMaxNumberOfMessages variable.
	ReceiveMaxNumberOfMessages int `min:"1" max:"10"`

	// The duration for which the receive call waits for a message to arrive in the queue before returning.
	//
	// Minimum value must be greater than zero to enable the use of long polling.
	// Default value is set via DefaultSQSReceiveWaitTime variable.
	ReceiveWaitTime time.Duration `min:"5s"`

	// The duration that the received messages are hidden from subsequent retrieve requests.
	//
	// Default value is set via DefaultSQSMessageVisibilityTimeout variable.
	MessageVisibilityTimeout time.Duration `min:"1s"`

	// Maximum number of retries for a failed message before it is dropped.
	//
	// Default value is set via DefaultSQSMessageMaxRetryCount variable.
	MessageMaxRetryCount int `min:"1"`

	// Size of events buffered channel.
	//
	// Default value is set via DefaultSQSEventsChanSize variable.
	EventsChanSize int `min:"1"`

	// Breaker enables tracking AWS SQS client error rate.
	//
	// Default value is set via DefaultSQSBreaker variable.
	Breaker core.Breaker
}

// SQSSegmentEventSource is the worker for AWS S3 notification events sent to AWS SQS
type SQSSegmentEventSource struct {
	config        SQSSegmentEventSourceConfig
	sqsClient     *sqs.SQS
	queueURL      string
	context       context.Context
	contextCancel context.CancelFunc
	eventsChan    chan core.SegmentEventRequest
}

type handleMessageRequest struct {
	wg      *sync.WaitGroup
	message *sqs.Message
}

type s3Event struct {
	Records []struct {
		EventSource string `json:"eventSource"`
		EventName   string `json:"eventName"`
		EventTime   string `json:"eventTime"`
		S3          struct {
			Object struct {
				Key  string `json:"key"`
				Size uint64 `json:"size"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

// NewSQSSegmentEventSource returns a new SQSSegmentEventSource instance
func NewSQSSegmentEventSource(config SQSSegmentEventSourceConfig) (*SQSSegmentEventSource, error) {
	utils.SetDefaultString(&config.S3KeyPrefix, DefaultS3KeyPrefix)
	utils.SetDefaultInt(&config.ReceiveMaxNumberOfMessages, DefaultSQSReceiveMaxNumberOfMessages)
	utils.SetDefaultDuration(&config.ReceiveWaitTime, DefaultSQSReceiveWaitTime)
	utils.SetDefaultDuration(&config.MessageVisibilityTimeout, DefaultSQSMessageVisibilityTimeout)
	utils.SetDefaultInt(&config.MessageMaxRetryCount, DefaultSQSMessageMaxRetryCount)
	utils.SetDefaultInt(&config.EventsChanSize, DefaultSQSEventsChanSize)
	utils.SetDefaultBreaker(&config.Breaker, core.DefaultSQSBreaker)

	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		return nil, errors.Errorf("NewSQSSegmentEventSource: invalid config %s", errMessage)
	}

	sqsClient := sqs.New(config.AWSSession, config.AWSConfig)

	queueURLResponse, err := sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(config.QueueName),
	})
	if err != nil {
		return nil, errors.Errorf("NewSQSSegmentEventSource: failed to get queue url")
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	return &SQSSegmentEventSource{
		config:        config,
		sqsClient:     sqsClient,
		queueURL:      *queueURLResponse.QueueUrl,
		context:       ctx,
		contextCancel: ctxCancel,
		eventsChan:    make(chan core.SegmentEventRequest, config.EventsChanSize),
	}, nil
}

// Get creates the corresponding instance
func (c SQSSegmentEventSourceConfig) Get() (interface{}, error) {
	return NewSQSSegmentEventSource(c)
}

// Start will start the worker
func (w *SQSSegmentEventSource) Start() error {
	go w.receiveLoop()
	return nil
}

// Stop will stop the worker
func (w *SQSSegmentEventSource) Stop() {
	w.contextCancel()
}

// Events returns the segment event channel
func (w *SQSSegmentEventSource) Events() <-chan core.SegmentEventRequest {
	return w.eventsChan
}

func (w *SQSSegmentEventSource) receiveLoop() {
	maxNumberOfMessages := int64(w.config.ReceiveMaxNumberOfMessages)
	waitTimeSeconds := int64(w.config.ReceiveWaitTime.Seconds())
	visibilityTimeout := int64(w.config.MessageVisibilityTimeout.Seconds())

	requestChan := make(chan handleMessageRequest, w.config.ReceiveMaxNumberOfMessages)
	for i := 0; i < w.config.ReceiveMaxNumberOfMessages; i++ {
		go w.handleMessages(requestChan)
	}

	var wg sync.WaitGroup
LOOP:
	for {
		select {
		case <-w.context.Done():
			break LOOP
		default:
			response, err := w.sqsClient.ReceiveMessageWithContext(w.context, &sqs.ReceiveMessageInput{
				MaxNumberOfMessages: aws.Int64(maxNumberOfMessages),
				WaitTimeSeconds:     aws.Int64(waitTimeSeconds),
				QueueUrl:            aws.String(w.queueURL),
				VisibilityTimeout:   aws.Int64(visibilityTimeout),
				AttributeNames: aws.StringSlice([]string{
					"ApproximateReceiveCount",
				}),
			})
			if err != nil {
				if utils.IsCanceledError(err) {
					break LOOP
				}

				w.log().Errorf("Failed to receive message. Error: %+v", err)
				w.markError()
				continue
			}

			if len(response.Messages) == 0 {
				continue
			}

			sqsMessagesReceivedMeter.Mark(int64(len(response.Messages)))

			wg.Add(len(response.Messages))
			for _, msg := range response.Messages {
				requestChan <- handleMessageRequest{
					wg:      &wg,
					message: msg,
				}
			}

			wg.Wait()
		}
	}

	close(requestChan)
}

func (w *SQSSegmentEventSource) handleMessages(handleMessageChan <-chan handleMessageRequest) {
	for request := range handleMessageChan {
		err := w.handleMessage(request.message)

		if err != nil {
			if utils.IsCanceledError(err) {
				break
			}

			sqsMessagesFailedMeter.Mark(1)
			w.log().Errorf("Failed to process message. Error: %+v", err)

			count, ok := w.getMessageReceiveCount(request.message)
			if !ok || count >= w.config.MessageMaxRetryCount {
				w.log().Errorf("Message %s failed too many times", request.message)
				w.deleteMessage(request.message)
			}
		} else {
			sqsMessagesSuccessMeter.Mark(1)
		}

		request.wg.Done()
	}
}

func (w *SQSSegmentEventSource) handleMessage(msg *sqs.Message) error {
	event, err := w.parseS3Event(msg.Body)
	if err != nil {
		sqsMessagesInvalidMeter.Mark(1)
		w.log().Errorf("Failed to parse s3 event for message %s", msg)

		w.deleteMessage(msg)
		return nil
	}

	for _, record := range event.Records {
		if record.EventSource != s3EventSource {
			sqsMessagesInvalidMeter.Mark(1)
			w.log().Warnf("Skipping event for unknown source %s", record.EventSource)
			continue
		}

		var eventType core.SegmentEvent_Type
		switch {
		case strings.HasPrefix(record.EventName, s3EventNameCreatedPrefix):
			eventType = core.SegmentEvent_CREATED
		case strings.HasPrefix(record.EventName, s3EventNameRemovedPrefix):
			eventType = core.SegmentEvent_REMOVED
		default:
			w.log().Warnf("Skipping unknown event type %s", record.EventName)
			continue
		}

		timestamp, err := time.Parse(s3EventTimeFormat, record.EventTime)
		if err != nil {
			sqsMessagesInvalidMeter.Mark(1)
			w.log().Warnf("Skipping event with invalid time %s", record.EventTime)
			continue
		}

		sqsMessagesLagTimer.UpdateSince(timestamp)

		key, err := url.QueryUnescape(record.S3.Object.Key)
		if err != nil {
			sqsMessagesInvalidMeter.Mark(1)
			w.log().Warnf("Skipping event with invalid object key encoding %s", record.S3.Object.Key)
			continue
		}

		segment, err := parseSegmentKey(w.config.S3KeyPrefix, key)
		if err != nil {
			sqsMessagesInvalidMeter.Mark(1)
			w.log().Warnf("Skipping event with invalid object key %s. Error: %+v", key, err)
			continue
		}

		resultChan := make(chan error)
		request := core.SegmentEventRequest{
			SegmentEvent: core.SegmentEvent{
				Type:        eventType,
				Timestamp:   timestamp,
				Segment:     segment,
				SegmentSize: record.S3.Object.Size,
			},
			Result: resultChan,
		}

		select {
		case w.eventsChan <- request:
			if err := <-resultChan; err != nil {
				return err
			}
		case <-w.context.Done():
			return w.context.Err()
		}
	}

	w.deleteMessage(msg)
	return nil
}

func (w *SQSSegmentEventSource) deleteMessage(msg *sqs.Message) {
	if msg.ReceiptHandle == nil {
		sqsMessagesInvalidMeter.Mark(1)
		w.log().Warnf("Missing receipt handle for message %s", msg)
		return
	}

	_, err := w.sqsClient.DeleteMessageWithContext(w.context, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(w.queueURL),
		ReceiptHandle: msg.ReceiptHandle,
	})

	if err != nil {
		w.log().Errorf("Failed to delete message %s. Error: %+v", msg, err)
		w.markError()
	}
}

func (w *SQSSegmentEventSource) getMessageReceiveCount(msg *sqs.Message) (int, bool) {
	strValue, ok := msg.Attributes["ApproximateReceiveCount"]
	if !ok {
		sqsMessagesInvalidMeter.Mark(1)
		return 0, false
	}

	result, err := strconv.ParseInt(*strValue, 10, 64)
	if err != nil {
		sqsMessagesInvalidMeter.Mark(1)
		w.log().Errorf("Unable to parse receive count '%s' for message %s. Error: %+v", *strValue, msg, err)
		return 0, false
	}

	return int(result), true
}

func (w *SQSSegmentEventSource) parseS3Event(body *string) (s3Event, error) {
	if body == nil {
		return s3Event{}, errors.New("nil body")
	}

	var result s3Event
	if err := json.Unmarshal([]byte(*body), &result); err != nil {
		return s3Event{}, err
	}

	return result, nil
}

func (w *SQSSegmentEventSource) log() core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type": "SQSSegmentEventSource",
	})
}

func (w *SQSSegmentEventSource) markError() {
	sqsClientErrorsMeter.Mark(1)
	w.config.Breaker.Mark()
}
