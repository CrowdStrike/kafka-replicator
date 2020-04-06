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
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

const (
	s3URLFormat           = "s3://%s/%s"                 // s3://mybucket/mykey
	s3TempKeyPattern      = "%s/%s"                      // prefix/UUID
	s3SegmentKeyPattern   = "%s/%s/%s/%d/%d/%.20d-%.20d" // prefix/region/topic/partition/level/start_offset-end_offset
	s3ListSegmentsPattern = "%s/%s/%s/%d/"               // prefix/region/topic/partition/level/start_offset-end_offset
)

var (
	// DefaultS3KeyPrefix is the default prefix for stored segments.
	DefaultS3KeyPrefix = "data"

	// DefaultS3TempKeyPrefix is the default prefix for temporary segments.
	DefaultS3TempKeyPrefix = "temp"

	s3ClientErrorsMeter      = core.DefaultMetrics.GetMeter("s3.client.errors")
	s3ListSegmentsTimeTimer  = core.DefaultMetrics.GetTimer("s3.list-segments.time")
	s3ListSegmentsCallsMeter = core.DefaultMetrics.GetMeter("s3.list-segments.calls")

	_ core.SegmentStore = &S3SegmentStore{}
	_ core.Factory      = &S3SegmentStoreConfig{}
)

// S3SegmentStoreConfig is the configuration for AWS S3 segment storage
type S3SegmentStoreConfig struct {
	// The segment file format used to read and write segments.
	SegmentFormat core.Factory `required:"true"`

	// The AWS config object used to create the AWS S3 client.
	//
	// Field value is required.
	AWSConfig *aws.Config `required:"true"`

	// The AWS session object used to create the AWS S3 client.
	//
	// Field value is required.
	AWSSession *session.Session `required:"true"`

	// Bucket name to store segments.
	//
	// Field value is required.
	Bucket string `required:"true"`

	// Key prefix for written segment objects.
	//
	// Default value is set via DefaultS3KeyPrefix variable.
	KeyPrefix string

	// Key prefix for temporary segment objects.
	//
	// Default value is set via DefaultS3TempKeyPrefix variable.
	TempKeyPrefix string

	// Breaker enables tracking AWS S3 client error rate.
	//
	// Default value is set via DefaultS3Breaker variable.
	Breaker core.Breaker
}

// S3SegmentStore is the segment storage backed by AWS S3
type S3SegmentStore struct {
	config        S3SegmentStoreConfig
	s3Client      *s3.S3
	segmentFormat core.SegmentFormat
}

type s3SegmentWriter struct {
	store   *S3SegmentStore
	inner   core.SegmentWriter
	tempKey string
}

// NewS3SegmentStore returns a new S3SegmentStore instance
func NewS3SegmentStore(config S3SegmentStoreConfig) (*S3SegmentStore, error) {
	utils.SetDefaultString(&config.KeyPrefix, DefaultS3KeyPrefix)
	utils.SetDefaultString(&config.TempKeyPrefix, DefaultS3TempKeyPrefix)
	utils.SetDefaultBreaker(&config.Breaker, core.DefaultS3Breaker)

	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		return nil, errors.Errorf("NewCheckpointStore: invalid config %s", errMessage)
	}

	instances, err := utils.CallFactory(
		utils.FactoryType{Factory: config.SegmentFormat, Type: (*core.SegmentFormat)(nil)})
	if err != nil {
		return nil, err
	}

	s3Client := s3.New(config.AWSSession, config.AWSConfig)

	return &S3SegmentStore{
		config:        config,
		s3Client:      s3Client,
		segmentFormat: instances[0].(core.SegmentFormat),
	}, nil
}

// Get creates the corresponding instance
func (c S3SegmentStoreConfig) Get() (interface{}, error) {
	return NewS3SegmentStore(c)
}

// Create will create a new segment file
func (r *S3SegmentStore) Create(ctx context.Context) (core.SegmentWriter, error) {
	tempKey := r.getTempKey()
	url := r.formatS3URL(r.config.Bucket, tempKey)

	writer, err := r.segmentFormat.NewWriter(ctx, url)
	if err != nil {
		return nil, errors.Wrapf(err, "S3SegmentStore: failed to create writer")
	}

	return &s3SegmentWriter{
		store:   r,
		inner:   writer,
		tempKey: tempKey,
	}, nil
}

// Open will open the segment for reading
func (r *S3SegmentStore) Open(ctx context.Context, segment core.Segment) (core.SegmentReader, error) {
	key := r.getSegmentKey(segment)

	if err := r.s3Client.WaitUntilObjectExistsWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.config.Bucket),
		Key:    aws.String(key),
	}); err != nil {
		r.markError()
		return nil, errors.Wrapf(err, "S3SegmentStore: wait object %s exists failed", key)
	}

	url := r.formatS3URL(r.config.Bucket, key)
	reader, err := r.segmentFormat.NewReader(ctx, url)
	if err != nil {
		return nil, errors.Wrapf(err, "S3SegmentStore: failed to create reader")
	}

	metadata := reader.Metadata()

	// sanity check to detect scenario where segment is replicated/copied/written to wrong S3 key
	if metadata.Region != segment.Region || metadata.Topic != segment.Topic || metadata.Partition != segment.Partition ||
		metadata.Level != segment.Level || metadata.StartOffset != segment.StartOffset || metadata.EndOffset != segment.EndOffset {
		reader.Close(ctx)
		r.markError()
		return nil, errors.New("S3SegmentStore: segment metadata check failed")
	}

	return reader, nil
}

// ListSegments returns all segments for the provided parameters
func (r *S3SegmentStore) ListSegments(ctx context.Context, region, topic string, partition uint32) (map[core.Segment]core.SegmentInfo, error) {
	s3ListSegmentsCallsMeter.Mark(1)
	startTime := time.Now()
	defer s3ListSegmentsTimeTimer.UpdateSince(startTime)

	result := map[core.Segment]core.SegmentInfo{}

	handlePage := func(out *s3.ListObjectsV2Output, _ bool) bool {
		for _, object := range out.Contents {
			if object.Key == nil || object.LastModified == nil {
				continue
			}

			segment, err := parseSegmentKey(r.config.KeyPrefix, *object.Key)
			if err != nil {
				r.log().Errorf("Failed to parse segment key %s. Error: %+v", *object.Key, err)
				continue
			}

			result[segment] = core.SegmentInfo{
				Segment:   segment,
				Timestamp: *object.LastModified,
				Size:      uint64(*object.Size),
			}
		}

		return true
	}

	if err := r.s3Client.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(r.config.Bucket),
		Prefix: aws.String(fmt.Sprintf(s3ListSegmentsPattern, r.config.KeyPrefix, region, topic, partition)),
	}, handlePage); err != nil {
		r.markError()
		return nil, errors.Wrapf(err, "S3SegmentStore: failed to list objects")
	}

	return result, nil
}

// Delete removes the provided segment
func (r *S3SegmentStore) Delete(ctx context.Context, segment core.Segment) error {
	key := r.getSegmentKey(segment)

	if _, err := r.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(r.config.Bucket),
		Key:    aws.String(key),
	}); err != nil {
		r.markError()
		return errors.Wrapf(err, "S3SegmentStore: failed to delete object %s", key)
	}

	return nil
}

// Events channel is not supported in this implementation
func (r *S3SegmentStore) Events() <-chan core.SegmentEventRequest {
	r.log().Panic("not supported")
	panic("unreachable")
}

func (r *S3SegmentStore) getSegmentKey(s core.Segment) string {
	return r.getKey(s.Region, s.Topic, s.Partition, s.Level, s.StartOffset, s.EndOffset)
}

func (r *S3SegmentStore) getKey(region, topic string, partition, level uint32, startOffset, endOffset uint64) string {
	return fmt.Sprintf(s3SegmentKeyPattern, r.config.KeyPrefix, region, topic, partition, level, startOffset, endOffset)
}

func (r *S3SegmentStore) getTempKey() string {
	return fmt.Sprintf(s3TempKeyPattern, r.config.TempKeyPrefix, utils.NewUUID())
}

func (r *S3SegmentStore) formatS3URL(bucket, key string) string {
	return fmt.Sprintf(s3URLFormat, bucket, key)
}

func (r *S3SegmentStore) log() core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type": "S3SegmentStore",
	})
}

func (r *S3SegmentStore) markError() {
	s3ClientErrorsMeter.Mark(1)
	r.config.Breaker.Mark()
}

func (w *s3SegmentWriter) Write(ctx context.Context, message core.Message) error {
	return w.inner.Write(ctx, message)
}

func (w *s3SegmentWriter) Close(ctx context.Context, metadata core.SegmentMetadata) error {
	if err := w.inner.Close(ctx, metadata); err != nil {
		return err
	}

	key := w.store.getKey(metadata.Region, metadata.Topic, metadata.Partition, metadata.Level, metadata.StartOffset, metadata.EndOffset)

	if _, err := w.store.s3Client.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		CopySource: aws.String(url.PathEscape(w.store.config.Bucket + "/" + w.tempKey)),
		Bucket:     aws.String(w.store.config.Bucket),
		Key:        aws.String(key),
	}); err != nil {
		w.store.markError()
		return errors.Wrapf(err, "s3SegmentWriter: failed to copy from %s to %s", w.tempKey, key)
	}

	w.deleteTempObject(ctx)
	return nil
}

func (w *s3SegmentWriter) Abort(ctx context.Context) {
	w.inner.Abort(ctx)
	w.deleteTempObject(ctx)
}

func (w *s3SegmentWriter) deleteTempObject(ctx context.Context) {
	_, err := w.store.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(w.store.config.Bucket),
		Key:    aws.String(w.tempKey),
	})

	if err != nil {
		// error when deleting temp object is not considered critical
		// will eventually be deleted by S3 lifecycle rule
		w.log().Warnf("Failed to delete object %s. Error: %+v", w.tempKey, err)
		w.store.markError()
	}
}

func (w *s3SegmentWriter) log() core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type": "s3SegmentWriter",
	})
}

func parseSegmentKey(prefix, key string) (core.Segment, error) {
	if !strings.HasPrefix(key, prefix) {
		return core.Segment{}, errors.New("missing prefix")
	}

	parts := strings.Split(key[len(prefix)+1:], "/")
	if len(parts) != 5 {
		return core.Segment{}, errors.New("invalid key format")
	}

	if len(parts[0]) == 0 {
		return core.Segment{}, errors.New("invalid key: missing region name")
	}

	if len(parts[1]) == 0 {
		return core.Segment{}, errors.New("invalid key: missing topic name")
	}

	partition, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return core.Segment{}, errors.New("invalid key: bad partition")
	}

	level, err := strconv.ParseUint(parts[3], 10, 32)
	if err != nil {
		return core.Segment{}, errors.New("invalid key: bad level")
	}

	offsetParts := strings.Split(parts[4], "-")
	if len(offsetParts) != 2 {
		return core.Segment{}, errors.New("invalid key format")
	}

	startOffset, err := strconv.ParseUint(offsetParts[0], 10, 64)
	if err != nil {
		return core.Segment{}, errors.New("invalid key: bad start offset")
	}

	endOffset, err := strconv.ParseUint(offsetParts[1], 10, 64)
	if err != nil {
		return core.Segment{}, errors.New("invalid key: bad end offset")
	}

	return core.Segment{
		Region:      parts[0],
		Topic:       parts[1],
		Partition:   uint32(partition),
		Level:       uint32(level),
		StartOffset: startOffset,
		EndOffset:   endOffset,
	}, nil
}
