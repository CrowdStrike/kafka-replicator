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

package formats

import (
	"context"
	"net/url"
	"strconv"
	"time"

	"github.com/CrowdStrike/kafka-replicator/pkg/core"
	"github.com/CrowdStrike/kafka-replicator/pkg/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

const (
	s3UrlScheme                 = "s3"
	metadataKeyRegion           = "region"
	metadataKeyTopic            = "topic"
	metadataKeyPartition        = "partition"
	metadataKeyLevel            = "level"
	metadataKeyStartOffset      = "startOffset"
	metadataKeyEndOffset        = "endOffset"
	metadataKeyMessageCount     = "messageCount"
	metadataKeyCreatedTimestamp = "createdTimestamp"
	metadataTimeFormat          = time.RFC3339Nano
)

var (
	// DefaultParquetParallelism is the default Parquet library parallelism parameter for read/write operations.
	DefaultParquetParallelism = 10

	// DefaultParquetCreatedBy is the default value used for Parquet file CreatedBy attribute.
	DefaultParquetCreatedBy = "kafka-replicator"

	s3ParquetErrorsMeter = core.DefaultMetrics.GetMeter("s3.parquet.errors")

	allMetadataKeys = map[string]bool{
		metadataKeyRegion:           true,
		metadataKeyTopic:            true,
		metadataKeyPartition:        true,
		metadataKeyLevel:            true,
		metadataKeyStartOffset:      true,
		metadataKeyEndOffset:        true,
		metadataKeyMessageCount:     true,
		metadataKeyCreatedTimestamp: true,
	}

	_ core.Factory = &S3ParquetConfig{}
)

// S3ParquetConfig is the configuration for S3Parquet format
type S3ParquetConfig struct {
	// The AWS config object used to create the AWS S3 client.
	//
	// Field value is required.
	AWSConfig *aws.Config `required:"true"`

	// Allows configuration of AWS S3 uploader instance.
	//
	// Field value is optional.
	S3UploaderOptions []func(*s3manager.Uploader)

	// Parquet library parallelism parameter for read/write operations.
	//
	// Default value is set via DefaultParquetParallelism variable.
	Parallelism int `min:"1"`

	// Is the value used for Parquet file CreatedBy attribute.
	//
	// Default value is set via DefaultParquetCreatedBy variable.
	CreatedBy string

	// Breaker enables tracking consumer error rate.
	//
	// Default value is set via DefaultS3Breaker variable.
	Breaker core.Breaker
}

// S3Parquet is the segment format backed by AWS S3 and Parquet columnar data storage format
type S3Parquet struct {
	config S3ParquetConfig
	schema interface{}
}

type s3ParquetMessageHeader struct {
	Key   string `parquet:"name=key, type=BYTE_ARRAY"`
	Value string `parquet:"name=value, type=BYTE_ARRAY"`
}

type s3ParquetMessage struct {
	Key       string                   `parquet:"name=key, type=BYTE_ARRAY"`
	Value     string                   `parquet:"name=value, type=BYTE_ARRAY"`
	Offset    uint64                   `parquet:"name=offset, type=UINT_64"`
	Timestamp int64                    `parquet:"name=timestamp, type=INT_64"` // Unix nano
	Headers   []s3ParquetMessageHeader `parquet:"name=headers, type=LIST"`
}

type s3ParquetWriter struct {
	format *S3Parquet
	path   string
	file   source.ParquetFile
	writer *writer.ParquetWriter
}

type s3ParquetReader struct {
	format   *S3Parquet
	path     string
	file     source.ParquetFile
	reader   *reader.ParquetReader
	metadata *core.SegmentMetadata
}

// NewS3Parquet returns a new S3Parquet instance
func NewS3Parquet(config S3ParquetConfig) (*S3Parquet, error) {
	utils.SetDefaultInt(&config.Parallelism, DefaultParquetParallelism)
	utils.SetDefaultString(&config.CreatedBy, DefaultParquetCreatedBy)
	utils.SetDefaultBreaker(&config.Breaker, core.DefaultS3Breaker)

	if errMessage := utils.Validate(config); len(errMessage) > 0 {
		return nil, errors.Errorf("NewS3Parquet: invalid config %s", errMessage)
	}

	return &S3Parquet{
		config: config,
		schema: &s3ParquetMessage{},
	}, nil
}

// Get creates and returns the corresponding instance
func (c S3ParquetConfig) Get() (interface{}, error) {
	return NewS3Parquet(c)
}

// NewWriter creates a new segment writer
// Expected path format: s3://mybucket/mykey
func (f *S3Parquet) NewWriter(ctx context.Context, path string) (core.SegmentWriter, error) {
	bucket, key, err := f.parsePath(path)
	if err != nil {
		return nil, err
	}

	file, err := s3.NewS3FileWriter(ctx, bucket, key, f.config.S3UploaderOptions, f.config.AWSConfig)
	if err != nil {
		f.markError()
		return nil, errors.Wrapf(err, "S3Parquet: failed to create S3 writer")
	}

	writer, err := writer.NewParquetWriter(file, f.schema, int64(f.config.Parallelism))
	if err != nil {
		f.markError()
		return nil, errors.Wrapf(err, "S3Parquet: failed to create Parquet writer")
	}

	return &s3ParquetWriter{
		format: f,
		path:   path,
		file:   file,
		writer: writer,
	}, nil
}

// NewReader creates a new segment reader
// Expected path format: s3://mybucket/mykey
func (f *S3Parquet) NewReader(ctx context.Context, path string) (core.SegmentReader, error) {
	bucket, key, err := f.parsePath(path)
	if err != nil {
		return nil, err
	}

	file, err := s3.NewS3FileReader(ctx, bucket, key, f.config.AWSConfig)
	if err != nil {
		f.markError()
		return nil, errors.Wrapf(err, "S3Parquet: failed to create S3 reader")
	}

	reader, err := reader.NewParquetReader(file, f.schema, int64(f.config.Parallelism))
	if err != nil {
		f.markError()
		return nil, errors.Wrapf(err, "S3Parquet: failed to create Parquet reader")
	}

	metadata, err := f.keyValueToMetadata(reader.Footer.KeyValueMetadata)
	if err != nil {
		return nil, errors.Wrap(err, "S3Parquet: failed to read metadata")
	}

	return &s3ParquetReader{
		format:   f,
		path:     path,
		file:     file,
		reader:   reader,
		metadata: metadata,
	}, nil
}

func (f *S3Parquet) markError() {
	s3ParquetErrorsMeter.Mark(1)
	f.config.Breaker.Mark()
}

func (f *S3Parquet) parsePath(path string) (string, string, error) {
	u, err := url.Parse(path)
	if err != nil {
		return "", "", errors.Wrapf(err, "S3Parquet: invalid S3 URI: %s", path)
	}

	if u.Scheme != s3UrlScheme {
		return "", "", errors.Errorf("S3Parquet: invalid S3 URI scheme: %s", u.Scheme)
	}

	return u.Host, u.Path, nil
}

func (f *S3Parquet) keyValueToMetadata(keyValues []*parquet.KeyValue) (*core.SegmentMetadata, error) {
	result := &core.SegmentMetadata{}

	parseUint32 := func(dest *uint32, kv *parquet.KeyValue) error {
		v, err := strconv.ParseUint(*kv.Value, 10, 32)
		if err != nil {
			return errors.Wrapf(err, "invalid uint32 metadata value %s", kv.Key)
		}

		*dest = uint32(v)
		return nil
	}

	parseUint64 := func(dest *uint64, kv *parquet.KeyValue) error {
		v, err := strconv.ParseUint(*kv.Value, 10, 64)
		if err != nil {
			return errors.Wrapf(err, "invalid uint64 metadata value %s", kv.Key)
		}

		*dest = v
		return nil
	}

	parseTime := func(dest *time.Time, kv *parquet.KeyValue) error {
		v, err := time.Parse(metadataTimeFormat, *kv.Value)
		if err != nil {
			return errors.Wrapf(err, "invalid time metadata value %s", kv.Key)
		}

		*dest = v
		return nil
	}

	seen := map[string]bool{}
	for _, kv := range keyValues {
		if kv == nil || kv.Value == nil || len(*kv.Value) == 0 || !allMetadataKeys[kv.Key] {
			continue
		}

		if seen[kv.Key] {
			return nil, errors.Errorf("duplicate metadata key %s", kv.Key)
		}

		switch kv.Key {
		case metadataKeyRegion:
			result.Region = *kv.Value
		case metadataKeyTopic:
			result.Topic = *kv.Value
		case metadataKeyPartition:
			if err := parseUint32(&result.Partition, kv); err != nil {
				return nil, err
			}
		case metadataKeyLevel:
			if err := parseUint32(&result.Level, kv); err != nil {
				return nil, err
			}
		case metadataKeyStartOffset:
			if err := parseUint64(&result.StartOffset, kv); err != nil {
				return nil, err
			}
		case metadataKeyEndOffset:
			if err := parseUint64(&result.EndOffset, kv); err != nil {
				return nil, err
			}
		case metadataKeyMessageCount:
			if err := parseUint64(&result.MessageCount, kv); err != nil {
				return nil, err
			}
		case metadataKeyCreatedTimestamp:
			if err := parseTime(&result.CreatedTimestamp, kv); err != nil {
				return nil, err
			}
		}

		seen[kv.Key] = true
	}

	if len(seen) != len(allMetadataKeys) {
		return nil, errors.New("metadata has missing fields")
	}

	return result, nil
}

func (w *s3ParquetWriter) Write(ctx context.Context, message core.Message) error {
	msg := s3ParquetMessage{
		Key:       string(message.Key),
		Value:     string(message.Value),
		Offset:    message.Offset,
		Timestamp: message.Timestamp.UnixNano(),
	}

	if len(message.Headers) > 0 {
		msg.Headers = make([]s3ParquetMessageHeader, len(message.Headers))
		for i, header := range message.Headers {
			msg.Headers[i] = s3ParquetMessageHeader{
				Key:   header.Key,
				Value: string(header.Value),
			}
		}
	}

	err := w.writer.Write(msg)
	if err != nil {
		w.format.markError()
		return errors.Wrapf(err, "S3Parquet: write failed")
	}

	return nil
}

func (w *s3ParquetWriter) Close(ctx context.Context, metadata core.SegmentMetadata) error {
	w.writer.Footer.CreatedBy = &w.format.config.CreatedBy
	w.writer.Footer.KeyValueMetadata = w.metadataToKeyValue(metadata)

	return w.closeFile(true)
}

func (w *s3ParquetWriter) Abort(ctx context.Context) {
	if err := w.closeFile(false); err != nil {
		w.log().Warnf("Failed to close file %s. Error: %+v", w.path, err)
	}
}

func (w *s3ParquetWriter) closeFile(writeStop bool) error {
	var writeStopErr error
	if writeStop {
		writeStopErr = w.writer.WriteStop()
	}

	closeErr := w.file.Close()

	if writeStop && writeStopErr != nil {
		w.format.markError()
		return errors.Wrapf(writeStopErr, "S3Parquet: failed to stop writer")
	}

	if closeErr != nil {
		w.format.markError()
		return errors.Wrapf(writeStopErr, "S3Parquet: failed to close writer")
	}

	return nil
}

func (w *s3ParquetWriter) metadataToKeyValue(metadata core.SegmentMetadata) []*parquet.KeyValue {
	partition := strconv.FormatUint(uint64(metadata.Partition), 10)
	level := strconv.FormatUint(uint64(metadata.Level), 10)
	startOffset := strconv.FormatUint(metadata.StartOffset, 10)
	endOffset := strconv.FormatUint(metadata.EndOffset, 10)
	messageCount := strconv.FormatUint(metadata.MessageCount, 10)
	createdTimestamp := metadata.CreatedTimestamp.Format(metadataTimeFormat)

	return []*parquet.KeyValue{
		{Key: metadataKeyRegion, Value: &metadata.Region},
		{Key: metadataKeyTopic, Value: &metadata.Topic},
		{Key: metadataKeyPartition, Value: &partition},
		{Key: metadataKeyLevel, Value: &level},
		{Key: metadataKeyStartOffset, Value: &startOffset},
		{Key: metadataKeyEndOffset, Value: &endOffset},
		{Key: metadataKeyMessageCount, Value: &messageCount},
		{Key: metadataKeyCreatedTimestamp, Value: &createdTimestamp},
	}
}

func (w *s3ParquetWriter) log() core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type": "s3ParquetWriter",
	})
}

func (r *s3ParquetReader) Read(ctx context.Context, count int) ([]core.Message, error) {
	messages := make([]s3ParquetMessage, count)
	err := r.reader.Read(&messages)
	if err != nil {
		r.format.markError()
		return nil, errors.Wrapf(err, "S3Parquet: read failed")
	}

	result := make([]core.Message, len(messages))
	for i, msg := range messages {
		result[i] = core.Message{
			Key:       []byte(msg.Key),
			Value:     []byte(msg.Value),
			Offset:    msg.Offset,
			Timestamp: time.Unix(0, msg.Timestamp),
		}

		if len(msg.Headers) > 0 {
			result[i].Headers = make([]*core.Message_Header, len(msg.Headers))
			for j, header := range msg.Headers {
				result[i].Headers[j] = &core.Message_Header{
					Key:   header.Key,
					Value: []byte(header.Value),
				}
			}
		}
	}

	return result, nil
}

func (r *s3ParquetReader) Metadata() core.SegmentMetadata {
	return *r.metadata
}

func (r *s3ParquetReader) Close(ctx context.Context) {
	r.reader.ReadStop()

	err := r.file.Close()
	if err != nil {
		r.log().Warnf("Failed to close file %s. Error: %+v", r.path, err)
		r.format.markError()
	}
}

func (r *s3ParquetReader) log() core.Logger {
	return core.DefaultLogger.WithFields(core.Fields{
		"Type": "s3ParquetReader",
	})
}
