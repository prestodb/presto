/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

namespace facebook::velox::filesystems {

// The number of connections open for S3 read operations.
constexpr std::string_view kMetricS3ActiveConnections{
    "velox.s3_active_connections"};

// The number of S3 upload calls that started.
constexpr std::string_view kMetricS3StartedUploads{"velox.s3_started_uploads"};

// The number of S3 upload calls that were completed.
constexpr std::string_view kMetricS3SuccessfulUploads{
    "velox.s3_successful_uploads"};

// The number of S3 upload calls that failed.
constexpr std::string_view kMetricS3FailedUploads{"velox.s3_failed_uploads"};

// The number of S3 head (metadata) calls.
constexpr std::string_view kMetricS3MetadataCalls{"velox.s3_metadata_calls"};

// The number of S3 head (metadata) calls that failed.
constexpr std::string_view kMetricS3GetMetadataErrors{
    "velox.s3_get_metadata_errors"};

// The number of retries made during S3 head (metadata) calls.
constexpr std::string_view kMetricS3GetMetadataRetries{
    "velox.s3_get_metadata_retries"};

// The number of S3 getObject calls.
constexpr std::string_view kMetricS3GetObjectCalls{"velox.s3_get_object_calls"};

// The number of S3 getObject calls that failed.
constexpr std::string_view kMetricS3GetObjectErrors{
    "velox.s3_get_object_errors"};

// The number of retries made during S3 getObject calls.
constexpr std::string_view kMetricS3GetObjectRetries{
    "velox.s3_get_object_retries"};

} // namespace facebook::velox::filesystems
