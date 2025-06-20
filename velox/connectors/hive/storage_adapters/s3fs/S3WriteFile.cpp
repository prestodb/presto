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

#include "velox/connectors/hive/storage_adapters/s3fs/S3WriteFile.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Counters.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "velox/dwio/common/DataBuffer.h"

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

namespace facebook::velox::filesystems {

class S3WriteFile::Impl {
 public:
  explicit Impl(
      std::string_view path,
      Aws::S3::S3Client* client,
      memory::MemoryPool* pool)
      : client_(client), pool_(pool) {
    VELOX_CHECK_NOT_NULL(client);
    VELOX_CHECK_NOT_NULL(pool);
    getBucketAndKeyFromPath(path, bucket_, key_);
    currentPart_ = std::make_unique<dwio::common::DataBuffer<char>>(*pool_);
    currentPart_->reserve(kPartUploadSize);
    // Check that the object doesn't exist, if it does throw an error.
    {
      Aws::S3::Model::HeadObjectRequest request;
      request.SetBucket(awsString(bucket_));
      request.SetKey(awsString(key_));
      RECORD_METRIC_VALUE(kMetricS3MetadataCalls);
      auto objectMetadata = client_->HeadObject(request);
      if (!objectMetadata.IsSuccess()) {
        RECORD_METRIC_VALUE(kMetricS3GetMetadataErrors);
      }
      RECORD_METRIC_VALUE(
          kMetricS3GetObjectRetries, objectMetadata.GetRetryCount());
      VELOX_CHECK(!objectMetadata.IsSuccess(), "S3 object already exists");
    }

    // Create bucket if not present.
    {
      Aws::S3::Model::HeadBucketRequest request;
      request.SetBucket(awsString(bucket_));
      auto bucketMetadata = client_->HeadBucket(request);
      if (!bucketMetadata.IsSuccess()) {
        Aws::S3::Model::CreateBucketRequest request;
        request.SetBucket(bucket_);
        auto outcome = client_->CreateBucket(request);
        VELOX_CHECK_AWS_OUTCOME(
            outcome, "Failed to create S3 bucket", bucket_, "");
      }
    }

    // Initiate the multi-part upload.
    {
      Aws::S3::Model::CreateMultipartUploadRequest request;
      request.SetBucket(awsString(bucket_));
      request.SetKey(awsString(key_));

      /// If we do not set anything then the SDK will default to application/xml
      /// which confuses some tools
      /// (https://github.com/apache/arrow/issues/11934). So we instead default
      /// to application/octet-stream which is less misleading.
      request.SetContentType(kApplicationOctetStream);
      // The default algorithm used is MD5. However, MD5 is not supported with
      // fips and can cause a SIGSEGV. Set CRC32 instead which is a standard for
      // checksum computation and is not restricted by fips.
      request.SetChecksumAlgorithm(Aws::S3::Model::ChecksumAlgorithm::CRC32);

      auto outcome = client_->CreateMultipartUpload(request);
      VELOX_CHECK_AWS_OUTCOME(
          outcome, "Failed initiating multiple part upload", bucket_, key_);
      uploadState_.id = outcome.GetResult().GetUploadId();
    }

    fileSize_ = 0;
  }

  // Appends data to the end of the file.
  void append(std::string_view data) {
    VELOX_CHECK(!closed(), "File is closed");
    if (data.size() + currentPart_->size() >= kPartUploadSize) {
      upload(data);
    } else {
      // Append to current part.
      currentPart_->unsafeAppend(data.data(), data.size());
    }
    fileSize_ += data.size();
  }

  // No-op.
  void flush() {
    VELOX_CHECK(!closed(), "File is closed");
    /// currentPartSize must be less than kPartUploadSize since
    /// append() would have already flushed after reaching kUploadPartSize.
    VELOX_CHECK_LT(currentPart_->size(), kPartUploadSize);
  }

  // Complete the multipart upload and close the file.
  void close() {
    if (closed()) {
      return;
    }
    RECORD_METRIC_VALUE(kMetricS3StartedUploads);
    uploadPart({currentPart_->data(), currentPart_->size()}, true);
    VELOX_CHECK_EQ(uploadState_.partNumber, uploadState_.completedParts.size());
    // Complete the multipart upload.
    {
      Aws::S3::Model::CompletedMultipartUpload completedUpload;
      completedUpload.SetParts(uploadState_.completedParts);
      Aws::S3::Model::CompleteMultipartUploadRequest request;
      request.SetBucket(awsString(bucket_));
      request.SetKey(awsString(key_));
      request.SetUploadId(uploadState_.id);
      request.SetMultipartUpload(std::move(completedUpload));

      auto outcome = client_->CompleteMultipartUpload(request);
      if (outcome.IsSuccess()) {
        RECORD_METRIC_VALUE(kMetricS3SuccessfulUploads);
      } else {
        RECORD_METRIC_VALUE(kMetricS3FailedUploads);
      }
      VELOX_CHECK_AWS_OUTCOME(
          outcome, "Failed to complete multiple part upload", bucket_, key_);
    }
    currentPart_->clear();
  }

  // Current file size, i.e. the sum of all previous appends.
  uint64_t size() const {
    return fileSize_;
  }

  int numPartsUploaded() const {
    return uploadState_.partNumber;
  }

 private:
  static constexpr int64_t kPartUploadSize = 10 * 1024 * 1024;
  static constexpr const char* kApplicationOctetStream =
      "application/octet-stream";

  bool closed() const {
    return (currentPart_->capacity() == 0);
  }

  // Holds state for the multipart upload.
  struct UploadState {
    Aws::Vector<Aws::S3::Model::CompletedPart> completedParts;
    int64_t partNumber = 0;
    Aws::String id;
  };
  UploadState uploadState_;

  // Data can be smaller or larger than the kPartUploadSize.
  // Complete the currentPart_ and upload kPartUploadSize chunks of data.
  // Save the remaining into currentPart_.
  void upload(const std::string_view data) {
    auto dataPtr = data.data();
    auto dataSize = data.size();
    // Fill-up the remaining currentPart_.
    auto remainingBufferSize = currentPart_->capacity() - currentPart_->size();
    currentPart_->unsafeAppend(dataPtr, remainingBufferSize);
    uploadPart({currentPart_->data(), currentPart_->size()});
    dataPtr += remainingBufferSize;
    dataSize -= remainingBufferSize;
    while (dataSize > kPartUploadSize) {
      uploadPart({dataPtr, kPartUploadSize});
      dataPtr += kPartUploadSize;
      dataSize -= kPartUploadSize;
    }
    // Stash the remaining at the beginning of currentPart.
    currentPart_->unsafeAppend(0, dataPtr, dataSize);
  }

  void uploadPart(const std::string_view part, bool isLast = false) {
    // Only the last part can be less than kPartUploadSize.
    VELOX_CHECK(isLast || (!isLast && (part.size() == kPartUploadSize)));
    // Upload the part.
    {
      Aws::S3::Model::UploadPartRequest request;
      request.SetBucket(bucket_);
      request.SetKey(key_);
      request.SetUploadId(uploadState_.id);
      request.SetPartNumber(++uploadState_.partNumber);
      request.SetContentLength(part.size());
      request.SetBody(
          std::make_shared<StringViewStream>(part.data(), part.size()));
      // The default algorithm used is MD5. However, MD5 is not supported with
      // fips and can cause a SIGSEGV. Set CRC32 instead which is a standard for
      // checksum computation and is not restricted by fips.
      request.SetChecksumAlgorithm(Aws::S3::Model::ChecksumAlgorithm::CRC32);
      auto outcome = client_->UploadPart(request);
      VELOX_CHECK_AWS_OUTCOME(outcome, "Failed to upload", bucket_, key_);
      // Append ETag and part number for this uploaded part.
      // This will be needed for upload completion in Close().
      auto result = outcome.GetResult();
      Aws::S3::Model::CompletedPart part;

      part.SetPartNumber(uploadState_.partNumber);
      part.SetETag(result.GetETag());
      // Don't add the checksum to the part if the checksum is empty.
      // Some filesystems such as IBM COS require this to be not set.
      if (!result.GetChecksumCRC32().empty()) {
        part.SetChecksumCRC32(result.GetChecksumCRC32());
      }
      uploadState_.completedParts.push_back(std::move(part));
    }
  }

  Aws::S3::S3Client* client_;
  memory::MemoryPool* pool_;
  std::unique_ptr<dwio::common::DataBuffer<char>> currentPart_;
  std::string bucket_;
  std::string key_;
  size_t fileSize_ = -1;
};

S3WriteFile::S3WriteFile(
    std::string_view path,
    Aws::S3::S3Client* client,
    memory::MemoryPool* pool) {
  impl_ = std::make_shared<Impl>(path, client, pool);
}

void S3WriteFile::append(std::string_view data) {
  return impl_->append(data);
}

void S3WriteFile::flush() {
  impl_->flush();
}

void S3WriteFile::close() {
  impl_->close();
}

uint64_t S3WriteFile::size() const {
  return impl_->size();
}

int S3WriteFile::numPartsUploaded() const {
  return impl_->numPartsUploaded();
}

} // namespace facebook::velox::filesystems
