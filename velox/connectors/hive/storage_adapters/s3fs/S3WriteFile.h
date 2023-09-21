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

#include "velox/common/file/File.h"
#include "velox/common/memory/MemoryPool.h"

namespace Aws::S3 {
class S3Client;
}

namespace facebook::velox::filesystems {

/// S3WriteFile uses the Apache Arrow implementation as a reference.
/// AWS C++ SDK allows streaming writes via the MultiPart upload API.
/// Multipart upload allows you to upload a single object as a set of parts.
/// Each part is a contiguous portion of the object's data.
/// While AWS and Minio support different sizes for each
/// part (only requiring a minimum of 5MB), Cloudflare R2 requires that every
/// part be exactly equal (except for the last part). We set this to 10 MiB, so
/// that in combination with the maximum number of parts of 10,000, this gives a
/// file limit of 100k MiB (or about 98 GiB).
/// (see https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html)
/// (for rational, see: https://github.com/apache/arrow/issues/34363)
/// You can upload these object parts independently and in any order.
/// After all parts of your object are uploaded, Amazon S3 assembles these parts
/// and creates the object.
/// https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
/// https://github.com/apache/arrow/blob/main/cpp/src/arrow/filesystem/s3fs.cc
/// S3WriteFile is not thread-safe.
/// UploadPart is currently synchronous during append and flush.
/// TODO: Evaluate and add option for asynchronous part uploads.
/// TODO: Implement retry on failure.
class S3WriteFile : public WriteFile {
 public:
  S3WriteFile(
      const std::string& path,
      Aws::S3::S3Client* client,
      memory::MemoryPool* pool);

  /// Appends data to the end of the file.
  /// Uploads a part on reaching part size limit.
  void append(std::string_view data) override;

  /// No-op. Append handles the flush.
  void flush() override;

  /// Close the file. Any cleanup (disk flush, etc.) will be done here.
  void close() override;

  /// Current file size, i.e. the sum of all previous Appends.
  uint64_t size() const override;

  /// Return the number of parts uploaded so far.
  int numPartsUploaded() const;

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace facebook::velox::filesystems
