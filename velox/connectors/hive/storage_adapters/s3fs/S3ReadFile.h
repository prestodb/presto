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

namespace Aws::S3 {
class S3Client;
}

namespace facebook::velox::filesystems {

/// Implementation of s3 read file.
class S3ReadFile : public ReadFile {
 public:
  S3ReadFile(std::string_view path, Aws::S3::S3Client* client);

  ~S3ReadFile() override;

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      filesystems::File::IoStats* stats = nullptr) const final;

  std::string pread(
      uint64_t offset,
      uint64_t length,
      filesystems::File::IoStats* stats = nullptr) const final;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      filesystems::File::IoStats* stats = nullptr) const final;

  uint64_t size() const final;

  uint64_t memoryUsage() const final;

  bool shouldCoalesce() const final;

  std::string getName() const final;

  uint64_t getNaturalReadSize() const final {
    return 72 << 20;
  }

  void initialize(const filesystems::FileOptions& options);

 private:
  void preadInternal(uint64_t offset, uint64_t length, char* position) const;

  class Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace facebook::velox::filesystems
