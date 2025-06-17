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

#include <google/cloud/storage/client.h>
#include "velox/common/file/File.h"

namespace facebook::velox::filesystems {

/**
 * Implementation of gcs read file.
 */
class GcsReadFile : public ReadFile {
 public:
  GcsReadFile(
      const std::string& path,
      std::shared_ptr<::google::cloud::storage::Client> client);

  ~GcsReadFile() override;

  void initialize(const filesystems::FileOptions& options);

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buffer,
      filesystems::File::IoStats* stats = nullptr) const override;

  std::string pread(
      uint64_t offset,
      uint64_t length,
      filesystems::File::IoStats* stats = nullptr) const override;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      filesystems::File::IoStats* stats = nullptr) const override;

  uint64_t size() const override;

  uint64_t memoryUsage() const override;

  bool shouldCoalesce() const final {
    return false;
  }

  std::string getName() const override;

  uint64_t getNaturalReadSize() const override;

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace facebook::velox::filesystems
