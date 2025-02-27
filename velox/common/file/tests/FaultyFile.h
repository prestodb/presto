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
#include "velox/common/file/tests/FaultyFileSystemOperations.h"

namespace facebook::velox::tests::utils {

class FaultyReadFile : public ReadFile {
 public:
  FaultyReadFile(
      const std::string& path,
      std::shared_ptr<ReadFile> delegatedFile,
      FileFaultInjectionHook injectionHook,
      folly::Executor* executor);

  ~FaultyReadFile() override{};

  uint64_t size() const override {
    return delegatedFile_->size();
  }

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      filesystems::File::IoStats* stats = nullptr) const override;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      filesystems::File::IoStats* stats = nullptr) const override;

  uint64_t memoryUsage() const override {
    return delegatedFile_->memoryUsage();
  }

  bool shouldCoalesce() const override {
    return delegatedFile_->shouldCoalesce();
  }

  std::string getName() const override {
    return delegatedFile_->getName();
  }

  uint64_t getNaturalReadSize() const override {
    return delegatedFile_->getNaturalReadSize();
  }

  bool hasPreadvAsync() const override {
    if (executor_ != nullptr) {
      return true;
    }
    return delegatedFile_->hasPreadvAsync();
  }

  folly::SemiFuture<uint64_t> preadvAsync(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      filesystems::File::IoStats* stats = nullptr) const override;

 private:
  const std::string path_;
  const std::shared_ptr<ReadFile> delegatedFile_;
  const FileFaultInjectionHook injectionHook_;
  folly::Executor* const executor_;
};

class FaultyWriteFile : public WriteFile {
 public:
  FaultyWriteFile(
      const std::string& path,
      std::shared_ptr<WriteFile> delegatedFile,
      FileFaultInjectionHook injectionHook);

  ~FaultyWriteFile() override{};

  void append(std::string_view data) override;

  void append(std::unique_ptr<folly::IOBuf> data) override;

  void write(const std::vector<iovec>& iovecs, int64_t offset, int64_t length)
      override;

  void truncate(int64_t newSize) override;

  void flush() override;

  void setAttributes(
      const std::unordered_map<std::string, std::string>& attributes) override;

  std::unordered_map<std::string, std::string> getAttributes() const override;

  void close() override;

  uint64_t size() const override;

  const std::string getName() const override;

 private:
  const std::string path_;
  const std::shared_ptr<WriteFile> delegatedFile_;
  const FileFaultInjectionHook injectionHook_;
};

} // namespace facebook::velox::tests::utils
