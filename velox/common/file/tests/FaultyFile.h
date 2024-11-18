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

namespace facebook::velox::tests::utils {

/// Defines the per-file operation fault injection.
struct FaultFileOperation {
  enum class Type {
    /// Injects faults for file read operations.
    kRead,
    kReadv,
    kWrite,
    /// TODO: add to support fault injections for the other file operation
    /// types.
  };
  static std::string typeString(Type type);

  const Type type;

  /// The delegated file path.
  const std::string path;

  /// Indicates to forward this operation to the delegated file or not. If not,
  /// then the file fault injection hook must have processed the request. For
  /// instance, if this is a file read injection, then the hook must have filled
  /// the fake read data for data corruption tests.
  bool delegate{true};

  FaultFileOperation(Type _type, const std::string& _path)
      : type(_type), path(_path) {}
};

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& o,
    const FaultFileOperation::Type& type) {
  return o << FaultFileOperation::typeString(type);
}

/// Fault injection parameters for file read API.
struct FaultFileReadOperation : FaultFileOperation {
  const uint64_t offset;
  const uint64_t length;
  void* const buf;

  FaultFileReadOperation(
      const std::string& _path,
      uint64_t _offset,
      uint64_t _length,
      void* _buf)
      : FaultFileOperation(FaultFileOperation::Type::kRead, _path),
        offset(_offset),
        length(_length),
        buf(_buf) {}
};

/// Fault injection parameters for file readv API.
struct FaultFileReadvOperation : FaultFileOperation {
  const uint64_t offset;
  const std::vector<folly::Range<char*>>& buffers;
  uint64_t readBytes{0};

  FaultFileReadvOperation(
      const std::string& _path,
      uint64_t _offset,
      const std::vector<folly::Range<char*>>& _buffers)
      : FaultFileOperation(FaultFileOperation::Type::kReadv, _path),
        offset(_offset),
        buffers(_buffers) {}
};

/// Fault injection parameters for file write API.
struct FaultFileWriteOperation : FaultFileOperation {
  std::string_view* data;

  FaultFileWriteOperation(
      const std::string& _path,
      const std::string_view& _data)
      : FaultFileOperation(FaultFileOperation::Type::kWrite, _path),
        data(const_cast<std::string_view*>(&_data)) {}
};

/// The fault injection hook on the file operation path.
using FileFaultInjectionHook = std::function<void(FaultFileOperation*)>;

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

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const override;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override;

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
      const std::vector<folly::Range<char*>>& buffers) const override;

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

  uint64_t size() const override {
    return delegatedFile_->size();
  }

 private:
  const std::string path_;
  const std::shared_ptr<WriteFile> delegatedFile_;
  const FileFaultInjectionHook injectionHook_;
};

} // namespace facebook::velox::tests::utils
