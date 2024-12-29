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

#include <string>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/FileSystems.h"

namespace facebook::velox::tests::utils {
using namespace filesystems;

/// Defines the base class for file and filesystem operations
struct BaseFaultOperation {
  explicit BaseFaultOperation(const std::string& _path) : path(_path) {}
  /// The delegated file path.
  const std::string path;

  /// Indicates to forward this operation to the delegated file or not. If not,
  /// then the file fault injection hook must have processed the request. For
  /// instance, if this is a file read injection, then the hook must have filled
  /// the fake read data for data corruption tests.
  bool delegate{true};
};

/// Defines the per-file operation fault injection.
struct FaultFileOperation : public BaseFaultOperation {
  enum class Type {
    /// Injects faults for file read operations.
    kRead,
    kReadv,
    kWrite,
    kAppend,
    /// TODO: add to support fault injections for the other file operation
    /// types.
  };
  static std::string typeString(FaultFileOperation::Type type) {
    switch (type) {
      case FaultFileOperation::Type::kReadv:
        return "READV";
      case FaultFileOperation::Type::kRead:
        return "READ";
      case FaultFileOperation::Type::kWrite:
        return "WRITE";
      case FaultFileOperation::Type::kAppend:
        return "APPEND";
      default:
        VELOX_UNSUPPORTED(
            "Unknown file operation type: {}", static_cast<int>(type));
        break;
    }
  }

  FaultFileOperation(Type _type, const std::string& _path)
      : BaseFaultOperation(_path), type(_type) {}

  const Type type;
};

/// Defines the per-filesystem operation fault injection.
struct FaultFileSystemOperation : public BaseFaultOperation {
  enum class Type {
    kMkdir = 0,
    /// TODO: Add support for fileSystem operations.
  };
  FaultFileSystemOperation(Type _type, const std::string& _path)
      : BaseFaultOperation(_path), type(_type) {}

  static std::string typeString(FaultFileSystemOperation::Type type) {
    switch (type) {
      case FaultFileSystemOperation::Type::kMkdir:
        return "mkdir";
      default:
        VELOX_UNSUPPORTED(
            "Unknown filesystem operation type: {}", static_cast<int>(type));
    }
  }

  const Type type;
};

/// The fault injection hook on the file operation path.
using FileFaultInjectionHook = std::function<void(FaultFileOperation*)>;

/// The fault injection hook on the file operation path.
using FileSystemFaultInjectionHook =
    std::function<void(FaultFileSystemOperation*)>;

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

/// Fault injection parameters for file append API.
struct FaultFileAppendOperation : FaultFileOperation {
  std::string_view* data;

  FaultFileAppendOperation(
      const std::string& _path,
      const std::string_view& _data)
      : FaultFileOperation(FaultFileOperation::Type::kAppend, _path),
        data(const_cast<std::string_view*>(&_data)) {}
};

/// Fault injection parameters for file write API.
struct FaultFileWriteOperation : FaultFileOperation {
  const std::vector<iovec>& iovecs;
  int64_t offset;
  int64_t length;

  FaultFileWriteOperation(
      const std::string& _path,
      const std::vector<iovec>& _iovecs,
      int64_t _offset,
      int64_t _length)
      : FaultFileOperation(FaultFileOperation::Type::kWrite, _path),
        iovecs(_iovecs),
        offset(_offset),
        length(_length) {}
};

/// Fault injection parameters for file system mkdir API.
struct FaultFileSystemMkdirOperation : FaultFileSystemOperation {
  DirectoryOptions options;

  FaultFileSystemMkdirOperation(
      const std::string& _path,
      const DirectoryOptions& _options)
      : FaultFileSystemOperation(Type::kMkdir, _path), options(_options) {}
};
} // namespace facebook::velox::tests::utils
