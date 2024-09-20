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

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryPool.h"

#include <functional>
#include <memory>
#include <string_view>

namespace facebook::velox {
namespace config {
class ConfigBase;
}
class ReadFile;
class WriteFile;
} // namespace facebook::velox

namespace facebook::velox::filesystems {

/// Defines the options for per-file operations. It contains a key-value pairs
/// which can be easily extended to different storage systems.
/// MemoryPool to allocate buffers needed to read/write files on FileSystems
/// such as S3.
struct FileOptions {
  /// A free form option in 'values' that is provided for file creation. The
  /// form should be defined by specific implementations of file system. e.g.
  /// inside this property there could be things like block size, encoding, and
  /// etc.
  static constexpr folly::StringPiece kFileCreateConfig{"file-create-config"};

  std::unordered_map<std::string, std::string> values;
  memory::MemoryPool* pool{nullptr};
  /// If specified then can be trusted to be the file size.
  std::optional<int64_t> fileSize;

  /// Whether to create parent directories if they don't exist.
  ///
  /// NOTE: this only applies for write open file.
  bool shouldCreateParentDirectories{false};

  /// Whether to throw an error if a file already exists.
  ///
  /// NOTE: this only applies for write open file.
  bool shouldThrowOnFileAlreadyExists{true};

  /// Whether to buffer the write data in file system client or not. For local
  /// filesystem on Unix-like operating system, this corresponds to the direct
  /// IO mode if set.
  ///
  /// NOTE: this only applies for write open file.
  bool bufferWrite{true};
};

/// An abstract FileSystem
class FileSystem {
 public:
  FileSystem(std::shared_ptr<const config::ConfigBase> config)
      : config_(std::move(config)) {}
  virtual ~FileSystem() = default;

  /// Returns the name of the File System
  virtual std::string name() const = 0;

  /// Returns the file path without the fs scheme prefix such as "local:" prefix
  /// for local file system.
  virtual std::string_view extractPath(std::string_view path) {
    VELOX_NYI();
  }

  /// Returns a ReadFile handle for a given file path
  virtual std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const FileOptions& options = {}) = 0;

  /// Returns a WriteFile handle for a given file path
  virtual std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const FileOptions& options = {}) = 0;

  /// Deletes the file at 'path'. Throws on error.
  virtual void remove(std::string_view path) = 0;

  /// Rename the file at 'path' to `newpath`. Throws on error. If 'overwrite' is
  /// true, then rename does overwrite if file at 'newPath' already exists.
  /// Throws a velox user exception on error.
  virtual void rename(
      std::string_view oldPath,
      std::string_view newPath,
      bool overwrite = false) = 0;

  /// Returns true if the file exists.
  virtual bool exists(std::string_view path) = 0;

  /// Returns the list of files or folders in a path. Currently, this method
  /// will be used for testing, but we will need change this to an iterator
  /// output method to avoid potential heavy output if there are many entries in
  /// the folder.
  virtual std::vector<std::string> list(std::string_view path) = 0;

  /// Create a directory (recursively). Throws velox exception on failure.
  virtual void mkdir(std::string_view path) = 0;

  /// Remove a directory (all the files and sub-directories underneath
  /// recursively). Throws velox exception on failure.
  virtual void rmdir(std::string_view path) = 0;

 protected:
  std::shared_ptr<const config::ConfigBase> config_;
};

std::shared_ptr<FileSystem> getFileSystem(
    std::string_view filename,
    std::shared_ptr<const config::ConfigBase> config);

/// Returns true if filePath is supported by any registered file system,
/// otherwise false.
bool isPathSupportedByRegisteredFileSystems(const std::string_view& filePath);

/// FileSystems must be registered explicitly.
/// The registration function takes two parameters:
/// a std::function<bool(std::string_view)> that says whether the registered
/// FileSystem subclass should be used for that filename, and a lambda that
/// generates the actual file system.
void registerFileSystem(
    std::function<bool(std::string_view)> schemeMatcher,
    std::function<std::shared_ptr<FileSystem>(
        std::shared_ptr<const config::ConfigBase>,
        std::string_view)> fileSystemGenerator);

/// Register the local filesystem.
void registerLocalFileSystem();

} // namespace facebook::velox::filesystems
