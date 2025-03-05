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
#include "velox/common/base/RuntimeMetrics.h"
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
namespace filesystems::File {
class IoStats;
}
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

  /// Whether to buffer the data in file system client or not. For local
  /// filesystem on Unix-like operating system, this corresponds to the direct
  /// IO mode if set.
  bool bufferIo{true};

  /// Property bag to set onto files/directories. Think something similar to
  /// ioctl(2). For other remote filesystems, this can be PutObjectTagging in
  /// S3.
  std::optional<std::unordered_map<std::string, std::string>> properties{
      std::nullopt};

  File::IoStats* stats{nullptr};

  /// A raw string that client can encode as anything they want to describe the
  /// file. For example, extraFileInfo can contain serialized file descriptors
  /// or other specific backend filesystem metadata can be used during for a
  /// more optimized lookup.
  std::shared_ptr<std::string> extraFileInfo{nullptr};

  /// A hint to the file system for which region size of the file should be
  /// read. Specifically, the read length.
  std::optional<int64_t> readRangeHint{std::nullopt};
};

/// Defines directory options
struct DirectoryOptions : FileOptions {
  /// Whether to throw an error if the directory already exists.
  /// For POSIX systems, this is equivalent to handling EEXIST.
  ///
  /// NOTE: This is only applicable for mkdir
  bool failIfExists{false};

  /// This is similar to kFileCreateConfig
  static constexpr folly::StringPiece kMakeDirectoryConfig{
      "make-directory-config"};
};

struct FileSystemOptions {
  /// As for now, only local file system respects this option. It implements
  /// async read by using a background cpu executor. Some filesystem might has
  /// native async read-ahead support.
  bool readAheadEnabled{false};
};

/// Free form statistics for a file system. The keys are arbitrary strings, and
/// values are RuntimeMetric. The underlying filesystem implementation can use
/// this class to record observability about filesystem operations.
namespace File {
class IoStats {
 public:
  IoStats() = default;

  void addCounter(const std::string& name, RuntimeCounter counter) {
    auto locked = stats_.wlock();
    auto it = locked->find(name);
    if (it == locked->end()) {
      auto [ptr, inserted] = locked->emplace(name, RuntimeMetric(counter.unit));
      VELOX_CHECK(inserted);
      ptr->second.addValue(counter.value);
    } else {
      VELOX_CHECK_EQ(it->second.unit, counter.unit);
      it->second.addValue(counter.value);
    }
  }

  void merge(const IoStats& other) {
    auto otherStats = other.stats();
    auto locked = stats_.wlock();
    for (const auto& [name, metric] : otherStats) {
      auto it = locked->find(name);
      if (it == locked->end()) {
        locked->emplace(name, metric);
      } else {
        it->second.merge(metric);
      }
    }
  }

  folly::F14FastMap<std::string, RuntimeMetric> stats() const {
    return stats_.copy();
  }

 private:
  folly::Synchronized<folly::F14FastMap<std::string, RuntimeMetric>> stats_;
};
} // namespace File

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
  virtual std::string_view extractPath(std::string_view path) const {
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

  /// Returns true if it is a directory.
  virtual bool isDirectory(std::string_view path) const {
    VELOX_UNSUPPORTED("isDirectory not implemented");
  }

  /// Returns the list of files or folders in a path. Currently, this method
  /// will be used for testing, but we will need change this to an iterator
  /// output method to avoid potential heavy output if there are many entries in
  /// the folder.
  virtual std::vector<std::string> list(std::string_view path) = 0;

  /// Create a directory (recursively). Throws velox exception on failure.
  virtual void mkdir(
      std::string_view path,
      const DirectoryOptions& options = {}) = 0;

  /// Remove a directory (all the files and sub-directories underneath
  /// recursively). Throws velox exception on failure.
  virtual void rmdir(std::string_view path) = 0;

  /// Sets the property for a directory. The user provides the key-value pairs
  /// inside 'properties' field of options. Throws velox exception on failure.
  virtual void setDirectoryProperty(
      std::string_view /*path*/,
      const DirectoryOptions& options = {}) {
    VELOX_UNSUPPORTED("setDirectoryProperty not implemented");
  }

  /// Gets the property for a directory. If no property is found, std::nullopt
  /// is returned. Throws velox exception on failure.
  virtual std::optional<std::string> getDirectoryProperty(
      std::string_view /*path*/,
      std::string_view /*propertyKey*/) {
    VELOX_UNSUPPORTED("getDirectoryProperty not implemented");
  }

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
void registerLocalFileSystem(
    const FileSystemOptions& options = FileSystemOptions());

} // namespace facebook::velox::filesystems
