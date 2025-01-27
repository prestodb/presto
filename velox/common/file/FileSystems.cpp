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

#include "velox/common/file/FileSystems.h"
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/CallOnce.h>
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"

#include <cstdio>
#include <filesystem>

namespace facebook::velox::filesystems {

namespace {

constexpr std::string_view kFileScheme("file:");

using RegisteredFileSystems = std::vector<std::pair<
    std::function<bool(std::string_view)>,
    std::function<std::shared_ptr<FileSystem>(
        std::shared_ptr<const config::ConfigBase>,
        std::string_view)>>>;

RegisteredFileSystems& registeredFileSystems() {
  // Meyers singleton.
  static RegisteredFileSystems* fss = new RegisteredFileSystems();
  return *fss;
}

} // namespace

void registerFileSystem(
    std::function<bool(std::string_view)> schemeMatcher,
    std::function<std::shared_ptr<FileSystem>(
        std::shared_ptr<const config::ConfigBase>,
        std::string_view)> fileSystemGenerator) {
  registeredFileSystems().emplace_back(schemeMatcher, fileSystemGenerator);
}

std::shared_ptr<FileSystem> getFileSystem(
    std::string_view filePath,
    std::shared_ptr<const config::ConfigBase> properties) {
  const auto& filesystems = registeredFileSystems();
  for (const auto& p : filesystems) {
    if (p.first(filePath)) {
      return p.second(properties, filePath);
    }
  }
  VELOX_FAIL("No registered file system matched with file path '{}'", filePath);
}

bool isPathSupportedByRegisteredFileSystems(const std::string_view& filePath) {
  const auto& filesystems = registeredFileSystems();
  for (const auto& p : filesystems) {
    if (p.first(filePath)) {
      return true;
    }
  }
  return false;
}

namespace {

folly::once_flag localFSInstantiationFlag;

// Implement Local FileSystem.
class LocalFileSystem : public FileSystem {
 public:
  LocalFileSystem(
      std::shared_ptr<const config::ConfigBase> config,
      const FileSystemOptions& options)
      : FileSystem(config),
        executor_(
            options.readAheadEnabled
                ? std::make_unique<folly::CPUThreadPoolExecutor>(
                      std::max(
                          1,
                          static_cast<int32_t>(
                              std::thread::hardware_concurrency() / 2)),
                      std::make_shared<folly::NamedThreadFactory>(
                          "LocalReadahead"))
                : nullptr) {}

  ~LocalFileSystem() override {
    if (executor_) {
      executor_->stop();
      LOG(INFO) << "Executor " << executor_->getName() << " stopped.";
    }
  }

  std::string name() const override {
    return "Local FS";
  }

  inline std::string_view extractPath(std::string_view path) const override {
    if (path.find(kFileScheme) == 0) {
      return path.substr(kFileScheme.length());
    }
    return path;
  }

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const FileOptions& options) override {
    return std::make_unique<LocalReadFile>(
        extractPath(path), executor_.get(), options.bufferIo);
  }

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const FileOptions& options) override {
    return std::make_unique<LocalWriteFile>(
        extractPath(path),
        options.shouldCreateParentDirectories,
        options.shouldThrowOnFileAlreadyExists,
        options.bufferIo);
  }

  void remove(std::string_view path) override {
    auto file = extractPath(path);
    int32_t rc = std::remove(std::string(file).c_str());
    if (rc < 0 && std::filesystem::exists(file)) {
      VELOX_USER_FAIL(
          "Failed to delete file {} with errno {}", file, strerror(errno));
    }
    VLOG(1) << "LocalFileSystem::remove " << path;
  }

  void rename(
      std::string_view oldPath,
      std::string_view newPath,
      bool overwrite) override {
    auto oldFile = extractPath(oldPath);
    auto newFile = extractPath(newPath);
    if (!overwrite && exists(newPath)) {
      VELOX_USER_FAIL(
          "Failed to rename file {} to {} with as {} exists.",
          oldFile,
          newFile,
          newFile);
      return;
    }
    int32_t rc =
        ::rename(std::string(oldFile).c_str(), std::string(newFile).c_str());
    if (rc != 0) {
      VELOX_USER_FAIL(
          "Failed to rename file {} to {} with errno {}",
          oldFile,
          newFile,
          folly::errnoStr(errno));
    }
    VLOG(1) << "LocalFileSystem::rename oldFile: " << oldFile
            << ", newFile:" << newFile;
  }

  bool exists(std::string_view path) override {
    const auto file = extractPath(path);
    return std::filesystem::exists(file);
  }

  bool isDirectory(std::string_view path) const override {
    const auto file = extractPath(path);
    return std::filesystem::is_directory(file);
  }

  virtual std::vector<std::string> list(std::string_view path) override {
    auto directoryPath = extractPath(path);
    const std::filesystem::path folder{directoryPath};
    std::vector<std::string> filePaths;
    for (auto const& entry : std::filesystem::directory_iterator{folder}) {
      filePaths.push_back(entry.path());
    }
    return filePaths;
  }

  void mkdir(std::string_view path, const DirectoryOptions& options) override {
    std::error_code ec;

    const bool created = std::filesystem::create_directories(path, ec);
    // This API is unlike POSIX for mkdir when the directory already exists
    // because in POSIX, the error_code will return EEXIST, but in this API, the
    // error_code will return 0. The indication for whether or not a directory
    // is created is based on the return boolean of create_directories. A value
    // of true indicates that the directory was created. Thus here, we check the
    // underlying mkdir call is successful, and then check if the directory was
    // created or not.
    if (ec.value() == 0 && !created && options.failIfExists) {
      VELOX_FAIL("Directory: {} already exists", path);
    }

    VELOX_CHECK_EQ(
        0,
        ec.value(),
        "Mkdir {} failed: {}, message: {}",
        std::string(path),
        ec.value(),
        ec.message());
    VLOG(1) << "LocalFileSystem::mkdir " << path;
  }

  void rmdir(std::string_view path) override {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    VELOX_CHECK_EQ(
        0,
        ec.value(),
        "Rmdir {} failed: {}, message: {}",
        std::string(path),
        ec.value(),
        ec.message());
    VLOG(1) << "LocalFileSystem::rmdir " << path;
  }

  static std::function<bool(std::string_view)> schemeMatcher() {
    // Note: presto behavior is to prefix local paths with 'file:'.
    // Check for that prefix and prune to absolute regular paths as needed.
    return [](std::string_view filePath) {
      return filePath.find("/") == 0 || filePath.find(kFileScheme) == 0;
    };
  }

  static std::function<std::shared_ptr<
      FileSystem>(std::shared_ptr<const config::ConfigBase>, std::string_view)>
  fileSystemGenerator(const FileSystemOptions& options) {
    return [options](
               std::shared_ptr<const config::ConfigBase> properties,
               std::string_view filePath) {
      // One instance of Local FileSystem is sufficient.
      // Initialize on first access and reuse after that.
      static std::shared_ptr<FileSystem> lfs;
      folly::call_once(localFSInstantiationFlag, [properties, options]() {
        lfs = std::make_shared<LocalFileSystem>(properties, options);
      });
      return lfs;
    };
  }

 private:
  const std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
};
} // namespace

void registerLocalFileSystem(const FileSystemOptions& options) {
  registerFileSystem(
      LocalFileSystem::schemeMatcher(),
      LocalFileSystem::fileSystemGenerator(options));
}
} // namespace facebook::velox::filesystems
