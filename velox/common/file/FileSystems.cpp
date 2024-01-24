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
    std::function<std::shared_ptr<
        FileSystem>(std::shared_ptr<const Config>, std::string_view)>>>;

RegisteredFileSystems& registeredFileSystems() {
  // Meyers singleton.
  static RegisteredFileSystems* fss = new RegisteredFileSystems();
  return *fss;
}

} // namespace

void registerFileSystem(
    std::function<bool(std::string_view)> schemeMatcher,
    std::function<std::shared_ptr<FileSystem>(
        std::shared_ptr<const Config>,
        std::string_view)> fileSystemGenerator) {
  registeredFileSystems().emplace_back(schemeMatcher, fileSystemGenerator);
}

std::shared_ptr<FileSystem> getFileSystem(
    std::string_view filePath,
    std::shared_ptr<const Config> properties) {
  const auto& filesystems = registeredFileSystems();
  for (const auto& p : filesystems) {
    if (p.first(filePath)) {
      return p.second(properties, filePath);
    }
  }
  VELOX_FAIL("No registered file system matched with file path '{}'", filePath);
}

namespace {

folly::once_flag localFSInstantiationFlag;

// Implement Local FileSystem.
class LocalFileSystem : public FileSystem {
 public:
  explicit LocalFileSystem(std::shared_ptr<const Config> config)
      : FileSystem(config) {}

  ~LocalFileSystem() override {}

  std::string name() const override {
    return "Local FS";
  }

  inline std::string_view extractPath(std::string_view path) {
    if (path.find(kFileScheme) == 0) {
      return path.substr(kFileScheme.length());
    }
    return path;
  }

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const FileOptions& /*unused*/) override {
    return std::make_unique<LocalReadFile>(extractPath(path));
  }

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const FileOptions& /*unused*/) override {
    return std::make_unique<LocalWriteFile>(extractPath(path));
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
    auto file = extractPath(path);
    return std::filesystem::exists(file);
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

  void mkdir(std::string_view path) override {
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
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
      FileSystem>(std::shared_ptr<const Config>, std::string_view)>
  fileSystemGenerator() {
    return [](std::shared_ptr<const Config> properties,
              std::string_view filePath) {
      // One instance of Local FileSystem is sufficient.
      // Initialize on first access and reuse after that.
      static std::shared_ptr<FileSystem> lfs;
      folly::call_once(localFSInstantiationFlag, [&properties]() {
        lfs = std::make_shared<LocalFileSystem>(properties);
      });
      return lfs;
    };
  }
};
} // namespace

void registerLocalFileSystem() {
  registerFileSystem(
      LocalFileSystem::schemeMatcher(), LocalFileSystem::fileSystemGenerator());
}
} // namespace facebook::velox::filesystems
