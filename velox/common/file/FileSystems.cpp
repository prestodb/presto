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
#include "velox/core/Context.h"

#include <cstdio>

namespace facebook::velox::filesystems {

namespace {

constexpr std::string_view kFileScheme("file:");

using RegisteredFileSystems = std::vector<std::pair<
    std::function<bool(std::string_view)>,
    std::function<std::shared_ptr<FileSystem>(std::shared_ptr<const Config>)>>>;

RegisteredFileSystems& registeredFileSystems() {
  // Meyers singleton.
  static RegisteredFileSystems* fss = new RegisteredFileSystems();
  return *fss;
}

} // namespace

void registerFileSystem(
    std::function<bool(std::string_view)> schemeMatcher,
    std::function<std::shared_ptr<FileSystem>(std::shared_ptr<const Config>)>
        fileSystemGenerator) {
  registeredFileSystems().emplace_back(schemeMatcher, fileSystemGenerator);
}

std::shared_ptr<FileSystem> getFileSystem(
    std::string_view filename,
    std::shared_ptr<const Config> properties) {
  const auto& filesystems = registeredFileSystems();
  for (const auto& p : filesystems) {
    if (p.first(filename)) {
      return p.second(properties);
    }
  }
  VELOX_FAIL("No registered file system matched with filename '{}'", filename);
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

  std::unique_ptr<ReadFile> openFileForRead(std::string_view path) override {
    if (path.find(kFileScheme) == 0) {
      return std::make_unique<LocalReadFile>(path.substr(kFileScheme.length()));
    }
    return std::make_unique<LocalReadFile>(path);
  }

  std::unique_ptr<WriteFile> openFileForWrite(std::string_view path) override {
    if (path.find(kFileScheme) == 0) {
      return std::make_unique<LocalWriteFile>(
          path.substr(kFileScheme.length()));
    }
    return std::make_unique<LocalWriteFile>(path);
  }

  void remove(std::string_view path) override {
    auto file =
        path.find(kFileScheme) == 0 ? path.substr(kFileScheme.length()) : path;
    int32_t rc = ::remove(std::string(file).c_str());
    if (rc < 0) {
      VELOX_USER_FAIL("Failed to delete file {} with errno {}", file, errno);
    }
  }

  static std::function<bool(std::string_view)> schemeMatcher() {
    // Note: presto behavior is to prefix local paths with 'file:'.
    // Check for that prefix and prune to absolute regular paths as needed.
    return [](std::string_view filename) {
      return filename.find("/") == 0 || filename.find(kFileScheme) == 0;
    };
  }

  static std::function<
      std::shared_ptr<FileSystem>(std::shared_ptr<const Config>)>
  fileSystemGenerator() {
    return [](std::shared_ptr<const Config> properties) {
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
