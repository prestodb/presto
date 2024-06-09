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

#include "velox/common/file/tests/FaultyFileSystem.h"
#include <folly/synchronization/CallOnce.h>

#include <filesystem>

namespace facebook::velox::tests::utils {
namespace {
// Constructs the faulty file path based on the delegated read file 'path'. It
// pre-appends the faulty file system scheme.
inline std::string faultyPath(const std::string& path) {
  return fmt::format("{}{}", FaultyFileSystem::scheme(), path);
}

std::function<bool(std::string_view)> schemeMatcher() {
  // Note: presto behavior is to prefix local paths with 'file:'.
  // Check for that prefix and prune to absolute regular paths as needed.
  return [](std::string_view filePath) {
    return filePath.find(FaultyFileSystem::scheme()) == 0;
  };
}

folly::once_flag faultFilesystemInitOnceFlag;

std::function<std::shared_ptr<
    FileSystem>(std::shared_ptr<const Config>, std::string_view)>
fileSystemGenerator() {
  return [](std::shared_ptr<const Config> properties,
            std::string_view /*unused*/) {
    // One instance of faulty FileSystem is sufficient. Initializes on first
    // access and reuse after that.
    static std::shared_ptr<FileSystem> lfs;
    folly::call_once(faultFilesystemInitOnceFlag, [&properties]() {
      lfs = std::make_shared<FaultyFileSystem>(std::move(properties));
    });
    return lfs;
  };
}
} // namespace

std::unique_ptr<ReadFile> FaultyFileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& options) {
  const std::string delegatedPath = std::string(extractPath(path));
  auto delegatedFile = getFileSystem(delegatedPath, config_)
                           ->openFileForRead(delegatedPath, options);
  return std::make_unique<FaultyReadFile>(
      std::string(path),
      std::move(delegatedFile),
      [&](FaultFileOperation* op) { maybeInjectFileFault(op); },
      executor_);
}

std::unique_ptr<WriteFile> FaultyFileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& options) {
  const std::string delegatedPath = std::string(extractPath(path));
  auto delegatedFile = getFileSystem(delegatedPath, config_)
                           ->openFileForWrite(delegatedPath, options);
  return std::make_unique<FaultyWriteFile>(
      std::string(path), std::move(delegatedFile), [&](FaultFileOperation* op) {
        maybeInjectFileFault(op);
      });
}

void FaultyFileSystem::remove(std::string_view path) {
  const std::string delegatedPath = std::string(extractPath(path));
  getFileSystem(delegatedPath, config_)->remove(delegatedPath);
}

void FaultyFileSystem::rename(
    std::string_view oldPath,
    std::string_view newPath,
    bool overwrite) {
  const auto delegatedOldPath = extractPath(oldPath);
  const auto delegatedNewPath = extractPath(newPath);
  getFileSystem(delegatedOldPath, config_)
      ->rename(delegatedOldPath, delegatedNewPath, overwrite);
}

bool FaultyFileSystem::exists(std::string_view path) {
  const auto delegatedPath = extractPath(path);
  return getFileSystem(delegatedPath, config_)->exists(delegatedPath);
}

std::vector<std::string> FaultyFileSystem::list(std::string_view path) {
  const auto delegatedDirPath = extractPath(path);
  const auto delegatedFiles =
      getFileSystem(delegatedDirPath, config_)->list(delegatedDirPath);
  // NOTE: we shall return the faulty file paths instead of the delegated file
  // paths for list result.
  std::vector<std::string> files;
  files.reserve(delegatedFiles.size());
  for (const auto& delegatedFile : delegatedFiles) {
    files.push_back(faultyPath(delegatedFile));
  }
  return files;
}

void FaultyFileSystem::mkdir(std::string_view path) {
  const auto delegatedDirPath = extractPath(path);
  getFileSystem(delegatedDirPath, config_)->mkdir(delegatedDirPath);
}

void FaultyFileSystem::rmdir(std::string_view path) {
  const auto delegatedDirPath = extractPath(path);
  getFileSystem(delegatedDirPath, config_)->rmdir(delegatedDirPath);
}

void FaultyFileSystem::setFileInjectionHook(
    FileFaultInjectionHook injectionHook) {
  std::lock_guard<std::mutex> l(mu_);
  fileInjections_ = FileInjections(std::move(injectionHook));
}

void FaultyFileSystem::setFileInjectionError(
    std::exception_ptr error,
    std::unordered_set<FaultFileOperation::Type> opTypes) {
  std::lock_guard<std::mutex> l(mu_);
  fileInjections_ = FileInjections(std::move(error), std::move(opTypes));
}

void FaultyFileSystem::setFileInjectionDelay(
    uint64_t delayUs,
    std::unordered_set<FaultFileOperation::Type> opTypes) {
  std::lock_guard<std::mutex> l(mu_);
  fileInjections_ = FileInjections(delayUs, std::move(opTypes));
}

void FaultyFileSystem::clearFileFaultInjections() {
  std::lock_guard<std::mutex> l(mu_);
  fileInjections_.reset();
}

void FaultyFileSystem::maybeInjectFileFault(FaultFileOperation* op) {
  FileInjections injections;
  {
    std::lock_guard<std::mutex> l(mu_);
    if (!fileInjections_.has_value()) {
      return;
    }
    injections = fileInjections_.value();
  }

  if (injections.fileInjectionHook != nullptr) {
    injections.fileInjectionHook(op);
    return;
  }

  if (!injections.opTypes.empty() && injections.opTypes.count(op->type) == 0) {
    return;
  }

  if (injections.fileException != nullptr) {
    std::rethrow_exception(injections.fileException);
  }

  if (injections.fileDelayUs != 0) {
    std::this_thread::sleep_for(
        std::chrono::microseconds(injections.fileDelayUs));
  }
}

void registerFaultyFileSystem() {
  registerFileSystem(schemeMatcher(), fileSystemGenerator());
}

std::shared_ptr<FaultyFileSystem> faultyFileSystem() {
  return std::dynamic_pointer_cast<FaultyFileSystem>(
      getFileSystem(FaultyFileSystem::scheme(), {}));
}
} // namespace facebook::velox::tests::utils
