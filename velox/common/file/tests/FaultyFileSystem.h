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

#include "velox/common/file/FileSystems.h"

#include <functional>
#include <memory>
#include <string_view>
#include "velox/common/file/tests/FaultyFile.h"

namespace facebook::velox::tests::utils {

using namespace filesystems;

/// Implements faulty filesystem for io fault injection in unit test. It is a
/// wrapper on top of a real file system, and by default it delegates the the
/// file operation to the real file system underneath.
class FaultyFileSystem : public FileSystem {
 public:
  explicit FaultyFileSystem(std::shared_ptr<const Config> config)
      : FileSystem(std::move(config)) {}

  ~FaultyFileSystem() override {}

  static inline std::string scheme() {
    return "faulty:";
  }

  std::string name() const override {
    return "Faulty FS";
  }

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const FileOptions& options) override;

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const FileOptions& options) override;

  void remove(std::string_view path) override;

  void rename(
      std::string_view oldPath,
      std::string_view newPath,
      bool overwrite) override;

  bool exists(std::string_view path) override;

  std::vector<std::string> list(std::string_view path) override;

  void mkdir(std::string_view path) override;

  void rmdir(std::string_view path) override;

  /// Setups hook for file fault injection.
  void setFileInjectionHook(FileFaultInjectionHook hook);

  /// Setups to inject 'error' for a particular set of file operation types. If
  /// 'opTypes' is empty, it injects error for all kinds of file operation
  /// types.
  void setFileInjectionError(
      std::exception_ptr error,
      std::unordered_set<FaultFileOperation::Type> opTypes = {});

  /// Setups to inject delay for a particular set of file operation types. If
  /// 'opTypes' is empty, it injects delay for all kinds of file operation
  /// types.
  void setFileInjectionDelay(
      uint64_t delayUs,
      std::unordered_set<FaultFileOperation::Type> opTypes = {});

  /// Clears the file fault injections.
  void clearFileFaultInjections();

 private:
  // Defines the file injection setup and only one type of injection can be set
  // at a time.
  struct FileInjections {
    FileFaultInjectionHook fileInjectionHook{nullptr};

    std::exception_ptr fileException{nullptr};

    uint64_t fileDelayUs{0};

    std::unordered_set<FaultFileOperation::Type> opTypes{};

    FileInjections() = default;

    explicit FileInjections(FileFaultInjectionHook _fileInjectionHook)
        : fileInjectionHook(std::move(_fileInjectionHook)) {}

    FileInjections(
        uint64_t _fileDelayUs,
        std::unordered_set<FaultFileOperation::Type> _opTypes)
        : fileDelayUs(_fileDelayUs), opTypes(std::move(_opTypes)) {}

    FileInjections(
        std::exception_ptr _fileException,
        std::unordered_set<FaultFileOperation::Type> _opTypes)
        : fileException(std::move(_fileException)),
          opTypes(std::move(_opTypes)) {}
  };

  // Invoked to inject file fault to 'op' if configured.
  void maybeInjectFileFault(FaultFileOperation* op);

  mutable std::mutex mu_;
  std::optional<FileInjections> fileInjections_;
};

/// Registers the faulty filesystem.
void registerFaultyFileSystem();

/// Gets the fault filesystem instance.
std::shared_ptr<FaultyFileSystem> faultyFileSystem();
} // namespace facebook::velox::tests::utils
