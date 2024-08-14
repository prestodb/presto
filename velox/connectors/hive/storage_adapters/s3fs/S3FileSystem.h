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
#include "velox/connectors/hive/HiveConfig.h"

namespace facebook::velox::filesystems {
using namespace facebook::velox::connector::hive;

bool initializeS3(const config::ConfigBase* config);

void finalizeS3();

/// Implementation of S3 filesystem and file interface.
/// We provide a registration method for read and write files so the appropriate
/// type of file can be constructed based on a filename.
class S3FileSystem : public FileSystem {
 public:
  explicit S3FileSystem(std::shared_ptr<const config::ConfigBase> config);

  std::string name() const override;

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const FileOptions& options = {}) override;

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const FileOptions& options) override;

  void remove(std::string_view path) override {
    VELOX_UNSUPPORTED("remove for S3 not implemented");
  }

  void rename(
      std::string_view path,
      std::string_view newPath,
      bool overWrite = false) override {
    VELOX_UNSUPPORTED("rename for S3 not implemented");
  }

  bool exists(std::string_view path) override {
    VELOX_UNSUPPORTED("exists for S3 not implemented");
  }

  std::vector<std::string> list(std::string_view path) override {
    VELOX_UNSUPPORTED("list for S3 not implemented");
  }

  void mkdir(std::string_view path) override {
    VELOX_UNSUPPORTED("mkdir for S3 not implemented");
  }

  void rmdir(std::string_view path) override {
    VELOX_UNSUPPORTED("rmdir for S3 not implemented");
  }

  std::string getLogLevelName() const;

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace facebook::velox::filesystems
