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

namespace Aws::Auth {
// Forward-declare the AWSCredentialsProvider class from the AWS SDK.
class AWSCredentialsProvider;
} // namespace Aws::Auth

namespace facebook::velox::filesystems {

bool initializeS3(
    std::string_view logLevel = "FATAL",
    std::optional<std::string_view> logLocation = std::nullopt);

void finalizeS3();

class S3Config;

using AWSCredentialsProviderFactory =
    std::function<std::shared_ptr<Aws::Auth::AWSCredentialsProvider>(
        const S3Config& config)>;

void registerCredentialsProvider(
    const std::string& providerName,
    const AWSCredentialsProviderFactory& factory);

/// Implementation of S3 filesystem and file interface.
/// We provide a registration method for read and write files so the appropriate
/// type of file can be constructed based on a filename.
class S3FileSystem : public FileSystem {
 public:
  S3FileSystem(
      std::string_view bucketName,
      const std::shared_ptr<const config::ConfigBase> config);

  std::string name() const override;

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view s3Path,
      const FileOptions& options = {}) override;

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view s3Path,
      const FileOptions& options) override;

  void remove(std::string_view path) override {
    VELOX_UNSUPPORTED("remove for S3 not implemented");
  }

  // Renames the path.
  void rename(
      std::string_view path,
      std::string_view newPath,
      bool overWrite = false) override;

  /// Checks that the path exists.
  bool exists(std::string_view path) override;

  /// List the objects associated to a path.
  std::vector<std::string> list(std::string_view path) override;

  void mkdir(std::string_view path, const DirectoryOptions& options = {})
      override;

  void rmdir(std::string_view path) override {
    VELOX_UNSUPPORTED("rmdir for S3 not implemented");
  }

  std::string getLogLevelName() const;

  std::string getLogPrefix() const;

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace facebook::velox::filesystems
