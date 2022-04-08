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

namespace facebook::velox::filesystems {

// Implementation of S3 filesystem and file interface.
// We provide a registration method for read and write files so the appropriate
// type of file can be constructed based on a filename. See the
// (register|generate)ReadFile and (register|generate)WriteFile functions.
class S3FileSystem : public FileSystem {
 public:
  explicit S3FileSystem(std::shared_ptr<const Config> config);

  // Initialize the Aws::S3::S3Client from the input Config parameters.
  void initializeClient();

  std::unique_ptr<ReadFile> openFileForRead(std::string_view path) override;

  std::unique_ptr<WriteFile> openFileForWrite(std::string_view path) override;

  std::string name() const override;

  void remove(std::string_view path) override {
    VELOX_UNSUPPORTED("remove for S3 not implemented");
  }

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

// Register the S3 filesystem.
void registerS3FileSystem();
} // namespace facebook::velox::filesystems
