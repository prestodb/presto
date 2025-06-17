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

#include <google/cloud/storage/client.h>
#include "velox/common/file/File.h"

namespace facebook::velox::filesystems {

/**
 * Implementation of gcs write file.
 */
class GcsWriteFile : public WriteFile {
 public:
  GcsWriteFile(
      const std::string& path,
      std::shared_ptr<::google::cloud::storage::Client> client);

  ~GcsWriteFile() override;

  void initialize();

  /// Writes the data by append mode.
  void append(std::string_view data) override;

  /// Flushs the data.
  void flush() override;

  /// Closes the file.
  void close() override;

  /// Gets the file size.
  uint64_t size() const override;

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace facebook::velox::filesystems
