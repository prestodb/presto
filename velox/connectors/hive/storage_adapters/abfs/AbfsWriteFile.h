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

#include "velox/common/file/File.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureDataLakeFileClient.h"

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::velox::filesystems {

/// We are using the DFS (Data Lake Storage) endpoint for Azure Blob File write
/// operations because the DFS endpoint is designed to be compatible with file
/// operation semantics, such as `Append` to a file and file `Flush` operations.
/// The legacy Blob endpoint can only be used for blob level append and flush
/// operations. When using the Blob endpoint, we would need to manually manage
/// the creation, appending, and committing of file-related blocks.

/// Implementation of abfs write file. Nothing written to the file should be
/// read back until it is closed.
class AbfsWriteFile : public WriteFile {
 public:
  constexpr static uint64_t kNaturalWriteSize = 8 << 20; // 8M

  /// @param path The file path to write.
  /// @param connectStr The connection string used to auth the storage account.
  AbfsWriteFile(std::string_view path, const config::ConfigBase& config);

  /// @param path The file path to write.
  /// @param client The AdlsFileClient.
  AbfsWriteFile(
      std::string_view path,
      std::unique_ptr<AzureDataLakeFileClient>& client);

  ~AbfsWriteFile();

  /// Get the file size.
  uint64_t size() const override;

  /// Flush the data.
  void flush() override;

  /// Write the data by append mode.
  void append(std::string_view data) override;

  /// Close the file.
  void close() override;

 protected:
  class Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace facebook::velox::filesystems
