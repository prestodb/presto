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
#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h"

namespace Azure::Storage::Files::DataLake::Models {
class PathProperties;
}

namespace facebook::velox::filesystems::abfs {
using namespace Azure::Storage::Files::DataLake;
using namespace Azure::Storage::Files::DataLake::Models;

/*
 * We are using the DFS (Data Lake Storage) endpoint for Azure Blob File write
 * operations because the DFS endpoint is designed to be compatible with file
 * operation semantics, such as `Append` to a file and file `Flush` operations.
 * The legacy Blob endpoint can only be used for blob level append and flush
 * operations. When using the Blob endpoint, we would need to manually manage
 * the creation, appending, and committing of file-related blocks.
 *
 * However, the Azurite Simulator does not yet support the DFS endpoint.
 * (For more information, see https://github.com/Azure/Azurite/issues/553 and
 * https://github.com/Azure/Azurite/issues/409).
 * You can find a comparison between DFS and Blob endpoints here:
 * https://github.com/Azure/Azurite/wiki/ADLS-Gen2-Implementation-Guidance
 *
 * To facilitate unit testing of file write scenarios, we define the
 * IBlobStorageFileClient here, which can be mocked during testing.
 */
class IBlobStorageFileClient {
 public:
  virtual void create() = 0;
  virtual PathProperties getProperties() = 0;
  virtual void append(const uint8_t* buffer, size_t size, uint64_t offset) = 0;
  virtual void flush(uint64_t position) = 0;
  virtual void close() = 0;
};

/// Implementation of abfs write file. Nothing written to the file should be
/// read back until it is closed.
class AbfsWriteFile : public WriteFile {
 public:
  constexpr static uint64_t kNaturalWriteSize = 8 << 20; // 8M
  /// The constructor.
  /// @param path The file path to write.
  /// @param connectStr the connection string used to auth the storage account.
  AbfsWriteFile(const std::string& path, const std::string& connectStr);

  /// check any issue reading file.
  void initialize();

  /// Get the file size.
  uint64_t size() const override;

  /// Flush the data.
  void flush() override;

  /// Write the data by append mode.
  void append(std::string_view data) override;

  /// Close the file.
  void close() override;

  /// Used by tests to override the FileSystem client.
  void testingSetFileClient(
      const std::shared_ptr<IBlobStorageFileClient>& fileClient);

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};
} // namespace facebook::velox::filesystems::abfs
