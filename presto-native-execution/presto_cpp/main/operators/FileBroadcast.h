/*
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

#include <fmt/format.h>
#include "velox/common/file/FileSystems.h"
#include "velox/exec/Operator.h"

using namespace facebook::velox;

namespace facebook::presto::operators {

class BroadcastFileWriter {
 public:
  BroadcastFileWriter(
      std::unique_ptr<WriteFile> writeFile,
      std::string_view filename,
      velox::memory::MemoryPool* pool,
      const RowTypePtr& inputType);

  virtual ~BroadcastFileWriter() = default;

  /// Write to file
  void collect(RowVectorPtr input);

  /// Flush the data
  void noMoreData();

  // File stats - path, size, checksum, num rows
  RowVectorPtr fileStats();

  void serialize(
      const RowVectorPtr& rowVector,
      const VectorSerde::Options* serdeOptions = nullptr);

 private:
  std::unique_ptr<WriteFile> writeFile_;
  std::string filename_;
  velox::memory::MemoryPool* pool_;
  std::unique_ptr<VectorSerde> serde_;
  const RowTypePtr& inputType_;
};

class BroadcastFileReader {
 public:
  BroadcastFileReader(
      std::unique_ptr<ReadFile> readFile,
      std::string_view filename,
      velox::memory::MemoryPool* pool);

  ~BroadcastFileReader() = default;

  /// Check by the reader to see if more blocks are available
  bool hasNext();

  /// Read the next block of data.
  velox::BufferPtr next();

 private:
  std::unique_ptr<ReadFile> readFile_;
  std::string filename_;
  velox::memory::MemoryPool* pool_;
};

class FileBroadcast {
 public:
  FileBroadcast(const std::string& basePath);

  virtual ~FileBroadcast() = default;

  std::shared_ptr<BroadcastFileReader> createReader(
      const std::string& broadcastFilePath, // todo - List of locations
      velox::memory::MemoryPool*
          pool); // Broadcast file details received from mappers

  std::shared_ptr<BroadcastFileWriter> createWriter(
      memory::MemoryPool* pool,
      const RowTypePtr& inputType); // add stage, task id

 private:
  std::shared_ptr<velox::filesystems::FileSystem> fileSystem_;
  const std::string& basePath_;
};
} // namespace facebook::presto::operators
