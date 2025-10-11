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

#include "velox/common/file/FileInputStream.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/serializers/SerializedPageFile.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::presto::operators {

/// Struct for single broadcast file info.
/// This struct is a 1:1 strict API mapping to
/// presto-spark-base/src/main/java/com/facebook/presto/spark/execution/BroadcastFileInfo.java
/// Please refrain from making changes to this API class. If any changes have to
/// be made to this struct, one should make sure to make corresponding changes
/// in the above Java classes and its corresponding serde functionalities.
struct BroadcastFileInfo {
  std::string filePath_;
  // TODO: Add additional stats including checksum, num rows, size.

  static std::unique_ptr<BroadcastFileInfo> deserialize(
      const std::string& info);
};

/// Writes broadcast data to a single file.
class BroadcastFileWriter : velox::serializer::SerializedPageFileWriter {
 public:
  BroadcastFileWriter(
      const std::string& pathPrefix,
      uint64_t writeBufferSize,
      std::unique_ptr<velox::VectorSerde::Options> serdeOptions,
      velox::memory::MemoryPool* pool);

  virtual ~BroadcastFileWriter() = default;

  /// Serializes input rowVector using PrestoVectorSerde and
  /// writes serialized data to file using buffered writes.
  void write(const velox::RowVectorPtr& rowVector);

  /// Flush the data.
  void noMoreData();

  /// Returns file name if non zero rows written to file.
  /// Returns nullptr if there were no rows written.
  velox::RowVectorPtr fileStats();

 private:
  uint64_t flush() override;

  void closeFile() override;

  // Writes a footer at the end of the file containing metadata about all pages.
  // The reader needs to know all page sizes ahead of time for exchange client's
  // memory planning.
  //
  // Footer structure using thrift serialization:
  // [serialized-thrift-footer][footer_size(8)]
  void writeFooter();

  int64_t numRows_{0};
  std::vector<int64_t> pageSizes_;
  velox::RowVectorPtr fileStats_{nullptr};
};

/// Reads broadcast data back from files.
class BroadcastFileReader {
 public:
  BroadcastFileReader(
      std::unique_ptr<BroadcastFileInfo>& broadcastFileInfo,
      std::shared_ptr<velox::filesystems::FileSystem> fileSystem,
      velox::memory::MemoryPool* pool);

  ~BroadcastFileReader() = default;

  /// Return true if more data is available.
  bool hasNext();

  /// Read next page of data. Returns nullptr when no more pages.
  velox::BufferPtr next();

  /// Reader stats - number of bytes and pages.
  folly::F14FastMap<std::string, int64_t> stats();

  /// Get page sizes for pages that haven't been read yet.
  std::vector<int64_t> remainingPageSizes() const;

 private:
  // Read the footer to get all page sizes
  void readFooter(velox::ReadFile* readFile);

  velox::memory::MemoryPool* const pool_;
  const std::unique_ptr<BroadcastFileInfo> broadcastFileInfo_;

  std::unique_ptr<velox::common::FileInputStream> inputStream_;
  int64_t numBytes_{0};
  uint32_t numPagesRead_{0};
  std::vector<int64_t> pageSizes_;
};

/// Factory to create Writers & Reader for file based broadcast.
class BroadcastFactory {
 public:
  /// Create FileBroadcast to write files under specified basePath.
  BroadcastFactory(const std::string& basePath);

  virtual ~BroadcastFactory() = default;

  std::unique_ptr<BroadcastFileWriter> createWriter(
      uint64_t writeBufferSize,
      velox::memory::MemoryPool* pool,
      std::unique_ptr<velox::VectorSerde::Options> serdeOptions);

  std::shared_ptr<BroadcastFileReader> createReader(
      const std::unique_ptr<BroadcastFileInfo> fileInfo,
      velox::memory::MemoryPool* pool);

 private:
  const std::string basePath_;
  std::shared_ptr<velox::filesystems::FileSystem> fileSystem_;
};
} // namespace facebook::presto::operators
