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

#include <limits>
#include <unordered_set>

#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/ErrorTolerance.h"
#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/common/encryption/Encryption.h"

namespace facebook::velox::dwrf {
class ColumnReaderFactory;
} // namespace facebook::velox::dwrf

namespace facebook {
namespace velox {
namespace dwio {
namespace common {

enum class FileFormat {
  UNKNOWN = 0,
  DWRF = 1, // DWRF
  RC = 2, // RC with unknown serialization
  RC_TEXT = 3, // RC with text serialization
  RC_BINARY = 4, // RC with binary serialization
  TEXT = 5,
  JSON = 6,
  PARQUET = 7,
  ALPHA = 8,
  ORC = 9,
};

FileFormat toFileFormat(std::string s);
std::string toString(FileFormat fmt);

/**
 * Formatting options for serialization.
 */
enum class SerDeSeparator {
  FIELD_DELIM = 0,
  COLLECTION_DELIM = 1,
  MAP_KEY_DELIM = 2,
};

class SerDeOptions {
 public:
  std::array<uint8_t, 8> separators;
  std::string nullString;
  bool lastColumnTakesRest;
  uint8_t escapeChar;
  bool isEscaped;

  explicit SerDeOptions(
      uint8_t fieldDelim = '\1',
      uint8_t collectionDelim = '\2',
      uint8_t mapKeyDelim = '\3',
      uint8_t escape = '\\',
      bool isEscapedFlag = false)
      : separators{{fieldDelim, collectionDelim, mapKeyDelim, 4, 5, 6, 7, 8}},
        nullString("\\N"),
        lastColumnTakesRest(false),
        escapeChar(escape),
        isEscaped(isEscapedFlag) {}
  ~SerDeOptions() = default;
};

/**
 * Options for creating a RowReader.
 */
class RowReaderOptions {
 private:
  uint64_t dataStart;
  uint64_t dataLength;
  bool preloadStripe;
  bool projectSelectedType;
  bool returnFlatVector_ = false;
  ErrorTolerance errorTolerance_;
  std::shared_ptr<ColumnSelector> selector_;
  std::shared_ptr<velox::common::ScanSpec> scanSpec_ = nullptr;
  // Node id for map column to a list of keys to be projected as a struct.
  std::unordered_map<uint32_t, std::vector<std::string>> flatmapNodeIdAsStruct_;
  // Optional executors to enable internal reader parallelism.
  // 'decodingExecutor' allow parallelising the vector decoding process.
  // 'ioExecutor' enables parallelism when performing file system read
  // operations.
  std::shared_ptr<folly::Executor> decodingExecutor_;
  std::shared_ptr<folly::Executor> ioExecutor_;

 public:
  RowReaderOptions(const RowReaderOptions& other) {
    dataStart = other.dataStart;
    dataLength = other.dataLength;
    preloadStripe = other.preloadStripe;
    projectSelectedType = other.projectSelectedType;
    errorTolerance_ = other.errorTolerance_;
    selector_ = other.selector_;
    scanSpec_ = other.scanSpec_;
    returnFlatVector_ = other.returnFlatVector_;
    flatmapNodeIdAsStruct_ = other.flatmapNodeIdAsStruct_;
  }

  RowReaderOptions() noexcept
      : dataStart(0),
        dataLength(std::numeric_limits<uint64_t>::max()),
        preloadStripe(false),
        projectSelectedType(false) {}

  /**
   * For files that have structs as the top-level object, select the fields
   * to read. The first field is 0, the second 1, and so on. By default,
   * all columns are read. This option clears any previous setting of
   * the selected columns.
   * @param include a list of fields to read
   * @return this
   */
  RowReaderOptions& select(const std::shared_ptr<ColumnSelector>& selector) {
    selector_ = selector;
    return *this;
  }

  /**
   * Set the section of the file to process.
   * @param offset the starting byte offset
   * @param length the number of bytes to read
   * @return this
   */
  RowReaderOptions& range(uint64_t offset, uint64_t length) {
    dataStart = offset;
    dataLength = length;
    return *this;
  }

  /**
   * Get the list of selected field or type ids to read.
   */
  const std::shared_ptr<ColumnSelector>& getSelector() const {
    return selector_;
  }

  /**
   * Get the start of the range for the data being processed.
   * @return if not set, return 0
   */
  uint64_t getOffset() const {
    return dataStart;
  }

  /**
   * Get the length of the range for the data being processed.
   * @return if not set, return the maximum unsigned long.
   */
  uint64_t getLength() const {
    return dataLength;
  }

  /**
   * Get the limit of the range (lowest offset not in the range).
   * @return if not set, return the maximum unsigned long.
   */
  uint64_t getLimit() const {
    return ((std::numeric_limits<uint64_t>::max() - dataStart) > dataLength)
        ? (dataStart + dataLength)
        : std::numeric_limits<uint64_t>::max();
  }

  /**
   * Request that stripes be pre-loaded.
   */
  void setPreloadStripe(bool preload) {
    preloadStripe = preload;
  }

  /**
   * Are stripes to be pre-loaded?
   */
  bool getPreloadStripe() const {
    return preloadStripe;
  }

  // For flat map, return flat vector representation
  bool getReturnFlatVector() const {
    return returnFlatVector_;
  }

  // For flat map, request that flat vector representation is used
  void setReturnFlatVector(bool value) {
    returnFlatVector_ = value;
  }

  /**
   * Request that the selected type be projected.
   */
  void setProjectSelectedType(bool vProjectSelectedType) {
    projectSelectedType = vProjectSelectedType;
  }

  /**
   * Is the selected type to be projected?
   */
  bool getProjectSelectedType() const {
    return projectSelectedType;
  }

  /**
   * set RowReader error tolerance.
   */
  void setErrorTolerance(const ErrorTolerance& errorTolerance) {
    errorTolerance_ = errorTolerance;
  }

  /**
   * get RowReader error tolerance.
   */
  const ErrorTolerance& getErrorTolerance() const {
    return errorTolerance_;
  }

  const std::shared_ptr<velox::common::ScanSpec> getScanSpec() const {
    return scanSpec_;
  }

  void setScanSpec(std::shared_ptr<velox::common::ScanSpec> scanSpec) {
    scanSpec_ = scanSpec;
  }

  void setFlatmapNodeIdsAsStruct(
      std::unordered_map<uint32_t, std::vector<std::string>>
          flatmapNodeIdsAsStruct) {
    VELOX_CHECK(
        std::all_of(
            flatmapNodeIdsAsStruct.begin(),
            flatmapNodeIdsAsStruct.end(),
            [](const auto& kv) { return !kv.second.empty(); }),
        "To use struct encoding for flatmap, keys to project must be specified");
    flatmapNodeIdAsStruct_ = std::move(flatmapNodeIdsAsStruct);
  }

  const std::unordered_map<uint32_t, std::vector<std::string>>&
  getMapColumnIdAsStruct() const {
    return flatmapNodeIdAsStruct_;
  }

  void setDecodingExecutor(std::shared_ptr<folly::Executor> executor) {
    decodingExecutor_ = executor;
  }

  void setIOExecutor(std::shared_ptr<folly::Executor> executor) {
    ioExecutor_ = executor;
  }

  const std::shared_ptr<folly::Executor>& getDecodingExecutor() const {
    return decodingExecutor_;
  }

  const std::shared_ptr<folly::Executor>& getIOExecutor() const {
    return ioExecutor_;
  }
};

/**
 * Mode for prefetching data.
 *
 * This mode may be ignored for a reader, such as DWRF, where it does not
 * make sense.
 *
 * To enable single-buffered reading, using the default autoPreloadLength:
 *         ReaderOptions readerOpts;
 *         readerOpts.setPrefetchMode(PrefetchMode::PRELOAD);
 * To enable double-buffered reading, using the default autoPreloadLength:
 *         ReaderOptions readerOpts;
 *         readerOpts.setPrefetchMode(PrefetchMode::PREFETCH);
 * To select unbuffered reading:
 *         ReaderOptions readerOpts;
 *         readerOpts.setPrefetchMode(PrefetchMode::NOT_SET);
 *
 * Single-buffered reading (as in dwio::PreloadableInputStream)
 * reads ahead into a buffer.   Double-buffered reading additionally reads
 * asynchronously into a second buffer, swaps the buffers when the
 * first is fully consumed and the second has been filled, and then starts
 * a new parallel read.  For clients with a slow network connection to
 * Warm Storage, enabling PREFETCH reduces elapsed time by 10% or more,
 * at the cost of a second buffer.   The relative improvment would be greater
 * for cases where the network throughput is higher.
 */
enum class PrefetchMode {
  NOT_SET = 0,
  PRELOAD = 1, // read a buffer of autoPreloadLength bytes on a read beyond the
               // current buffer, if any.
  PREFETCH = 2, // read a second buffer of autoPreloadLength bytes ahead of
                // actual reads.
};

/**
 * Options for creating a Reader.
 */
class ReaderOptions {
 private:
  uint64_t tailLocation;
  velox::memory::MemoryPool* memoryPool;
  FileFormat fileFormat;
  RowTypePtr fileSchema;
  uint64_t autoPreloadLength;
  PrefetchMode prefetchMode;
  int32_t loadQuantum_{kDefaultLoadQuantum};
  int32_t maxCoalesceDistance_{kDefaultCoalesceDistance};
  SerDeOptions serDeOptions;
  uint64_t fileNum;
  std::shared_ptr<encryption::DecrypterFactory> decrypterFactory_;
  std::shared_ptr<BufferedInputFactory> bufferedInputFactory_;

 public:
  static constexpr int32_t kDefaultLoadQuantum = 8 << 20; // 8MB
  static constexpr int32_t kDefaultCoalesceDistance = 512 << 10; // 512K

  ReaderOptions(
      velox::memory::MemoryPool* pool =
          &facebook::velox::memory::getProcessDefaultMemoryManager().getRoot())
      : tailLocation(std::numeric_limits<uint64_t>::max()),
        memoryPool(pool),
        fileFormat(FileFormat::UNKNOWN),
        fileSchema(nullptr),
        autoPreloadLength(DEFAULT_AUTO_PRELOAD_SIZE),
        prefetchMode(PrefetchMode::PREFETCH) {
    // PASS
  }

  ReaderOptions& operator=(const ReaderOptions& other) {
    tailLocation = other.tailLocation;
    memoryPool = other.memoryPool;
    fileFormat = other.fileFormat;
    if (other.fileSchema != nullptr) {
      fileSchema = other.getFileSchema();
    } else {
      fileSchema = nullptr;
    }
    autoPreloadLength = other.autoPreloadLength;
    prefetchMode = other.prefetchMode;
    serDeOptions = other.serDeOptions;
    fileNum = other.fileNum;
    decrypterFactory_ = other.decrypterFactory_;
    bufferedInputFactory_ = other.bufferedInputFactory_;
    return *this;
  }

  ReaderOptions(const ReaderOptions& other) {
    *this = other;
  }

  /**
   * Set the memory allocator.
   */
  ReaderOptions& setMemoryPool(velox::memory::MemoryPool& pool) {
    memoryPool = &pool;
    return *this;
  }

  /**
   * Set the format of the file, such as "rc" or "dwrf".  The
   * default is "dwrf".
   */
  ReaderOptions& setFileFormat(FileFormat format) {
    fileFormat = format;
    return *this;
  }

  ReaderOptions& setFileNum(uint64_t num) {
    fileNum = num;
    return *this;
  }

  /**
   * Set the schema of the file (a Type tree).
   * For "dwrf" format, a default schema is derived from the file.
   * For "rc" format, there is no default schema.
   */
  ReaderOptions& setFileSchema(
      const std::shared_ptr<const velox::RowType>& schema) {
    if (schema != nullptr) {
      fileSchema = schema;
    } else {
      fileSchema = nullptr;
    }
    return *this;
  }

  /**
   * Set the location of the tail as defined by the logical length of the
   * file.
   */
  ReaderOptions& setTailLocation(uint64_t offset) {
    tailLocation = offset;
    return *this;
  }

  /**
   * Modify the autoPreloadLength
   */
  ReaderOptions& setAutoPreloadLength(uint64_t len) {
    autoPreloadLength = len;
    return *this;
  }

  /**
   * Modify the prefetch mode.
   */
  ReaderOptions& setPrefetchMode(PrefetchMode mode) {
    prefetchMode = mode;
    return *this;
  }

  /**
   * Modify the load quantum.
   */
  ReaderOptions& setLoadQuantum(int32_t quantum) {
    loadQuantum_ = quantum;
    return *this;
  }
  /**
   * Modify the maximum load coalesce distance.
   */
  ReaderOptions& setMaxCoalesceDistance(int32_t distance) {
    maxCoalesceDistance_ = distance;
    return *this;
  }

  /**
   * Modify the serialization-deserialization options.
   */
  ReaderOptions& setSerDeOptions(const SerDeOptions& sdo) {
    serDeOptions = sdo;
    return *this;
  }

  ReaderOptions& setDecrypterFactory(
      const std::shared_ptr<encryption::DecrypterFactory>& factory) {
    decrypterFactory_ = factory;
    return *this;
  }

  ReaderOptions& setBufferedInputFactory(
      std::shared_ptr<BufferedInputFactory> factory) {
    bufferedInputFactory_ = factory;
    return *this;
  }

  /**
   * Get the desired tail location.
   * @return if not set, return the maximum long.
   */
  uint64_t getTailLocation() const {
    return tailLocation;
  }

  /**
   * Get the memory allocator.
   */
  velox::memory::MemoryPool& getMemoryPool() const {
    return *memoryPool;
  }

  uint64_t getFileNum() const {
    return fileNum;
  }

  /**
   * Get the file format.
   */
  FileFormat getFileFormat() const {
    return fileFormat;
  }

  /**
   * Get the file schema.
   */
  const std::shared_ptr<const velox::RowType>& getFileSchema() const {
    return fileSchema;
  }

  uint64_t getAutoPreloadLength() const {
    return autoPreloadLength;
  }

  PrefetchMode getPrefetchMode() const {
    return prefetchMode;
  }

  int32_t loadQuantum() const {
    return loadQuantum_;
  }

  int32_t maxCoalesceDistance() const {
    return maxCoalesceDistance_;
  }

  SerDeOptions& getSerDeOptions() {
    return serDeOptions;
  }

  const SerDeOptions& getSerDeOptions() const {
    return serDeOptions;
  }

  const std::shared_ptr<encryption::DecrypterFactory> getDecrypterFactory()
      const {
    return decrypterFactory_;
  }

  std::shared_ptr<BufferedInputFactory> getBufferedInputFactory() const {
    return bufferedInputFactory_;
  }
};

} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
