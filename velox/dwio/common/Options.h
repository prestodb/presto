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

#include <folly/Executor.h>
#include "velox/common/base/SpillConfig.h"
#include "velox/common/compression/Compression.h"
#include "velox/common/io/Options.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/ErrorTolerance.h"
#include "velox/dwio/common/FlatMapHelper.h"
#include "velox/dwio/common/FlushPolicy.h"
#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/common/encryption/Encryption.h"

namespace facebook::velox::dwio::common {

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

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& output,
    const FileFormat& fmt) {
  output << toString(fmt);
  return output;
}

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

  inline static const std::string kFieldDelim{"field.delim"};
  inline static const std::string kCollectionDelim{"collection.delim"};
  inline static const std::string kMapKeyDelim{"mapkey.delim"};

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

struct TableParameter {
  static constexpr const char* kSkipHeaderLineCount = "skip.header.line.count";
  static constexpr const char* kSerializationNullFormat =
      "serialization.null.format";
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
  std::shared_ptr<velox::common::MetadataFilter> metadataFilter_;
  // Node id for map column to a list of keys to be projected as a struct.
  std::unordered_map<uint32_t, std::vector<std::string>> flatmapNodeIdAsStruct_;
  // Optional executors to enable internal reader parallelism.
  // 'decodingExecutor' allow parallelising the vector decoding process.
  // 'ioExecutor' enables parallelism when performing file system read
  // operations.
  std::shared_ptr<folly::Executor> decodingExecutor_;
  size_t decodingParallelismFactor_{0};
  bool appendRowNumberColumn_ = false;
  // Function to populate metrics related to feature projection stats
  // in Koski. This gets fired in FlatMapColumnReader.
  // This is a bit of a hack as there is (by design) no good way
  // To propogate information from column reader to Koski
  std::function<void(
      facebook::velox::dwio::common::flatmap::FlatMapKeySelectionStats)>
      keySelectionCallback_;

  // Function to track how much time we spend waiting on IO before reading rows
  // (in dwrf row reader). todo: encapsulate this and keySelectionCallBack_ in a
  // struct
  std::function<void(uint64_t)> blockedOnIoCallback_;
  std::function<void(uint64_t)> decodingTimeUsCallback_;
  std::function<void(uint16_t)> stripeCountCallback_;
  bool eagerFirstStripeLoad = true;
  uint64_t skipRows_ = 0;

 public:
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

  /*
   * Will load the first stripe on RowReader creation, if true.
   * This behavior is already happening in DWRF, but isn't desired for some use
   * cases. So this flag allows us to turn it off.
   */
  void setEagerFirstStripeLoad(bool load) {
    eagerFirstStripeLoad = load;
  }

  /*
   * Will load the first stripe on RowReader creation, if true.
   * This behavior is already happening in DWRF, but isn't desired for some use
   * cases. So this flag allows us to turn it off.
   */
  bool getEagerFirstStripeLoad() const {
    return eagerFirstStripeLoad;
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

  const std::shared_ptr<velox::common::ScanSpec>& getScanSpec() const {
    return scanSpec_;
  }

  void setScanSpec(std::shared_ptr<velox::common::ScanSpec> scanSpec) {
    scanSpec_ = std::move(scanSpec);
  }

  const std::shared_ptr<velox::common::MetadataFilter>& getMetadataFilter()
      const {
    return metadataFilter_;
  }

  void setMetadataFilter(
      std::shared_ptr<velox::common::MetadataFilter> metadataFilter) {
    metadataFilter_ = std::move(metadataFilter);
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

  void setDecodingParallelismFactor(size_t factor) {
    decodingParallelismFactor_ = factor;
  }

  /*
   * Set to true, if you want to add a new column to the results containing the
   * row numbers.  These row numbers are relative to the beginning of file (0 as
   * first row) and does not affected by filtering or deletion during the read
   * (it always counts all rows in the file).
   */
  void setAppendRowNumberColumn(bool value) {
    appendRowNumberColumn_ = value;
  }

  bool getAppendRowNumberColumn() const {
    return appendRowNumberColumn_;
  }

  void setKeySelectionCallback(
      std::function<void(
          facebook::velox::dwio::common::flatmap::FlatMapKeySelectionStats)>
          keySelectionCallback) {
    keySelectionCallback_ = std::move(keySelectionCallback);
  }

  const std::function<
      void(facebook::velox::dwio::common::flatmap::FlatMapKeySelectionStats)>
  getKeySelectionCallback() const {
    return keySelectionCallback_;
  }

  void setBlockedOnIoCallback(
      std::function<void(int64_t)> blockedOnIoCallback) {
    blockedOnIoCallback_ = std::move(blockedOnIoCallback);
  }

  const std::function<void(int64_t)> getBlockedOnIoCallback() const {
    return blockedOnIoCallback_;
  }

  void setDecodingTimeUsCallback(std::function<void(int64_t)> decodingTimeUs) {
    decodingTimeUsCallback_ = std::move(decodingTimeUs);
  }

  std::function<void(int64_t)> getDecodingTimeUsCallback() const {
    return decodingTimeUsCallback_;
  }

  void setStripeCountCallback(
      std::function<void(uint16_t)> stripeCountCallback) {
    stripeCountCallback_ = std::move(stripeCountCallback);
  }

  std::function<void(uint16_t)> getStripeCountCallback() const {
    return stripeCountCallback_;
  }

  void setSkipRows(uint64_t skipRows) {
    skipRows_ = skipRows;
  }

  uint64_t getSkipRows() const {
    return skipRows_;
  }

  const std::shared_ptr<folly::Executor>& getDecodingExecutor() const {
    return decodingExecutor_;
  }

  size_t getDecodingParallelismFactor() const {
    return decodingParallelismFactor_;
  }
};

/**
 * Options for creating a Reader.
 */
class ReaderOptions : public io::ReaderOptions {
 private:
  uint64_t tailLocation;
  FileFormat fileFormat;
  RowTypePtr fileSchema;
  SerDeOptions serDeOptions;
  std::shared_ptr<encryption::DecrypterFactory> decrypterFactory_;
  uint64_t footerEstimatedSize{kDefaultFooterEstimatedSize};
  uint64_t filePreloadThreshold{kDefaultFilePreloadThreshold};
  bool fileColumnNamesReadAsLowerCase{false};
  bool useColumnNamesForColumnMapping_{false};
  std::shared_ptr<folly::Executor> ioExecutor_;

 public:
  static constexpr uint64_t kDefaultFooterEstimatedSize = 1024 * 1024; // 1MB
  static constexpr uint64_t kDefaultFilePreloadThreshold =
      1024 * 1024 * 8; // 8MB

  explicit ReaderOptions(velox::memory::MemoryPool* pool)
      : io::ReaderOptions(pool),
        tailLocation(std::numeric_limits<uint64_t>::max()),
        fileFormat(FileFormat::UNKNOWN),
        fileSchema(nullptr) {}

  ReaderOptions& operator=(const ReaderOptions& other) {
    io::ReaderOptions::operator=(other);
    tailLocation = other.tailLocation;
    fileFormat = other.fileFormat;
    if (other.fileSchema != nullptr) {
      fileSchema = other.getFileSchema();
    } else {
      fileSchema = nullptr;
    }
    serDeOptions = other.serDeOptions;
    decrypterFactory_ = other.decrypterFactory_;
    footerEstimatedSize = other.footerEstimatedSize;
    filePreloadThreshold = other.filePreloadThreshold;
    fileColumnNamesReadAsLowerCase = other.fileColumnNamesReadAsLowerCase;
    useColumnNamesForColumnMapping_ = other.useColumnNamesForColumnMapping_;
    return *this;
  }

  ReaderOptions(const ReaderOptions& other)
      : io::ReaderOptions(other),
        tailLocation(other.tailLocation),
        fileFormat(other.fileFormat),
        fileSchema(other.fileSchema),
        serDeOptions(other.serDeOptions),
        decrypterFactory_(other.decrypterFactory_),
        footerEstimatedSize(other.footerEstimatedSize),
        filePreloadThreshold(other.filePreloadThreshold),
        fileColumnNamesReadAsLowerCase(other.fileColumnNamesReadAsLowerCase),
        useColumnNamesForColumnMapping_(other.useColumnNamesForColumnMapping_) {
  }

  /**
   * Set the format of the file, such as "rc" or "dwrf".  The
   * default is "dwrf".
   */
  ReaderOptions& setFileFormat(FileFormat format) {
    fileFormat = format;
    return *this;
  }

  /**
   * Set the schema of the file (a Type tree).
   * For "dwrf" format, a default schema is derived from the file.
   * For "rc" format, there is no default schema.
   */
  ReaderOptions& setFileSchema(const RowTypePtr& schema) {
    fileSchema = schema;
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

  ReaderOptions& setFooterEstimatedSize(uint64_t size) {
    footerEstimatedSize = size;
    return *this;
  }

  ReaderOptions& setFilePreloadThreshold(uint64_t threshold) {
    filePreloadThreshold = threshold;
    return *this;
  }

  ReaderOptions& setFileColumnNamesReadAsLowerCase(bool flag) {
    fileColumnNamesReadAsLowerCase = flag;
    return *this;
  }

  ReaderOptions& setUseColumnNamesForColumnMapping(bool flag) {
    useColumnNamesForColumnMapping_ = flag;
    return *this;
  }

  ReaderOptions& setIOExecutor(std::shared_ptr<folly::Executor> executor) {
    ioExecutor_ = std::move(executor);
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

  uint64_t getFooterEstimatedSize() const {
    return footerEstimatedSize;
  }

  uint64_t getFilePreloadThreshold() const {
    return filePreloadThreshold;
  }

  const std::shared_ptr<folly::Executor>& getIOExecutor() const {
    return ioExecutor_;
  }

  bool isFileColumnNamesReadAsLowerCase() const {
    return fileColumnNamesReadAsLowerCase;
  }

  bool isUseColumnNamesForColumnMapping() const {
    return useColumnNamesForColumnMapping_;
  }
};

struct WriterOptions {
  TypePtr schema;
  velox::memory::MemoryPool* memoryPool;
  const velox::common::SpillConfig* spillConfig{nullptr};
  tsan_atomic<bool>* nonReclaimableSection{nullptr};
  std::optional<velox::common::CompressionKind> compressionKind;
  std::optional<uint64_t> maxStripeSize{std::nullopt};
  std::optional<uint64_t> maxDictionaryMemory{std::nullopt};
  std::map<std::string, std::string> serdeParameters;
};

} // namespace facebook::velox::dwio::common
