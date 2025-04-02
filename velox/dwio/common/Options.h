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
#include "velox/common/base/RandomUtil.h"
#include "velox/common/base/SpillConfig.h"
#include "velox/common/compression/Compression.h"
#include "velox/common/config/Config.h"
#include "velox/common/io/Options.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/ErrorTolerance.h"
#include "velox/dwio/common/FlatMapHelper.h"
#include "velox/dwio/common/FlushPolicy.h"
#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/common/UnitLoader.h"
#include "velox/dwio/common/encryption/Encryption.h"
#include "velox/type/Timestamp.h"
#include "velox/type/tz/TimeZoneMap.h"

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
  NIMBLE = 8,
  ORC = 9,
  SST = 10, // rocksdb sst format
};

FileFormat toFileFormat(std::string_view s);
std::string_view toString(FileFormat fmt);

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& output,
    const FileFormat& fmt) {
  output << toString(fmt);
  return output;
}

/// Formatting options for serialization.
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
  inline static const std::string kEscapeChar{"escape.delim"};

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
  /// If present in the table parameters, the option is passed to the row reader
  /// to instruct it to skip the number of rows from the current position. Used
  /// to skip the column header row(s).
  static constexpr const char* kSkipHeaderLineCount = "skip.header.line.count";
  /// If present in the table parameters, the option overrides the default value
  /// of the SerDeOptions::nullString. It causes any field read from the file
  /// (usually of the TEXT format) to be considered NULL if it is equal to this
  /// string.
  static constexpr const char* kSerializationNullFormat =
      "serialization.null.format";
};

/// Implicit row number column to be added.  This column will be removed in the
/// output of split reader.  Should use the ScanSpec::ColumnType::kRowIndex if
/// the column is suppose to be explicit and kept in the output.
struct RowNumberColumnInfo {
  column_index_t insertPosition;
  std::string name;
};

class FormatSpecificOptions {
 public:
  virtual ~FormatSpecificOptions() = default;
};

/// Options for creating a RowReader.
class RowReaderOptions {
 public:
  RowReaderOptions() noexcept
      : dataStart_(0),
        dataLength_(std::numeric_limits<uint64_t>::max()),
        preloadStripe_(false),
        projectSelectedType_(false) {}

  /// For files that have structs as the top-level object, select the fields
  /// to read. The first field is 0, the second 1, and so on. By default,
  /// all columns are read. This option clears any previous setting of
  /// the selected columns.
  /// @param include a list of fields to read
  /// @return this
  RowReaderOptions& select(const std::shared_ptr<ColumnSelector>& selector) {
    selector_ = selector;
    if (selector) {
      VELOX_CHECK_NULL(requestedType_);
      requestedType_ = selector->getSchema();
    }
    return *this;
  }

  /// Sets the section of the file to process.
  /// @param offset the starting byte offset
  /// @param length the number of bytes to read
  /// @return this
  RowReaderOptions& range(uint64_t offset, uint64_t length) {
    dataStart_ = offset;
    dataLength_ = length;
    return *this;
  }

  /// Gets the list of selected field or type ids to read.
  const std::shared_ptr<ColumnSelector>& selector() const {
    return selector_;
  }

  /// Gets the start of the range for the data being processed.
  /// @return if not set, return 0
  uint64_t offset() const {
    return dataStart_;
  }

  /// Gets the length of the range for the data being processed.
  /// @return if not set, return the maximum unsigned long.
  uint64_t length() const {
    return dataLength_;
  }

  /// Gets the limit of the range (lowest offset not in the range).
  /// @return if not set, return the maximum unsigned long.
  uint64_t limit() const {
    return ((std::numeric_limits<uint64_t>::max() - dataStart_) > dataLength_)
        ? (dataStart_ + dataLength_)
        : std::numeric_limits<uint64_t>::max();
  }

  /// Requests that stripes be pre-loaded.
  void setPreloadStripe(bool preload) {
    preloadStripe_ = preload;
  }

  /// Are stripes to be pre-loaded?
  bool preloadStripe() const {
    return preloadStripe_;
  }

  /// Will load the first stripe on RowReader creation, if true.
  /// This behavior is already happening in DWRF, but isn't desired for some use
  /// cases. So this flag allows us to turn it off.
  void setEagerFirstStripeLoad(bool load) {
    eagerFirstStripeLoad_ = load;
  }

  /// Will load the first stripe on RowReader creation, if true.
  /// This behavior is already happening in DWRF, but isn't desired for some use
  /// cases. So this flag allows us to turn it off.
  bool eagerFirstStripeLoad() const {
    return eagerFirstStripeLoad_;
  }

  /// For flat map, return flat vector representation
  bool returnFlatVector() const {
    return returnFlatVector_;
  }

  /// For flat map, request that flat vector representation is used
  void setReturnFlatVector(bool value) {
    returnFlatVector_ = value;
  }

  /// Requests that the selected type be projected.
  void setProjectSelectedType(bool value) {
    projectSelectedType_ = value;
  }

  /// Is the selected type to be projected?
  bool projectSelectedType() const {
    return projectSelectedType_;
  }

  /// Set RowReader error tolerance.
  void setErrorTolerance(const ErrorTolerance& errorTolerance) {
    errorTolerance_ = errorTolerance;
  }

  /// Get RowReader error tolerance.
  const ErrorTolerance& errorTolerance() const {
    return errorTolerance_;
  }

  const RowTypePtr& requestedType() const {
    return requestedType_;
  }

  void setRequestedType(RowTypePtr requestedType) {
    VELOX_CHECK_NULL(selector_);
    requestedType_ = std::move(requestedType);
  }

  const std::shared_ptr<velox::common::ScanSpec>& scanSpec() const {
    return scanSpec_;
  }

  void setScanSpec(std::shared_ptr<velox::common::ScanSpec> scanSpec) {
    scanSpec_ = std::move(scanSpec);
  }

  const std::shared_ptr<velox::common::MetadataFilter>& metadataFilter() const {
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
  mapColumnIdAsStruct() const {
    return flatmapNodeIdAsStruct_;
  }

  void setDecodingExecutor(std::shared_ptr<folly::Executor> executor) {
    decodingExecutor_ = executor;
  }

  void setDecodingParallelismFactor(size_t factor) {
    decodingParallelismFactor_ = factor;
  }

  void setRowNumberColumnInfo(
      std::optional<RowNumberColumnInfo> rowNumberColumnInfo) {
    rowNumberColumnInfo_ = std::move(rowNumberColumnInfo);
  }

  const std::optional<RowNumberColumnInfo>& rowNumberColumnInfo() const {
    return rowNumberColumnInfo_;
  }

  void setKeySelectionCallback(
      std::function<void(
          facebook::velox::dwio::common::flatmap::FlatMapKeySelectionStats)>
          keySelectionCallback) {
    keySelectionCallback_ = std::move(keySelectionCallback);
  }

  const std::function<
      void(facebook::velox::dwio::common::flatmap::FlatMapKeySelectionStats)>
  keySelectionCallback() const {
    return keySelectionCallback_;
  }

  void setBlockedOnIoCallback(
      std::function<void(std::chrono::high_resolution_clock::duration)>
          blockedOnIoCallback) {
    blockedOnIoCallback_ = std::move(blockedOnIoCallback);
  }

  const std::function<void(std::chrono::high_resolution_clock::duration)>
  blockedOnIoCallback() const {
    return blockedOnIoCallback_;
  }

  void setDecodingTimeCallback(
      std::function<void(std::chrono::high_resolution_clock::duration)>
          decodingTime) {
    decodingTimeCallback_ = std::move(decodingTime);
  }

  std::function<void(std::chrono::high_resolution_clock::duration)>
  decodingTimeCallback() const {
    return decodingTimeCallback_;
  }

  void setStripeCountCallback(
      std::function<void(uint16_t)> stripeCountCallback) {
    stripeCountCallback_ = std::move(stripeCountCallback);
  }

  std::function<void(uint16_t)> stripeCountCallback() const {
    return stripeCountCallback_;
  }

  void setSkipRows(uint64_t skipRows) {
    skipRows_ = skipRows;
  }

  uint64_t skipRows() const {
    return skipRows_;
  }

  void setUnitLoaderFactory(
      std::shared_ptr<UnitLoaderFactory> unitLoaderFactory) {
    unitLoaderFactory_ = std::move(unitLoaderFactory);
  }

  const std::shared_ptr<UnitLoaderFactory>& unitLoaderFactory() const {
    return unitLoaderFactory_;
  }

  const std::shared_ptr<folly::Executor>& decodingExecutor() const {
    return decodingExecutor_;
  }

  size_t decodingParallelismFactor() const {
    return decodingParallelismFactor_;
  }

  TimestampPrecision timestampPrecision() const {
    return timestampPrecision_;
  }

  void setTimestampPrecision(TimestampPrecision precision) {
    timestampPrecision_ = precision;
  }

  const std::shared_ptr<FormatSpecificOptions>& formatSpecificOptions() const {
    return formatSpecificOptions_;
  }

  void setFormatSpecificOptions(
      std::shared_ptr<FormatSpecificOptions> options) {
    formatSpecificOptions_ = std::move(options);
  }

  const std::unordered_map<std::string, std::string>& storageParameters()
      const {
    return storageParameters_;
  }

  void setStorageParameters(
      std::unordered_map<std::string, std::string> storageParameters) {
    storageParameters_ = std::move(storageParameters);
  }

 private:
  uint64_t dataStart_;
  uint64_t dataLength_;
  bool preloadStripe_;
  bool projectSelectedType_;
  bool returnFlatVector_ = false;
  ErrorTolerance errorTolerance_;
  std::shared_ptr<ColumnSelector> selector_;
  RowTypePtr requestedType_;
  std::shared_ptr<velox::common::ScanSpec> scanSpec_{nullptr};
  std::shared_ptr<velox::common::MetadataFilter> metadataFilter_;
  // Node id for map column to a list of keys to be projected as a struct.
  std::unordered_map<uint32_t, std::vector<std::string>> flatmapNodeIdAsStruct_;
  // Optional executors to enable internal reader parallelism.
  // 'decodingExecutor' allow parallelising the vector decoding process.
  // 'ioExecutor' enables parallelism when performing file system read
  // operations.
  std::shared_ptr<folly::Executor> decodingExecutor_;
  size_t decodingParallelismFactor_{0};
  std::optional<RowNumberColumnInfo> rowNumberColumnInfo_{std::nullopt};
  // Parameters that are provided as the physical storage properties.
  std::unordered_map<std::string, std::string> storageParameters_ = {};

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
  std::function<void(std::chrono::high_resolution_clock::duration)>
      blockedOnIoCallback_;
  std::function<void(std::chrono::high_resolution_clock::duration)>
      decodingTimeCallback_;
  std::function<void(uint16_t)> stripeCountCallback_;
  bool eagerFirstStripeLoad_{true};
  uint64_t skipRows_{0};

  std::shared_ptr<UnitLoaderFactory> unitLoaderFactory_;

  TimestampPrecision timestampPrecision_ = TimestampPrecision::kMilliseconds;

  std::shared_ptr<FormatSpecificOptions> formatSpecificOptions_;
};

/// Options for creating a Reader.
class ReaderOptions : public io::ReaderOptions {
 public:
  static constexpr uint64_t kDefaultFooterEstimatedSize = 1024 * 1024; // 1MB
  static constexpr uint64_t kDefaultFilePreloadThreshold =
      1024 * 1024 * 8; // 8MB

  explicit ReaderOptions(velox::memory::MemoryPool* pool)
      : io::ReaderOptions(pool),
        tailLocation_(std::numeric_limits<uint64_t>::max()),
        fileFormat_(FileFormat::UNKNOWN),
        fileSchema_(nullptr) {}

  /// Sets the format of the file, such as "rc" or "dwrf". The default is
  /// "dwrf".
  ReaderOptions& setFileFormat(FileFormat format) {
    fileFormat_ = format;
    return *this;
  }

  /// Sets the current table schema of the file (a Type tree).  This could be
  /// different from the actual schema in file if schema evolution happened.
  /// For "dwrf" format, a default schema is derived from the file. For "rc"
  /// format, there is no default schema.
  ReaderOptions& setFileSchema(const RowTypePtr& schema) {
    fileSchema_ = schema;
    return *this;
  }

  /// Sets the location of the tail as defined by the logical length of the
  /// file.
  ReaderOptions& setTailLocation(uint64_t offset) {
    tailLocation_ = offset;
    return *this;
  }

  /// Modifies the serialization-deserialization options.
  ReaderOptions& setSerDeOptions(const SerDeOptions& serdeOpts) {
    serDeOptions_ = serdeOpts;
    return *this;
  }

  ReaderOptions& setDecrypterFactory(
      const std::shared_ptr<encryption::DecrypterFactory>& factory) {
    decrypterFactory_ = factory;
    return *this;
  }

  ReaderOptions& setFooterEstimatedSize(uint64_t size) {
    footerEstimatedSize_ = size;
    return *this;
  }

  ReaderOptions& setFilePreloadThreshold(uint64_t threshold) {
    filePreloadThreshold_ = threshold;
    return *this;
  }

  ReaderOptions& setFileColumnNamesReadAsLowerCase(bool flag) {
    fileColumnNamesReadAsLowerCase_ = flag;
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

  ReaderOptions& setSessionTimezone(const tz::TimeZone* sessionTimezone) {
    sessionTimezone_ = sessionTimezone;
    return *this;
  }

  ReaderOptions& setAdjustTimestampToTimezone(bool adjustTimestampToTimezone) {
    adjustTimestampToTimezone_ = adjustTimestampToTimezone;
    return *this;
  }

  /// Gets the desired tail location.
  uint64_t tailLocation() const {
    return tailLocation_;
  }

  /// Gets the file format.
  FileFormat fileFormat() const {
    return fileFormat_;
  }

  /// Gets the file schema.
  const std::shared_ptr<const velox::RowType>& fileSchema() const {
    return fileSchema_;
  }

  SerDeOptions& serDeOptions() {
    return serDeOptions_;
  }

  const SerDeOptions& serDeOptions() const {
    return serDeOptions_;
  }

  const std::shared_ptr<encryption::DecrypterFactory> decrypterFactory() const {
    return decrypterFactory_;
  }

  uint64_t footerEstimatedSize() const {
    return footerEstimatedSize_;
  }

  uint64_t filePreloadThreshold() const {
    return filePreloadThreshold_;
  }

  const std::shared_ptr<folly::Executor>& ioExecutor() const {
    return ioExecutor_;
  }

  const tz::TimeZone* sessionTimezone() const {
    return sessionTimezone_;
  }

  bool adjustTimestampToTimezone() const {
    return adjustTimestampToTimezone_;
  }

  bool fileColumnNamesReadAsLowerCase() const {
    return fileColumnNamesReadAsLowerCase_;
  }

  bool useColumnNamesForColumnMapping() const {
    return useColumnNamesForColumnMapping_;
  }

  const std::shared_ptr<random::RandomSkipTracker>& randomSkip() const {
    return randomSkip_;
  }

  void setRandomSkip(std::shared_ptr<random::RandomSkipTracker> randomSkip) {
    randomSkip_ = std::move(randomSkip);
  }

  bool noCacheRetention() const {
    return noCacheRetention_;
  }

  void setNoCacheRetention(bool noCacheRetention) {
    noCacheRetention_ = noCacheRetention;
  }

  const std::shared_ptr<velox::common::ScanSpec>& scanSpec() const {
    return scanSpec_;
  }

  void setScanSpec(std::shared_ptr<velox::common::ScanSpec> scanSpec) {
    scanSpec_ = std::move(scanSpec);
  }

  bool selectiveNimbleReaderEnabled() const {
    return selectiveNimbleReaderEnabled_;
  }

  void setSelectiveNimbleReaderEnabled(bool value) {
    selectiveNimbleReaderEnabled_ = value;
  }

 private:
  uint64_t tailLocation_;
  FileFormat fileFormat_;
  RowTypePtr fileSchema_;
  SerDeOptions serDeOptions_;
  std::shared_ptr<encryption::DecrypterFactory> decrypterFactory_;
  uint64_t footerEstimatedSize_{kDefaultFooterEstimatedSize};
  uint64_t filePreloadThreshold_{kDefaultFilePreloadThreshold};
  bool fileColumnNamesReadAsLowerCase_{false};
  bool useColumnNamesForColumnMapping_{false};
  std::shared_ptr<folly::Executor> ioExecutor_;
  std::shared_ptr<random::RandomSkipTracker> randomSkip_;
  std::shared_ptr<velox::common::ScanSpec> scanSpec_;
  const tz::TimeZone* sessionTimezone_{nullptr};
  bool adjustTimestampToTimezone_{false};
  bool selectiveNimbleReaderEnabled_{false};
};

struct WriterOptions {
  TypePtr schema{nullptr};
  velox::memory::MemoryPool* memoryPool{nullptr};
  const velox::common::SpillConfig* spillConfig{nullptr};
  tsan_atomic<bool>* nonReclaimableSection{nullptr};

  /// A ready-to-use default memory reclaimer factory. It shall be provided by
  /// the system that creates writers to ensure a smooth memory system
  /// integration (e.g. graceful suspension upon arbitration request). Writer
  /// can choose to implement its custom memory reclaimer if needed and not use
  /// this default one.
  std::function<std::unique_ptr<velox::memory::MemoryReclaimer>()>
      memoryReclaimerFactory{[]() { return nullptr; }};

  std::optional<velox::common::CompressionKind> compressionKind;
  std::map<std::string, std::string> serdeParameters;
  std::function<std::unique_ptr<dwio::common::FlushPolicy>()>
      flushPolicyFactory;

  std::string sessionTimezoneName;
  bool adjustTimestampToTimezone{false};

  // WriterOption implementations can implement this function to specify how to
  // process format-specific session and connector configs.
  virtual void processConfigs(
      const config::ConfigBase& connectorConfig,
      const config::ConfigBase& session) {}

  virtual ~WriterOptions() = default;
};

} // namespace facebook::velox::dwio::common

template <>
struct fmt::formatter<facebook::velox::dwio::common::FileFormat>
    : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(facebook::velox::dwio::common::FileFormat fmt, FormatContext& ctx)
      const {
    return formatter<std::string_view>::format(
        facebook::velox::dwio::common::toString(fmt), ctx);
  }
};
