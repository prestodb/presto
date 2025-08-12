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

#include "velox/experimental/cudf/connectors/parquet/ParquetConfig.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetConnectorSplit.h"
#include "velox/experimental/cudf/connectors/parquet/ParquetTableHandle.h"
#include "velox/experimental/cudf/connectors/parquet/WriterOptions.h"

#include "velox/common/compression/Compression.h"
#include "velox/connectors/Connector.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/type/Type.h"

#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/types.hpp>

namespace facebook::velox::cudf_velox::connector::parquet {

using namespace facebook::velox::connector;

class LocationHandle;
using LocationHandlePtr = std::shared_ptr<const LocationHandle>;

/// Location related properties of the Parquet table to be written.
class LocationHandle : public ISerializable {
 public:
  enum class TableType {
    /// Write to a new table to be created.
    kNew,
  };

  LocationHandle(
      std::string targetPath,
      TableType tableType,
      std::string targetFileName = "")
      : targetPath_(std::move(targetPath)),
        targetFileName_(std::move(targetFileName)),
        tableType_(tableType) {}

  const std::string& targetPath() const {
    return targetPath_;
  }

  const std::string& targetFileName() const {
    return targetFileName_;
  }

  TableType tableType() const {
    return tableType_;
  }

  std::string toString() const;

  static void registerSerDe();

  folly::dynamic serialize() const override;

  static LocationHandlePtr create(const folly::dynamic& obj);

  static const std::string tableTypeName(LocationHandle::TableType type);

  static LocationHandle::TableType tableTypeFromName(const std::string& name);

 private:
  // Target directory path.
  const std::string targetPath_;
  // If non-empty, use this name instead of generating our own.
  const std::string targetFileName_;
  // Whether the table to be written is new, already existing or temporary.
  const TableType tableType_;
};

/// Parameters for Hive writers.
class ParquetWriterParameters {
 public:
  enum class UpdateMode {
    kNew, // Write files to a new directory.
  };

  /// @param updateMode Write the files to a new directory, or append to an
  /// existing directory or overwrite an existing directory.
  /// @param targetFileName The final name of a file after committing.
  /// @param targetDirectory The final directory that a file should be in after
  /// committing.
  /// @param writeFileName The temporary name of the file that a running writer
  /// writes to. If a running writer writes directory to the target file, set
  /// writeFileName to targetFileName by default.
  /// @param writeDirectory The temporary directory that a running writer writes
  /// to. If a running writer writes directory to the target directory, set
  /// writeDirectory to targetDirectory by default.
  ParquetWriterParameters(
      UpdateMode updateMode,
      std::string targetFileName,
      std::string targetDirectory,
      std::optional<std::string> writeFileName = std::nullopt,
      std::optional<std::string> writeDirectory = std::nullopt)
      : updateMode_(updateMode),
        targetFileName_(std::move(targetFileName)),
        targetDirectory_(std::move(targetDirectory)),
        writeFileName_(writeFileName.value_or(targetFileName_)),
        writeDirectory_(writeDirectory.value_or(targetDirectory_)) {}

  UpdateMode updateMode() const {
    return updateMode_;
  }

  static std::string updateModeToString(UpdateMode updateMode) {
    switch (updateMode) {
      case UpdateMode::kNew:
        return "NEW";
      default:
        VELOX_UNSUPPORTED("Unsupported update mode.");
    }
  }

  const std::string& targetFileName() const {
    return targetFileName_;
  }

  const std::string& writeFileName() const {
    return writeFileName_;
  }

  const std::string& targetDirectory() const {
    return targetDirectory_;
  }

  const std::string& writeDirectory() const {
    return writeDirectory_;
  }

 private:
  const UpdateMode updateMode_;
  const std::optional<std::string> partitionName_;
  const std::string targetFileName_;
  const std::string targetDirectory_;
  const std::string writeFileName_;
  const std::string writeDirectory_;
};

struct ParquetWriterInfo {
  ParquetWriterInfo(
      ParquetWriterParameters parameters,
      std::shared_ptr<memory::MemoryPool> _writerPool,
      std::shared_ptr<memory::MemoryPool> _sinkPool,
      std::shared_ptr<memory::MemoryPool> _sortPool)
      : writerParameters(std::move(parameters)),
        nonReclaimableSectionHolder(new tsan_atomic<bool>(false)),
        spillStats(std::make_unique<folly::Synchronized<common::SpillStats>>()),
        writerPool(std::move(_writerPool)),
        sinkPool(std::move(_sinkPool)),
        sortPool(std::move(_sortPool)) {}

  const ParquetWriterParameters writerParameters;
  const std::unique_ptr<tsan_atomic<bool>> nonReclaimableSectionHolder;
  /// Collects the spill stats from sort writer if the spilling has been
  /// triggered.
  const std::unique_ptr<folly::Synchronized<common::SpillStats>> spillStats;
  const std::shared_ptr<memory::MemoryPool> writerPool;
  const std::shared_ptr<memory::MemoryPool> sinkPool;
  const std::shared_ptr<memory::MemoryPool> sortPool;
  int64_t numWrittenRows = 0;
  int64_t inputSizeInBytes = 0;
};

class ParquetInsertTableHandle;
using ParquetInsertTableHandlePtr = std::shared_ptr<ParquetInsertTableHandle>;

/// Represents a request for Parquet write.
class ParquetInsertTableHandle : public ConnectorInsertTableHandle {
 public:
  ParquetInsertTableHandle(
      std::vector<std::shared_ptr<const ParquetColumnHandle>> inputColumns,
      std::shared_ptr<const LocationHandle> locationHandle,
      std::optional<common::CompressionKind> compressionKind = {},
      const std::unordered_map<std::string, std::string>& serdeParameters = {},
      const std::shared_ptr<dwio::common::WriterOptions>& writerOptions =
          nullptr)
      : inputColumns_(std::move(inputColumns)),
        locationHandle_(std::move(locationHandle)),
        compressionKind_(compressionKind),
        serdeParameters_(serdeParameters),
        writerOptions_(writerOptions) {
    if (compressionKind.has_value()) {
      VELOX_CHECK(
          compressionKind.value() != common::CompressionKind_MAX,
          "Unsupported compression type: CompressionKind_MAX");
      VELOX_CHECK(
          compressionKind.value() == common::CompressionKind_NONE or
              compressionKind.value() == common::CompressionKind_SNAPPY or
              compressionKind.value() == common::CompressionKind_LZ4 or
              compressionKind.value() == common::CompressionKind_ZSTD,
          "Parquet DataSink only supports NONE, SNAPPY, LZ4, and ZSTD compressions.");
    }
  }

  virtual ~ParquetInsertTableHandle() = default;

  const std::vector<std::shared_ptr<const ParquetColumnHandle>>& inputColumns()
      const {
    return inputColumns_;
  }

  const std::shared_ptr<const LocationHandle>& locationHandle() const {
    return locationHandle_;
  }

  std::optional<common::CompressionKind> compressionKind() const {
    return compressionKind_;
  }

  const dwio::common::FileFormat storageFormat() const {
    return storageFormat_;
  }

  const std::unordered_map<std::string, std::string>& serdeParameters() const {
    return serdeParameters_;
  }

  const std::shared_ptr<dwio::common::WriterOptions>& writerOptions() const {
    return writerOptions_;
  }

  bool supportsMultiThreading() const override {
    return true; // TODO: Needs more testing if this is ok
  }

  bool isExistingTable() const {
    return false; // This is always false as cudf's Parquet writer doesn't yet
                  // support updating existing Parquet files
  }

  folly::dynamic serialize() const override;

  static ParquetInsertTableHandlePtr create(const folly::dynamic& obj);

  static void registerSerDe();

  std::string toString() const override;

 private:
  const std::vector<std::shared_ptr<const ParquetColumnHandle>> inputColumns_;
  const std::shared_ptr<const LocationHandle> locationHandle_;
  const std::optional<common::CompressionKind> compressionKind_;
  const dwio::common::FileFormat storageFormat_ =
      dwio::common::FileFormat::PARQUET;
  const std::unordered_map<std::string, std::string> serdeParameters_;
  const std::shared_ptr<dwio::common::WriterOptions> writerOptions_;
};

class ParquetDataSink : public DataSink {
 public:
  /// The list of runtime stats reported by parquet data sink
  static constexpr const char* kEarlyFlushedRawBytes = "earlyFlushedRawBytes";

  /// Defines the execution states of a parquet data sink running internally.
  enum class State {
    /// The data sink accepts new append data in this state.
    kRunning = 0,
    /// The data sink flushes any buffered data to the underlying file writer
    /// but no more data can be appended.
    kFinishing = 1,
    /// The data sink is aborted on error and no more data can be appended.
    kAborted = 2,
    /// The data sink is closed on error and no more data can be appended.
    kClosed = 3
  };
  static std::string stateString(State state);

  ParquetDataSink(
      RowTypePtr inputType,
      std::shared_ptr<const ParquetInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy,
      const std::shared_ptr<const ParquetConfig>& parquetConfig);

  void appendData(RowVectorPtr input) override;

  bool finish() override;

  Stats stats() const override;

  std::vector<std::string> close() override;

  void abort() override;

  bool canReclaim() const {
    return false;
  };

 private:
  // Creates a new cudf chunked parquet writer.
  std::unique_ptr<cudf::io::chunked_parquet_writer> createCudfWriter(
      cudf::table_view cudfTable);
  cudf::io::table_input_metadata createCudfTableInputMetadata(
      cudf::table_view cudfTable);

  // Validates the state transition from 'oldState' to 'newState'.
  void checkStateTransition(State oldState, State newState);
  void setState(State newState);

  std::shared_ptr<memory::MemoryPool> createWriterPool();

  FOLLY_ALWAYS_INLINE bool sortWrite() const {
    return not sortingColumns_.empty();
  }

  FOLLY_ALWAYS_INLINE bool isCommitRequired() const {
    return false; // Since we always immediately write
  }

  FOLLY_ALWAYS_INLINE void checkRunning() const {
    VELOX_CHECK_EQ(state_, State::kRunning, "Parquet data sink is not running");
  }

  void closeInternal();
  void makeWriterOptions(ParquetWriterParameters writerParameters);

  const RowTypePtr inputType_;
  const std::shared_ptr<const ParquetInsertTableHandle> insertTableHandle_;
  const ConnectorQueryCtx* const connectorQueryCtx_;
  const CommitStrategy commitStrategy_;
  const std::shared_ptr<const ParquetConfig> parquetConfig_;
  const common::SpillConfig* const spillConfig_;
  const uint64_t sortWriterFinishTimeSliceLimitMs_{0};
  State state_{State::kRunning};

  // Below are structures for partitions from all inputs. writerInfo_ and
  // writers_ are both indexed by partitionId.
  std::unique_ptr<cudf::io::chunked_parquet_writer> writer_;

  std::vector<cudf::io::sorting_column> sortingColumns_;

  std::shared_ptr<ParquetWriterInfo> writerInfo_;

  // IO statistics collected for writer.
  std::shared_ptr<io::IoStatistics> ioStats_;
};

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& os,
    ParquetDataSink::State state) {
  os << ParquetDataSink::stateString(state);
  return os;
}
} // namespace facebook::velox::cudf_velox::connector::parquet

template <>
struct fmt::formatter<
    facebook::velox::cudf_velox::connector::parquet::ParquetDataSink::State>
    : formatter<std::string> {
  auto format(
      facebook::velox::cudf_velox::connector::parquet::ParquetDataSink::State s,
      format_context& ctx) const {
    return formatter<std::string>::format(
        facebook::velox::cudf_velox::connector::parquet::ParquetDataSink::
            stateString(s),
        ctx);
  }
};

template <>
struct fmt::formatter<
    facebook::velox::cudf_velox::connector::parquet::LocationHandle::TableType>
    : formatter<int> {
  auto format(
      facebook::velox::cudf_velox::connector::parquet::LocationHandle::TableType
          s,
      format_context& ctx) const {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
