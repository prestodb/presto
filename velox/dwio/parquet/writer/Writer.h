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

#include "velox/common/compression/Compression.h"
#include "velox/common/config/Config.h"
#include "velox/dwio/common/DataBuffer.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/FlushPolicy.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Writer.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/dwio/parquet/writer/arrow/Types.h"
#include "velox/dwio/parquet/writer/arrow/util/Compression.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::parquet {

using facebook::velox::parquet::arrow::util::CodecOptions;

class ArrowDataBufferSink;

struct ArrowContext;

class DefaultFlushPolicy : public dwio::common::FlushPolicy {
 public:
  DefaultFlushPolicy()
      : rowsInRowGroup_(1'024 * 1'024), bytesInRowGroup_(128 * 1'024 * 1'024) {}
  DefaultFlushPolicy(uint64_t rowsInRowGroup, int64_t bytesInRowGroup)
      : rowsInRowGroup_(rowsInRowGroup), bytesInRowGroup_(bytesInRowGroup) {}

  bool shouldFlush(
      const dwio::common::StripeProgress& stripeProgress) override {
    return stripeProgress.stripeRowCount >= rowsInRowGroup_ ||
        stripeProgress.stripeSizeEstimate >= bytesInRowGroup_;
  }

  void onClose() override {
    // No-op
  }

  uint64_t rowsInRowGroup() const {
    return rowsInRowGroup_;
  }

  int64_t bytesInRowGroup() const {
    return bytesInRowGroup_;
  }

 private:
  const uint64_t rowsInRowGroup_;
  const int64_t bytesInRowGroup_;
};

class LambdaFlushPolicy : public DefaultFlushPolicy {
 public:
  explicit LambdaFlushPolicy(
      uint64_t rowsInRowGroup,
      int64_t bytesInRowGroup,
      std::function<bool()> lambda)
      : DefaultFlushPolicy(rowsInRowGroup, bytesInRowGroup) {
    lambda_ = std::move(lambda);
  }
  virtual ~LambdaFlushPolicy() override = default;

  bool shouldFlush(
      const dwio::common::StripeProgress& stripeProgress) override {
    return lambda_() || DefaultFlushPolicy::shouldFlush(stripeProgress);
  }

 private:
  std::function<bool()> lambda_;
};

struct WriterOptions : public dwio::common::WriterOptions {
  bool enableDictionary = true;
  int64_t dataPageSize = 1'024 * 1'024;
  int64_t dictionaryPageSizeLimit = 1'024 * 1'024;
  // Growth ratio passed to ArrowDataBufferSink. The default value is a
  // heuristic borrowed from
  // folly/FBVector(https://github.com/facebook/folly/blob/main/folly/docs/FBVector.md#memory-handling).
  double bufferGrowRatio = 1.5;

  arrow::Encoding::type encoding = arrow::Encoding::PLAIN;

  std::shared_ptr<CodecOptions> codecOptions;
  std::unordered_map<std::string, common::CompressionKind>
      columnCompressionsMap;

  /// Timestamp unit for Parquet write through Arrow bridge.
  /// Default if not specified: TimestampUnit::kNano (9).
  std::optional<TimestampUnit> parquetWriteTimestampUnit;
  /// Timestamp time zone for Parquet write through Arrow bridge.
  std::optional<std::string> parquetWriteTimestampTimeZone;
  bool writeInt96AsTimestamp = false;

  // Parsing session and hive configs.

  // This isn't a typo; session and hive connector config names are different
  // ('_' vs '-').
  static constexpr const char* kParquetSessionWriteTimestampUnit =
      "hive.parquet.writer.timestamp_unit";
  static constexpr const char* kParquetHiveConnectorWriteTimestampUnit =
      "hive.parquet.writer.timestamp-unit";
};

// Writes Velox vectors into  a DataSink using Arrow Parquet writer.
class Writer : public dwio::common::Writer {
 public:
  // Constructs a writer with output to 'sink'. A new row group is
  // started every 'rowsInRowGroup' top level rows. 'pool' is used for
  // temporary memory. 'properties' specifies Parquet-specific
  // options. 'schema' specifies the file's overall schema, and it is always
  // non-null.
  Writer(
      std::unique_ptr<dwio::common::FileSink> sink,
      const WriterOptions& options,
      std::shared_ptr<memory::MemoryPool> pool,
      RowTypePtr schema);

  Writer(
      std::unique_ptr<dwio::common::FileSink> sink,
      const WriterOptions& options,
      RowTypePtr schema);

  ~Writer() override = default;

  static bool isCodecAvailable(common::CompressionKind compression);

  // Appends 'data' into the writer.
  void write(const VectorPtr& data) override;

  void flush() override;

  // Forces a row group boundary before the data added by next write().
  void newRowGroup(int32_t numRows);

  // Closes 'this', After close, data can no longer be added and the completed
  // Parquet file is flushed into 'sink' provided at construction. 'sink' stays
  // live until destruction of 'this'.
  void close() override;

  void abort() override;

 private:
  // Sets the memory reclaimers for all the memory pools used by this writer.
  void setMemoryReclaimers();

  // Pool for 'stream_'.
  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<memory::MemoryPool> generalPool_;

  // Temporary Arrow stream for capturing the output.
  std::shared_ptr<ArrowDataBufferSink> stream_;

  std::shared_ptr<ArrowContext> arrowContext_;

  std::unique_ptr<DefaultFlushPolicy> flushPolicy_;

  const RowTypePtr schema_;

  ArrowOptions options_{.flattenDictionary = true, .flattenConstant = true};

  // Whether to write Int96 timestamps in Arrow Parquet write.
  bool writeInt96AsTimestamp_;
};

class ParquetWriterFactory : public dwio::common::WriterFactory {
 public:
  ParquetWriterFactory() : WriterFactory(dwio::common::FileFormat::PARQUET) {}

  std::unique_ptr<dwio::common::Writer> createWriter(
      std::unique_ptr<dwio::common::FileSink> sink,
      const std::shared_ptr<dwio::common::WriterOptions>& options) override;

  std::unique_ptr<dwio::common::WriterOptions> createWriterOptions() override;
};

} // namespace facebook::velox::parquet
