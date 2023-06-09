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

#include "velox/dwio/common/Common.h"
#include "velox/dwio/common/DataBuffer.h"
#include "velox/dwio/common/DataSink.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Writer.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::parquet {

class ArrowDataBufferSink;

struct ArrowContext;

struct WriterOptions {
  bool enableDictionary = true;
  int64_t dataPageSize = 1'024 * 1'024;
  int32_t rowsInRowGroup = 10'000;
  int64_t maxRowGroupLength = 1'024 * 1'024;
  int64_t dictionaryPageSizeLimit = 1'024 * 1'024;
  double bufferGrowRatio = 1;
  dwio::common::CompressionKind compression =
      dwio::common::CompressionKind_NONE;
  velox::memory::MemoryPool* memoryPool;
};

// Writes Velox vectors into  a DataSink using Arrow Parquet writer.
class Writer : public dwio::common::Writer {
 public:
  // Constructs a writer with output to 'sink'. A new row group is
  // started every 'rowsInRowGroup' top level rows. 'pool' is used for
  // temporary memory. 'properties' specifies Parquet-specific
  // options.
  Writer(
      std::unique_ptr<dwio::common::DataSink> sink,
      const WriterOptions& options,
      std::shared_ptr<memory::MemoryPool> pool);

  Writer(
      std::unique_ptr<dwio::common::DataSink> sink,
      const WriterOptions& options);

  static bool isArrowCodecAvailable(dwio::common::CompressionKind compression);

  // Appends 'data' into the writer.
  void write(const VectorPtr& data) override;

  void flush();

  // Forces a row group boundary before the data added by next write().
  void newRowGroup(int32_t numRows);

  // Closes 'this', After close, data can no longer be added and the completed
  // Parquet file is flushed into 'sink' provided at construction. 'sink' stays
  // live until destruction of 'this'.
  void close();

 private:
  const int32_t rowsInRowGroup_;
  const double bufferGrowRatio_;

  // Pool for 'stream_'.
  std::shared_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<memory::MemoryPool> generalPool_;

  // Final destination of output.
  std::unique_ptr<dwio::common::DataSink> finalSink_;

  // Temporary Arrow stream for capturing the output.
  std::shared_ptr<ArrowDataBufferSink> stream_;

  std::shared_ptr<ArrowContext> arrowContext_;
};

class ParquetWriterFactory : public dwio::common::WriterFactory {
 public:
  ParquetWriterFactory() : WriterFactory(dwio::common::FileFormat::PARQUET) {}

  std::unique_ptr<dwio::common::Writer> createWriter(
      std::unique_ptr<dwio::common::DataSink> sink,
      const dwio::common::WriterOptions& options) override;
};

} // namespace facebook::velox::parquet
