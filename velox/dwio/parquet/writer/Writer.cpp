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

#include "velox/vector/arrow/Bridge.h"

#include <arrow/c/bridge.h> // @manual
#include <arrow/table.h> // @manual
#include <parquet/arrow/writer.h> // @manual
#include "velox/dwio/parquet/writer/Writer.h"

namespace facebook::velox::parquet {

// Utility for capturing Arrow output into a DataBuffer.
class ArrowDataBufferSink : public arrow::io::OutputStream {
 public:
  explicit ArrowDataBufferSink(memory::MemoryPool& pool, double growRatio = 1)
      : growRatio_(growRatio), buffer_(pool) {}

  arrow::Status Write(const std::shared_ptr<arrow::Buffer>& data) override {
    auto requestCapacity = buffer_.size() + data->size();
    if (requestCapacity > buffer_.capacity()) {
      buffer_.reserve(growRatio_ * (requestCapacity));
    }
    buffer_.append(
        buffer_.size(),
        reinterpret_cast<const char*>(data->data()),
        data->size());
    return arrow::Status::OK();
  }

  arrow::Status Write(const void* data, int64_t nbytes) override {
    auto requestCapacity = buffer_.size() + nbytes;
    if (requestCapacity > buffer_.capacity()) {
      buffer_.reserve(growRatio_ * (requestCapacity));
    }
    buffer_.append(buffer_.size(), reinterpret_cast<const char*>(data), nbytes);
    return arrow::Status::OK();
  }

  arrow::Status Flush() override {
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const override {
    return buffer_.size();
  }

  arrow::Status Close() override {
    return arrow::Status::OK();
  }

  bool closed() const override {
    return false;
  }

  dwio::common::DataBuffer<char>& dataBuffer() {
    return buffer_;
  }

 private:
  const double growRatio_ = 1;
  dwio::common::DataBuffer<char> buffer_;
};

struct ArrowContext {
  std::unique_ptr<::parquet::arrow::FileWriter> writer;
  std::shared_ptr<::parquet::WriterProperties> properties;
};

::parquet::Compression::type getArrowParquetCompression(
    dwio::common::CompressionKind compression) {
  if (compression == dwio::common::CompressionKind_SNAPPY) {
    return ::parquet::Compression::SNAPPY;
  } else if (compression == dwio::common::CompressionKind_GZIP) {
    return ::parquet::Compression::GZIP;
  } else if (compression == dwio::common::CompressionKind_ZSTD) {
    return ::parquet::Compression::ZSTD;
  } else if (compression == dwio::common::CompressionKind_NONE) {
    return ::parquet::Compression::UNCOMPRESSED;
  } else {
    VELOX_FAIL("Unsupported compression {}", compression);
  }
}

std::shared_ptr<::parquet::WriterProperties> getArrowParquetWriterOptions(
    const parquet::WriterOptions& options) {
  auto builder = ::parquet::WriterProperties::Builder();
  ::parquet::WriterProperties::Builder* properties = &builder;
  if (!options.enableDictionary) {
    properties = properties->disable_dictionary();
  }
  properties =
      properties->compression(getArrowParquetCompression(options.compression));
  properties = properties->data_pagesize(options.dataPageSize);
  return properties->build();
}

Writer::Writer(
    std::unique_ptr<dwio::common::DataSink> sink,
    const WriterOptions& options,
    std::shared_ptr<memory::MemoryPool> pool)
    : rowsInRowGroup_(options.rowsInRowGroup),
      bufferGrowRatio_(options.bufferGrowRatio),
      pool_(std::move(pool)),
      generalPool_{pool_->addLeafChild(".general")},
      finalSink_(std::move(sink)) {
  arrowContext_ = std::make_shared<ArrowContext>();
  arrowContext_->properties = getArrowParquetWriterOptions(options);
}

Writer::Writer(
    std::unique_ptr<dwio::common::DataSink> sink,
    const WriterOptions& options)
    : Writer{
          std::move(sink),
          options,
          options.memoryPool->addAggregateChild(fmt::format(
              "writer_node_{}",
              folly::to<std::string>(folly::Random::rand64())))} {}

void Writer::write(const VectorPtr& data) {
  ArrowArray array;
  ArrowSchema schema;
  exportToArrow(data, array, generalPool_.get());
  exportToArrow(data, schema);
  PARQUET_ASSIGN_OR_THROW(
      auto recordBatch, arrow::ImportRecordBatch(&array, &schema));
  auto table = arrow::Table::Make(
      recordBatch->schema(), recordBatch->columns(), data->size());
  if (!arrowContext_->writer) {
    stream_ =
        std::make_shared<ArrowDataBufferSink>(*generalPool_, bufferGrowRatio_);
    auto arrowProperties = ::parquet::ArrowWriterProperties::Builder().build();
    PARQUET_THROW_NOT_OK(::parquet::arrow::FileWriter::Open(
        *recordBatch->schema(),
        arrow::default_memory_pool(),
        stream_,
        arrowContext_->properties,
        arrowProperties,
        &arrowContext_->writer));
  }

  PARQUET_THROW_NOT_OK(arrowContext_->writer->WriteTable(*table, 10000));
}

bool Writer::isArrowCodecAvailable(dwio::common::CompressionKind compression) {
  return arrow::util::Codec::IsAvailable(
      getArrowParquetCompression(compression));
}

void Writer::flush() {
  if (arrowContext_->writer) {
    PARQUET_THROW_NOT_OK(arrowContext_->writer->Close());
    arrowContext_->writer.reset();
    finalSink_->write(std::move(stream_->dataBuffer()));
  }
}

void Writer::newRowGroup(int32_t numRows) {
  PARQUET_THROW_NOT_OK(arrowContext_->writer->NewRowGroup(numRows));
}

void Writer::close() {
  flush();
  finalSink_->close();
}

parquet::WriterOptions getParquetOptions(
    const dwio::common::WriterOptions& options) {
  parquet::WriterOptions parquetOptions;
  parquetOptions.memoryPool = options.memoryPool;
  return parquetOptions;
}

std::unique_ptr<dwio::common::Writer> ParquetWriterFactory::createWriter(
    std::unique_ptr<dwio::common::DataSink> sink,
    const dwio::common::WriterOptions& options) {
  auto parquetOptions = getParquetOptions(options);
  return std::make_unique<Writer>(std::move(sink), parquetOptions);
}

} // namespace facebook::velox::parquet
