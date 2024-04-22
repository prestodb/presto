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

#include "velox/dwio/parquet/writer/Writer.h"
#include <arrow/c/bridge.h>
#include <arrow/io/interfaces.h>
#include <arrow/table.h>
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/parquet/writer/arrow/Properties.h"
#include "velox/dwio/parquet/writer/arrow/Writer.h"
#include "velox/exec/MemoryReclaimer.h"

namespace facebook::velox::parquet {

using facebook::velox::parquet::arrow::ArrowWriterProperties;
using facebook::velox::parquet::arrow::Compression;
using facebook::velox::parquet::arrow::WriterProperties;
using facebook::velox::parquet::arrow::arrow::FileWriter;

// Utility for buffering Arrow output with a DataBuffer.
class ArrowDataBufferSink : public ::arrow::io::OutputStream {
 public:
  /// @param growRatio Growth factor used when invoking the reserve() method of
  /// DataSink, thereby helping to minimize frequent memcpy operations.
  ArrowDataBufferSink(
      std::unique_ptr<dwio::common::FileSink> sink,
      memory::MemoryPool& pool,
      double growRatio)
      : sink_(std::move(sink)), growRatio_(growRatio), buffer_(pool) {}

  ::arrow::Status Write(const std::shared_ptr<::arrow::Buffer>& data) override {
    auto requestCapacity = buffer_.size() + data->size();
    if (requestCapacity > buffer_.capacity()) {
      buffer_.reserve(growRatio_ * (requestCapacity));
    }
    buffer_.append(
        buffer_.size(),
        reinterpret_cast<const char*>(data->data()),
        data->size());
    return ::arrow::Status::OK();
  }

  ::arrow::Status Write(const void* data, int64_t nbytes) override {
    auto requestCapacity = buffer_.size() + nbytes;
    if (requestCapacity > buffer_.capacity()) {
      buffer_.reserve(growRatio_ * (requestCapacity));
    }
    buffer_.append(buffer_.size(), reinterpret_cast<const char*>(data), nbytes);
    return ::arrow::Status::OK();
  }

  ::arrow::Status Flush() override {
    bytesFlushed_ += buffer_.size();
    sink_->write(std::move(buffer_));
    return ::arrow::Status::OK();
  }

  ::arrow::Result<int64_t> Tell() const override {
    return bytesFlushed_ + buffer_.size();
  }

  ::arrow::Status Close() override {
    ARROW_RETURN_NOT_OK(Flush());
    sink_->close();
    return ::arrow::Status::OK();
  }

  bool closed() const override {
    return sink_->isClosed();
  }

  void abort() {
    sink_.reset();
    buffer_.clear();
  }

 private:
  std::unique_ptr<dwio::common::FileSink> sink_;
  const double growRatio_;
  dwio::common::DataBuffer<char> buffer_;
  int64_t bytesFlushed_ = 0;
};

struct ArrowContext {
  std::unique_ptr<FileWriter> writer;
  std::shared_ptr<::arrow::Schema> schema;
  std::shared_ptr<WriterProperties> properties;
  uint64_t stagingRows = 0;
  int64_t stagingBytes = 0;
  // columns, Arrays
  std::vector<std::vector<std::shared_ptr<::arrow::Array>>> stagingChunks;
};

Compression::type getArrowParquetCompression(
    common::CompressionKind compression) {
  if (compression == common::CompressionKind_SNAPPY) {
    return Compression::SNAPPY;
  } else if (compression == common::CompressionKind_GZIP) {
    return Compression::GZIP;
  } else if (compression == common::CompressionKind_ZSTD) {
    return Compression::ZSTD;
  } else if (compression == common::CompressionKind_NONE) {
    return Compression::UNCOMPRESSED;
  } else if (compression == common::CompressionKind_LZ4) {
    return Compression::LZ4_HADOOP;
  } else {
    VELOX_FAIL("Unsupported compression {}", compression);
  }
}

namespace {

std::shared_ptr<WriterProperties> getArrowParquetWriterOptions(
    const parquet::WriterOptions& options,
    const std::unique_ptr<DefaultFlushPolicy>& flushPolicy) {
  auto builder = WriterProperties::Builder();
  WriterProperties::Builder* properties = &builder;
  if (!options.enableDictionary) {
    properties = properties->disable_dictionary();
  }
  properties =
      properties->compression(getArrowParquetCompression(options.compression));
  for (const auto& columnCompressionValues : options.columnCompressionsMap) {
    properties->compression(
        columnCompressionValues.first,
        getArrowParquetCompression(columnCompressionValues.second));
  }
  properties = properties->encoding(options.encoding);
  properties = properties->data_pagesize(options.dataPageSize);
  properties = properties->max_row_group_length(
      static_cast<int64_t>(flushPolicy->rowsInRowGroup()));
  properties = properties->codec_options(options.codecOptions);
  properties = properties->enable_store_decimal_as_integer();
  return properties->build();
}

void validateSchemaRecursive(const RowTypePtr& schema) {
  // Check the schema's field names is not empty and unique.
  VELOX_USER_CHECK_NOT_NULL(schema, "Field schema must not be empty.");
  const auto& fieldNames = schema->names();

  folly::F14FastSet<std::string> uniqueNames;
  for (const auto& name : fieldNames) {
    VELOX_USER_CHECK(!name.empty(), "Field name must not be empty.")
    auto result = uniqueNames.insert(name);
    VELOX_USER_CHECK(
        result.second,
        "File schema should not have duplicate columns: {}",
        name);
  }

  for (auto i = 0; i < schema->size(); ++i) {
    if (auto childSchema =
            std::dynamic_pointer_cast<const RowType>(schema->childAt(i))) {
      validateSchemaRecursive(childSchema);
    }
  }
}

std::shared_ptr<::arrow::Field> updateFieldNameRecursive(
    const std::shared_ptr<::arrow::Field>& field,
    const Type& type,
    const std::string& name = "") {
  if (type.isRow()) {
    auto rowType = type.asRow();
    auto newField = field->WithName(name);
    auto structType =
        std::dynamic_pointer_cast<::arrow::StructType>(newField->type());
    auto childrenSize = rowType.size();
    std::vector<std::shared_ptr<::arrow::Field>> newFields;
    newFields.reserve(childrenSize);
    for (auto i = 0; i < childrenSize; i++) {
      newFields.push_back(updateFieldNameRecursive(
          structType->fields()[i], *rowType.childAt(i), rowType.nameOf(i)));
    }
    return newField->WithType(::arrow::struct_(newFields));
  } else if (type.isArray()) {
    auto newField = field->WithName(name);
    auto listType =
        std::dynamic_pointer_cast<::arrow::BaseListType>(newField->type());
    auto elementType = type.asArray().elementType();
    auto elementField = listType->value_field();
    return newField->WithType(
        ::arrow::list(updateFieldNameRecursive(elementField, *elementType)));
  } else if (type.isMap()) {
    auto mapType = type.asMap();
    auto newField = field->WithName(name);
    auto arrowMapType =
        std::dynamic_pointer_cast<::arrow::MapType>(newField->type());
    auto newKeyField =
        updateFieldNameRecursive(arrowMapType->key_field(), *mapType.keyType());
    auto newValueField = updateFieldNameRecursive(
        arrowMapType->item_field(), *mapType.valueType());
    return newField->WithType(
        ::arrow::map(newKeyField->type(), newValueField->type()));
  } else if (name != "") {
    return field->WithName(name);
  } else {
    return field;
  }
}

} // namespace

Writer::Writer(
    std::unique_ptr<dwio::common::FileSink> sink,
    const WriterOptions& options,
    std::shared_ptr<memory::MemoryPool> pool,
    RowTypePtr schema)
    : pool_(std::move(pool)),
      generalPool_{pool_->addLeafChild(".general")},
      stream_(std::make_shared<ArrowDataBufferSink>(
          std::move(sink),
          *generalPool_,
          options.bufferGrowRatio)),
      arrowContext_(std::make_shared<ArrowContext>()),
      schema_(std::move(schema)) {
  validateSchemaRecursive(schema_);

  if (options.flushPolicyFactory) {
    flushPolicy_ = options.flushPolicyFactory();
  } else {
    flushPolicy_ = std::make_unique<DefaultFlushPolicy>();
  }
  options_.timestampUnit =
      static_cast<TimestampUnit>(options.parquetWriteTimestampUnit);
  arrowContext_->properties =
      getArrowParquetWriterOptions(options, flushPolicy_);
  setMemoryReclaimers();
}

Writer::Writer(
    std::unique_ptr<dwio::common::FileSink> sink,
    const WriterOptions& options,
    RowTypePtr schema)
    : Writer{
          std::move(sink),
          options,
          options.memoryPool->addAggregateChild(fmt::format(
              "writer_node_{}",
              folly::to<std::string>(folly::Random::rand64()))),
          std::move(schema)} {}

void Writer::flush() {
  if (arrowContext_->stagingRows > 0) {
    if (!arrowContext_->writer) {
      auto arrowProperties = ArrowWriterProperties::Builder().build();
      PARQUET_ASSIGN_OR_THROW(
          arrowContext_->writer,
          FileWriter::Open(
              *arrowContext_->schema.get(),
              ::arrow::default_memory_pool(),
              stream_,
              arrowContext_->properties,
              arrowProperties));
    }

    auto fields = arrowContext_->schema->fields();
    std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunks;
    for (int colIdx = 0; colIdx < fields.size(); colIdx++) {
      auto dataType = fields.at(colIdx)->type();
      auto chunk =
          ::arrow::ChunkedArray::Make(
              std::move(arrowContext_->stagingChunks.at(colIdx)), dataType)
              .ValueOrDie();
      chunks.push_back(chunk);
    }
    auto table = ::arrow::Table::Make(
        arrowContext_->schema,
        std::move(chunks),
        static_cast<int64_t>(arrowContext_->stagingRows));
    PARQUET_THROW_NOT_OK(arrowContext_->writer->WriteTable(
        *table, static_cast<int64_t>(flushPolicy_->rowsInRowGroup())));
    PARQUET_THROW_NOT_OK(stream_->Flush());
    for (auto& chunk : arrowContext_->stagingChunks) {
      chunk.clear();
    }
    arrowContext_->stagingRows = 0;
    arrowContext_->stagingBytes = 0;
  }
}

dwio::common::StripeProgress getStripeProgress(
    uint64_t stagingRows,
    int64_t stagingBytes) {
  return dwio::common::StripeProgress{
      .stripeRowCount = stagingRows, .stripeSizeEstimate = stagingBytes};
}

/**
 * This method would cache input `ColumnarBatch` to make the size of row group
 * big. It would flush when:
 * - the cached numRows bigger than `rowsInRowGroup_`
 * - the cached bytes bigger than `bytesInRowGroup_`
 *
 * This method assumes each input `ColumnarBatch` have same schema.
 */
void Writer::write(const VectorPtr& data) {
  VELOX_USER_CHECK(
      data->type()->equivalent(*schema_),
      "The file schema type should be equal with the input rowvector type.");

  ArrowArray array;
  ArrowSchema schema;
  exportToArrow(data, array, generalPool_.get(), options_);
  exportToArrow(data, schema, options_);

  // Convert the arrow schema to Schema and then update the column names based
  // on schema_.
  auto arrowSchema = ::arrow::ImportSchema(&schema).ValueOrDie();
  common::testutil::TestValue::adjust(
      "facebook::velox::parquet::Writer::write", arrowSchema.get());
  std::vector<std::shared_ptr<::arrow::Field>> newFields;
  auto childSize = schema_->size();
  for (auto i = 0; i < childSize; i++) {
    newFields.push_back(updateFieldNameRecursive(
        arrowSchema->fields()[i], *schema_->childAt(i), schema_->nameOf(i)));
  }

  PARQUET_ASSIGN_OR_THROW(
      auto recordBatch,
      ::arrow::ImportRecordBatch(&array, ::arrow::schema(newFields)));
  if (!arrowContext_->schema) {
    arrowContext_->schema = recordBatch->schema();
    for (int colIdx = 0; colIdx < arrowContext_->schema->num_fields();
         colIdx++) {
      arrowContext_->stagingChunks.push_back(
          std::vector<std::shared_ptr<::arrow::Array>>());
    }
  }

  auto bytes = data->estimateFlatSize();
  auto numRows = data->size();
  if (flushPolicy_->shouldFlush(getStripeProgress(
          arrowContext_->stagingRows, arrowContext_->stagingBytes))) {
    flush();
  }

  for (int colIdx = 0; colIdx < recordBatch->num_columns(); colIdx++) {
    arrowContext_->stagingChunks.at(colIdx).push_back(
        recordBatch->column(colIdx));
  }
  arrowContext_->stagingRows += numRows;
  arrowContext_->stagingBytes += bytes;
}

bool Writer::isCodecAvailable(common::CompressionKind compression) {
  return arrow::util::Codec::IsAvailable(
      getArrowParquetCompression(compression));
}

void Writer::newRowGroup(int32_t numRows) {
  PARQUET_THROW_NOT_OK(arrowContext_->writer->NewRowGroup(numRows));
}

void Writer::close() {
  flush();

  if (arrowContext_->writer) {
    PARQUET_THROW_NOT_OK(arrowContext_->writer->Close());
    arrowContext_->writer.reset();
  }
  PARQUET_THROW_NOT_OK(stream_->Close());

  arrowContext_->stagingChunks.clear();
}

void Writer::abort() {
  stream_->abort();
  arrowContext_.reset();
}

parquet::WriterOptions getParquetOptions(
    const dwio::common::WriterOptions& options) {
  parquet::WriterOptions parquetOptions;
  parquetOptions.memoryPool = options.memoryPool;
  if (options.compressionKind.has_value()) {
    parquetOptions.compression = options.compressionKind.value();
  }
  if (options.parquetWriteTimestampUnit.has_value()) {
    parquetOptions.parquetWriteTimestampUnit =
        options.parquetWriteTimestampUnit.value();
  }
  return parquetOptions;
}

void Writer::setMemoryReclaimers() {
  VELOX_CHECK(
      !pool_->isLeaf(),
      "The root memory pool for parquet writer can't be leaf: {}",
      pool_->name());
  VELOX_CHECK_NULL(pool_->reclaimer());

  if ((pool_->parent() == nullptr) ||
      (pool_->parent()->reclaimer() == nullptr)) {
    return;
  }

  // TODO https://github.com/facebookincubator/velox/issues/8190
  pool_->setReclaimer(exec::MemoryReclaimer::create());
  generalPool_->setReclaimer(exec::MemoryReclaimer::create());
}

std::unique_ptr<dwio::common::Writer> ParquetWriterFactory::createWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    const dwio::common::WriterOptions& options) {
  auto parquetOptions = getParquetOptions(options);
  return std::make_unique<Writer>(
      std::move(sink), parquetOptions, asRowType(options.schema));
}

} // namespace facebook::velox::parquet
