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

#include "velox/dwio/text/writer/TextWriter.h"

#include <utility>
#include "velox/common/base/Pointers.h"
#include "velox/common/encode/Base64.h"
#include "velox/exec/MemoryReclaimer.h"

namespace facebook::velox::text {
template <typename T>
std::optional<std::string> toTextStr(T val) {
  return std::optional(std::to_string(val));
}

template <>
std::optional<std::string> toTextStr<bool>(bool val) {
  return val ? std::optional("true") : std::optional("false");
}

template <>
std::optional<std::string> toTextStr<float>(float val) {
  if (std::isnan(val)) {
    return std::optional("NaN");
  } else if (std::isinf(val)) {
    return std::optional("Infinity");
  } else {
    return {std::to_string(val)};
  }
}

template <>
std::optional<std::string> toTextStr<double>(double val) {
  if (std::isnan(val)) {
    return std::optional("NaN");
  } else if (std::isinf(val)) {
    return std::optional("Infinity");
  } else {
    return {std::to_string(val)};
  }
}

template <>
std::optional<std::string> toTextStr<Timestamp>(Timestamp val) {
  TimestampToStringOptions options;
  options.dateTimeSeparator = ' ';
  options.precision = TimestampPrecision::kMilliseconds;
  return {val.toString(options)};
}

TextWriter::TextWriter(
    RowTypePtr schema,
    std::unique_ptr<dwio::common::FileSink> sink,
    const std::shared_ptr<text::WriterOptions>& options)
    : schema_(std::move(schema)),
      bufferedWriterSink_(std::make_unique<BufferedWriterSink>(
          std::move(sink),
          options->memoryPool->addLeafChild(fmt::format(
              "{}.text_writer_node.{}",
              options->memoryPool->name(),
              folly::to<std::string>(folly::Random::rand64()))),
          options->defaultFlushCount)) {}

void TextWriter::write(const VectorPtr& data) {
  VELOX_CHECK_EQ(
      data->encoding(),
      VectorEncoding::Simple::ROW,
      "Text writer expects row vector input");
  VELOX_CHECK(
      data->type()->equivalent(*schema_),
      "The file schema type should be equal with the input row vector type.");
  const RowVector* dataRowVector = data->as<RowVector>();

  std::vector<std::shared_ptr<DecodedVector>> decodedColumnVectors;
  const auto numColumns = dataRowVector->childrenSize();
  for (size_t column = 0; column < numColumns; ++column) {
    auto decodedColumnVector = std::make_shared<DecodedVector>(DecodedVector(
        *dataRowVector->childAt(column),
        SelectivityVector(dataRowVector->size())));
    decodedColumnVectors.push_back(std::move(decodedColumnVector));
  }

  for (vector_size_t row = 0; row < data->size(); ++row) {
    for (size_t column = 0; column < numColumns; ++column) {
      if (column != 0) {
        bufferedWriterSink_->write(TextFileTraits::kSOH);
      }
      writeCellValue(
          decodedColumnVectors.at(column), schema_->childAt(column), row);
    }
    bufferedWriterSink_->write(TextFileTraits::kNewLine);
  }
}

void TextWriter::flush() {
  bufferedWriterSink_->flush();
}

void TextWriter::close() {
  bufferedWriterSink_->close();
}

void TextWriter::abort() {
  bufferedWriterSink_->abort();
}

void TextWriter::writeCellValue(
    const std::shared_ptr<DecodedVector>& decodedColumnVector,
    const TypePtr& type,
    vector_size_t row) {
  std::optional<std::string> dataStr;
  std::optional<StringView> dataSV;

  if (decodedColumnVector->isNullAt(row)) {
    bufferedWriterSink_->write(
        TextFileTraits::kNullData.data(), TextFileTraits::kNullData.length());
    return;
  }
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      dataStr =
          toTextStr(folly::to<bool>(decodedColumnVector->valueAt<bool>(row)));
      break;
    case TypeKind::TINYINT:
      dataStr = toTextStr(decodedColumnVector->valueAt<int8_t>(row));
      break;
    case TypeKind::SMALLINT:
      dataStr = toTextStr(decodedColumnVector->valueAt<int16_t>(row));
      break;
    case TypeKind::INTEGER:
      dataStr = toTextStr(decodedColumnVector->valueAt<int32_t>(row));
      break;
    case TypeKind::BIGINT:
      dataStr = toTextStr(decodedColumnVector->valueAt<int64_t>(row));
      break;
    case TypeKind::REAL:
      dataStr = toTextStr(decodedColumnVector->valueAt<float>(row));
      break;
    case TypeKind::DOUBLE:
      dataStr = toTextStr(decodedColumnVector->valueAt<double>(row));
      break;
    case TypeKind::TIMESTAMP:
      dataStr = toTextStr(decodedColumnVector->valueAt<Timestamp>(row));
      break;
    case TypeKind::VARCHAR:
      dataSV = std::optional(decodedColumnVector->valueAt<StringView>(row));
      break;
    case TypeKind::VARBINARY: {
      auto data = decodedColumnVector->valueAt<StringView>(row);
      dataStr =
          std::optional(encoding::Base64::encode(data.data(), data.size()));
      break;
    }
    // TODO Add support for complex types
    case TypeKind::ARRAY:
      [[fallthrough]];
    case TypeKind::MAP:
      [[fallthrough]];
    case TypeKind::ROW:
      [[fallthrough]];
    case TypeKind::UNKNOWN:
      [[fallthrough]];
    default:
      VELOX_NYI("{} is not supported yet in TextWriter", type->kind());
  }

  if (dataStr.has_value()) {
    VELOX_CHECK(!dataSV.has_value());
    bufferedWriterSink_->write(
        dataStr.value().data(), dataStr.value().length());
    return;
  }

  VELOX_CHECK(dataSV.has_value());
  bufferedWriterSink_->write(dataSV.value().data(), dataSV.value().size());
}

std::unique_ptr<dwio::common::Writer> TextWriterFactory::createWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    const std::shared_ptr<dwio::common::WriterOptions>& options) {
  auto textOptions = std::dynamic_pointer_cast<text::WriterOptions>(options);
  VELOX_CHECK_NOT_NULL(
      textOptions, "Text writer factory expected a Text WriterOptions object.");
  return std::make_unique<TextWriter>(
      asRowType(options->schema), std::move(sink), textOptions);
}

std::unique_ptr<dwio::common::WriterOptions>
TextWriterFactory::createWriterOptions() {
  return std::make_unique<text::WriterOptions>();
}

} // namespace facebook::velox::text
