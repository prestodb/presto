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
#include "velox/common/encode/Base64.h"

#include <utility>

namespace facebook::velox::text {

using dwio::common::SerDeOptions;

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
  val.toTimezone(Timestamp::defaultTimezone());
  options.dateTimeSeparator = ' ';
  options.precision = TimestampPrecision::kMilliseconds;
  return {val.toString(options)};
}

TextWriter::TextWriter(
    RowTypePtr schema,
    std::unique_ptr<dwio::common::FileSink> sink,
    const std::shared_ptr<text::WriterOptions>& options,
    const SerDeOptions& serDeOptions)
    : schema_(std::move(schema)),
      bufferedWriterSink_(std::make_unique<BufferedWriterSink>(
          std::move(sink),
          options->memoryPool->addLeafChild(fmt::format(
              "{}.text_writer_node.{}",
              options->memoryPool->name(),
              folly::to<std::string>(folly::Random::rand64()))),
          options->defaultFlushCount)),
      headerLineCount_(options->headerLineCount),
      serDeOptions_(serDeOptions) {
  VELOX_CHECK_LE(headerLineCount_, 1, "Header line count must be <= 1");
}

uint8_t TextWriter::getDelimiterForDepth(uint8_t depth) const {
  VELOX_CHECK_LT(
      depth,
      serDeOptions_.separators.size(),
      "Depth {} exceeds maximum supported depth",
      depth);
  return serDeOptions_.separators[depth];
}

// Adds escape characters before separator/delimiting characters in the input
// string.
std::string TextWriter::addEscapeChar(std::string&& data, uint8_t depth) {
  if (!serDeOptions_.isEscaped) {
    return std::move(data);
  }

  std::string escapedData;
  escapedData.reserve(data.length() * 2);

  for (size_t i = 0; i < data.length(); ++i) {
    // Break out of the loop earlier if we count down.
    for (int j = depth - 1; j >= 0; --j) {
      if (data[i] == serDeOptions_.separators[j]) {
        escapedData += static_cast<char>(serDeOptions_.escapeChar);
        break;
      }
    }
    escapedData += data[i];
  }

  return escapedData;
}

void TextWriter::write(const VectorPtr& data) {
  VELOX_CHECK_EQ(
      data->encoding(),
      VectorEncoding::Simple::ROW,
      "Text writer expects row vector input");

  VELOX_CHECK(
      data->type()->equivalent(*schema_),
      "The file schema type should be equal with the input row vector type.");

  // write 1 row of header
  if (headerLineCount_ == 1) {
    const auto numCols = schema_->size();
    for (column_index_t col = 0; col < numCols; ++col) {
      if (col != 0) {
        bufferedWriterSink_->write((char)serDeOptions_.separators[0]);
      }

      std::string escapedcolName =
          addEscapeChar(std::string(schema_->nameOf(col)), 0);
      bufferedWriterSink_->write(
          escapedcolName.data(), escapedcolName.length());
    }

    bufferedWriterSink_->write((char)serDeOptions_.newLine);
  }

  const RowVector* dataRowVector = data->as<RowVector>();

  std::vector<std::shared_ptr<DecodedVector>> decodedColumnVectors;
  const auto numColumns = dataRowVector->childrenSize();
  for (size_t column = 0; column < numColumns; ++column) {
    auto decodedColumnVector = std::make_shared<DecodedVector>(DecodedVector(
        *dataRowVector->childAt(column),
        SelectivityVector(dataRowVector->size())));
    decodedColumnVectors.push_back(std::move(decodedColumnVector));
  }

  std::optional<uint8_t> delimiter;
  for (vector_size_t row = 0; row < data->size(); ++row) {
    for (size_t column = 0; column < numColumns; ++column) {
      delimiter = (column == 0) ? std::nullopt
                                : std::optional(serDeOptions_.separators[0]);
      writeCellValue(
          decodedColumnVectors.at(column),
          schema_->childAt(column)->kind(),
          row,
          0,
          delimiter);
    }
    bufferedWriterSink_->write((char)serDeOptions_.newLine);
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
    const TypeKind type,
    vector_size_t row,
    uint8_t depth,
    std::optional<uint8_t> delimiter) {
  if (delimiter.has_value()) {
    bufferedWriterSink_->write((char)delimiter.value());
  }

  if (decodedColumnVector->isNullAt(row)) {
    std::string escapedNullString =
        addEscapeChar(std::string(serDeOptions_.nullString), depth);
    bufferedWriterSink_->write(
        escapedNullString.data(), escapedNullString.length());
    return;
  }

  ++depth;

  /// TODO: Increase supported depth in future
  VELOX_CHECK_LE(depth, 4, "Depth {} exceeds maximum supported depth", 4);

  std::optional<std::string> dataStr = std::nullopt;
  std::optional<StringView> dataSV = std::nullopt;

  switch (type) {
    case TypeKind::BOOLEAN: {
      dataStr =
          toTextStr(folly::to<bool>(decodedColumnVector->valueAt<bool>(row)));
      break;
    }
    case TypeKind::TINYINT: {
      dataStr = toTextStr(decodedColumnVector->valueAt<int8_t>(row));
      break;
    }
    case TypeKind::SMALLINT: {
      dataStr = toTextStr(decodedColumnVector->valueAt<int16_t>(row));
      break;
    }
    case TypeKind::INTEGER: {
      dataStr = toTextStr(decodedColumnVector->valueAt<int32_t>(row));
      break;
    }
    case TypeKind::BIGINT: {
      dataStr = toTextStr(decodedColumnVector->valueAt<int64_t>(row));
      break;
    }
    case TypeKind::REAL: {
      dataStr = toTextStr(decodedColumnVector->valueAt<float>(row));
      break;
    }
    case TypeKind::DOUBLE: {
      dataStr = toTextStr(decodedColumnVector->valueAt<double>(row));
      break;
    }
    case TypeKind::TIMESTAMP: {
      dataStr = toTextStr(decodedColumnVector->valueAt<Timestamp>(row));
      break;
    }
    case TypeKind::VARCHAR: {
      dataSV = std::optional(decodedColumnVector->valueAt<StringView>(row));
      break;
    }
    case TypeKind::VARBINARY: {
      auto data = decodedColumnVector->valueAt<StringView>(row);
      dataStr =
          std::optional(encoding::Base64::encode(data.data(), data.size()));
      break;
    }
    case TypeKind::ARRAY: {
      // ARRAY vector members
      const auto& arrVecPtr = decodedColumnVector->base()->as<ArrayVector>();
      const auto& indices = decodedColumnVector->indices();
      const auto& size = arrVecPtr->sizeAt(indices[row]);
      const auto& offset = arrVecPtr->offsetAt(indices[row]);

      auto slice = arrVecPtr->elements()->slice(offset, size);
      auto decodedElement =
          std::make_shared<DecodedVector>(DecodedVector(*slice));
      for (int i = 0; i < size; ++i) {
        delimiter = (i == 0) ? std::nullopt
                             : std::optional(getDelimiterForDepth(depth));
        writeCellValue(
            decodedElement,
            arrVecPtr->elements().get()->typeKind(),
            i,
            depth,
            delimiter);
      }
      return;
    }
    case TypeKind::MAP: {
      // MAP vector members
      const auto& mapVecPtr = decodedColumnVector->base()->as<MapVector>();
      const auto& indices = decodedColumnVector->indices();
      const auto& size = mapVecPtr->sizeAt(indices[row]);
      const auto& offset = mapVecPtr->offsetAt(indices[row]);

      auto keySlice = mapVecPtr->mapKeys()->slice(offset, size);
      auto decodedKeys =
          std::make_shared<DecodedVector>(DecodedVector(*keySlice));

      auto valSlice = mapVecPtr->mapValues()->slice(offset, size);
      auto decodedValues =
          std::make_shared<DecodedVector>(DecodedVector(*valSlice));

      for (int i = 0; i < size; ++i) {
        delimiter = (i == 0) ? std::nullopt
                             : std::optional(getDelimiterForDepth(depth));
        writeCellValue(
            decodedKeys,
            mapVecPtr->mapKeys().get()->typeKind(),
            i,
            depth,
            delimiter);

        delimiter = std::optional(getDelimiterForDepth(depth + 1));
        writeCellValue(
            decodedValues,
            mapVecPtr->mapValues().get()->typeKind(),
            i,
            depth,
            delimiter);
      }

      return;
    }
    case TypeKind::ROW: {
      const RowVector* rowVecPtr = decodedColumnVector->base()->as<RowVector>();
      const auto& indices = decodedColumnVector->indices();
      const auto actualRowIndex = indices[row];

      std::vector<std::shared_ptr<DecodedVector>> decodedColumnVectors;
      const auto numColumns = rowVecPtr->childrenSize();
      for (size_t column = 0; column < numColumns; ++column) {
        auto decodedColumnVector =
            std::make_shared<DecodedVector>(DecodedVector(
                *rowVecPtr->childAt(column),
                SelectivityVector(rowVecPtr->size())));
        decodedColumnVectors.push_back(std::move(decodedColumnVector));
      }

      std::optional<char> nestedRowDelimiter;
      for (size_t column = 0; column < numColumns; ++column) {
        nestedRowDelimiter = (column == 0)
            ? std::nullopt
            : std::optional(getDelimiterForDepth(depth));
        writeCellValue(
            decodedColumnVectors.at(column),
            rowVecPtr->childAt(column)->typeKind(),
            actualRowIndex,
            depth,
            nestedRowDelimiter);
      }
      return;
    }
    case TypeKind::UNKNOWN:
      [[fallthrough]];
    case TypeKind::FUNCTION:
      [[fallthrough]];
    case TypeKind::OPAQUE:
      [[fallthrough]];
    case TypeKind::INVALID:
      [[fallthrough]];
    default:
      VELOX_NYI(
          "Text writer does not support type {}", mapTypeKindToName(type));
  }

  VELOX_CHECK(
      dataStr.has_value() ^ dataSV.has_value(),
      "Exactly one of dataStr or dataSV must be set. Currently dataStr is {} and dataSV is {}",
      dataStr.has_value(),
      dataSV.has_value());

  std::string data = dataStr.has_value()
      ? dataStr.value()
      : std::string(dataSV.value().data(), dataSV.value().size());

  std::string escapedData = addEscapeChar(std::move(data), depth);
  bufferedWriterSink_->write(escapedData.data(), escapedData.length());
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
