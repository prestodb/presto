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
#include "velox/serializers/PrestoVectorLexer.h"

#include "velox/serializers/PrestoHeader.h"
#include "velox/serializers/PrestoSerializerDeserializationUtils.h"
#include "velox/serializers/PrestoSerializerSerializationUtils.h"

namespace facebook::velox::serializer::presto::detail {
Status PrestoVectorLexer::lex(std::vector<Token>& out) && {
  VELOX_RETURN_NOT_OK(lexHeader());

  int32_t numColumns;
  VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_COLUMNS, &numColumns));

  for (int32_t col = 0; col < numColumns; ++col) {
    VELOX_RETURN_NOT_OK(lexColumn());
  }

  VELOX_RETURN_IF(
      !source_.empty(), Status::Invalid("Source not fully consumed"));

  out = std::move(tokens_);
  return Status::OK();
}

Status PrestoVectorLexer::lexHeader() {
  assertCommitted();

  const auto header = PrestoHeader::read(&source_);
  VELOX_RETURN_IF(
      !header.has_value(), Status::Invalid("PrestoPage header invalid"));
  VELOX_RETURN_IF(
      isCompressedBitSet(header->pageCodecMarker),
      Status::Invalid("Compression is not supported"));
  VELOX_RETURN_IF(
      isEncryptedBitSet(header->pageCodecMarker),
      Status::Invalid("Encryption is not supported"));
  VELOX_RETURN_IF(
      header->uncompressedSize != header->compressedSize,
      Status::Invalid(
          "Compressed size must match uncompressed size: {} != {}",
          header->uncompressedSize,
          header->compressedSize));
  VELOX_RETURN_IF(
      header->uncompressedSize != source_.size(),
      Status::Invalid(
          "Uncompressed size does not match content size: {} != {}",
          header->uncompressedSize,
          source_.size()));

  commit(TokenType::HEADER);

  return Status::OK();
}

Status PrestoVectorLexer::lexColumEncoding(std::string& out) {
  assertCommitted();
  // Don't use readLengthPrefixedString because it doesn't validate the length
  int32_t encodingLength;
  VELOX_RETURN_NOT_OK(lexInt(TokenType::COLUMN_ENCODING, &encodingLength));
  // Control encoding length to avoid large allocations
  VELOX_RETURN_IF(
      encodingLength < 0 || encodingLength > 100,
      Status::Invalid("Invalid column encoding length: {}", encodingLength));

  std::string encoding;
  encoding.resize(encodingLength);
  VELOX_RETURN_NOT_OK(
      lexBytes(encodingLength, TokenType::COLUMN_ENCODING, encoding.data()));

  out = std::move(encoding);

  return Status::OK();
}

Status PrestoVectorLexer::lexColumn() {
  std::string encoding;
  VELOX_RETURN_NOT_OK(lexColumEncoding(encoding));

  if (encoding == kByteArray) {
    VELOX_RETURN_NOT_OK(lexFixedArray<int8_t>(TokenType::BYTE_ARRAY));
  } else if (encoding == kShortArray) {
    VELOX_RETURN_NOT_OK(lexFixedArray<int16_t>(TokenType::SHORT_ARRAY));
  } else if (encoding == kIntArray) {
    VELOX_RETURN_NOT_OK(lexFixedArray<int32_t>(TokenType::INT_ARRAY));
  } else if (encoding == kLongArray) {
    VELOX_RETURN_NOT_OK(lexFixedArray<int64_t>(TokenType::LONG_ARRAY));
  } else if (encoding == kInt128Array) {
    VELOX_RETURN_NOT_OK(lexFixedArray<int128_t>(TokenType::INT128_ARRAY));
  } else if (encoding == kVariableWidth) {
    VELOX_RETURN_NOT_OK(lexVariableWidth());
  } else if (encoding == kArray) {
    VELOX_RETURN_NOT_OK(lexArray());
  } else if (encoding == kMap) {
    VELOX_RETURN_NOT_OK(lexMap());
  } else if (encoding == kRow) {
    VELOX_RETURN_NOT_OK(lexRow());
  } else if (encoding == kDictionary) {
    VELOX_RETURN_NOT_OK(lexDictionary());
  } else if (encoding == kRLE) {
    VELOX_RETURN_NOT_OK(lexRLE());
  } else {
    return Status::Invalid("Unknown encoding: {}", encoding);
  }

  return Status::OK();
}

Status PrestoVectorLexer::lexVariableWidth() {
  int32_t numRows;
  VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_ROWS, &numRows));
  const auto numOffsetBytes = numRows * sizeof(int32_t);
  VELOX_RETURN_NOT_OK(lexBytes(numOffsetBytes, TokenType::OFFSETS));
  VELOX_RETURN_NOT_OK(lexNulls(numRows));
  int32_t dataBytes;
  VELOX_RETURN_NOT_OK(lexInt(TokenType::VARIABLE_WIDTH_DATA_SIZE, &dataBytes));
  VELOX_RETURN_NOT_OK(lexBytes(dataBytes, TokenType::VARIABLE_WIDTH_DATA));
  return Status::OK();
}

Status PrestoVectorLexer::lexArray() {
  VELOX_RETURN_NOT_OK(lexColumn());
  int32_t numRows;
  VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_ROWS, &numRows));
  const auto offsetBytes = (numRows + 1) * sizeof(int32_t);
  VELOX_RETURN_NOT_OK(lexBytes(offsetBytes, TokenType::OFFSETS));
  VELOX_RETURN_NOT_OK(lexNulls(numRows));
  return Status::OK();
}

Status PrestoVectorLexer::lexMap() {
  // Key column
  VELOX_RETURN_NOT_OK(lexColumn());
  // Value column
  VELOX_RETURN_NOT_OK(lexColumn());
  int32_t hashTableBytes;
  VELOX_RETURN_NOT_OK(lexInt(TokenType::HASH_TABLE_SIZE, &hashTableBytes));
  if (hashTableBytes != -1) {
    VELOX_RETURN_NOT_OK(lexBytes(hashTableBytes, TokenType::HASH_TABLE));
  }
  int32_t numRows;
  VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_ROWS, &numRows));
  const auto offsetBytes = (numRows + 1) * sizeof(int32_t);
  VELOX_RETURN_NOT_OK(lexBytes(offsetBytes, TokenType::OFFSETS));
  VELOX_RETURN_NOT_OK(lexNulls(numRows));
  return Status::OK();
}

Status PrestoVectorLexer::lexRow() {
  int32_t numFields;
  VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_FIELDS, &numFields));
  for (int32_t field = 0; field < numFields; ++field) {
    VELOX_RETURN_NOT_OK(lexColumn());
  }
  int32_t numRows;
  VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_ROWS, &numRows));
  const auto offsetBytes = (numRows + 1) * sizeof(int32_t);
  VELOX_RETURN_NOT_OK(lexBytes(offsetBytes, TokenType::OFFSETS));
  VELOX_RETURN_NOT_OK(lexNulls(numRows));

  return Status::OK();
}

Status PrestoVectorLexer::lexDictionary() {
  int32_t numRows;
  VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_ROWS, &numRows));
  // Dictionary column
  VELOX_RETURN_NOT_OK(lexColumn());
  const auto indicesBytes = numRows * sizeof(int32_t);
  VELOX_RETURN_NOT_OK(lexBytes(indicesBytes, TokenType::DICTIONARY_INDICES));
  // Dictionary ID
  VELOX_RETURN_NOT_OK(lexBytes(24, TokenType::DICTIONARY_ID));
  return Status::OK();
}

Status PrestoVectorLexer::lexRLE() {
  // Num rows
  VELOX_RETURN_NOT_OK(lexInt<int32_t>(TokenType::NUM_ROWS));
  // RLE length one column
  VELOX_RETURN_NOT_OK(lexColumn());
  return Status::OK();
}

Status PrestoVectorLexer::lexNulls(int32_t& numRows) {
  assertCommitted();
  VELOX_RETURN_IF(
      numRows < 0, Status::Invalid("Negative num rows: {}", numRows));

  int8_t hasNulls;
  VELOX_RETURN_NOT_OK(lexInt(TokenType::NULLS, &hasNulls));
  if (hasNulls != 0) {
    const auto numBytes = bits::nbytes(numRows);
    VELOX_RETURN_IF(
        numBytes > source_.size(),
        Status::Invalid(
            "More rows than bytes in source: {} > {}",
            numRows,
            source_.size()));
    if (nullsBuffer_.size() < numBytes) {
      constexpr auto eltBytes = sizeof(nullsBuffer_[0]);
      nullsBuffer_.resize(bits::roundUp(numBytes, eltBytes) / eltBytes);
    }
    auto* nulls = nullsBuffer_.data();
    VELOX_RETURN_NOT_OK(
        lexBytes(numBytes, TokenType::NULLS, reinterpret_cast<char*>(nulls)));

    bits::reverseBits(reinterpret_cast<uint8_t*>(nulls), numBytes);
    const auto numNulls = bits::countBits(nulls, 0, numRows);

    numRows -= numNulls;
  }
  commit(TokenType::NULLS);
  return Status::OK();
}

Status
PrestoVectorLexer::lexBytes(int32_t numBytes, TokenType tokenType, char* dst) {
  assertCommitted();
  VELOX_RETURN_IF(
      numBytes < 0,
      Status::Invalid("Attempting to read negative numBytes: {}", numBytes));
  VELOX_RETURN_IF(
      numBytes > source_.size(),
      Status::Invalid(
          "Attempting to read more bytes than in source: {} > {}",
          numBytes,
          source_.size()));
  if (dst != nullptr) {
    std::copy(source_.begin(), source_.begin() + numBytes, dst);
  }
  source_.remove_prefix(numBytes);
  commit(tokenType);
  return Status::OK();
}

void PrestoVectorLexer::commit(TokenType tokenType) {
  const auto* newPtr = source_.data();
  assert(committedPtr_ <= newPtr);
  assert(
      int64_t(newPtr - committedPtr_) <=
      int64_t(std::numeric_limits<uint32_t>::max()));
  if (newPtr != committedPtr_) {
    const uint32_t length = uint32_t(newPtr - committedPtr_);
    if (!tokens_.empty() && tokens_.back().tokenType == tokenType) {
      tokens_.back().length += length;
    } else {
      Token token;
      token.tokenType = tokenType;
      token.length = length;
      tokens_.push_back(token);
    }
  }
  committedPtr_ = newPtr;
}
} // namespace facebook::velox::serializer::presto::detail
