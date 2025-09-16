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

#include "velox/common/base/Status.h"
#include "velox/serializers/PrestoHeader.h"

namespace facebook::velox::serializer::presto {
enum class TokenType {
  HEADER,
  NUM_COLUMNS,
  COLUMN_ENCODING,
  NUM_ROWS,
  NULLS,
  BYTE_ARRAY,
  SHORT_ARRAY,
  INT_ARRAY,
  LONG_ARRAY,
  INT128_ARRAY,
  VARIABLE_WIDTH_DATA_SIZE,
  VARIABLE_WIDTH_DATA,
  DICTIONARY_INDICES,
  DICTIONARY_ID,
  HASH_TABLE_SIZE,
  HASH_TABLE,
  NUM_FIELDS,
  OFFSETS,
};

struct Token {
  TokenType tokenType;
  uint32_t length;
};

namespace detail {

class PrestoVectorLexer {
 public:
  explicit PrestoVectorLexer(std::string_view source)
      : source_{source}, committedPtr_{source.data()} {}

  Status lex(std::vector<Token>& out) &&;

 private:
  Status lexHeader();
  Status lexColumEncoding(std::string& out);
  Status lexColumn();

  template <typename T>
  Status lexFixedArray(TokenType tokenType) {
    int32_t numRows;
    VELOX_RETURN_NOT_OK(lexInt(TokenType::NUM_ROWS, &numRows));
    VELOX_RETURN_NOT_OK(lexNulls(numRows));
    const auto numBytes = numRows * sizeof(T);
    VELOX_RETURN_NOT_OK(lexBytes(numBytes, tokenType));
    return Status::OK();
  }

  Status lexVariableWidth();
  Status lexArray();
  Status lexMap();
  Status lexRow();
  Status lexDictionary();
  Status lexRLE();
  Status lexNulls(int32_t& numRows);
  Status lexBytes(int32_t numBytes, TokenType tokenType, char* dst = nullptr);

  template <typename T>
  Status lexInt(TokenType tokenType, T* out = nullptr) {
    assertCommitted();
    VELOX_RETURN_IF(
        source_.size() < sizeof(T),
        Status::Invalid(
            "Source size less than int size: {} < {}",
            source_.size(),
            sizeof(T)));
    const auto value = PrestoHeader::readInt<T>(&source_);
    if (out != nullptr) {
      *out = value;
    }
    commit(tokenType);
    return Status::OK();
  }

  void assertCommitted() const {
    assert(committedPtr_ == source_.begin());
  }

  void commit(TokenType tokenType);

  std::string_view source_;
  const char* committedPtr_;
  std::vector<uint64_t> nullsBuffer_;
  std::vector<Token> tokens_;
};
} // namespace detail
} // namespace facebook::velox::serializer::presto
