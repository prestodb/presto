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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "folly/Range.h"
#include "velox/dwio/type/TypeParser.h"

namespace facebook::velox::dwio::type::fbhive {

// TODO : Find out what to do with these types
// NUMERIC, INTERVAL, VARCHAR, VOID
enum class TokenType {
  Boolean = 0,
  Byte = 1,
  Short = 2,
  Integer = 3,
  Long = 4,
  Float = 5,
  Double = 6,
  String = 7,
  Binary = 8,
  Timestamp = 9,
  List = 10,
  Map = 11,
  Struct = 12,
  StartSubType = 13,
  EndSubType = 14,
  Colon = 15,
  Comma = 16,
  Number = 17,
  Identifier = 18,
  EndOfStream = 19,
  ShortDecimal = 20,
  LongDecimal = 21,
  LeftRoundBracket = 22,
  RightRoundBracket = 23,
  MaxTokenType = 23
};

struct TokenMetadata {
  TokenType tokenType;
  velox::TypeKind typeKind;
  std::vector<std::string> tokenString;
  bool isPrimitiveType;

  TokenMetadata(
      TokenType typ,
      velox::TypeKind kind,
      std::vector<std::string>&& ts,
      bool ip)
      : tokenType(typ),
        typeKind(kind),
        tokenString(std::move(ts)),
        isPrimitiveType(ip) {}
};

struct Token {
  TokenMetadata* metadata;
  folly::StringPiece value;

  TokenType tokenType() const;

  velox::TypeKind typeKind() const;

  bool isPrimitiveType() const;

  bool isValidType() const;

  bool isEOS() const;
};

struct TokenAndRemaining : public Token {
  folly::StringPiece remaining;
};

struct Result {
  std::shared_ptr<const velox::Type> type;
};

struct ResultList {
  std::vector<std::shared_ptr<const velox::Type>> typelist;
  std::vector<std::string> names;
};

class HiveTypeParser : public type::TypeParser {
 public:
  HiveTypeParser();

  ~HiveTypeParser() override = default;

  std::shared_ptr<const velox::Type> parse(const std::string& ser) override;

 private:
  int8_t makeTokenId(TokenType tokenType) const;

  Result parseType();

  ResultList parseTypeList(bool hasFieldNames);

  TokenType lookAhead() const;

  Token eatToken(TokenType tokenType, bool ignorePredefined = false);

  Token nextToken(bool ignorePredefined = false);

  TokenAndRemaining nextToken(
      folly::StringPiece sp,
      bool ignorePredefined = false) const;

  TokenAndRemaining makeExtendedToken(
      TokenMetadata* tokenMetadata,
      folly::StringPiece sp,
      size_t len) const;

  template <TokenType KIND, velox::TypeKind TYPEKIND>
  void setupMetadata(const char* tok = "") {
    setupMetadata<KIND, TYPEKIND>(std::vector<std::string>{std::string{tok}});
  }

  template <TokenType KIND, velox::TypeKind TYPEKIND>
  void setupMetadata(std::vector<std::string>&& tokens) {
    static constexpr bool isPrimitive =
        velox::TypeTraits<TYPEKIND>::isPrimitiveType;
    metadata_[makeTokenId(KIND)] = std::make_unique<TokenMetadata>(
        KIND, TYPEKIND, std::move(tokens), isPrimitive);
  }

  TokenMetadata* getMetadata(TokenType type) const;

 private:
  std::vector<std::unique_ptr<TokenMetadata>> metadata_;
  folly::StringPiece remaining_;
};

} // namespace facebook::velox::dwio::type::fbhive
