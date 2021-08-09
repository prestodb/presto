/*
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

#include "velox/dwio/type/fbhive/HiveTypeParser.h"

#include <cctype>
#include <stdexcept>
#include <string>
#include <utility>

#include "velox/dwio/common/exception/Exception.h"

using facebook::velox::Type;
using facebook::velox::TypeKind;

namespace facebook {
namespace dwio {
namespace type {
namespace fbhive {

HiveTypeParser::HiveTypeParser() {
  metadata_.resize(static_cast<size_t>(TokenType::MaxTokenType) + 1);
  setupMetadata<TokenType::Boolean, TypeKind::BOOLEAN>("boolean");
  setupMetadata<TokenType::Byte, TypeKind::TINYINT>("tinyint");
  setupMetadata<TokenType::Short, TypeKind::SMALLINT>("smallint");
  setupMetadata<TokenType::Integer, TypeKind::INTEGER>({"integer", "int"});
  setupMetadata<TokenType::Long, TypeKind::BIGINT>("bigint");
  setupMetadata<TokenType::Float, TypeKind::REAL>({"float", "real"});
  setupMetadata<TokenType::Double, TypeKind::DOUBLE>("double");
  setupMetadata<TokenType::String, TypeKind::VARCHAR>({"string", "varchar"});
  setupMetadata<TokenType::Binary, TypeKind::VARBINARY>(
      {"binary", "varbinary"});
  setupMetadata<TokenType::Timestamp, TypeKind::TIMESTAMP>("timestamp");
  setupMetadata<TokenType::List, TypeKind::ARRAY>("array");
  setupMetadata<TokenType::Map, TypeKind::MAP>("map");
  setupMetadata<TokenType::Struct, TypeKind::ROW>({"struct", "row"});
  setupMetadata<TokenType::StartSubType, TypeKind::INVALID>("<");
  setupMetadata<TokenType::EndSubType, TypeKind::INVALID>(">");
  setupMetadata<TokenType::Colon, TypeKind::INVALID>(":");
  setupMetadata<TokenType::Comma, TypeKind::INVALID>(",");
  setupMetadata<TokenType::Number, TypeKind::INVALID>();
  setupMetadata<TokenType::Identifier, TypeKind::INVALID>();
  setupMetadata<TokenType::EndOfStream, TypeKind::INVALID>();
}

std::shared_ptr<const Type> HiveTypeParser::parse(const std::string& ser) {
  remaining_ = folly::StringPiece(ser);
  Result result = parseType();
  if (remaining_.size() != 0 && (TokenType::EndOfStream != lookAhead())) {
    throw std::invalid_argument("Input remaining after type parsing");
  }
  return result.type;
}

Result HiveTypeParser::parseType() {
  Token nt = nextToken();
  if (nt.isEOS()) {
    throw std::invalid_argument("Unexpected end of stream parsing type!!!");
  } else if (nt.isValidType() && nt.isPrimitiveType()) {
    auto scalarType = createScalarType(nt.typeKind());
    DWIO_ENSURE_NOT_NULL(
        scalarType, "Returned a null scalar type for ", nt.typeKind());
    return Result{scalarType};
  } else if (nt.isValidType()) {
    ResultList resultList = parseTypeList(TypeKind::ROW == nt.typeKind());
    switch (nt.typeKind()) {
      case velox::TypeKind::ROW:
        return Result{velox::ROW(
            std::move(resultList.names), std::move(resultList.typelist))};
      case velox::TypeKind::MAP: {
        if (resultList.typelist.size() != 2) {
          throw std::invalid_argument{"wrong param count for map type def"};
        }
        return Result{
            velox::MAP(resultList.typelist.at(0), resultList.typelist.at(1))};
      }
      case velox::TypeKind::ARRAY: {
        if (resultList.typelist.size() != 1) {
          throw std::invalid_argument{"wrong param count for array type def"};
        }
        return Result{velox::ARRAY(resultList.typelist.at(0))};
      }
      default:
        throw std::invalid_argument{
            "unsupported kind: " + std::to_string((int)nt.typeKind())};
    }
  } else {
    throw std::invalid_argument(fmt::format(
        "Unexpected token {} at {}", nt.value, remaining_.toString()));
  }
}

ResultList HiveTypeParser::parseTypeList(bool hasFieldNames) {
  std::vector<std::shared_ptr<const Type>> subTypeList{};
  std::vector<std::string> names{};
  eatToken(TokenType::StartSubType);
  while (true) {
    if (TokenType::EndSubType == lookAhead()) {
      eatToken(TokenType::EndSubType);
      return ResultList{std::move(subTypeList), std::move(names)};
    }

    folly::StringPiece fieldName;
    if (hasFieldNames) {
      fieldName = eatToken(TokenType::Identifier, true).value;
      eatToken(TokenType::Colon);
      names.push_back(fieldName.toString());
    }

    Result result = parseType();
    subTypeList.push_back(result.type);
    if (TokenType::Comma == lookAhead()) {
      eatToken(TokenType::Comma);
    }
  }
}

TokenType HiveTypeParser::lookAhead() const {
  return nextToken(remaining_).tokenType();
}

Token HiveTypeParser::eatToken(TokenType tokenType, bool ignorePredefined) {
  TokenAndRemaining token = nextToken(remaining_, ignorePredefined);
  if (token.tokenType() == tokenType) {
    remaining_ = token.remaining;
    return token;
  }

  throw std::invalid_argument("Unexpected token " + token.remaining.toString());
}

Token HiveTypeParser::nextToken(bool ignorePredefined) {
  TokenAndRemaining token = nextToken(remaining_, ignorePredefined);
  remaining_ = token.remaining;
  return token;
}

TokenAndRemaining HiveTypeParser::nextToken(
    folly::StringPiece sp,
    bool ignorePredefined) const {
  while (!sp.empty() && isspace(sp.front())) {
    sp.advance(1);
  }

  if (sp.empty()) {
    return makeExtendedToken(getMetadata(TokenType::EndOfStream), sp, 0);
  }

  if (!ignorePredefined) {
    for (auto& metadata : metadata_) {
      for (auto& token : metadata->tokenString) {
        folly::StringPiece match(token);
        if (match.size() > 0 &&
            sp.startsWith(match, folly::AsciiCaseInsensitive{})) {
          return makeExtendedToken(metadata.get(), sp, match.size());
        }
      }
    }
  }

  auto iter = sp.cbegin();
  size_t len = 0;
  while (isalnum(*iter) || '_' == *iter || '$' == *iter) {
    ++len;
    ++iter;
  }

  if (len > 0) {
    return makeExtendedToken(getMetadata(TokenType::Identifier), sp, len);
  }

  throw std::invalid_argument("Bad Token at " + sp.toString());
}

TokenType Token::tokenType() const {
  return metadata->tokenType;
}

TypeKind Token::typeKind() const {
  return metadata->typeKind;
}

bool Token::isPrimitiveType() const {
  return metadata->isPrimitiveType;
}

bool Token::isValidType() const {
  return metadata->typeKind != TypeKind::INVALID;
}

bool Token::isEOS() const {
  return metadata->tokenType == TokenType::EndOfStream;
}

int8_t HiveTypeParser::makeTokenId(TokenType tokenType) const {
  return static_cast<int8_t>(tokenType);
}

TokenAndRemaining HiveTypeParser::makeExtendedToken(
    TokenMetadata* tokenMetadata,
    folly::StringPiece sp,
    size_t len) const {
  folly::StringPiece spmatch(sp.cbegin(), sp.cbegin() + len);
  sp.advance(len);

  TokenAndRemaining result;
  result.metadata = tokenMetadata;
  result.value = spmatch;
  result.remaining = sp;
  return result;
}

TokenMetadata* HiveTypeParser::getMetadata(TokenType type) const {
  auto& value = metadata_[makeTokenId(type)];
  return value.get();
}

} // namespace fbhive
} // namespace type
} // namespace dwio
} // namespace facebook
