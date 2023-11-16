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
#include <boost/algorithm/string.hpp>
#include <iostream>

#include <antlr4-runtime/antlr4-runtime.h>
#include "presto_cpp/main/types/TypeParser.h"
#include "presto_cpp/main/types/antlr/TypeSignatureLexer.h"
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

using namespace facebook::velox;

namespace facebook::presto {
namespace {
class TypeSignatureTypeConverter : public type::TypeSignatureBaseVisitor {
 public:
  virtual antlrcpp::Any visitStart(
      type::TypeSignatureParser::StartContext* ctx) override;
  virtual antlrcpp::Any visitNamed_type(
      type::TypeSignatureParser::Named_typeContext* ctx) override;
  virtual antlrcpp::Any visitType_spec(
      type::TypeSignatureParser::Type_specContext* ctx) override;
  virtual antlrcpp::Any visitType(
      type::TypeSignatureParser::TypeContext* ctx) override;
  virtual antlrcpp::Any visitSimple_type(
      type::TypeSignatureParser::Simple_typeContext* ctx) override;
  virtual antlrcpp::Any visitDecimal_type(
      type::TypeSignatureParser::Decimal_typeContext* ctx) override;
  virtual antlrcpp::Any visitVariable_type(
      type::TypeSignatureParser::Variable_typeContext* ctx) override;
  virtual antlrcpp::Any visitType_list(
      type::TypeSignatureParser::Type_listContext* ctx) override;
  virtual antlrcpp::Any visitRow_type(
      type::TypeSignatureParser::Row_typeContext* ctx) override;
  virtual antlrcpp::Any visitMap_type(
      type::TypeSignatureParser::Map_typeContext* ctx) override;
  virtual antlrcpp::Any visitArray_type(
      type::TypeSignatureParser::Array_typeContext* ctx) override;
  virtual antlrcpp::Any visitFunction_type(
      type::TypeSignatureParser::Function_typeContext* ctx) override;
  virtual antlrcpp::Any visitIdentifier(
      type::TypeSignatureParser::IdentifierContext* ctx) override;
};

TypePtr typeFromString(const std::string& typeName) {
  auto upper = boost::to_upper_copy(typeName);

  if (upper == "UNKNOWN") {
    return UNKNOWN();
  }

  if (upper == TIMESTAMP_WITH_TIME_ZONE()->toString()) {
    return TIMESTAMP_WITH_TIME_ZONE();
  }

  if (upper == HYPERLOGLOG()->toString()) {
    return HYPERLOGLOG();
  }

  if (upper == JSON()->toString()) {
    return JSON();
  }

  if (upper == "INT") {
    upper = "INTEGER";
  } else if (upper == "DOUBLE PRECISION") {
    upper = "DOUBLE";
  }

  if (upper == INTERVAL_DAY_TIME()->toString()) {
    return INTERVAL_DAY_TIME();
  }

  if (upper == INTERVAL_YEAR_MONTH()->toString()) {
    return INTERVAL_YEAR_MONTH();
  }

  if (upper == DATE()->toString()) {
    return DATE();
  }

  return createScalarType(mapNameToTypeKind(upper));
}

struct NamedType {
  std::string name;
  velox::TypePtr type;
};

antlrcpp::Any TypeSignatureTypeConverter::visitStart(
    type::TypeSignatureParser::StartContext* ctx) {
  NamedType named = visit(ctx->type_spec()).as<NamedType>();
  return named.type;
}

antlrcpp::Any TypeSignatureTypeConverter::visitType_spec(
    type::TypeSignatureParser::Type_specContext* ctx) {
  if (ctx->named_type()) {
    return visit(ctx->named_type());
  } else {
    return NamedType{"", visit(ctx->type()).as<TypePtr>()};
  }
}

antlrcpp::Any TypeSignatureTypeConverter::visitNamed_type(
    type::TypeSignatureParser::Named_typeContext* ctx) {
  return NamedType{
      visit(ctx->identifier()).as<std::string>(),
      visit(ctx->type()).as<TypePtr>()};
}

antlrcpp::Any TypeSignatureTypeConverter::visitType(
    type::TypeSignatureParser::TypeContext* ctx) {
  return visitChildren(ctx);
}

antlrcpp::Any TypeSignatureTypeConverter::visitSimple_type(
    type::TypeSignatureParser::Simple_typeContext* ctx) {
  return ctx->WORD() ? typeFromString(ctx->WORD()->getText())
                     : typeFromString(ctx->TYPE_WITH_SPACES()->getText());
}

antlrcpp::Any TypeSignatureTypeConverter::visitDecimal_type(
    type::TypeSignatureParser::Decimal_typeContext* ctx) {
  VELOX_USER_CHECK_EQ(2, ctx->NUMBER().size(), "Invalid decimal type");

  const auto precision = ctx->NUMBER(0)->getText();
  const auto scale = ctx->NUMBER(1)->getText();
  return DECIMAL(std::atoi(precision.c_str()), std::atoi(scale.c_str()));
}

antlrcpp::Any TypeSignatureTypeConverter::visitVariable_type(
    type::TypeSignatureParser::Variable_typeContext* ctx) {
  return typeFromString(ctx->WORD()->getText());
}

antlrcpp::Any TypeSignatureTypeConverter::visitType_list(
    type::TypeSignatureParser::Type_listContext* ctx) {
  std::vector<NamedType> types;
  for (auto type_spec : ctx->type_spec()) {
    types.emplace_back(visit(type_spec).as<NamedType>());
  }
  return types;
}

antlrcpp::Any TypeSignatureTypeConverter::visitRow_type(
    type::TypeSignatureParser::Row_typeContext* ctx) {
  const auto namedTypes = visit(ctx->type_list()).as<std::vector<NamedType>>();

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(namedTypes.size());
  types.reserve(namedTypes.size());
  for (const auto& namedType : namedTypes) {
    names.push_back(namedType.name);
    types.push_back(namedType.type);
  }

  const TypePtr rowType = ROW(std::move(names), std::move(types));
  return rowType;
}

antlrcpp::Any TypeSignatureTypeConverter::visitMap_type(
    type::TypeSignatureParser::Map_typeContext* ctx) {
  const auto keyType = visit(ctx->type()[0]).as<TypePtr>();
  const auto valueType = visit(ctx->type()[1]).as<TypePtr>();
  const TypePtr mapType = MAP(keyType, valueType);
  return mapType;
}

antlrcpp::Any TypeSignatureTypeConverter::visitArray_type(
    type::TypeSignatureParser::Array_typeContext* ctx) {
  const TypePtr arrayType = ARRAY(visit(ctx->type()).as<TypePtr>());
  return arrayType;
}

antlrcpp::Any TypeSignatureTypeConverter::visitFunction_type(
    type::TypeSignatureParser::Function_typeContext* ctx) {
  const auto numArgs = ctx->type().size() - 1;

  std::vector<TypePtr> argumentTypes;
  argumentTypes.reserve(numArgs);
  for (auto i = 0; i < numArgs; ++i) {
    argumentTypes.push_back(visit(ctx->type()[i]).as<TypePtr>());
  }

  auto returnType = visit(ctx->type().back()).as<TypePtr>();

  TypePtr functionType = FUNCTION(std::move(argumentTypes), returnType);
  return functionType;
}

antlrcpp::Any TypeSignatureTypeConverter::visitIdentifier(
    type::TypeSignatureParser::IdentifierContext* ctx) {
  if (ctx->WORD()) {
    return ctx->WORD()->getText();
  } else {
    return ctx->QUOTED_ID()->getText().substr(
        1, ctx->QUOTED_ID()->getText().length() - 2);
  }
}
}

TypePtr TypeParser::parse(const std::string& text) const {
  auto it = cache_.find(text);
  if (it != cache_.end()) {
    return it->second;
  }

  antlr4::ANTLRInputStream input(text);
  type::TypeSignatureLexer lexer(&input);
  antlr4::CommonTokenStream tokens(&lexer);
  type::TypeSignatureParser parser(&tokens);

  parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());

  try {
    auto ctx = parser.start();
    TypeSignatureTypeConverter c;
    auto result = c.visit(ctx).as<TypePtr>();
    cache_.insert({text, result});
    return result;
  } catch (const std::exception& e) {
    VELOX_USER_FAIL("Failed to parse type [{}]: {}", text, e.what());
  }
}
} // namespace facebook::presto
