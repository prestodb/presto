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
#include "presto_cpp/main/types/ParseTypeSignature.h"
#include "presto_cpp/main/types/TypeSignatureTypeConverter.h"
#include "presto_cpp/main/types/antlr/TypeSignatureLexer.h"
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

using namespace facebook::velox;
namespace facebook::presto {

TypePtr parseTypeSignature(const std::string& signature) {
  return TypeSignatureTypeConverter::parse(signature);
}

// static
TypePtr TypeSignatureTypeConverter::parse(const std::string& text) {
  antlr4::ANTLRInputStream input(text);
  TypeSignatureLexer lexer(&input);
  antlr4::CommonTokenStream tokens(&lexer);
  TypeSignatureParser parser(&tokens);

  parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());

  try {
    auto ctx = parser.start();
    TypeSignatureTypeConverter c;
    return c.visit(ctx).as<TypePtr>();
  } catch (const std::exception& e) {
    VELOX_USER_FAIL("Failed to parse type [{}]: {}", text, e.what());
  }
}

antlrcpp::Any TypeSignatureTypeConverter::visitStart(
    TypeSignatureParser::StartContext* ctx) {
  NamedType named = visit(ctx->type_spec()).as<NamedType>();
  return named.type;
}

antlrcpp::Any TypeSignatureTypeConverter::visitType_spec(
    TypeSignatureParser::Type_specContext* ctx) {
  if (ctx->named_type()) {
    return visit(ctx->named_type());
  } else {
    return NamedType{"", visit(ctx->type()).as<TypePtr>()};
  }
}

antlrcpp::Any TypeSignatureTypeConverter::visitNamed_type(
    TypeSignatureParser::Named_typeContext* ctx) {
  return NamedType{
      visit(ctx->identifier()).as<std::string>(),
      visit(ctx->type()).as<TypePtr>()};
}

antlrcpp::Any TypeSignatureTypeConverter::visitType(
    TypeSignatureParser::TypeContext* ctx) {
  return visitChildren(ctx);
}

antlrcpp::Any TypeSignatureTypeConverter::visitSimple_type(
    TypeSignatureParser::Simple_typeContext* ctx) {
  return ctx->WORD() ? typeFromString(ctx->WORD()->getText())
                     : typeFromString(ctx->TYPE_WITH_SPACES()->getText());
}

antlrcpp::Any TypeSignatureTypeConverter::visitDecimal_type(
    TypeSignatureParser::Decimal_typeContext* ctx) {
  if (ctx->NUMBER().size() != 2) {
    VELOX_USER_FAIL("Invalid decimal type");
  }
  auto precision = ctx->NUMBER(0)->getText();
  auto scale = ctx->NUMBER(1)->getText();
  return DECIMAL(std::atoi(precision.c_str()), std::atoi(scale.c_str()));
}

antlrcpp::Any TypeSignatureTypeConverter::visitVariable_type(
    TypeSignatureParser::Variable_typeContext* ctx) {
  return typeFromString(ctx->WORD()->getText());
}

antlrcpp::Any TypeSignatureTypeConverter::visitType_list(
    TypeSignatureParser::Type_listContext* ctx) {
  std::vector<NamedType> types;
  for (auto type_spec : ctx->type_spec()) {
    types.emplace_back(visit(type_spec).as<NamedType>());
  }
  return types;
}

antlrcpp::Any TypeSignatureTypeConverter::visitRow_type(
    TypeSignatureParser::Row_typeContext* ctx) {
  return rowFromNamedTypes(
      visit(ctx->type_list()).as<std::vector<NamedType>>());
}

antlrcpp::Any TypeSignatureTypeConverter::visitMap_type(
    TypeSignatureParser::Map_typeContext* ctx) {
  return mapFromKeyValueType(
      visit(ctx->type()[0]).as<TypePtr>(), visit(ctx->type()[1]).as<TypePtr>());
}

antlrcpp::Any TypeSignatureTypeConverter::visitArray_type(
    TypeSignatureParser::Array_typeContext* ctx) {
  return arrayFromType(visit(ctx->type()).as<TypePtr>());
}

antlrcpp::Any TypeSignatureTypeConverter::visitIdentifier(
    TypeSignatureParser::IdentifierContext* ctx) {
  if (ctx->WORD()) {
    return ctx->WORD()->getText();
  } else {
    return ctx->QUOTED_ID()->getText().substr(
        1, ctx->QUOTED_ID()->getText().length() - 2);
  }
}

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

  return createScalarType(mapNameToTypeKind(upper));
}

TypePtr rowFromNamedTypes(const std::vector<NamedType>& named) {
  std::vector<std::string> names{};
  std::transform(
      named.begin(), named.end(), std::back_inserter(names), [](NamedType v) {
        return v.name;
      });
  std::vector<TypePtr> types{};
  std::transform(
      named.begin(), named.end(), std::back_inserter(types), [](NamedType v) {
        return v.type;
      });

  return TypeFactory<TypeKind::ROW>::create(std::move(names), std::move(types));
}

TypePtr mapFromKeyValueType(TypePtr keyType, TypePtr valueType) {
  return TypeFactory<TypeKind::MAP>::create(keyType, valueType);
}

TypePtr arrayFromType(TypePtr valueType) {
  return TypeFactory<TypeKind::ARRAY>::create(valueType);
}
} // namespace facebook::presto
