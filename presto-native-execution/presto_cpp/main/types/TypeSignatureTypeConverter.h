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

#pragma once

#include "velox/type/Type.h"

#include "presto_cpp/main/types/antlr/TypeSignatureBaseVisitor.h"

namespace facebook {
namespace presto {
using namespace type;

class TypeSignatureTypeConverter : TypeSignatureBaseVisitor {
  virtual antlrcpp::Any visitStart(
      TypeSignatureParser::StartContext* ctx) override;
  virtual antlrcpp::Any visitNamed_type(
      TypeSignatureParser::Named_typeContext* ctx) override;
  virtual antlrcpp::Any visitType_spec(
      TypeSignatureParser::Type_specContext* ctx) override;
  virtual antlrcpp::Any visitType(
      TypeSignatureParser::TypeContext* ctx) override;
  virtual antlrcpp::Any visitSimple_type(
      TypeSignatureParser::Simple_typeContext* ctx) override;
  virtual antlrcpp::Any visitDecimal_type(
      TypeSignatureParser::Decimal_typeContext* ctx) override;
  virtual antlrcpp::Any visitVariable_type(
      TypeSignatureParser::Variable_typeContext* ctx) override;
  virtual antlrcpp::Any visitType_list(
      TypeSignatureParser::Type_listContext* ctx) override;
  virtual antlrcpp::Any visitRow_type(
      TypeSignatureParser::Row_typeContext* ctx) override;
  virtual antlrcpp::Any visitMap_type(
      TypeSignatureParser::Map_typeContext* ctx) override;
  virtual antlrcpp::Any visitArray_type(
      TypeSignatureParser::Array_typeContext* ctx) override;
  virtual antlrcpp::Any visitIdentifier(
      TypeSignatureParser::IdentifierContext* ctx) override;

 public:
  static std::shared_ptr<const velox::Type> parse(const std::string& text);
};

struct NamedType {
  std::string name;
  velox::TypePtr type;
};

velox::TypePtr typeFromString(const std::string& typeName);
velox::TypePtr rowFromNamedTypes(const std::vector<NamedType>& named);
velox::TypePtr mapFromKeyValueType(
    velox::TypePtr keyType,
    velox::TypePtr valueType);
velox::TypePtr arrayFromType(velox::TypePtr valueType);

} // namespace presto
} // namespace facebook
