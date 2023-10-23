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

namespace facebook::presto {

class TypeSignatureTypeConverter : type::TypeSignatureBaseVisitor {
 public:
  static velox::TypePtr parse(const std::string& text);

 private:
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

} // namespace facebook::presto
