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
#include "presto_cpp/main/types/TypeSignatureTypeConverter.h"

// Generated from TypeSignature.g4 by ANTLR 4.9.3

#pragma once

#include "TypeSignatureParser.h"
#include "antlr4-runtime.h"

namespace facebook::presto::type {

/**
 * This class defines an abstract visitor for a parse tree
 * produced by TypeSignatureParser.
 */
class TypeSignatureVisitor : public antlr4::tree::AbstractParseTreeVisitor {
 public:
  /**
   * Visit parse trees produced by TypeSignatureParser.
   */
  virtual antlrcpp::Any visitStart(
      TypeSignatureParser::StartContext* context) = 0;

  virtual antlrcpp::Any visitType_spec(
      TypeSignatureParser::Type_specContext* context) = 0;

  virtual antlrcpp::Any visitNamed_type(
      TypeSignatureParser::Named_typeContext* context) = 0;

  virtual antlrcpp::Any visitType(
      TypeSignatureParser::TypeContext* context) = 0;

  virtual antlrcpp::Any visitSimple_type(
      TypeSignatureParser::Simple_typeContext* context) = 0;

  virtual antlrcpp::Any visitVariable_type(
      TypeSignatureParser::Variable_typeContext* context) = 0;

  virtual antlrcpp::Any visitDecimal_type(
      TypeSignatureParser::Decimal_typeContext* context) = 0;

  virtual antlrcpp::Any visitType_list(
      TypeSignatureParser::Type_listContext* context) = 0;

  virtual antlrcpp::Any visitRow_type(
      TypeSignatureParser::Row_typeContext* context) = 0;

  virtual antlrcpp::Any visitMap_type(
      TypeSignatureParser::Map_typeContext* context) = 0;

  virtual antlrcpp::Any visitArray_type(
      TypeSignatureParser::Array_typeContext* context) = 0;

  virtual antlrcpp::Any visitIdentifier(
      TypeSignatureParser::IdentifierContext* context) = 0;
};

} // namespace facebook::presto::type
