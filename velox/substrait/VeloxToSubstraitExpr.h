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

#include "velox/core/PlanNode.h"

#include "velox/substrait/VeloxToSubstraitType.h"
#include "velox/substrait/proto/substrait/algebra.pb.h"

namespace facebook::velox::substrait {

class VeloxToSubstraitExprConvertor {
 public:
  /// @param functionMap: A pre-constructed map
  /// storing the relations between the function name and the function id.
  explicit VeloxToSubstraitExprConvertor(
      const std::unordered_map<std::string, uint64_t>& functionMap)
      : functionMap_(functionMap) {}

  /// Convert Velox Expression to Substrait Expression.
  /// @param arena Arena to use for allocating Substrait plan objects.
  /// @param expr Velox expression needed to be converted.
  /// @param inputType The input row Type of the current processed node,
  /// which also equals the output row type of the previous node of the current.
  /// @return A pointer to Substrait expression object allocated on the arena
  /// and representing the input Velox expression.
  const ::substrait::Expression& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const core::TypedExprPtr& expr,
      const RowTypePtr& inputType);

  /// Convert Velox Constant Expression to Substrait
  /// Literal Expression.
  /// @param arena Arena to use for allocating Substrait plan objects.
  /// @param constExpr Velox Constant expression needed to be converted.
  /// @param inputType The input row Type of the current processed node,
  /// which also equals the output row type of the previous node of the current.
  /// @param litValue The Struct that returned literal expression belong to.
  /// @return A pointer to Substrait Literal expression object allocated on
  /// the arena and representing the input Velox Constant expression.
  const ::substrait::Expression_Literal& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::ConstantTypedExpr>& constExpr,
      ::substrait::Expression_Literal_Struct* litValue = nullptr);

  /// Convert Velox null literal to Substrait null literal.
  const ::substrait::Expression_Literal& toSubstraitNullLiteral(
      google::protobuf::Arena& arena,
      const velox::TypePtr& type);

  /// Convert Velox variant to Substrait Literal Expression.
  const ::substrait::Expression_Literal& toSubstraitLiteral(
      google::protobuf::Arena& arena,
      const velox::variant& variantValue);

 private:
  /// Convert Velox Cast Expression to Substrait Cast Expression.
  const ::substrait::Expression_Cast& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::CastTypedExpr>& castExpr,
      const RowTypePtr& inputType);

  /// Convert Velox FieldAccessTypedExpr to Substrait FieldReference Expression.
  const ::substrait::Expression_FieldReference& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::FieldAccessTypedExpr>& fieldExpr,
      const RowTypePtr& inputType);

  /// Convert Velox CallTypedExpr Expression to Substrait Expression.
  const ::substrait::Expression& toSubstraitExpr(
      google::protobuf::Arena& arena,
      const std::shared_ptr<const core::CallTypedExpr>& callTypeExpr,
      const RowTypePtr& inputType);

  /// Convert Velox vector to Substrait literal.
  const ::substrait::Expression_Literal& toSubstraitLiteral(
      google::protobuf::Arena& arena,
      const velox::VectorPtr& vectorValue,
      ::substrait::Expression_Literal_Struct* litValue);

  std::shared_ptr<VeloxToSubstraitTypeConvertor> typeConvertor_;

  /// The map storing the relations between the function name and the function
  /// id.
  std::unordered_map<std::string, uint64_t> functionMap_;
};

} // namespace facebook::velox::substrait
