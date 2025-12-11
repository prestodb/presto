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

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/core/Expressions.h"
#include "velox/serializers/PrestoSerializer.h"

using RowExpressionPtr =
    std::shared_ptr<facebook::presto::protocol::RowExpression>;
using ConstantExpressionPtr =
    std::shared_ptr<facebook::presto::protocol::ConstantExpression>;
using CallExpressionPtr =
    std::shared_ptr<facebook::presto::protocol::CallExpression>;
using SpecialFormExpressionPtr =
    std::shared_ptr<facebook::presto::protocol::SpecialFormExpression>;

namespace facebook::presto::expression {

/// Helper class to convert a Velox expression of type `core::ITypedExpr` to its
/// equivalent Presto expression of type `protocol::RowExpression`:
///  1. A constant Velox expression of type `core::ConstantTypedExpr` is
///     converted to a Presto expression of type `protocol::ConstantExpression`.
///     If the Velox constant expression is of `ROW` type, it is converted to a
///     Presto expression of type `protocol::SpecialFormExpression` with Form
///     as `ROW_CONSTRUCTOR`.
///  2. A Velox expression representing a field in the input, of type
///     `core::FieldAccessTypedExpr`, is converted to a Presto expression of
///     type `protocol::VariableReferenceExpression`.
///  3. A Velox dereference expression of type `core::DereferenceTypedExpr` is
///     converted to a Presto expression of type
///     `protocol::SpecialFormExpression` with Form as `DEREFERENCE`.
///  4. A Velox cast expression of type `core::CastTypedExpr` is converted to a
///     Presto expression of type `protocol::CallExpression`.
///  5. A Velox call expression of type `core::CallTypedExpr` is converted
///     either to a Presto expression of type `protocol::CallExpression` or of
///     type `protocol::SpecialFormExpression`. This is because Velox does
///     not have a separate expression kind for special form expressions and the
///     special forms in Presto and Velox do not have a one to one mapping. If
///     the velox call expression's name belongs to the set of Presto's
///     `SpecialFormExpression` names, it is converted to a Presto
///     `protocol::SpecialFormExpression`; else it is converted to a Presto
///     `protocol::CallExpression`.
class VeloxToPrestoExprConverter {
 public:
  explicit VeloxToPrestoExprConverter(velox::memory::MemoryPool* pool)
      : pool_(pool) {}

  /// Converts a Velox expression `expr` to a Presto protocol RowExpression.
  /// The input Presto RowExpression is returned as the `defaultResult` in case
  /// the Velox to Presto expression conversion fails.
  RowExpressionPtr getRowExpression(
      const velox::core::TypedExprPtr& expr,
      const RowExpressionPtr& defaultResult = nullptr) const;

 private:
  /// This function is used to serialize a constant velox vector to
  /// 'ValueBlock'. 'ValueBlock' is a serialized version of the constant value
  /// from the vector without the Presto serialized page header.
  std::string getValueBlock(const velox::VectorPtr& vector) const;

  /// Helper function to get a Presto `protocol::ConstantExpression` from a
  /// Velox constant expression.
  ConstantExpressionPtr getConstantExpression(
      const velox::core::ConstantTypedExpr* constantExpr) const;

  /// Helper function to get the arguments for Presto `SWITCH` expression from
  /// Velox switch expression.
  std::vector<RowExpressionPtr> getSwitchSpecialFormExpressionArgs(
      const velox::core::CallTypedExpr* switchExpr) const;

  /// Helper function to construct a Presto `protocol::SpecialFormExpression`
  /// from a Velox call expression. This function should be called only on call
  /// expressions that map to a Presto `SpecialFormExpression`. This can be
  /// determined by checking if the call expression's name belongs to the set of
  /// Presto `SpecialFormExpression` names.
  SpecialFormExpressionPtr getSpecialFormExpression(
      const velox::core::CallTypedExpr* expr) const;

  /// Helper function to construct a Presto `protocol::SpecialFormExpression` of
  /// type `ROW_CONSTRUCTOR` from a Velox constant expression.
  SpecialFormExpressionPtr getRowConstructorExpression(
      const velox::core::ConstantTypedExpr* constantExpr) const;

  /// Helper function to construct a Presto `protocol::SpecialFormExpression` of
  /// type `DEREFERENCE` from a Velox dereference expression.
  SpecialFormExpressionPtr getDereferenceExpression(
      const velox::core::DereferenceTypedExpr* dereferenceExpr) const;

  /// Helper function to construct a Presto `protocol::CallExpression` from a
  /// Velox call expression.
  CallExpressionPtr getCallExpression(
      const velox::core::CallTypedExpr* expr) const;

  velox::memory::MemoryPool* pool_;
  const std::unique_ptr<velox::serializer::presto::PrestoVectorSerde> serde_ =
      std::make_unique<velox::serializer::presto::PrestoVectorSerde>();
};
} // namespace facebook::presto::expression
