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
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/Expr.h"
#include "velox/expression/SwitchExpr.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox;

using RowExpressionPtr =
    std::shared_ptr<facebook::presto::protocol::RowExpression>;
using SpecialFormExpressionPtr =
    std::shared_ptr<facebook::presto::protocol::SpecialFormExpression>;

namespace facebook::presto::expression {

/// Helper class to convert a velox expression to its corresponding Presto
/// protocol RowExpression. The function `veloxToPrestoRowExpression` is used in
/// RowExpressionOptimizer to convert the constant folded velox expression to
/// Presto protocol RowExpression:
/// 1. A velox constant expression of type exec::ConstantExpr without any inputs
///    is converted to Presto protocol expression of type ConstantExpression.
///    If the velox constant expression is of ROW type, it is converted to a
///    Presto protocol expression of type SpecialFormExpression (with Form as
///    ROW_CONSTRUCTOR).
/// 2. Velox expression representing a variable is of type exec::FieldReference,
///    it is converted to a Presto protocol expression of type
///    VariableReferenceExpression.
/// 3. Special form expressions and expressions with a vector function in velox
///    map either to a Presto protocol SpecialFormExpression or to a Presto
///    protocol CallExpression. This is because special form expressions in
///    velox and in Presto do not have a one to one mapping. If the velox
///    expression name belongs to the set of possible Presto protocol
///    SpecialFormExpression names, it is converted to a Presto protocol
///    SpecialFormExpression; else it is converted to a Presto protocol
///    CallExpression.
///
/// The function `getConstantRowExpression` is used in RowExpressionOptimizer to
/// convert a velox constant expression to Presto protocol ConstantExpression.
class RowExpressionConverter {
 public:
  explicit RowExpressionConverter(memory::MemoryPool* pool) : pool_(pool) {}

  /// Converts a velox constant expression `constantExpr` to a Presto protocol
  /// ConstantExpression.
  json getConstantRowExpression(
      const std::shared_ptr<const exec::ConstantExpr>& constantExpr);

  /// Converts a velox expression `expr` to a Presto protocol RowExpression.
  /// Argument `inputRowExpr` is the input Presto protocol RowExpression before
  /// it is constant folded in velox.
  json veloxToPrestoRowExpression(
      const exec::ExprPtr& expr,
      const RowExpressionPtr& inputRowExpr);

 private:
  /// When 'isSimplified' is true, the cases (or arguments) in switch expression
  /// have been simplified when the expression was constant folded in velox. In
  /// this case, the expression corresponding to the first switch case that
  /// always evaluates to true is returned in 'caseExpression'. When
  /// 'isSimplified' is false, the cases in switch expression have not been
  /// simplified, and the switch expression arguments required by Presto are
  /// returned in 'arguments'.
  struct SwitchFormArguments {
    bool isSimplified = false;
    json caseExpression;
    json::array_t arguments;
  };

  /// ValueBlock in Presto protocol ConstantExpression requires only the column
  /// from the serialized PrestoPage without the page header. This function is
  /// used to serialize a velox vector to ValueBlock.
  std::string getValueBlock(const VectorPtr& vector) const;

  /// Helper function to get arguments for Presto protocol SpecialFormExpression
  /// of type SWITCH.
  RowExpressionConverter::SwitchFormArguments getSwitchSpecialFormArgs(
      const exec::SwitchExpr* switchExpr,
      const std::vector<RowExpressionPtr>& arguments);

  /// Helper function to construct a Presto protocol SpecialFormExpression from
  /// a velox expression `expr`.
  json getSpecialForm(const exec::ExprPtr& expr, const RowExpressionPtr& input);

  /// Helper function to construct a Presto protocol SpecialFormExpression of
  /// type ROW_CONSTRUCTOR from a velox constant expression `constantExpr`.
  json getRowConstructorSpecialForm(
      std::shared_ptr<const exec::ConstantExpr>& constantExpr);

  /// Helper function to construct a Presto protocol CallExpression from a velox
  /// expression `expr`.
  json toCallRowExpression(
      const exec::ExprPtr& expr,
      const RowExpressionPtr& input);

  memory::MemoryPool* pool_;
  const std::unique_ptr<serializer::presto::PrestoVectorSerde> serde_ =
      std::make_unique<serializer::presto::PrestoVectorSerde>();
};
} // namespace facebook::presto::expression
