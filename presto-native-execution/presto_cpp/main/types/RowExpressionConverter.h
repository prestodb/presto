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

#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/Expr.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox;

namespace facebook::presto::expression {

// When 'isSimplified' is true, the cases (or arguments) in switch expression
// have been simplified when the expression was constant folded in Velox. In
// this case, the expression corresponding to the first switch case that always
// evaluates to true is returned in 'caseExpression'. For example, consider the
// switch expression: CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two'. The first
// switch case always evaluates to true here so 'one' is returned in the field
// 'caseExpression'.
// When 'isSimplified' is false, the cases in switch expression have not been
// simplified, and the switch expression arguments required by Presto are
// returned in 'arguments'. Consider the same example from before with a slight
// modification: CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two'. There is no switch
// case that can be simplified since 'a' is a variable here, so the WHEN clauses
// that are required by Presto as switch expression arguments are returned in
// the field 'arguments'.
struct SwitchFormArguments {
  bool isSimplified = false;
  json caseExpression;
  json::array_t arguments;
};

// Helper class to convert Velox expressions of type exec::Expr to their
// corresponding type of RowExpression in Presto.
// 1. Constant velox expressions of type exec::ConstantExpr are converted to
//    Presto ConstantExpression. However, if the velox constant expression is of
//    ROW type, it is converted to the Presto SpecialFormExpression
//    of type ROW_CONSTRUCTOR.
// 2. Velox expressions representing variables are of type exec::FieldReference
//    and they are converted to Presto VariableReferenceExpression.
// 3. Special form expressions and expressions with a vector function in Velox
//    can map either to a Presto SpecialFormExpression or to a Presto
//    CallExpression. Such velox expressions are converted to the appropriate
//    Presto RowExpression based on the expression name.
class RowExpressionConverter {
 public:
  explicit RowExpressionConverter(memory::MemoryPool* pool)
      : pool_(pool), veloxToPrestoOperatorMap_(veloxToPrestoOperatorMap()) {}

  std::shared_ptr<protocol::ConstantExpression> getConstantRowExpression(
      const std::shared_ptr<const exec::ConstantExpr>& constantExpr);

  json veloxToPrestoRowExpression(
      const exec::ExprPtr& expr,
      const json& inputRowExpr);

 private:
  std::string getValueBlock(const VectorPtr& vector);

  SwitchFormArguments getSimpleSwitchFormArgs(
      const json::array_t& inputArgs,
      const exec::ExprPtr& switchExpr);

  SwitchFormArguments getSwitchSpecialFormArgs(
      const exec::ExprPtr& switchExpr,
      const json& input);

  json getSpecialForm(const exec::ExprPtr& expr, const json& inputRowExpr);

  json getRowConstructorSpecialForm(
      std::shared_ptr<const exec::ConstantExpr>& constantExpr);

  json toConstantRowExpression(const exec::ExprPtr& expr);

  json toCallRowExpression(const exec::ExprPtr& expr, const json& input);

  memory::MemoryPool* pool_;
  const std::unordered_map<std::string, std::string> veloxToPrestoOperatorMap_;
  const std::unique_ptr<serializer::presto::PrestoVectorSerde> serde_ =
      std::make_unique<serializer::presto::PrestoVectorSerde>();
};
} // namespace facebook::presto::expression
