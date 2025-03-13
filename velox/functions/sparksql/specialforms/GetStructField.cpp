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

#include "velox/functions/sparksql/specialforms/GetStructField.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::functions::sparksql {
namespace {

// Returns the value of nested subfield in the input struct.
// The input must be of row type and nested complex type is allowed.
// The subfield position is specified by 'ordinal'.
// If 'ordinal' is negative or greater than the children size of input,
// exception is thrown.
class GetStructFieldFunction : public exec::VectorFunction {
 public:
  explicit GetStructFieldFunction(int32_t ordinal) : ordinal_(ordinal) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*resultType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::LocalDecodedVector decoded(context, *args[0], rows);
    auto rowData = decoded->base()->as<RowVector>();
    VELOX_USER_CHECK_LT(
        ordinal_,
        rowData->childrenSize(),
        "Invalid ordinal. Should be smaller than the children size of input row vector.");
    if (decoded->isIdentityMapping()) {
      result = rowData->childAt(ordinal_);
    } else {
      result =
          decoded->wrap(rowData->childAt(ordinal_), *args[0], decoded->size());
    }
  }

 private:
  // The position to select subfield from the struct.
  const int32_t ordinal_;
};

} // namespace

TypePtr GetStructFieldCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& /*argTypes*/) {
  VELOX_FAIL("GetStructField function does not support type resolution.");
}

exec::ExprPtr GetStructFieldCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& args,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  VELOX_USER_CHECK_EQ(
      args.size(), 2, "get_struct_field expects two arguments.");

  VELOX_USER_CHECK_EQ(
      args[0]->type()->kind(),
      TypeKind::ROW,
      "The first argument of get_struct_field should be of row type.");

  VELOX_USER_CHECK_EQ(
      args[1]->type()->kind(),
      TypeKind::INTEGER,
      "The second argument of get_struct_field should be of integer type.");

  auto constantExpr = std::dynamic_pointer_cast<exec::ConstantExpr>(args[1]);
  VELOX_USER_CHECK_NOT_NULL(
      constantExpr,
      "The second argument of get_struct_field should be constant expression.");
  VELOX_USER_CHECK(
      constantExpr->value()->isConstantEncoding(),
      "The second argument of get_struct_field should be wrapped in constant vector.");
  auto constantVector =
      constantExpr->value()->asUnchecked<ConstantVector<int32_t>>();
  VELOX_USER_CHECK(
      !constantVector->isNullAt(0),
      "The second argument of get_struct_field should be non-nullable.");

  auto ordinal = constantVector->valueAt(0);

  VELOX_USER_CHECK_GE(ordinal, 0, "Invalid ordinal. Should be greater than 0.");
  auto getStructFieldFunction =
      std::make_shared<GetStructFieldFunction>(ordinal);

  return std::make_shared<exec::Expr>(
      type,
      std::move(args),
      std::move(getStructFieldFunction),
      exec::VectorFunctionMetadata{},
      kGetStructField,
      trackCpuUsage);
}
} // namespace facebook::velox::functions::sparksql
