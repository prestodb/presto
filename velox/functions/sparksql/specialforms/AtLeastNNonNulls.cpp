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

#include "velox/functions/sparksql/specialforms/AtLeastNNonNulls.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/SpecialForm.h"

using namespace facebook::velox::exec;

namespace facebook::velox::functions::sparksql {
namespace {
class AtLeastNNonNullsExpr : public SpecialForm {
 public:
  AtLeastNNonNullsExpr(
      TypePtr type,
      bool trackCpuUsage,
      std::vector<ExprPtr>&& inputs,
      int n)
      : SpecialForm(
            SpecialFormKind::kCustom,
            std::move(type),
            std::move(inputs),
            AtLeastNNonNullsCallToSpecialForm::kAtLeastNNonNulls,
            true,
            trackCpuUsage),
        n_(n) {}

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override {
    context.ensureWritable(rows, type(), result);
    (*result).clearNulls(rows);
    auto flatResult = result->asFlatVector<bool>();
    LocalSelectivityVector activeRowsHolder(context, rows);
    auto activeRows = activeRowsHolder.get();
    VELOX_DCHECK_NOT_NULL(activeRows);
    auto values = flatResult->mutableValues()->asMutable<uint64_t>();
    // If 'n_' <= 0, set result to all true.
    if (n_ <= 0) {
      bits::orBits(values, rows.asRange().bits(), rows.begin(), rows.end());
      return;
    }

    bits::andWithNegatedBits(
        values, rows.asRange().bits(), rows.begin(), rows.end());
    // If 'n_' > inputs_.size() - 1, result should be all false.
    if (n_ > inputs_.size() - 1) {
      return;
    }

    // Create a temp buffer to track count of non null values for active rows.
    auto nonNullCounts =
        AlignedBuffer::allocate<int32_t>(activeRows->size(), context.pool(), 0);
    auto* rawNonNullCounts = nonNullCounts->asMutable<int32_t>();
    for (column_index_t i = 1; i < inputs_.size(); ++i) {
      VectorPtr input;
      inputs_[i]->eval(*activeRows, context, input);
      if (context.errors()) {
        context.deselectErrors(*activeRows);
      }
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          updateResultTyped,
          inputs_[i]->type()->kind(),
          input.get(),
          n_,
          context,
          rawNonNullCounts,
          flatResult,
          activeRows);
      if (activeRows->countSelected() == 0) {
        break;
      }
    }
  }

 private:
  void computePropagatesNulls() override {
    propagatesNulls_ = false;
  }

  template <TypeKind Kind>
  void updateResultTyped(
      BaseVector* input,
      int32_t n,
      EvalCtx& context,
      int32_t* rawNonNullCounts,
      FlatVector<bool>* result,
      SelectivityVector* activeRows) {
    using T = typename TypeTraits<Kind>::NativeType;
    exec::LocalDecodedVector decodedVector(context);
    decodedVector.get()->decode(*input, *activeRows);
    bool updateBounds = false;
    activeRows->applyToSelected([&](auto row) {
      bool nonNull = !decodedVector->isNullAt(row);
      if constexpr (std::is_same_v<T, double> || std::is_same_v<T, float>) {
        nonNull = nonNull && !std::isnan(decodedVector->valueAt<T>(row));
      }
      if (nonNull) {
        rawNonNullCounts[row]++;
        if (rawNonNullCounts[row] >= n) {
          updateBounds = true;
          result->set(row, true);
          // Exclude the 'row' from active rows after finding 'n' non-NULL /
          // non-NaN values.
          activeRows->setValid(row, false);
        }
      }
    });
    if (updateBounds) {
      activeRows->updateBounds();
    }
  }

  // Result is true if there are at least `n_` non-null and non-NaN values.
  const int n_;
};
} // namespace

TypePtr AtLeastNNonNullsCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& /*argTypes*/) {
  return BOOLEAN();
}

ExprPtr AtLeastNNonNullsCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  VELOX_USER_CHECK_GT(
      compiledChildren.size(),
      1,
      "AtLeastNNonNulls expects to receive at least 2 arguments.");
  VELOX_USER_CHECK(
      compiledChildren[0]->type()->isInteger(),
      "The first input type should be INTEGER but got {}.",
      compiledChildren[0]->type()->toString());
  auto constantExpr =
      std::dynamic_pointer_cast<exec::ConstantExpr>(compiledChildren[0]);
  VELOX_USER_CHECK_NOT_NULL(
      constantExpr, "The first parameter should be constant expression.");
  VELOX_USER_CHECK(
      constantExpr->value()->isConstantEncoding(),
      "The first parameter should be wrapped in constant vector.");
  auto constVector =
      constantExpr->value()->asUnchecked<ConstantVector<int32_t>>();
  VELOX_USER_CHECK(
      !constVector->isNullAt(0), "The first parameter should not be null.");
  const int32_t n = constVector->valueAt(0);
  return std::make_shared<AtLeastNNonNullsExpr>(
      type, trackCpuUsage, std::move(compiledChildren), n);
}
} // namespace facebook::velox::functions::sparksql
