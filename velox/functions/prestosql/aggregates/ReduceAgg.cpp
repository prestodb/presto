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
#include "velox/core/QueryCtx.h"
#include "velox/exec/Aggregate.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/aggregates/SingleValueAccumulator.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"

namespace facebook::velox::aggregate::prestosql {
namespace {

using ReduceAggAccumulator = functions::aggregate::SingleValueAccumulator;

// clang-format off
//  TODO: reduce_agg aggregate function doesn't lend itself well to vectorized
//  execution as it is effectively a data-dependent loop. It is hard to avoid
//  evaluating lambda expressions on small number of rows at a time. This
//  particular implementation doesn't try very hard and simply evaluates lambda
//  expressions one row at a time. A relatively straightforward improvement
//  could be similar to the algorithm implemented in the 'reduce' scalar lambda
//  function.
//
//   Consider dataset {1, 10, 2, 20, 3} where values 1, 2, 3 belong to group 1
//   and values 10, 20 belong to group 2.
//
//   This implementation goes like so:
//
//   s0 - initialValue, s1 - state for group 1, s2 - state for group 2.
//
//   s1 = s0
//   s2 = s0
//   s1 = f(s1, 1)
//   s2 = f(s2, 10)
//   s1 = f(s1, 2)
//   s2 = f(s2, 20)
//   s1 = f(s1, 3)
//
//   The inputFunction lambda expression is evaluated 5 times, once per row.
//
//   A more efficient approach would be to evaluate a set of rows that contains
//   one row per group.
//
//   s1 = s0
//   s2 = s0
//
//   [s1, s2] = f([s1, s2], [1, 10])
//   [s1, s2] = f([s1, s2], [2, 20])
//   s1 = f(s1, 3)
//
//   Here, inputFunction lambda expression is evaluated only 3 times (compared
//   to 5 times above).
//
//   Global aggregation would go slightly differently.
//
//   Consider dataset {1, 2, 3, 4, 5, 6,..100}.
//
//   This implementation goes like so:
//
//   s0 - initialValue, s - state of the only group.
//
//   s = s0
//   s = f(s, 1)
//   s = f(s, 2)
//   s = f(s, 3)
//   s = f(s, 4)
//   s = f(s, 5)
//   ...
//
//   The inputFunction lambda expression is evaluated 100 times, once per row.
//
//   A more efficient approach would be:
//
//   f - inputFunction, g - combineFunction
//
//   Convert all inputs into states:
//
//   [s1, s2, s3, s4, s5,..s100] = f([s0, s0, s0, s0, s0,..], [1, 2, 3, 4, 5,..100])
//
//   Combine 100 states into 50:
//
//   [s1, s2,...s50] = g([s1, s3,...s99], [s2, s4,...s100])
//
//   Combine these 50 states into 25 states:
//
//   [s1, s2,...s25] = g([s1, s3,...s49], [s2, s4,...s50])
//
//   Continue in this manner until all states are combined. This requires only
//   log2(100), ~7, expression evaluations.
//
// Note that the more efficient algorithm for global aggregation doesn't support
// applying reduce_agg to sorted inputs.
//
// Also, note that the more efficient algorithm for global aggregation can be
// used in a group-by as well. Each evaluation of the lambda could be reducing
// the cardinality of each group by a factor of 2. This would be require
// log2(max-group-cardinality) evaluations.
//
// A common use case for reduce_agg is to compute a product of input values.
//
//   reduce_agg(x, 1, (a, b) -> (a * b), (a, b) -> (a * b))
//
// In this case, the best option is to identify this pattern and invoke
// specialized 'product(x)' aggregate function instead.
//
// clang-format on
class ReduceAgg : public exec::Aggregate {
 public:
  ReduceAgg(TypePtr resultType) : exec::Aggregate(std::move(resultType)) {}

  bool isFixedSize() const override {
    return false;
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(ReduceAggAccumulator);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto i : indices) {
      new (groups[i] + offset_) ReduceAggAccumulator();
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK_NOT_NULL(result);
    (*result)->resize(numGroups);

    auto* rawNulls = exec::Aggregate::getRawNulls(result->get());

    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto accumulator = value<ReduceAggAccumulator>(group);
      if (!accumulator->hasValue()) {
        (*result)->setNull(i, true);
      } else {
        exec::Aggregate::clearNull(rawNulls, i);
        accumulator->read(*result, i);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<ReduceAggAccumulator>(group)->destroy(allocator_);
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    const auto& lambda = initializeInputLambda();

    const auto& input = args[0];
    const auto& initialValue = args[1];

    auto* pool = allocator_->pool();
    const auto lambdaInputType = lambda->signature();

    const auto& stateType = lambdaInputType->childAt(0);
    VectorPtr state = BaseVector::create(stateType, 1, pool);

    auto lambdaInput = makeLambdaInput(lambdaInputType, 1, state, nullptr);

    SelectivityVector singleRow(1);

    BufferPtr indices = allocateIndices(1, pool);
    auto* rawIndices = indices->asMutable<vector_size_t>();

    VectorPtr newState;

    rows.applyToSelected([&](auto row) {
      if (input->isNullAt(row)) {
        return;
      }

      auto* group = groups[row];
      auto& accumulator = *value<ReduceAggAccumulator>(group);
      if (!accumulator.hasValue()) {
        // Copy initial value.
        VELOX_USER_CHECK(
            !initialValue->isNullAt(row),
            "Initial value in reduce_agg cannot be null")
        auto tracker = trackRowSize(group);
        accumulator.write(initialValue.get(), row, allocator_);
      }

      accumulator.read(state, 0);

      rawIndices[0] = row;
      lambdaInput->children()[1] =
          BaseVector::wrapInDictionary(nullptr, indices, 1, input);

      expressionEvaluator_->evaluate(
          inputExprSet_.get(), singleRow, *lambdaInput, newState);

      VELOX_USER_CHECK(
          !newState->isNullAt(0),
          "Lambda expressions in reduce_agg should not return null for non-null inputs")

      auto tracker = trackRowSize(group);
      accumulator.write(newState.get(), 0, allocator_);
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    const auto& lambda = initializeCombineLambda();

    const auto& input = args[0];

    auto* pool = allocator_->pool();
    const auto lambdaInputType = lambda->signature();

    const auto& stateType = lambdaInputType->childAt(0);
    VectorPtr state = BaseVector::create(stateType, 1, pool);

    auto lambdaInput = makeLambdaInput(lambdaInputType, 1, state, nullptr);

    SelectivityVector singleRow(1);

    BufferPtr indices = allocateIndices(1, pool);
    auto* rawIndices = indices->asMutable<vector_size_t>();

    VectorPtr newState;

    rows.applyToSelected([&](auto row) {
      if (input->isNullAt(row)) {
        return;
      }

      auto* group = groups[row];
      auto& accumulator = *value<ReduceAggAccumulator>(group);
      if (!accumulator.hasValue()) {
        // Copy input.
        auto tracker = trackRowSize(group);
        accumulator.write(input.get(), row, allocator_);
        return;
      }

      accumulator.read(state, 0);

      rawIndices[0] = row;
      lambdaInput->children()[1] =
          BaseVector::wrapInDictionary(nullptr, indices, 1, input);

      expressionEvaluator_->evaluate(
          combineExprSet_.get(), singleRow, *lambdaInput, newState);

      VELOX_USER_CHECK(
          !newState->isNullAt(0),
          "Lambda expressions in reduce_agg should not return null for non-null inputs")

      auto tracker = trackRowSize(group);
      accumulator.write(newState.get(), 0, allocator_);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    verifyInitialValueArg(args[1], rows);

    const auto& lambda = initializeInputLambda();

    auto tracker = trackRowSize(group);
    auto& accumulator = *value<ReduceAggAccumulator>(group);
    if (!accumulator.hasValue()) {
      // Copy initial value.
      accumulator.write(args[1].get(), rows.begin(), allocator_);
    }

    applyLambda(
        accumulator, args[0], rows, std::nullopt, lambda, *inputExprSet_);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    auto& accumulator = *value<ReduceAggAccumulator>(group);

    std::optional<vector_size_t> skipRow;
    if (!accumulator.hasValue()) {
      // Copy first non-null value.
      rows.testSelected([&](auto row) {
        if (args[0]->isNullAt(row)) {
          return true;
        }

        auto tracker = trackRowSize(group);
        accumulator.write(args[0].get(), row, allocator_);
        skipRow = row;
        return false;
      });
    }

    // All input values are null.
    if (!accumulator.hasValue()) {
      return;
    }

    // No rows left to process.
    if (skipRow.has_value() && skipRow.value() == rows.end()) {
      return;
    }

    const auto& lambda = initializeCombineLambda();

    auto tracker = trackRowSize(group);
    applyLambda(accumulator, args[0], rows, skipRow, lambda, *combineExprSet_);
  }

  bool supportsToIntermediate() const override {
    return true;
  }

  void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    if (!rows.hasSelections()) {
      // Set all rows in 'result' to null.
      result->resize(rows.size());
      ::memset(
          result->mutableRawNulls(),
          bits::nbytes(rows.size()),
          bits::kNullByte);
      return;
    }

    const auto& lambda = initializeInputLambda();

    const auto& input = args[0];
    const auto& initialValue = args[1];

    verifyInitialValueArg(initialValue, rows);

    // Do not evaluate on null input.
    SelectivityVector remainingRows = rows;
    if (input->mayHaveNulls()) {
      remainingRows.deselectNulls(input->rawNulls(), 0, rows.size());
    }

    auto lambdaInput = std::make_shared<RowVector>(
        allocator_->pool(),
        lambda->signature(),
        nullptr,
        rows.size(),
        std::vector<VectorPtr>{initialValue, input});

    expressionEvaluator_->evaluate(
        inputExprSet_.get(), remainingRows, *lambdaInput, result);

    if (!remainingRows.isAllSelected()) {
      // Set rows not in 'rows' to null.
      result->resize(remainingRows.size());
      bits::andBits(
          result->mutableRawNulls(),
          remainingRows.asRange().bits(),
          0,
          remainingRows.size());
    }
  }

 private:
  RowVectorPtr makeLambdaInput(
      const RowTypePtr& type,
      vector_size_t size,
      const VectorPtr& firstArg,
      const VectorPtr& secondArg) {
    return std::make_shared<RowVector>(
        allocator_->pool(),
        type,
        nullptr,
        size,
        std::vector<VectorPtr>{firstArg, secondArg});
  }

  void applyLambda(
      ReduceAggAccumulator& accumulator,
      const VectorPtr& input,
      const SelectivityVector& rows,
      std::optional<vector_size_t> skipRow,
      const core::LambdaTypedExprPtr& lambda,
      exec::ExprSet& exprSet) {
    auto* pool = allocator_->pool();

    const auto lambdaInputType = lambda->signature();

    const auto& stateType = lambdaInputType->childAt(0);
    VectorPtr state = BaseVector::create(stateType, 1, pool);
    accumulator.read(state, 0);

    auto lambdaInput = makeLambdaInput(lambdaInputType, 1, state, nullptr);

    SelectivityVector singleRow(1);

    BufferPtr indices = allocateIndices(1, pool);
    auto* rawIndices = indices->asMutable<vector_size_t>();

    rows.applyToSelected([&](vector_size_t row) {
      if (skipRow.has_value() && row <= skipRow.value()) {
        return;
      }

      if (input->isNullAt(row)) {
        return;
      }

      rawIndices[0] = row;
      lambdaInput->children()[1] =
          BaseVector::wrapInDictionary(nullptr, indices, 1, input);

      VectorPtr newState;
      expressionEvaluator_->evaluate(
          &exprSet, singleRow, *lambdaInput, newState);

      VELOX_USER_CHECK(
          !newState->isNullAt(0),
          "Lambda expressions in reduce_agg should not return null for non-null inputs");

      lambdaInput->children()[0] = newState;
    });

    accumulator.write(lambdaInput->children()[0].get(), 0, allocator_);
  }

  static void verifyInitialValueArg(
      const VectorPtr& arg,
      const SelectivityVector& rows) {
    auto firstRow = rows.begin();

    VELOX_USER_CHECK(
        !arg->isNullAt(firstRow), "Initial value in reduce_agg cannot be null");

    rows.applyToSelected([&](vector_size_t row) {
      if (!arg->equalValueAt(arg.get(), firstRow, row)) {
        VELOX_USER_FAIL(
            "Initial value in reduce_agg must be constant: {} vs. {}",
            arg->toString(firstRow),
            arg->toString(row));
      }
    });
  }

  const core::LambdaTypedExprPtr& initializeInputLambda() const {
    const auto& lambda = getLambda(0);

    if (inputExprSet_ == nullptr) {
      inputExprSet_ = expressionEvaluator_->compile(lambda->body());
    }

    return lambda;
  }

  const core::LambdaTypedExprPtr& initializeCombineLambda() const {
    const auto& lambda = getLambda(1);

    if (combineExprSet_ == nullptr) {
      combineExprSet_ = expressionEvaluator_->compile(lambda->body());
    }

    return lambda;
  }

  const core::LambdaTypedExprPtr& getLambda(int32_t index) const {
    VELOX_CHECK_EQ(2, lambdaExpressions_.size());
    VELOX_CHECK_LT(index, 2);

    const auto& lambda = lambdaExpressions_[index];
    VELOX_CHECK_NOT_NULL(lambda);
    return lambda;
  }

  mutable std::unique_ptr<exec::ExprSet> inputExprSet_;
  mutable std::unique_ptr<exec::ExprSet> combineExprSet_;
};

} // namespace

void registerReduceAgg(const std::string& prefix) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .typeVariable("S")
          .returnType("S")
          .intermediateType("S")
          .argumentType("T")
          .argumentType("S")
          .argumentType("function(S,T,S)")
          .argumentType("function(S,S,S)")
          .build()};

  const std::string name = prefix + kReduceAgg;

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        return std::make_unique<ReduceAgg>(resultType);
      });
}

} // namespace facebook::velox::aggregate::prestosql
