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
//  reduce_agg aggregate function doesn't lend itself well to vectorized
//  execution as it is effectively a data-dependent loop. It is hard to avoid
//  evaluating lambda expressions on small number of rows at a time. An original
//  implementation didn't try very hard and simply evaluated lambda
//  expressions one row at a time. This implementation is optimized to evaluate
//  expressions ~log2(n) times, where n is the number of rows in the batch in
//  case of global aggregation and maximum cardinality of a single group in the
//  batch in case of group by.
//
//   Consider global aggregation over dataset {1, 2, 3, 4, 5, 6,..100}.
//
//   The original implementation went like so:
//
//   s0 - initialValue, s - state of the only group. f - inputFunction.
//
//   s = s0
//   s = f(s, 1)
//   s = f(s, 2)
//   s = f(s, 3)
//   s = f(s, 4)
//   s = f(s, 5)
//   ...
//
//   The inputFunction lambda expression was evaluated 100 times, once per row.
//
//   This implementation goes like this:
//
//   f - inputFunction, g - combineFunction
//
//   Convert all inputs into states using inputFunction and initialValue:
//
//   [s1, s2, s3, s4, s5,..s100] = f([s0, s0, s0, s0, s0,..], [1, 2, 3, 4, 5,..100])
//
//   Combine 100 states into 50:
//
//   [s1, s2,...s50] = g([s1, s2,...s50], [s51, s52,...s100])
//
//   Combine these 50 states into 25 states:
//
//   [s1, s2,...s25] = g([s1, s2,...s25], [s26, s27,...s50])
//
//   Combine these 25 states into 13 (12 + 1) states:
//
//   [s1, s2,...s12] = g([s1, s2,...s12], [s13, s14,...s24])
//   s13 = s25
//
//   Continue in this manner until all states are combined. This requires only
//   log2(100), ~7, expression evaluations.
//
//   Note: When adding a batch of rows to a non-empty accumulator, the
//   accumulated state is appended to the list of initial states. In the
//   example above, we would get 101 states to combine.
//
//   Group by applies this technique to each group.
//
//   Consider dataset {1, 10, 2, 20, 3} where values 1, 2, 3 belong to group 1
//   and values 10, 20 belong to group 2.
//
//   The original implementation went like so:
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
//   The inputFunction lambda expression was evaluated 5 times, once per row.
//
//   This implementation converts all inputs into states using inputFunction
//   and initialValue, then combines multiple pairs of states similar to global
//   aggregation.
//
//   First, put rows that belong to same groups together.
//
//   [1, 2, 3, 10, 30]
//
//   Convert values into states:
//
//   [s1, s2, s3, s4, s5] -> f([s0, s0, s0, s0, s0], [1, 2, 3, 10, 30])
//
//   Combine states within each group:
//
//   [s1, s2] = g([s1, s4], [s2, s5])
//   s3 = s3 // extra state for group 1
//
//   We have 2 states for group 1 (s1, s3) and 1 state for group 2 (s2).
//   Group 2 is done. We proceed to combine states for group 1.
//
//   s1 = g([s1], [s3])
//
//   In this example, inputFunction lambda expression is evaluated once and
//   combineFunction is evaluated twice (compared to evaluating inputFunction 5
//   times above).
//
//   Note: When adding a batch of rows to non-empty accumulators, the
//   accumulated states are added to the list of initial states above.
//
// Note: This more efficient algorithm doesn't support applying reduce_agg
// to sorted inputs.
//
// TODO A common use case for reduce_agg is to compute a product of input values.
//
//   reduce_agg(x, 1, (a, b) -> (a * b), (a, b) -> (a * b))
//
// In this case, the best option would be to identify this pattern and invoke
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
      bool /*mayPushdown*/) override {
    add(groups, rows, args, true /*rowInput*/);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    add(groups, rows, args, false /*rowInput*/);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    const auto& input = args[0];

    auto nonNullIndices = collectNonNullRows(input, rows);
    if (nonNullIndices->size() == 0) {
      // Nothing to do. All input values are null.
      return;
    }

    const auto& initialValue = args[1];
    verifyInitialValueArg(initialValue, rows);

    const auto& lambda = initializeInputLambda();
    const auto& lambdaInputType = lambda->signature();

    // Convert non-null input values into 'state' values by applying
    // inputFunction to a pair of (value, initialValue).
    const auto numRows = nonNullIndices->size() / sizeof(vector_size_t);
    auto nonNullInput =
        BaseVector::wrapInDictionary(nullptr, nonNullIndices, numRows, input);
    auto initialState = BaseVector::wrapInDictionary(
        nullptr, nonNullIndices, numRows, initialValue);

    auto lambdaInput =
        makeLambdaInput(lambdaInputType, numRows, initialState, nonNullInput);

    SelectivityVector nonNullRows(numRows);
    VectorPtr combined;
    expressionEvaluator_->evaluate(
        inputExprSet_.get(), nonNullRows, *lambdaInput, combined);

    checkStatesNotNull(combined, numRows);

    addSingleGroup(combined, numRows, group);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    const auto& input = args[0];

    auto nonNullIndices = collectNonNullRows(input, rows);
    if (nonNullIndices->size() == 0) {
      // Nothing to do. All input values are null.
      return;
    }

    auto numRows = nonNullIndices->size() / sizeof(vector_size_t);
    auto combined =
        BaseVector::wrapInDictionary(nullptr, nonNullIndices, numRows, input);

    addSingleGroup(combined, numRows, group);
  }

  bool supportsToIntermediate() const override {
    return true;
  }

  void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    const auto& input = args[0];

    bool allResultNull = false;
    if (!rows.hasSelections()) {
      allResultNull = true;
    } else if (input->isConstantEncoding() && input->isNullAt(0)) {
      allResultNull = true;
    }

    if (allResultNull) {
      // Set all rows in 'result' to null.
      result->resize(rows.size());
      ::memset(
          result->mutableRawNulls(),
          bits::kNullByte,
          bits::nbytes(rows.size()));
      return;
    }

    const auto& initialValue = args[1];
    verifyInitialValueArg(initialValue, rows);

    // Do not evaluate on null input.
    SelectivityVector remainingRows = rows;
    if (input->mayHaveNulls()) {
      DecodedVector decoded(*input, rows);
      if (auto* rawNulls = decoded.nulls(&rows)) {
        remainingRows.deselectNulls(rawNulls, rows.begin(), rows.end());
      }
    }

    const auto& lambda = initializeInputLambda();
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

  static void verifyInitialValueArg(
      const VectorPtr& arg,
      const SelectivityVector& rows) {
    auto firstRow = rows.begin();

    VELOX_USER_CHECK(
        !arg->isNullAt(firstRow), "Initial value in reduce_agg cannot be null");

    if (arg->isConstantEncoding()) {
      return;
    }

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

  // Combines 'numRows' states from 'input' with an optional 'accumulator' state
  // and writes the result back to the accumulator.
  void addSingleGroup(VectorPtr& input, vector_size_t numRows, char* group) {
    // Combine 'input' states and an optional 'accumulator' state into one.
    vector_size_t numCombined = numRows;
    auto& accumulator = *value<ReduceAggAccumulator>(group);
    if (accumulator.hasValue()) {
      prepareToAppendOne(input, numRows);
      accumulator.read(input, numRows);
      ++numCombined;
    }

    if (numCombined > 1) {
      input = combine(input, numCombined);
    }

    auto tracker = trackRowSize(group);
    accumulator.write(input.get(), 0, allocator_);
  }

  BufferPtr collectNonNullRows(
      const VectorPtr& input,
      const SelectivityVector& rows) {
    BufferPtr indices = allocateIndices(rows.size(), allocator_->pool());
    auto* rawIndices = indices->asMutable<vector_size_t>();

    vector_size_t i = 0;
    rows.applyToSelected([&](auto row) {
      if (!input->isNullAt(row)) {
        rawIndices[i++] = row;
      }
    });

    indices->setSize(sizeof(vector_size_t) * i);
    return indices;
  }

  void checkStatesNotNull(const VectorPtr& states, vector_size_t size) {
    if (!states->mayHaveNulls()) {
      return;
    }

    for (auto i = 0; i < size; ++i) {
      VELOX_USER_CHECK(
          !states->isNullAt(i),
          "Lambda expressions in reduce_agg should not return null for non-null inputs");
    }
  }

  void checkStatesNotNull(
      const VectorPtr& states,
      const SelectivityVector& rows) {
    if (!states->mayHaveNulls()) {
      return;
    }

    rows.applyToSelected([&](auto row) {
      VELOX_USER_CHECK(
          !states->isNullAt(row),
          "Lambda expressions in reduce_agg should not return null for non-null inputs");
    });
  }

  void prepareToAppendOne(VectorPtr& vector, vector_size_t size) {
    SelectivityVector extraRow(size + 1, false);
    extraRow.setValid(size, true);
    extraRow.updateBounds();

    BaseVector::ensureWritable(
        extraRow, vector->type(), allocator_->pool(), vector);
  }

  // Maps a group pointer to a pair of zero-based unique group index and a
  // number of rows in the group.
  using GroupMap = folly::F14FastMap<
      char*,
      std::pair<int32_t, int32_t>,
      std::hash<char*>,
      std::equal_to<char*>>;

  void populateGroupCountsAndOffsets(
      const GroupMap& groups,
      std::vector<vector_size_t>& groupCounts,
      std::vector<vector_size_t>& groupOffsets) {
    for (const auto& [group, indexAndCount] : groups) {
      groupCounts[indexAndCount.first] = indexAndCount.second;
    }

    vector_size_t offset = 0;
    for (auto i = 0; i < groupCounts.size(); ++i) {
      groupOffsets[i] = offset;
      offset += groupCounts[i];
    }
  }

  VectorPtr toStates(
      const VectorPtr& input,
      const VectorPtr& initialState,
      const BufferPtr& groupedIndices,
      const SelectivityVector& rows) {
    const auto& lambda = initializeInputLambda();
    const auto lambdaInputType = lambda->signature();

    const auto numRows = rows.size();
    auto lambdaInput = makeLambdaInput(
        lambdaInputType,
        numRows,
        BaseVector::wrapInDictionary(
            nullptr, groupedIndices, numRows, initialState),
        BaseVector::wrapInDictionary(nullptr, groupedIndices, numRows, input));

    VectorPtr states;
    expressionEvaluator_->evaluate(
        inputExprSet_.get(), rows, *lambdaInput, states);

    checkStatesNotNull(states, rows);
    return states;
  }

  void copyAccumulatorStates(
      VectorPtr& states,
      const SelectivityVector& groupRows,
      const std::vector<char*>& groups,
      std::vector<vector_size_t>& groupOffsets) {
    SelectivityVector accumulatorRows(groupRows.size());
    accumulatorRows.deselect(groupRows);

    BaseVector::ensureWritable(
        accumulatorRows, states->type(), allocator_->pool(), states);

    const auto numGroups = groups.size();
    for (auto i = 0; i < numGroups; ++i) {
      auto* group = groups[i];
      auto& accumulator = *value<ReduceAggAccumulator>(group);
      if (accumulator.hasValue()) {
        accumulator.read(states, groupOffsets[i]);
        ++groupOffsets[i];
      }
    }
  }

  void add(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool rawInput) {
    const auto& input = args[0];
    auto* pool = allocator_->pool();

    // Figure out a list of unique groups and number of entries per group.
    GroupMap uniqueGroups;

    vector_size_t numAccumulators = 0;
    vector_size_t numNotNull = 0;
    vector_size_t groupIndex = 0;

    rows.applyToSelected([&](auto row) {
      if (input->isNullAt(row)) {
        return;
      }

      auto* group = groups[row];
      ++numNotNull;

      auto [it, ok] = uniqueGroups.insert({group, {groupIndex, 1}});
      if (ok) {
        ++groupIndex;
        auto& accumulator = *value<ReduceAggAccumulator>(group);
        if (accumulator.hasValue()) {
          ++(it->second).second;
          ++numAccumulators;
        }
      } else {
        ++(it->second).second;
      }
    });

    const auto numGroups = uniqueGroups.size();

    std::vector<vector_size_t> groupCounts(numGroups, 0);
    std::vector<vector_size_t> groupOffsets(numGroups, 0);
    populateGroupCountsAndOffsets(uniqueGroups, groupCounts, groupOffsets);

    // Place rows from the same group together. Add one more row at the end of
    // each group for 'accumulator'.
    const auto numStates = numNotNull + numAccumulators;

    BufferPtr groupedIndices = allocateIndices(numStates, pool);
    auto* rawGroupedIndices = groupedIndices->asMutable<vector_size_t>();

    SelectivityVector groupRows(numStates, false);
    rows.applyToSelected([&](auto row) {
      if (input->isNullAt(row)) {
        return;
      }

      auto* group = groups[row];
      auto groupIndex = uniqueGroups.at(group).first;
      auto offset = groupOffsets[groupIndex];

      rawGroupedIndices[offset] = row;
      groupRows.setValid(offset, true);
      ++groupOffsets[groupIndex];
    });
    groupRows.updateBounds();

    VectorPtr states;
    if (rawInput) {
      // Convert values into states. Append 'accumulator' state to groups as
      // needed.
      const auto& initialValue = args[1];
      states = toStates(input, initialValue, groupedIndices, groupRows);
    } else {
      states = BaseVector::wrapInDictionary(
          nullptr, groupedIndices, numStates, input);
    }

    std::vector<char*> indexedGroups(numGroups);
    for (const auto& [group, indexAndCount] : uniqueGroups) {
      indexedGroups[indexAndCount.first] = group;
    }

    // Append accumulators.
    if (numAccumulators > 0) {
      copyAccumulatorStates(states, groupRows, indexedGroups, groupOffsets);
    }

    // Restore offsets.
    for (auto i = 0; i < numGroups; ++i) {
      groupOffsets[i] -= groupCounts[i];
    }

    // Recursively combine states into one state per group and write to
    // accumulators.
    combine(states, numStates, indexedGroups, groupOffsets, groupCounts);
  }

  // Combine multiple per-group states into one state per group and write final
  // states to the accumulators.
  //
  // @param states Per-group states to combine. All states from the same group
  // appear together starting at groupOffsets[i] and going for groupCounts[i].
  // @param groups Group pointer to lookup accumulators. The order of groups
  // here matches 'groupOffsets' and 'groupCounts'.
  // @param groupOffsets Offsets in 'states' vector for the start of the group.
  // @param groupCounts Number of group entries in 'states' vector.
  void combine(
      const VectorPtr& states,
      vector_size_t size,
      const std::vector<char*>& groups,
      std::vector<vector_size_t>& groupOffsets,
      std::vector<vector_size_t>& groupCounts) {
    const auto numGroups = groupOffsets.size();

    BufferPtr leftIndices = allocateIndices(size, allocator_->pool());
    auto rawLeftIndices = leftIndices->asMutable<vector_size_t>();
    BufferPtr rightIndices = allocateIndices(size, allocator_->pool());
    auto rawRightIndices = rightIndices->asMutable<vector_size_t>();

    vector_size_t totalCount = 0;
    SelectivityVector rows(size, false);

    std::vector<vector_size_t> extraRowNumbers(size, 0);

    for (auto i = 0; i < numGroups; ++i) {
      // Split each group that has > 1 state in half.
      auto count = groupCounts[i];
      auto offset = groupOffsets[i];

      // Update offset.
      groupOffsets[i] = totalCount;

      if (count == 0) {
        continue;
      }

      if (count == 1) {
        // Write the combined state to accumulator.
        auto group = groups[i];
        auto& accumulator = *value<ReduceAggAccumulator>(group);

        auto tracker = trackRowSize(group);
        accumulator.write(states.get(), offset, allocator_);

        --groupCounts[i];
        continue;
      }

      auto halfSize = count / 2;

      for (auto j = 0; j < halfSize; ++j) {
        rawLeftIndices[totalCount] = offset + j;
        rawRightIndices[totalCount] = offset + halfSize + j;
        rows.setValid(totalCount, true);
        ++totalCount;
      }

      if (count % 2 == 1) {
        // Leave space for the 'extra' state.
        extraRowNumbers[totalCount] = offset + count - 1;
        ++totalCount;
      }

      // Update count.
      groupCounts[i] = totalCount - groupOffsets[i];
    }
    rows.updateBounds();

    if (!rows.hasSelections()) {
      // No group has more than 1 entry.
      return;
    }

    // Combine states.
    const auto& lambda = initializeCombineLambda();

    const auto lambdaInputType = lambda->signature();

    auto lambdaInput = makeLambdaInput(
        lambdaInputType,
        totalCount,
        BaseVector::wrapInDictionary(nullptr, leftIndices, totalCount, states),
        BaseVector::wrapInDictionary(
            nullptr, rightIndices, totalCount, states));

    VectorPtr combined;
    expressionEvaluator_->evaluate(
        combineExprSet_.get(), rows, *lambdaInput, combined);

    checkStatesNotNull(combined, rows);

    // Copy 'extra' states.
    if (rows.countSelected() != totalCount) {
      SelectivityVector extraRows(totalCount);
      extraRows.deselect(rows);

      BaseVector::ensureWritable(
          extraRows, combined->type(), allocator_->pool(), combined);

      combined->copy(states.get(), extraRows, extraRowNumbers.data());
    }

    // Keep combining.
    combine(combined, totalCount, groups, groupOffsets, groupCounts);
  }

  // Combine first 'size' states into one.
  VectorPtr combine(const VectorPtr& states, vector_size_t size) {
    VELOX_CHECK_GT(size, 1);

    const auto& lambda = initializeCombineLambda();

    auto* pool = allocator_->pool();
    const auto lambdaInputType = lambda->signature();

    // [size/2, size) indices.
    const auto halfSize = size / 2;
    BufferPtr indices = allocateIndices(halfSize, pool);
    auto* rawIndices = indices->asMutable<vector_size_t>();
    std::iota(rawIndices, rawIndices + halfSize, halfSize);

    auto lambdaInput = makeLambdaInput(
        lambdaInputType,
        halfSize,
        states,
        BaseVector::wrapInDictionary(nullptr, indices, halfSize, states));

    SelectivityVector rows(halfSize);
    VectorPtr combined;
    expressionEvaluator_->evaluate(
        combineExprSet_.get(), rows, *lambdaInput, combined);

    checkStatesNotNull(combined, halfSize);

    const bool hasExtraState = (size % 2 == 1);
    const vector_size_t numCombined = halfSize + (hasExtraState ? 1 : 0);

    if (numCombined == 1) {
      return combined;
    }

    if (hasExtraState) {
      // Add 'extra' state to 'combined'.
      prepareToAppendOne(combined, halfSize);
      combined->copy(states.get(), halfSize, size - 1, 1);
    }

    return combine(combined, numCombined);
  }

  mutable std::unique_ptr<exec::ExprSet> inputExprSet_;
  mutable std::unique_ptr<exec::ExprSet> combineExprSet_;
};

} // namespace

void registerReduceAgg(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
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
      },
      {false /*orderSensitive*/},
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
