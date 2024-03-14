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

#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/lib/aggregates/SimpleNumericAggregate.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::aggregate::prestosql {

namespace {
std::unique_ptr<VectorSerde>& getVectorSerde() {
  static std::unique_ptr<VectorSerde> serde =
      std::make_unique<serializer::presto::PrestoVectorSerde>();
  return serde;
}

class SumDataSizeForStatsAggregate
    : public SimpleNumericAggregate<int64_t, int64_t, int64_t> {
 public:
  explicit SumDataSizeForStatsAggregate(TypePtr resultType)
      : BaseAggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(int64_t);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::doExtractValues(groups, numGroups, result, [&](char* group) {
      return *BaseAggregate::Aggregate::template value<int64_t>(group);
    });
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto index : indices) {
      *BaseAggregate ::value<int64_t>(groups[index]) = 0;
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    // Partial and final aggregations are the same.
    extractValues(groups, numGroups, result);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::template updateGroups<true, int64_t>(
        groups,
        rows,
        args[0],
        [](int64_t& result, int64_t value) {
          result = checkedPlus(result, value);
        },
        mayPushdown);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::updateOneGroup(
        group,
        rows,
        args[0],
        [](int64_t& result, int64_t value) {
          result = checkedPlus(result, value);
        },
        [](int64_t& result, int64_t value, int /* unused */) {
          result = checkedPlus(result, value);
        },
        mayPushdown,
        (int64_t)0);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    doUpdateSingleGroup(group, rows, args[0]);
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    doUpdate(groups, rows, args[0]);
  }

 protected:
  void
  updateOneAccumulator(char* const group, vector_size_t row, int64_t rowSize) {
    if (decoded_.isNullAt(row)) {
      return;
    }

    // Clear null.
    clearNull(group);
    // Set sum(current, this).
    int64_t& current = *value<int64_t>(group);
    current = checkedPlus(current, rowSize);
  }

  void
  doUpdate(char** groups, const SelectivityVector& rows, const VectorPtr& arg) {
    decoded_.decode(*arg, rows, true);
    estimateSerializedSizes(arg, rows);
    vector_size_t sizeIndex = 0;
    rows.applyToSelected([&](auto row) {
      updateOneAccumulator(groups[row], row, rowSizes_[sizeIndex++]);
    });
  }

  // Estimates the sizes of first numToProcess selected elements in vector.
  void estimateSerializedSizes(
      VectorPtr vector,
      const SelectivityVector& rows) {
    vector_size_t numRowsToProcess = rows.countSelected();
    rowSizes_.resize(numRowsToProcess);
    std::fill(rowSizes_.begin(), rowSizes_.end(), 0);
    rowIndices_.resize(numRowsToProcess);
    rowSizePtrs_.resize(numRowsToProcess);

    vector_size_t i = 0;
    rows.testSelected([&](auto row) {
      rowIndices_[i] = IndexRange{row, 1};
      rowSizePtrs_[i] = &rowSizes_[i];
      return ++i < numRowsToProcess;
    });

    getVectorSerde()->estimateSerializedSize(
        vector.get(),
        folly::Range(rowIndices_.data(), rowIndices_.size()),
        rowSizePtrs_.data());
  }

  void doUpdateSingleGroup(
      char* group,
      const SelectivityVector& rows,
      const VectorPtr& arg) {
    decoded_.decode(*arg, rows, true);
    estimateSerializedSizes(arg, rows);
    vector_size_t sizeIndex = 0;
    rows.applyToSelected([&](auto row) {
      updateOneAccumulator(group, row, rowSizes_[sizeIndex++]);
    });
  }

 private:
  using BaseAggregate = SimpleNumericAggregate<int64_t, int64_t, int64_t>;

  DecodedVector decoded_;
  // Reusable buffers to calculate the data size of input rows.
  std::vector<vector_size_t> rowSizes_;
  std::vector<vector_size_t*> rowSizePtrs_;
  std::vector<IndexRange> rowIndices_;
};

} // namespace

void registerSumDataSizeForStatsAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .typeVariable("T")
                           .returnType("bigint")
                           .intermediateType("bigint")
                           .argumentType("T")
                           .build());

  auto name = prefix + kSumDataSizeForStats;
  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& /*config*/)
          -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];

        return std::make_unique<SumDataSizeForStatsAggregate>(resultType);
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
