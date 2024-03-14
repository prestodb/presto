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

class MaxSizeForStatsAggregate
    : public SimpleNumericAggregate<int64_t, int64_t, int64_t> {
  using BaseAggregate = SimpleNumericAggregate<int64_t, int64_t, int64_t>;

 private:
  std::vector<vector_size_t> elementSizes_;
  std::vector<vector_size_t*> elementSizePtrs_;
  std::vector<IndexRange> elementIndices_;
  DecodedVector decoded_;

 public:
  explicit MaxSizeForStatsAggregate(TypePtr resultType)
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
    for (auto i : indices) {
      *BaseAggregate ::value<int64_t>(groups[i]) = 0;
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
          if (result < value) {
            result = value;
          }
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
          result = std::max(result, value);
        },
        [](int64_t& result, int64_t value, int /* unused */) {
          result = value;
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
    // Set max(current, this).
    int64_t& current = *value<int64_t>(group);
    current = std::max(current, rowSize);
  }

  void
  doUpdate(char** groups, const SelectivityVector& rows, const VectorPtr& arg) {
    decoded_.decode(*arg, rows, true);

    if (decoded_.isConstantMapping()) {
      if (!decoded_.isNullAt(0)) {
        estimateSerializedSizes(arg, rows, 1);
        rows.applyToSelected([&](auto row) {
          updateOneAccumulator(groups[row], row, elementSizes_[0]);
        });
      }
    } else {
      estimateSerializedSizes(arg, rows, rows.countSelected());
      vector_size_t sizeIndex = 0;
      rows.applyToSelected([&](auto row) {
        updateOneAccumulator(groups[row], row, elementSizes_[sizeIndex++]);
      });
    }
  }

  // Estimate the sizes of first numToProcess selected elements in vector.
  void estimateSerializedSizes(
      VectorPtr vector,
      const SelectivityVector& rows,
      vector_size_t numToProcess) {
    elementSizes_.resize(numToProcess);
    std::fill(elementSizes_.begin(), elementSizes_.end(), 0);
    elementIndices_.resize(numToProcess);
    elementSizePtrs_.resize(numToProcess);

    vector_size_t i = 0;
    rows.testSelected([&](auto row) {
      elementIndices_[i] = IndexRange{row, 1};
      elementSizePtrs_[i] = &elementSizes_[i];
      return ++i < numToProcess;
    });

    getVectorSerde()->estimateSerializedSize(
        vector.get(),
        folly::Range(elementIndices_.data(), elementIndices_.size()),
        elementSizePtrs_.data());
  }

  void doUpdateSingleGroup(
      char* group,
      const SelectivityVector& rows,
      const VectorPtr& arg) {
    decoded_.decode(*arg, rows, true);

    if (decoded_.isConstantMapping()) {
      if (!decoded_.isNullAt(0)) {
        // Estimate first element because it is constant mapping.
        estimateSerializedSizes(arg, rows, 1);
        updateOneAccumulator(group, 0, elementSizes_[0]);
      }
    } else {
      estimateSerializedSizes(arg, rows, rows.countSelected());
      vector_size_t sizeIndex = 0;
      rows.applyToSelected([&](auto row) {
        updateOneAccumulator(group, row, elementSizes_[sizeIndex++]);
      });
    }
  }
};

} // namespace

void registerMaxDataSizeForStatsAggregate(
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

  auto name = prefix + kMaxSizeForStats;
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

        return std::make_unique<MaxSizeForStatsAggregate>(resultType);
      },
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
