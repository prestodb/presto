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

#include "velox/aggregates/AggregateNames.h"
#include "velox/aggregates/SimpleNumerics.h"
#include "velox/exec/Aggregate.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::aggregate {

namespace {

class BoolAndOrAggregate : public SimpleNumericAggregate<bool, bool, bool> {
 protected:
  using BaseAggregate = SimpleNumericAggregate<bool, bool, bool>;

 public:
  explicit BoolAndOrAggregate(
      core::AggregationNode::Step step,
      bool initialValue)
      : BaseAggregate(step, BOOLEAN()), initialValue_(initialValue) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(bool);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto* vector = (*result)->as<FlatVector<bool>>();
    VELOX_CHECK(vector);
    vector->resize(numGroups);

    uint64_t* rawNulls = getRawNulls(vector);
    uint64_t* rawValues = vector->mutableRawValues<uint64_t>();

    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        bits::setBit(rawValues, i, *value<bool>(group));
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      *value<bool>(groups[i]) = initialValue_;
    }
  }

 protected:
  const bool initialValue_;
};

class BoolAndAggregate final : public BoolAndOrAggregate {
 public:
  explicit BoolAndAggregate(core::AggregationNode::Step step)
      : BoolAndOrAggregate(step, /* initialValue = */ true) {}

  void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::updateGroups<true>(
        groups,
        rows,
        args[0],
        [](bool& result, bool value) { result = result && value; },
        mayPushdown);
  }

  void updateFinal(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    return updatePartial(groups, rows, args, mayPushdown);
  }

  void updateSingleGroupPartial(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::updateOneGroup(
        group,
        allRows,
        args[0],
        [](bool& result, bool value) { result = result && value; },
        [](bool& result, bool value, int /* unused */) {
          result = result && value;
        },
        mayPushdown,
        this->initialValue_);
  }

  void updateSingleGroupFinal(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updateSingleGroupPartial(group, allRows, args, mayPushdown);
  }
};

class BoolOrAggregate final : public BoolAndOrAggregate {
 public:
  explicit BoolOrAggregate(core::AggregationNode::Step step)
      : BoolAndOrAggregate(step, /* initialValue = */ false) {}

  void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::updateGroups<true>(
        groups,
        rows,
        args[0],
        [](bool& result, bool value) { result = result || value; },
        mayPushdown);
  }

  void updateFinal(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updatePartial(groups, rows, args, mayPushdown);
  }

  void updateSingleGroupPartial(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::updateOneGroup(
        group,
        allRows,
        args[0],
        [](bool& result, bool value) { result = result || value; },
        [](bool& result, bool value, int /* unused */) {
          result = result || value;
        },
        mayPushdown,
        this->initialValue_);
  }

  void updateSingleGroupFinal(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updateSingleGroupPartial(group, allRows, args, mayPushdown);
  }
};

template <class T>
bool registerBoolAggregate(const std::string& name) {
  exec::AggregateFunctions().Register(
      name,
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr&
          /*resultType*/) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        VELOX_CHECK_EQ(
            inputType->kind(),
            TypeKind::BOOLEAN,
            "Unknown input type for {} aggregation {}",
            name,
            inputType->kindName());
        return std::make_unique<T>(step);
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerBoolAggregate<BoolAndAggregate>(kBoolAnd);
static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerBoolAggregate<BoolOrAggregate>(kBoolOr);

} // namespace
} // namespace facebook::velox::aggregate
