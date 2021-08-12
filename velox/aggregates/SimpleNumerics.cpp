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
#include "velox/aggregates/SimpleNumerics.h"
#include "velox/aggregates/AggregateNames.h"
#include "velox/aggregates/AggregationHook.h"
#include "velox/exec/Aggregate.h"

namespace facebook::velox::aggregate {

namespace {

template <typename T, typename ResultType>
class SumAggregate : public SimpleNumericAggregate<T, ResultType, ResultType> {
  using BaseAggregate = SimpleNumericAggregate<T, ResultType, ResultType>;

 public:
  explicit SumAggregate(core::AggregationNode::Step step, TypePtr resultType)
      : BaseAggregate(step, resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(ResultType);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<ResultType>(groups[i]) = 0;
    }
  }

  void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    auto& arg = args[0];

    if (mayPushdown) {
      BaseAggregate::template pushdown<SumHook<T, ResultType>>(
          groups, rows, arg);
      return;
    }
    if (exec::Aggregate::numNulls_) {
      BaseAggregate::template updateGroups<true>(
          groups,
          rows,
          arg,
          [](ResultType& result, T value) { result += value; },
          false);
    } else {
      BaseAggregate::template updateGroups<false>(
          groups,
          rows,
          arg,
          [](ResultType& result, T value) { result += value; },
          false);
    }
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
        [](ResultType& result, T value) { result += value; },
        [](ResultType& result, T value, int n) { result += n * value; },
        mayPushdown,
        0);
  }

  void updateSingleGroupFinal(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updateSingleGroupPartial(group, allRows, args, mayPushdown);
  }
};

class CountAggregate : public SimpleNumericAggregate<bool, int64_t, int64_t> {
 public:
  explicit CountAggregate(core::AggregationNode::Step step)
      : SimpleNumericAggregate<bool, int64_t, int64_t>(step, BIGINT()) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(int64_t);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto i : indices) {
      // result of count is never null
      *value<int64_t>(groups[i]) = (int64_t)0;
    }
  }

  void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    if (args.empty()) {
      rows.applyToSelected([&](vector_size_t i) { addToGroup(groups[i], 1); });
      return;
    }

    DecodedVector decoded(*args[0], rows);
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        rows.applyToSelected(
            [&](vector_size_t i) { addToGroup(groups[i], 1); });
      }
    } else if (decoded.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decoded.isNullAt(i)) {
          return;
        }
        addToGroup(groups[i], 1);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) { addToGroup(groups[i], 1); });
    }
  }

  void updateFinal(
      char** /*groups*/,
      const SelectivityVector& /*rows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {
    VELOX_UNREACHABLE();
  }

  void updateSingleGroupPartial(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    if (args.empty()) {
      addToGroup(group, allRows.size());
      return;
    }

    DecodedVector decoded(*args[0], allRows);
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        addToGroup(group, allRows.size());
      }
    } else if (decoded.mayHaveNulls()) {
      int64_t nonNullCount = 0;
      for (vector_size_t i = 0; i < allRows.end(); i++) {
        if (!decoded.isNullAt(i)) {
          ++nonNullCount;
        }
      }
      addToGroup(group, nonNullCount);
    } else {
      addToGroup(group, allRows.size());
    }
  }

  void updateSingleGroupFinal(
      char* /*group*/,
      const SelectivityVector& /*allRows*/,
      const std::vector<VectorPtr>& /*args*/,
      bool /*mayPushdown*/) override {
    VELOX_UNREACHABLE();
  }

 private:
  inline void addToGroup(char* group, int64_t count) {
    *value<int64_t>(group) += count;
  }
};

bool registerCountAggregate(const std::string& name) {
  exec::AggregateFunctions().Register(
      name,
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr&
          /*resultType*/) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(
            argTypes.size(), 1, "{} takes at most one argument", name);
        if (exec::isRawInput(step)) {
          return std::make_unique<CountAggregate>(step);
        } else {
          return std::make_unique<SumAggregate<int64_t, int64_t>>(
              step, BIGINT());
        }
      });
  return true;
}

template <template <typename U, typename V> class T>
bool registerSumAggregate(const std::string& name) {
  exec::AggregateFunctions().Register(
      name,
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr&
          /*resultType*/) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<T<int8_t, int64_t>>(step, BIGINT());
          case TypeKind::SMALLINT:
            return std::make_unique<T<int16_t, int64_t>>(step, BIGINT());
          case TypeKind::INTEGER:
            return std::make_unique<T<int32_t, int64_t>>(step, BIGINT());
          case TypeKind::BIGINT:
            return std::make_unique<T<int64_t, int64_t>>(step, BIGINT());
          case TypeKind::REAL:
            return std::make_unique<T<float, double>>(step, DOUBLE());
          case TypeKind::DOUBLE:
            return std::make_unique<T<double, double>>(step, DOUBLE());
          default:
            VELOX_CHECK(
                false,
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
            return nullptr;
        }
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerSumAggregate<SumAggregate>(kSum);
static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerCountAggregate(kCount);

} // namespace
} // namespace facebook::velox::aggregate
