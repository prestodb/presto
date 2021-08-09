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

#include <limits>
#include "velox/aggregates/AggregateNames.h"
#include "velox/aggregates/AggregationHook.h"
#include "velox/aggregates/SimpleNumerics.h"
#include "velox/aggregates/SingleValueAccumulator.h"
#include "velox/exec/Aggregate.h"

namespace facebook::velox::aggregate {

namespace {

template <typename T>
struct MinMaxTrait : public std::numeric_limits<T> {};

template <>
struct MinMaxTrait<Timestamp> {
  static constexpr Timestamp min() {
    return Timestamp(MinMaxTrait<int64_t>::min(), MinMaxTrait<uint64_t>::min());
  }

  static constexpr Timestamp max() {
    return Timestamp(MinMaxTrait<int64_t>::max(), MinMaxTrait<uint64_t>::max());
  }
};

template <typename T, typename ResultType>
class MaxAggregate : public SimpleNumericAggregate<T, ResultType> {
 public:
  explicit MaxAggregate(core::AggregationNode::Step step, TypePtr resultType)
      : SimpleNumericAggregate<T, ResultType>(step, resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(ResultType);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<ResultType>(groups[i]) = kInitialValue_;
    }
  }

  void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    if (mayPushdown && std::is_same<T, ResultType>::value) {
      SimpleNumericAggregate<T, ResultType>::template pushdown<
          MinMaxHook<T, false>>(groups, rows, args[0]);
      return;
    }
    SimpleNumericAggregate<T, ResultType>::template updateGroups<true>(
        groups,
        rows,
        args[0],
        [](ResultType& result, T value) {
          if (result < value) {
            result = value;
          }
        },
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
    SimpleNumericAggregate<T, ResultType>::updateOneGroup(
        group,
        allRows,
        args[0],
        [](ResultType& result, T value) {
          result = result > value ? result : value;
        },
        [](ResultType& result, T value, int /* unused */) { result = value; },
        mayPushdown,
        kInitialValue_);
  }

  void updateSingleGroupFinal(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updateSingleGroupPartial(group, allRows, args, mayPushdown);
  }

 private:
  static constexpr ResultType kInitialValue_{MinMaxTrait<ResultType>::min()};
};

template <typename T, typename ResultType>
class MinAggregate : public SimpleNumericAggregate<T, ResultType> {
 public:
  explicit MinAggregate(core::AggregationNode::Step step, TypePtr resultType)
      : SimpleNumericAggregate<T, ResultType>(step, resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(ResultType);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<ResultType>(groups[i]) = kInitialValue_;
    }
  }

  void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    if (mayPushdown && std::is_same<T, ResultType>::value) {
      SimpleNumericAggregate<T, ResultType>::template pushdown<
          MinMaxHook<T, true>>(groups, rows, args[0]);
      return;
    }
    SimpleNumericAggregate<T, ResultType>::template updateGroups<true>(
        groups,
        rows,
        args[0],
        [](ResultType& result, T value) {
          if (result > value) {
            result = value;
          }
        },
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
    SimpleNumericAggregate<T, ResultType>::updateOneGroup(
        group,
        allRows,
        args[0],
        [](ResultType& result, T value) {
          result = result < value ? result : value;
        },
        [](ResultType& result, T value, int /* unused */) { result = value; },
        mayPushdown,
        kInitialValue_);
  }

  void updateSingleGroupFinal(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updateSingleGroupPartial(group, allRows, args, mayPushdown);
  }

 private:
  static constexpr ResultType kInitialValue_{MinMaxTrait<ResultType>::max()};
};

class NonNumericMinMaxAggregateBase : public exec::Aggregate {
 public:
  explicit NonNumericMinMaxAggregateBase(
      core::AggregationNode::Step step,
      const TypePtr& resultType)
      : exec::Aggregate(step, resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(SingleValueAccumulator);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) SingleValueAccumulator();
    }
  }

  void finalize(char** /* groups */, int32_t /* numGroups */) override {
    // Nothing to do
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    (*result)->resize(numGroups);

    uint64_t* rawNulls = nullptr;
    if ((*result)->mayHaveNulls()) {
      BufferPtr nulls = (*result)->mutableNulls((*result)->size());
      rawNulls = nulls->asMutable<uint64_t>();
    }

    for (auto i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto accumulator = value<SingleValueAccumulator>(group);
      if (!accumulator->hasValue()) {
        (*result)->setNull(i, true);
      } else {
        if (rawNulls) {
          bits::clearBit(rawNulls, i);
        }
        accumulator->read(*result, i);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    // partial and final aggregations are the same
    extractValues(groups, numGroups, result);
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<SingleValueAccumulator>(group)->destroy(allocator_);
    }
  }

 protected:
  template <typename TCompareTest>
  void doUpdate(
      char** groups,
      const SelectivityVector& rows,
      const VectorPtr& arg,
      TCompareTest compareTest) {
    DecodedVector decoded(*arg, rows, true);
    auto indices = decoded.indices();
    auto baseVector = decoded.base();

    if (decoded.isConstantMapping() && decoded.isNullAt(0)) {
      // nothing to do; all values are nulls
      return;
    }

    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return;
      }
      auto accumulator = value<SingleValueAccumulator>(groups[i]);
      if (!accumulator->hasValue() ||
          compareTest(accumulator->compare(decoded, i))) {
        accumulator->write(baseVector, indices[i], allocator_);
      }
    });
  }

  template <typename TCompareTest>
  void doUpdateSingleGroup(
      char* group,
      const SelectivityVector& allRows,
      const VectorPtr& arg,
      TCompareTest compareTest) {
    DecodedVector decoded(*arg, allRows, true);
    auto indices = decoded.indices();
    auto baseVector = decoded.base();

    if (decoded.isConstantMapping()) {
      if (decoded.isNullAt(0)) {
        // nothing to do; all values are nulls
        return;
      }

      auto accumulator = value<SingleValueAccumulator>(group);
      if (!accumulator->hasValue() ||
          compareTest(accumulator->compare(decoded, 0))) {
        accumulator->write(baseVector, indices[0], allocator_);
      }
      return;
    }

    auto accumulator = value<SingleValueAccumulator>(group);
    allRows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return;
      }
      if (!accumulator->hasValue() ||
          compareTest(accumulator->compare(decoded, i))) {
        accumulator->write(baseVector, indices[i], allocator_);
      }
    });
  }
};

class NonNumericMaxAggregate : public NonNumericMinMaxAggregateBase {
 public:
  NonNumericMaxAggregate(
      core::AggregationNode::Step step,
      const TypePtr& resultType)
      : NonNumericMinMaxAggregateBase(step, resultType) {}

  void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    doUpdate(groups, rows, args[0], [](int32_t compareResult) {
      return compareResult < 0;
    });
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
      bool /*mayPushdown*/) override {
    doUpdateSingleGroup(group, allRows, args[0], [](int32_t compareResult) {
      return compareResult < 0;
    });
  }

  void updateSingleGroupFinal(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updateSingleGroupPartial(group, allRows, args, mayPushdown);
  }
};

class NonNumericMinAggregate : public NonNumericMinMaxAggregateBase {
 public:
  NonNumericMinAggregate(
      core::AggregationNode::Step step,
      const TypePtr& resultType)
      : NonNumericMinMaxAggregateBase(step, resultType) {}

  void updatePartial(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    doUpdate(groups, rows, args[0], [](int32_t compareResult) {
      return compareResult > 0;
    });
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
      bool /*mayPushdown*/) override {
    doUpdateSingleGroup(group, allRows, args[0], [](int32_t compareResult) {
      return compareResult > 0;
    });
  }

  void updateSingleGroupFinal(
      char* group,
      const SelectivityVector& allRows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    updateSingleGroupPartial(group, allRows, args, mayPushdown);
  }
};

template <
    template <typename U, typename V>
    class TNumeric,
    typename TNonNumeric>
bool registerMinMaxAggregate(const std::string& name) {
  exec::AggregateFunctions().Register(
      name,
      [name](
          core::AggregationNode::Step step,
          std::vector<TypePtr> argTypes,
          const TypePtr&
          /*resultType*/) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<TNumeric<int8_t, int64_t>>(step, BIGINT());
          case TypeKind::SMALLINT:
            return std::make_unique<TNumeric<int16_t, int64_t>>(step, BIGINT());
          case TypeKind::INTEGER:
            return std::make_unique<TNumeric<int32_t, int64_t>>(step, BIGINT());
          case TypeKind::BIGINT:
            return std::make_unique<TNumeric<int64_t, int64_t>>(step, BIGINT());
          case TypeKind::REAL:
            return std::make_unique<TNumeric<float, double>>(step, DOUBLE());
          case TypeKind::DOUBLE:
            return std::make_unique<TNumeric<double, double>>(step, DOUBLE());
          case TypeKind::TIMESTAMP:
            return std::make_unique<TNumeric<Timestamp, Timestamp>>(
                step, TIMESTAMP());
          case TypeKind::VARCHAR:
          case TypeKind::ARRAY:
          case TypeKind::MAP:
          case TypeKind::ROW:
            return std::make_unique<TNonNumeric>(step, inputType);
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
    registerMinMaxAggregate<MinAggregate, NonNumericMinAggregate>(kMin);
static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerMinMaxAggregate<MaxAggregate, NonNumericMaxAggregate>(kMax);

} // namespace
} // namespace facebook::velox::aggregate
