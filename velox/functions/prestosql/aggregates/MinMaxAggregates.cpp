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

#include <limits>
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregationHook.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/SimpleNumericAggregate.h"
#include "velox/functions/prestosql/aggregates/SingleValueAccumulator.h"

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
class MinMaxAggregate : public SimpleNumericAggregate<T, T, ResultType> {
  using BaseAggregate = SimpleNumericAggregate<T, T, ResultType>;

 public:
  explicit MinMaxAggregate(TypePtr resultType) : BaseAggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(T);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::doExtractValues(groups, numGroups, result, [&](char* group) {
      return *BaseAggregate::Aggregate::template value<T>(group);
    });
  }
};

template <>
void MinMaxAggregate<int64_t, Timestamp>::extractValues(
    char** groups,
    int32_t numGroups,
    VectorPtr* result) {
  BaseAggregate::template doExtractValues<Timestamp>(
      groups, numGroups, result, [&](char* group) {
        auto millis = *BaseAggregate::Aggregate::template value<int64_t>(group);
        return Timestamp::fromMillis(millis);
      });
}

template <>
void MinMaxAggregate<Timestamp, int64_t>::extractValues(
    char** groups,
    int32_t numGroups,
    VectorPtr* result) {
  BaseAggregate::template doExtractValues<int64_t>(
      groups, numGroups, result, [&](char* group) {
        auto ts = *BaseAggregate::Aggregate::template value<Timestamp>(group);
        return ts.toMillis();
      });
}

template <typename T, typename ResultType>
class MaxAggregate : public MinMaxAggregate<T, ResultType> {
  using BaseAggregate = SimpleNumericAggregate<T, T, ResultType>;

 public:
  explicit MaxAggregate(TypePtr resultType)
      : MinMaxAggregate<T, ResultType>(resultType) {}

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<T>(groups[i]) = kInitialValue_;
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    if (mayPushdown && args[0]->isLazy() &&
        std::is_same<T, ResultType>::value) {
      BaseAggregate::template pushdown<MinMaxHook<T, false>>(
          groups, rows, args[0]);
      return;
    }
    BaseAggregate::template updateGroups<true, T>(
        groups,
        rows,
        args[0],
        [](T& result, T value) {
          if (result < value) {
            result = value;
          }
        },
        mayPushdown);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::updateOneGroup(
        group,
        rows,
        args[0],
        [](T& result, T value) { result = result > value ? result : value; },
        [](T& result, T value, int /* unused */) { result = value; },
        mayPushdown,
        kInitialValue_);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addSingleGroupRawInput(group, rows, args, mayPushdown);
  }

 private:
  static constexpr T kInitialValue_{MinMaxTrait<T>::min()};
};

template <typename T, typename ResultType>
class MinAggregate : public MinMaxAggregate<T, ResultType> {
  using BaseAggregate = SimpleNumericAggregate<T, T, ResultType>;

 public:
  explicit MinAggregate(TypePtr resultType)
      : MinMaxAggregate<T, ResultType>(resultType) {}

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<T>(groups[i]) = kInitialValue_;
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    if (mayPushdown && args[0]->isLazy() &&
        std::is_same<T, ResultType>::value) {
      BaseAggregate::template pushdown<MinMaxHook<T, true>>(
          groups, rows, args[0]);
      return;
    }
    BaseAggregate::template updateGroups<true, T>(
        groups,
        rows,
        args[0],
        [](T& result, T value) {
          if (result > value) {
            result = value;
          }
        },
        mayPushdown);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    BaseAggregate::updateOneGroup(
        group,
        rows,
        args[0],
        [](T& result, T value) { result = result < value ? result : value; },
        [](T& result, T value, int /* unused */) { result = value; },
        mayPushdown,
        kInitialValue_);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addSingleGroupRawInput(group, rows, args, mayPushdown);
  }

 private:
  static constexpr T kInitialValue_{MinMaxTrait<T>::max()};
};

class NonNumericMinMaxAggregateBase : public exec::Aggregate {
 public:
  explicit NonNumericMinMaxAggregateBase(const TypePtr& resultType)
      : exec::Aggregate(resultType) {}

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
      const SelectivityVector& rows,
      const VectorPtr& arg,
      TCompareTest compareTest) {
    DecodedVector decoded(*arg, rows, true);
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
    rows.applyToSelected([&](vector_size_t i) {
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
  explicit NonNumericMaxAggregate(const TypePtr& resultType)
      : NonNumericMinMaxAggregateBase(resultType) {}

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    doUpdate(groups, rows, args[0], [](int32_t compareResult) {
      return compareResult < 0;
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    doUpdateSingleGroup(group, rows, args[0], [](int32_t compareResult) {
      return compareResult < 0;
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addSingleGroupRawInput(group, rows, args, mayPushdown);
  }
};

class NonNumericMinAggregate : public NonNumericMinMaxAggregateBase {
 public:
  explicit NonNumericMinAggregate(const TypePtr& resultType)
      : NonNumericMinMaxAggregateBase(resultType) {}

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    doUpdate(groups, rows, args[0], [](int32_t compareResult) {
      return compareResult > 0;
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    doUpdateSingleGroup(group, rows, args[0], [](int32_t compareResult) {
      return compareResult > 0;
    });
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addSingleGroupRawInput(group, rows, args, mayPushdown);
  }
};

template <typename TInput, template <typename U, typename V> class TNumeric>
std::unique_ptr<exec::Aggregate> createMinMaxIntegralAggregate(
    const std::string& name,
    const TypePtr& resultType) {
  switch (resultType->kind()) {
    case TypeKind::TINYINT:
      return std::make_unique<TNumeric<TInput, int8_t>>(resultType);
    case TypeKind::SMALLINT:
      return std::make_unique<TNumeric<TInput, int16_t>>(resultType);
    case TypeKind::INTEGER:
      return std::make_unique<TNumeric<TInput, int32_t>>(resultType);
    case TypeKind::BIGINT:
      return std::make_unique<TNumeric<TInput, int64_t>>(resultType);
    case TypeKind::REAL:
      return std::make_unique<TNumeric<TInput, float>>(resultType);
    case TypeKind::DOUBLE:
      return std::make_unique<TNumeric<TInput, double>>(resultType);
    default:
      VELOX_FAIL(
          "Unknown result type for {} aggregation with integral input type: {}",
          name,
          resultType->toString());
  }
}

template <typename TInput, template <typename U, typename V> class TNumeric>
std::unique_ptr<exec::Aggregate> createMinMaxTimestampAggregate(
    const std::string& name,
    const TypePtr& resultType) {
  switch (resultType->kind()) {
    case TypeKind::BIGINT:
      return std::make_unique<TNumeric<TInput, int64_t>>(resultType);
    case TypeKind::TIMESTAMP:
      return std::make_unique<TNumeric<TInput, Timestamp>>(resultType);
    default:
      VELOX_FAIL(
          "Unknown result type for {} aggregation with timestamp input type: {}",
          name,
          resultType->toString());
  }
}

template <template <typename U, typename V> class TNumeric>
std::unique_ptr<exec::Aggregate> createMinMaxDateAggregate(
    const std::string& name,
    const TypePtr& resultType) {
  switch (resultType->kind()) {
    case TypeKind::DATE:
      return std::make_unique<TNumeric<Date, Date>>(resultType);
    default:
      VELOX_FAIL(
          "Unknown result type for {} aggregation with date input type: {}",
          name,
          resultType->toString());
  }
}

template <typename TInput, template <typename U, typename V> class TNumeric>
std::unique_ptr<exec::Aggregate> createMinMaxFloatingPointAggregate(
    const std::string& name,
    const TypePtr& resultType) {
  switch (resultType->kind()) {
    case TypeKind::REAL:
      return std::make_unique<TNumeric<TInput, float>>(resultType);
    case TypeKind::DOUBLE:
      return std::make_unique<TNumeric<TInput, double>>(resultType);
    case TypeKind::BIGINT:
      return std::make_unique<TNumeric<TInput, int64_t>>(resultType);
    default:
      VELOX_FAIL(
          "Unknown result type for {} aggregation with floating point input type: {}",
          name,
          resultType->toString());
  }
}

template <
    template <typename U, typename V>
    class TNumeric,
    typename TNonNumeric>
bool registerMinMaxAggregate(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;

  for (const auto& inputType :
       {"tinyint", "smallint", "integer", "bigint", "timestamp"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(inputType)
                             .intermediateType("bigint")
                             .argumentType(inputType)
                             .build());
  }

  signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                           .typeVariable("T")
                           .returnType("T")
                           .intermediateType("T")
                           .argumentType("T")
                           .build());

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          std::vector<TypePtr> argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_EQ(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return createMinMaxIntegralAggregate<int8_t, TNumeric>(
                name, resultType);
          case TypeKind::SMALLINT:
            return createMinMaxIntegralAggregate<int16_t, TNumeric>(
                name, resultType);
          case TypeKind::INTEGER:
            return createMinMaxIntegralAggregate<int32_t, TNumeric>(
                name, resultType);
          case TypeKind::BIGINT:
            if (resultType->isTimestamp()) {
              return std::make_unique<TNumeric<int64_t, Timestamp>>(resultType);
            }
            return createMinMaxIntegralAggregate<int64_t, TNumeric>(
                name, resultType);
          case TypeKind::REAL:
            return createMinMaxFloatingPointAggregate<float, TNumeric>(
                name, resultType);
          case TypeKind::DOUBLE:
            return createMinMaxFloatingPointAggregate<double, TNumeric>(
                name, resultType);
          case TypeKind::TIMESTAMP:
            return createMinMaxTimestampAggregate<Timestamp, TNumeric>(
                name, resultType);
          case TypeKind::DATE:
            return createMinMaxDateAggregate<TNumeric>(name, resultType);
          case TypeKind::VARCHAR:
          case TypeKind::ARRAY:
          case TypeKind::MAP:
          case TypeKind::ROW:
            return std::make_unique<TNonNumeric>(inputType);
          default:
            VELOX_CHECK(
                false,
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
        }
      });
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerMinMaxAggregate<MinAggregate, NonNumericMinAggregate>(kMin);
static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerMinMaxAggregate<MaxAggregate, NonNumericMaxAggregate>(kMax);

} // namespace
} // namespace facebook::velox::aggregate
