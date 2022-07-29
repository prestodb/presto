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

/// TInput is type of data received by addRawInput() in partial aggregation and
/// addIntermediateResults() in final or intermediate aggregations.
///
/// TAccumulator is type of data returned by extractAccumulators() in partial or
/// intermediate aggregations and in case of spilling during final or single
/// aggregations.
///
/// TResult is type of data returned by extractValues() in final or single
/// aggregations.
///
/// For example, min(integer) uses the following classes:
///
/// Partial aggregation: MinMaxAggregate<int32_t, int64_t, int64_t>.
/// Final aggregation: MinMaxAggregate<int64_t, int64_t, int32_t>.
/// Single aggregation: MinMaxAggregate<int32_t, int64_t, int32_t>.
/// Intermediate aggregation: MinMaxAggregate<int64_t, int64_t, int64_t>.
template <typename TInput, typename TAccumulator, typename TResult>
class MinMaxAggregate
    : public SimpleNumericAggregate<TInput, TAccumulator, TResult> {
  using BaseAggregate = SimpleNumericAggregate<TInput, TAccumulator, TResult>;

 public:
  explicit MinMaxAggregate(TypePtr resultType) : BaseAggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(TInput);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::template doExtractValues<TResult>(
        groups, numGroups, result, [&](char* group) {
          return *BaseAggregate::Aggregate::template value<TInput>(group);
        });
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::template doExtractValues<TAccumulator>(
        groups, numGroups, result, [&](char* group) {
          return *BaseAggregate::Aggregate::template value<TInput>(group);
        });
  }
};

template <>
void MinMaxAggregate<int64_t, int64_t, Timestamp>::extractValues(
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
void MinMaxAggregate<Timestamp, int64_t, int64_t>::extractValues(
    char** groups,
    int32_t numGroups,
    VectorPtr* result) {
  BaseAggregate::template doExtractValues<int64_t>(
      groups, numGroups, result, [&](char* group) {
        auto ts = *BaseAggregate::Aggregate::template value<Timestamp>(group);
        return ts.toMillis();
      });
}

template <>
void MinMaxAggregate<Timestamp, int64_t, Timestamp>::extractValues(
    char** groups,
    int32_t numGroups,
    VectorPtr* result) {
  BaseAggregate::template doExtractValues<Timestamp>(
      groups, numGroups, result, [&](char* group) {
        auto ts = *BaseAggregate::Aggregate::template value<Timestamp>(group);
        return Timestamp::fromMillis(ts.toMillis());
      });
}

template <>
void MinMaxAggregate<Timestamp, int64_t, int64_t>::extractAccumulators(
    char** groups,
    int32_t numGroups,
    VectorPtr* result) {
  BaseAggregate::template doExtractValues<int64_t>(
      groups, numGroups, result, [&](char* group) {
        auto ts = *BaseAggregate::Aggregate::template value<Timestamp>(group);
        return ts.toMillis();
      });
}

template <>
void MinMaxAggregate<Timestamp, int64_t, Timestamp>::extractAccumulators(
    char** groups,
    int32_t numGroups,
    VectorPtr* result) {
  BaseAggregate::template doExtractValues<int64_t>(
      groups, numGroups, result, [&](char* group) {
        auto ts = *BaseAggregate::Aggregate::template value<Timestamp>(group);
        return ts.toMillis();
      });
}

template <typename TInput, typename TAccumulator, typename TResult>
class MaxAggregate : public MinMaxAggregate<TInput, TAccumulator, TResult> {
  using BaseAggregate = SimpleNumericAggregate<TInput, TAccumulator, TResult>;

 public:
  explicit MaxAggregate(TypePtr resultType)
      : MinMaxAggregate<TInput, TAccumulator, TResult>(resultType) {}

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<TInput>(groups[i]) = kInitialValue_;
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    if (mayPushdown && args[0]->isLazy() &&
        std::is_same<TInput, TResult>::value) {
      BaseAggregate::template pushdown<MinMaxHook<TInput, false>>(
          groups, rows, args[0]);
      return;
    }
    BaseAggregate::template updateGroups<true, TInput>(
        groups,
        rows,
        args[0],
        [](TInput& result, TInput value) {
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
        [](TInput& result, TInput value) {
          result = result > value ? result : value;
        },
        [](TInput& result, TInput value, int /* unused */) { result = value; },
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
  static constexpr TInput kInitialValue_{MinMaxTrait<TInput>::min()};
};

template <typename TInput, typename TAccumulator, typename TResult>
class MinAggregate : public MinMaxAggregate<TInput, TAccumulator, TResult> {
  using BaseAggregate = SimpleNumericAggregate<TInput, TAccumulator, TResult>;

 public:
  explicit MinAggregate(TypePtr resultType)
      : MinMaxAggregate<TInput, TAccumulator, TResult>(resultType) {}

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<TInput>(groups[i]) = kInitialValue_;
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    if (mayPushdown && args[0]->isLazy() &&
        std::is_same<TInput, TResult>::value) {
      BaseAggregate::template pushdown<MinMaxHook<TInput, true>>(
          groups, rows, args[0]);
      return;
    }
    BaseAggregate::template updateGroups<true, TInput>(
        groups,
        rows,
        args[0],
        [](TInput& result, TInput value) {
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
        [](TInput& result, TInput value) {
          result = result < value ? result : value;
        },
        [](TInput& result, TInput value, int /* unused */) { result = value; },
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
  static constexpr TInput kInitialValue_{MinMaxTrait<TInput>::max()};
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

template <
    typename TInput,
    template <typename T, typename U, typename V>
    class TNumeric>
std::unique_ptr<exec::Aggregate> createMinMaxIntegralAggregate(
    const std::string& name,
    const TypePtr& resultType) {
  switch (resultType->kind()) {
    case TypeKind::TINYINT:
      return std::make_unique<TNumeric<TInput, int64_t, int8_t>>(resultType);
    case TypeKind::SMALLINT:
      return std::make_unique<TNumeric<TInput, int64_t, int16_t>>(resultType);
    case TypeKind::INTEGER:
      return std::make_unique<TNumeric<TInput, int64_t, int32_t>>(resultType);
    case TypeKind::BIGINT:
      return std::make_unique<TNumeric<TInput, int64_t, int64_t>>(resultType);
    case TypeKind::REAL:
      return std::make_unique<TNumeric<TInput, float, float>>(resultType);
    case TypeKind::DOUBLE:
      return std::make_unique<TNumeric<TInput, double, double>>(resultType);
    default:
      VELOX_FAIL(
          "Unknown result type for {} aggregation with integral input type: {}",
          name,
          resultType->toString());
  }
}

template <
    typename TInput,
    template <typename T, typename U, typename V>
    class TNumeric>
std::unique_ptr<exec::Aggregate> createMinMaxTimestampAggregate(
    const std::string& name,
    const TypePtr& resultType) {
  switch (resultType->kind()) {
    case TypeKind::BIGINT:
      return std::make_unique<TNumeric<TInput, int64_t, int64_t>>(resultType);
    case TypeKind::TIMESTAMP:
      return std::make_unique<TNumeric<TInput, int64_t, Timestamp>>(resultType);
    default:
      VELOX_FAIL(
          "Unknown result type for {} aggregation with timestamp input type: {}",
          name,
          resultType->toString());
  }
}

template <template <typename T, typename U, typename V> class TNumeric>
std::unique_ptr<exec::Aggregate> createMinMaxDateAggregate(
    const std::string& name,
    const TypePtr& resultType) {
  switch (resultType->kind()) {
    case TypeKind::DATE:
      return std::make_unique<TNumeric<Date, Date, Date>>(resultType);
    default:
      VELOX_FAIL(
          "Unknown result type for {} aggregation with date input type: {}",
          name,
          resultType->toString());
  }
}

template <
    typename TInput,
    template <typename T, typename U, typename V>
    class TNumeric>
std::unique_ptr<exec::Aggregate> createMinMaxFloatingPointAggregate(
    const std::string& name,
    const TypePtr& resultType) {
  switch (resultType->kind()) {
    case TypeKind::REAL:
      return std::make_unique<TNumeric<TInput, float, float>>(resultType);
    case TypeKind::DOUBLE:
      return std::make_unique<TNumeric<TInput, double, double>>(resultType);
    case TypeKind::BIGINT:
      return std::make_unique<TNumeric<TInput, int64_t, int64_t>>(resultType);
    default:
      VELOX_FAIL(
          "Unknown result type for {} aggregation with floating point input type: {}",
          name,
          resultType->toString());
  }
}

template <
    template <typename T, typename U, typename V>
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
              return std::make_unique<TNumeric<int64_t, int64_t, Timestamp>>(
                  resultType);
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
