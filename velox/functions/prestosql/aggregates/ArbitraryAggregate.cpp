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
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/SimpleNumericAggregate.h"
#include "velox/functions/prestosql/aggregates/SingleValueAccumulator.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

// Arbitrary aggregate returns any arbitrary non-NULL value.
// We always keep the first (non-NULL) element seen.
template <typename T>
class ArbitraryAggregate : public SimpleNumericAggregate<T, T, T> {
  using BaseAggregate = SimpleNumericAggregate<T, T, T>;

 public:
  explicit ArbitraryAggregate(TypePtr resultType) : BaseAggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(T);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::doExtractValues(groups, numGroups, result, [&](char* group) {
      return *BaseAggregate::Aggregate::template value<T>(group);
    });
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    DecodedVector decoded(*args[0], rows);

    if (decoded.isConstantMapping()) {
      if (decoded.isNullAt(0)) {
        return;
      }
      auto value = decoded.valueAt<T>(0);
      rows.applyToSelected([&](vector_size_t i) {
        if (exec::Aggregate::isNull(groups[i])) {
          updateValue(groups[i], value);
        }
      });
    } else if (decoded.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (!decoded.isNullAt(i) && exec::Aggregate::isNull(groups[i])) {
          updateValue(groups[i], decoded.valueAt<T>(i));
        }
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        if (exec::Aggregate::isNull(groups[i])) {
          updateValue(groups[i], decoded.valueAt<T>(i));
        }
      });
    }
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
      bool /*unused*/) override {
    DecodedVector decoded(*args[0], rows);

    if (decoded.isConstantMapping()) {
      if (decoded.isNullAt(0)) {
        return;
      }
      updateValue(group, decoded.valueAt<T>(0));
    } else if (!decoded.mayHaveNulls()) {
      updateValue(group, decoded.valueAt<T>(0));
    } else {
      // Find the first non-null value.
      rows.testSelected([&](vector_size_t i) {
        if (!decoded.isNullAt(i)) {
          updateValue(group, decoded.valueAt<T>(i));
          return false; // Stop
        }
        return true; // Continue
      });
    }
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    addSingleGroupRawInput(group, rows, args, mayPushdown);
  }

 private:
  inline void updateValue(char* group, T value) {
    exec::Aggregate::clearNull(group);
    *exec::Aggregate::value<T>(group) = value;
  }
};

// Arbitrary for non-numeric types. We always keep the first (non-NULL) element
// seen. Arbitrary (x) will produce partial and final aggregations of type x.
class NonNumericArbitrary : public exec::Aggregate {
 public:
  explicit NonNumericArbitrary(const TypePtr& resultType)
      : exec::Aggregate(resultType) {}

  // We use singleValueAccumulator to save the results for each group. This
  // struct will allow us to save variable-width value.
  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(SingleValueAccumulator);
  }

  // Initialize each group, we will not use the null flags because
  // SingleValueAccumulator has its own flag.
  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    for (auto i : indices) {
      new (groups[i] + offset_) SingleValueAccumulator();
    }
  }

  void finalize(char** /* groups */, int32_t /* numGroups */) override {}

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VELOX_CHECK(result);
    (*result)->resize(numGroups);

    auto* rawNulls = exec::Aggregate::getRawNulls(result->get());

    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      auto accumulator = value<SingleValueAccumulator>(group);
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
      value<SingleValueAccumulator>(group)->destroy(allocator_);
    }
  }

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*unused*/) override {
    DecodedVector decoded(*args[0], rows, true);
    if (decoded.isConstantMapping() && decoded.isNullAt(0)) {
      // nothing to do; all values are nulls
      return;
    }

    const auto* indices = decoded.indices();
    const auto* baseVector = decoded.base();
    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return;
      }
      auto* accumulator = value<SingleValueAccumulator>(groups[i]);
      if (!accumulator->hasValue()) {
        accumulator->write(baseVector, indices[i], allocator_);
      }
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
      bool /*unused*/) override {
    DecodedVector decoded(*args[0], rows, true);
    if (decoded.isConstantMapping() && decoded.isNullAt(0)) {
      // nothing to do; all values are nulls
      return;
    }

    const auto* indices = decoded.indices();
    const auto* baseVector = decoded.base();
    auto* accumulator = value<SingleValueAccumulator>(group);
    // Find the first non-null value.
    rows.testSelected([&](vector_size_t i) {
      if (!decoded.isNullAt(i)) {
        accumulator->write(baseVector, indices[i], allocator_);
        return false; // Stop
      }
      return true; // Continue
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

bool registerArbitraryAggregate(const std::string& name) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures{
      exec::AggregateFunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .intermediateType("T")
          .argumentType("T")
          .build()};

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr&
          /*resultType*/) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<ArbitraryAggregate<int8_t>>(inputType);
          case TypeKind::SMALLINT:
            return std::make_unique<ArbitraryAggregate<int16_t>>(inputType);
          case TypeKind::INTEGER:
            return std::make_unique<ArbitraryAggregate<int32_t>>(inputType);
          case TypeKind::BIGINT:
            return std::make_unique<ArbitraryAggregate<int64_t>>(inputType);
          case TypeKind::REAL:
            return std::make_unique<ArbitraryAggregate<float>>(inputType);
          case TypeKind::DOUBLE:
            return std::make_unique<ArbitraryAggregate<double>>(inputType);
          case TypeKind::VARCHAR:
          case TypeKind::ARRAY:
          case TypeKind::MAP:
          case TypeKind::ROW:
            return std::make_unique<NonNumericArbitrary>(inputType);
          default:
            VELOX_FAIL(
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
        }
      });
}

} // namespace

void registerArbitraryAggregate() {
  registerArbitraryAggregate(kArbitrary);
}

} // namespace facebook::velox::aggregate::prestosql
