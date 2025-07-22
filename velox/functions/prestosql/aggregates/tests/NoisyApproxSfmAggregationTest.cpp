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

#include <gtest/gtest.h>
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/prestosql/aggregates/sfm/SfmSketch.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::aggregate::test {

using SfmSketch = functions::aggregate::SfmSketch;
using namespace facebook::velox::exec::test;

class NoisyApproxSfmAggregationTest
    : public functions::aggregate::test::AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }

  // Template helper to count distinct values for a specific type.
  template <typename T>
  size_t countDistinctForType(const VectorPtr& vector) {
    const auto size = vector->size();
    const auto* nulls = vector->rawNulls();
    std::unordered_set<T> uniqueValues;
    auto* flatVector = vector->asFlatVector<T>();

    for (vector_size_t i = 0; i < size; ++i) {
      if (!nulls || !bits::isBitNull(nulls, i)) {
        uniqueValues.insert(flatVector->valueAt(i));
      }
    }
    return uniqueValues.size();
  }

  // Dispatch function to count distinct non-null values in a vector.
  size_t countDistinct(const VectorPtr& vector) {
    if (!vector) {
      return 0;
    }

    switch (vector->typeKind()) {
      case TypeKind::BIGINT:
        return countDistinctForType<int64_t>(vector);

      case TypeKind::DOUBLE:
        return countDistinctForType<double>(vector);

      case TypeKind::VARCHAR:
      case TypeKind::VARBINARY:
        return countDistinctForType<StringView>(vector);

      default:
        VELOX_FAIL("Unsupported type.");
    }
  }

  // Helper method to test noisy_approx_sfm with a specific type.
  void testFuzzerWithType(const TypePtr& type) {
    const uint32_t seed = 1234;
    const vector_size_t numElements = 100'000;
    const double epsilon = 8.0;

    VectorFuzzer::Options options;
    options.vectorSize = numElements;
    options.nullRatio = 0.1;
    options.stringVariableLength = true;
    options.stringLength = 10;

    VectorFuzzer fuzzer(options, pool_.get(), seed);

    const auto inputVector = fuzzer.fuzzFlat(type);
    const auto vectors = makeRowVector(
        {inputVector, makeConstant<double>(epsilon, numElements)});

    const size_t actualCardinality = countDistinct(inputVector);

    const auto distinctResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values({vectors})
                .singleAggregation(
                    {}, {"noisy_approx_distinct_sfm(c0, c1)"}, {})
                .planNode())
            .copyResults(pool());

    const auto calculatedCardinality =
        distinctResult->childAt(0)->asFlatVector<int64_t>()->valueAt(0);

    ASSERT_NEAR(
        calculatedCardinality, actualCardinality, actualCardinality * 0.25);

    const auto setResult =
        AssertQueryBuilder(
            PlanBuilder()
                .values({vectors})
                .singleAggregation({}, {"noisy_approx_set_sfm(c0, c1)"}, {})
                .planNode())
            .copyResults(pool());

    const auto serializedView =
        setResult->childAt(0)->asFlatVector<StringView>()->valueAt(0);
    const auto deserialized =
        SfmSketch::deserialize(serializedView.data(), &allocator_);

    ASSERT_NEAR(
        deserialized.cardinality(),
        actualCardinality,
        actualCardinality * 0.25);
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  HashStringAllocator allocator_{pool_.get()};
};

TEST_F(NoisyApproxSfmAggregationTest, distinctNonPrivacy) {
  const auto vectors = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
       makeConstant(std::numeric_limits<double>::infinity(), 10)});

  const auto expectedResult = makeRowVector({makeConstant<int64_t>(10, 1)});
  testAggregations(
      {vectors}, {}, {"noisy_approx_distinct_sfm(c0, c1)"}, {expectedResult});
}

TEST_F(NoisyApproxSfmAggregationTest, setNonPrivacy) {
  const auto vectors = makeRowVector(
      {makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
       makeConstant(std::numeric_limits<double>::infinity(), 10)});

  const auto returnedSketch =
      AssertQueryBuilder(
          PlanBuilder()
              .values({vectors})
              .singleAggregation({}, {"noisy_approx_set_sfm(c0, c1)"}, {})
              .planNode())
          .copyResults(pool());

  const auto serializedView =
      returnedSketch->childAt(0)->asFlatVector<StringView>()->valueAt(0);
  const auto deserialized =
      SfmSketch::deserialize(serializedView.data(), &allocator_);
  ASSERT_EQ(deserialized.cardinality(), 10);
}

TEST_F(NoisyApproxSfmAggregationTest, distinctPrivacy) {
  const vector_size_t numElements = 100'000;
  const auto vectors = makeRowVector(
      {makeFlatVector<int64_t>(
           numElements, [](vector_size_t row) { return row + 1; }),
       makeConstant(8.0, numElements)});

  const auto result =
      AssertQueryBuilder(
          PlanBuilder()
              .values({vectors})
              .singleAggregation({}, {"noisy_approx_distinct_sfm(c0, c1)"}, {})
              .planNode())
          .copyResults(pool());

  ASSERT_NEAR(
      result->childAt(0)->asFlatVector<int64_t>()->valueAt(0),
      numElements,
      numElements * 0.25); // 25% tolerance for 100k elements, 8.0 epsilon.
}

TEST_F(NoisyApproxSfmAggregationTest, setPrivacy) {
  const vector_size_t numElements = 100'000;
  const auto vectors = makeRowVector(
      {makeFlatVector<int64_t>(
           numElements, [](vector_size_t row) { return row + 1; }),
       makeConstant(8.0, numElements)});

  const auto result =
      AssertQueryBuilder(
          PlanBuilder()
              .values({vectors})
              .singleAggregation({}, {"noisy_approx_set_sfm(c0, c1)"}, {})
              .planNode())
          .copyResults(pool());

  const auto serializedView =
      result->childAt(0)->asFlatVector<StringView>()->valueAt(0);
  const auto deserialized =
      SfmSketch::deserialize(serializedView.data(), &allocator_);
  ASSERT_NEAR(deserialized.cardinality(), numElements, numElements * 0.25);
}

TEST_F(NoisyApproxSfmAggregationTest, setBuckets) {
  const vector_size_t numElements = 100'000;
  const auto vectors = makeRowVector(
      {makeFlatVector<int64_t>(
           numElements, [](vector_size_t row) { return row + 1; }),
       makeConstant<double>(8.0, numElements),
       makeConstant<int64_t>(8192, numElements)}); // specify buckets.

  const auto result =
      AssertQueryBuilder(
          PlanBuilder()
              .values({vectors})
              .singleAggregation({}, {"noisy_approx_set_sfm(c0, c1, c2)"}, {})
              .planNode())
          .copyResults(pool());

  const auto serializedView =
      result->childAt(0)->asFlatVector<StringView>()->valueAt(0);
  const auto deserialized =
      SfmSketch::deserialize(serializedView.data(), &allocator_);
  ASSERT_NEAR(deserialized.cardinality(), numElements, numElements * 0.25);
}

TEST_F(NoisyApproxSfmAggregationTest, distinctBuckets) {
  const vector_size_t numElements = 100'000;
  const auto vectors = makeRowVector(
      {makeFlatVector<int64_t>(
           numElements, [](vector_size_t row) { return row + 1; }),
       makeConstant<double>(8.0, numElements),
       makeConstant<int64_t>(8192, numElements)});

  const auto result =
      AssertQueryBuilder(
          PlanBuilder()
              .values({vectors})
              .singleAggregation(
                  {}, {"noisy_approx_distinct_sfm(c0, c1, c2)"}, {})
              .planNode())
          .copyResults(pool());

  ASSERT_NEAR(
      result->childAt(0)->asFlatVector<int64_t>()->valueAt(0),
      numElements,
      numElements * 0.25);
}

TEST_F(NoisyApproxSfmAggregationTest, setBucketsAndPrecison) {
  const vector_size_t numElements = 100'000;
  const auto vectors = makeRowVector(
      {makeFlatVector<int64_t>(
           numElements, [](vector_size_t row) { return row + 1; }),
       makeConstant<double>(8.0, numElements),
       makeConstant<int64_t>(8192, numElements), // specify buckets.
       makeConstant<int64_t>(30, numElements)}); // specify precision.

  const auto result =
      AssertQueryBuilder(
          PlanBuilder()
              .values({vectors})
              .singleAggregation(
                  {}, {"noisy_approx_set_sfm(c0, c1, c2, c3)"}, {})
              .planNode())
          .copyResults(pool());

  const auto serializedView =
      result->childAt(0)->asFlatVector<StringView>()->valueAt(0);
  const auto deserialized =
      SfmSketch::deserialize(serializedView.data(), &allocator_);
  ASSERT_NEAR(deserialized.cardinality(), numElements, numElements * 0.25);
}

TEST_F(NoisyApproxSfmAggregationTest, distinctBucketsAndPrecison) {
  const vector_size_t numElements = 100'000;
  const auto vectors = makeRowVector(
      {makeFlatVector<int64_t>(
           numElements, [](vector_size_t row) { return row + 1; }),
       makeConstant<double>(8.0, numElements),
       makeConstant<int64_t>(8192, numElements),
       makeConstant<int64_t>(30, numElements)});

  const auto result =
      AssertQueryBuilder(
          PlanBuilder()
              .values({vectors})
              .singleAggregation(
                  {}, {"noisy_approx_distinct_sfm(c0, c1, c2, c3)"}, {})
              .planNode())
          .copyResults(pool());

  ASSERT_NEAR(
      result->childAt(0)->asFlatVector<int64_t>()->valueAt(0),
      numElements,
      numElements * 0.25);
}

TEST_F(NoisyApproxSfmAggregationTest, emptyInput) {
  const auto vectors =
      makeRowVector({makeFlatVector<int64_t>({}), makeConstant(3.0, 0)});

  const auto expectedResult =
      makeRowVector({makeNullConstant(TypeKind::BIGINT, 1)});
  testAggregations(
      {vectors}, {}, {"noisy_approx_distinct_sfm(c0, c1)"}, {expectedResult});
}

TEST_F(NoisyApproxSfmAggregationTest, fuzzerInput) {
  // Test with different data types.
  testFuzzerWithType(VARCHAR());
  testFuzzerWithType(DOUBLE());
  testFuzzerWithType(BIGINT());
  testFuzzerWithType(VARBINARY());
}

} // namespace facebook::velox::aggregate::test
