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

#include <algorithm>
#include <random>

#include "DataSketches/kll_sketch.hpp"

#include "presto_cpp/main/functions/kll_sketch/KllSketchRegistration.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::presto::functions::aggregate::test {
namespace {

class KllSketchAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    folly::SingletonVault::singleton()->registrationComplete();
    AggregationTestBase::SetUp();
    presto::functions::aggregate::kll_sketch::registerAllKllSketchFunctions("");
  }

  template <typename T>
  RowVectorPtr buildInput(const std::vector<T>& values) {
    std::vector<std::optional<T>> nullable;
    for (const auto& v : values) {
      nullable.push_back(v);
    }
    return makeRowVector({makeNullableFlatVector(nullable)});
  }

  RowVectorPtr buildSketch(RowVectorPtr input) {
    auto plan = PlanBuilder()
                    .values({input})
                    .singleAggregation({}, {"sketch_kll(c0)"})
                    .planNode();

    auto result = AssertQueryBuilder(plan).copyResults(pool());
    EXPECT_EQ(result->size(), 1);
    return result;
  }

  template <typename T>
  double rank(RowVectorPtr sketch, T value, bool inclusive = true) {
    std::string query;

    if constexpr (std::is_same_v<T, std::string>) {
      query = fmt::format(
          "sketch_kll_rank(a0, '{}'{})", value, inclusive ? "" : ", false");
    } else if constexpr (std::is_same_v<T, bool>) {
      query = fmt::format(
          "sketch_kll_rank(a0, {}{})",
          value ? "true" : "false",
          inclusive ? "" : ", false");
    } else if constexpr (std::is_same_v<T, double>) {
      query = fmt::format(
          "sketch_kll_rank(a0, CAST({} AS DOUBLE){})",
          value,
          inclusive ? "" : ", false");
    } else {
      query = fmt::format(
          "sketch_kll_rank(a0, CAST({} AS BIGINT){})",
          value,
          inclusive ? "" : ", false");
    }

    auto plan = PlanBuilder().values({sketch}).project({query}).planNode();

    return readSingleValue(plan).value<TypeKind::DOUBLE>();
  }

  template <typename T>
  T quantile(RowVectorPtr sketch, double rank, bool inclusive = true) {
    auto query = fmt::format(
        "sketch_kll_quantile(a0, CAST({} AS DOUBLE){})",
        rank,
        inclusive ? "" : ", false");

    auto plan = PlanBuilder().values({sketch}).project({query}).planNode();

    if constexpr (std::is_same_v<T, std::string>) {
      return readSingleValue(plan).value<TypeKind::VARCHAR>();
    } else if constexpr (std::is_same_v<T, bool>) {
      return readSingleValue(plan).value<TypeKind::BOOLEAN>();
    } else if constexpr (std::is_same_v<T, double>) {
      return readSingleValue(plan).value<TypeKind::DOUBLE>();
    } else {
      return readSingleValue(plan).value<TypeKind::BIGINT>();
    }
  }
};

//
// DOUBLE
//

TEST_F(KllSketchAggregationTest, rankDouble) {
  std::vector<double> values(100);
  std::iota(values.begin(), values.end(), 0);

  auto sketch = buildSketch(buildInput(values));

  EXPECT_NEAR(rank(sketch, -1.0), 0.0, 0.01);
  EXPECT_NEAR(rank(sketch, 49.0), 0.5, 0.02);
  EXPECT_NEAR(rank(sketch, 50.0, false), 0.5, 0.02);
  EXPECT_NEAR(rank(sketch, 99.0), 1.0, 0.01);
}

TEST_F(KllSketchAggregationTest, quantileDouble) {
  std::vector<double> values(100);
  std::iota(values.begin(), values.end(), 0);

  auto sketch = buildSketch(buildInput(values));

  EXPECT_NEAR(quantile<double>(sketch, 0.0), 0.0, 1.0);
  EXPECT_NEAR(quantile<double>(sketch, 0.5), 49.0, 2.0);
  EXPECT_NEAR(quantile<double>(sketch, 0.5, false), 49.0, 2.0);
  EXPECT_NEAR(quantile<double>(sketch, 1.0), 99.0, 1.0);
}

//
// BIGINT
//

TEST_F(KllSketchAggregationTest, rankBigint) {
  std::vector<int64_t> values(100);
  std::iota(values.begin(), values.end(), 0);

  auto sketch = buildSketch(buildInput(values));

  EXPECT_NEAR(rank(sketch, (int64_t)-1), 0.0, 0.01);
  EXPECT_NEAR(rank(sketch, (int64_t)49), 0.5, 0.02);
  EXPECT_NEAR(rank(sketch, (int64_t)99), 1.0, 0.01);
}

TEST_F(KllSketchAggregationTest, quantileBigint) {
  std::vector<int64_t> values(100);
  std::iota(values.begin(), values.end(), 0);

  auto sketch = buildSketch(buildInput(values));

  EXPECT_NEAR(quantile<int64_t>(sketch, 0.0), 0, 1);
  EXPECT_NEAR(quantile<int64_t>(sketch, 0.5), 49, 2);
  EXPECT_NEAR(quantile<int64_t>(sketch, 0.5, false), 49, 2);
  EXPECT_NEAR(quantile<int64_t>(sketch, 1.0), 99, 1);
}

//
// VARCHAR
//

TEST_F(KllSketchAggregationTest, rankString) {
  std::vector<std::string> values;
  for (char c = 'a'; c <= 'z'; ++c) {
    values.emplace_back(1, c);
  }

  auto sketch = buildSketch(buildInput(values));

  EXPECT_LT(rank(sketch, std::string("a")), 0.1);
  EXPECT_NEAR(rank(sketch, std::string("m")), 0.5, 0.05);
  EXPECT_NEAR(rank(sketch, std::string("z")), 1.0, 0.01);
}

TEST_F(KllSketchAggregationTest, quantileString) {
  std::vector<std::string> values;
  for (char c = 'a'; c <= 'z'; ++c) {
    values.emplace_back(1, c);
  }

  auto sketch = buildSketch(buildInput(values));

  auto q0 = quantile<std::string>(sketch, 0.0);
  auto q50 = quantile<std::string>(sketch, 0.5);
  auto q100 = quantile<std::string>(sketch, 1.0);

  EXPECT_EQ(q0, "a");
  EXPECT_TRUE(q50 == "m" || q50 == "n");
  EXPECT_EQ(q100, "z");

  // Test inclusive=false
  auto q50_excl = quantile<std::string>(sketch, 0.5, false);
  EXPECT_TRUE(q50_excl == "m" || q50_excl == "n");
}

//
// BOOLEAN
//

TEST_F(KllSketchAggregationTest, rankBoolean) {
  std::vector<bool> values;
  for (int i = 0; i < 100; i++) {
    values.push_back(i % 3 == 0);
  }

  auto sketch = buildSketch(buildInput(values));

  EXPECT_NEAR(rank(sketch, false, false), 0.0, 0.01);
  EXPECT_NEAR(rank(sketch, true, false), 0.66, 0.05);
  EXPECT_NEAR(rank(sketch, false), 0.66, 0.05);
  EXPECT_NEAR(rank(sketch, true), 1.0, 0.01);
}

TEST_F(KllSketchAggregationTest, quantileBoolean) {
  std::vector<bool> values;
  for (int i = 0; i < 100; i++) {
    values.push_back(i % 3 == 0);
  }

  auto sketch = buildSketch(buildInput(values));

  EXPECT_EQ(quantile<bool>(sketch, 0.0), false);
  EXPECT_EQ(quantile<bool>(sketch, 0.5), false);
  EXPECT_EQ(quantile<bool>(sketch, 0.7), true);
  EXPECT_EQ(quantile<bool>(sketch, 0.7, false), true);
  EXPECT_EQ(quantile<bool>(sketch, 1.0), true);
}

//
// Edge Cases
//

TEST_F(KllSketchAggregationTest, emptyInput) {
  auto input = buildInput<int64_t>({});
  auto sketch = buildSketch(input);

  EXPECT_EQ(sketch->size(), 1);
}

TEST_F(KllSketchAggregationTest, nullInput) {
  auto vector = makeNullableFlatVector<int64_t>({1, std::nullopt, 2, 3});

  auto input = makeRowVector({vector});
  auto sketch = buildSketch(input);

  EXPECT_EQ(sketch->size(), 1);

  // Verify sketch is queryable
  auto r = rank(sketch, (int64_t)2);
  EXPECT_GT(r, 0.0);
  EXPECT_LE(r, 1.0);
}

TEST_F(KllSketchAggregationTest, allNullInput) {
  auto vector = makeNullableFlatVector<int64_t>(
      {std::nullopt, std::nullopt, std::nullopt});

  auto input = makeRowVector({vector});
  auto sketch = buildSketch(input);

  EXPECT_EQ(sketch->size(), 1);
}

TEST_F(KllSketchAggregationTest, invalidRankNegative) {
  auto input = buildInput<double>({1.0, 2.0, 3.0, 4.0, 5.0});
  auto sketch = buildSketch(input);

  auto plan = PlanBuilder()
                  .values({sketch})
                  .project({"sketch_kll_quantile(a0, CAST(-0.5 AS DOUBLE))"})
                  .planNode();

  EXPECT_THROW(AssertQueryBuilder(plan).copyResults(pool()), VeloxException);
}

TEST_F(KllSketchAggregationTest, invalidRankTooLarge) {
  auto input = buildInput<double>({1.0, 2.0, 3.0, 4.0, 5.0});
  auto sketch = buildSketch(input);

  auto plan = PlanBuilder()
                  .values({sketch})
                  .project({"sketch_kll_quantile(a0, CAST(1.5 AS DOUBLE))"})
                  .planNode();

  EXPECT_THROW(AssertQueryBuilder(plan).copyResults(pool()), VeloxException);
}

TEST_F(KllSketchAggregationTest, partialAggregation) {
  std::vector<std::optional<int64_t>> batch1;
  std::vector<std::optional<int64_t>> batch2;

  for (int64_t i = 0; i < 50; i++) {
    batch1.push_back(i);
  }
  for (int64_t i = 50; i < 100; i++) {
    batch2.push_back(i);
  }

  auto input1 = makeRowVector({makeNullableFlatVector(batch1)});
  auto input2 = makeRowVector({makeNullableFlatVector(batch2)});

  auto plan = PlanBuilder()
                  .values({input1, input2})
                  .partialAggregation({}, {"sketch_kll(c0)"})
                  .finalAggregation()
                  .planNode();

  auto result = AssertQueryBuilder(plan).copyResults(pool());
  EXPECT_EQ(result->size(), 1);

  // Verify merged sketch
  auto r = rank(result, (int64_t)49);
  EXPECT_NEAR(r, 0.5, 0.02);
}

TEST_F(KllSketchAggregationTest, customK) {
  auto input = buildInput<double>({1.0, 2.0, 3.0, 4.0, 5.0});

  auto plan = PlanBuilder()
                  .values({input})
                  .singleAggregation({}, {"sketch_kll_with_k(c0, 400)"})
                  .planNode();

  auto sketch = AssertQueryBuilder(plan).copyResults(pool());
  EXPECT_EQ(sketch->size(), 1);

  // Verify sketch works
  auto r = rank(sketch, 3.0);
  EXPECT_GT(r, 0.0);
  EXPECT_LE(r, 1.0);
}

TEST_F(KllSketchAggregationTest, singleElement) {
  auto input = buildInput<int64_t>({42});
  auto sketch = buildSketch(input);

  EXPECT_EQ(quantile<int64_t>(sketch, 0.0), 42);
  EXPECT_EQ(quantile<int64_t>(sketch, 0.5), 42);
  EXPECT_EQ(quantile<int64_t>(sketch, 1.0), 42);

  EXPECT_NEAR(rank(sketch, (int64_t)42), 1.0, 0.01);
  EXPECT_NEAR(rank(sketch, (int64_t)41), 0.0, 0.01);
}

TEST_F(KllSketchAggregationTest, largeDataset) {
  std::vector<double> values;
  for (int i = 0; i < 10000; i++) {
    values.push_back(static_cast<double>(i));
  }

  // Shuffle to test realistic streaming behavior
  std::shuffle(values.begin(), values.end(), std::mt19937(1));

  auto sketch = buildSketch(buildInput(values));

  // Test compression works correctly at scale
  // Focus on median which is most stable for shuffled data
  EXPECT_NEAR(quantile<double>(sketch, 0.5), 5000.0, 150.0);
  EXPECT_NEAR(rank(sketch, 5000.0), 0.5, 0.03);

  // Min/max can have larger errors with shuffled data
  auto q0 = quantile<double>(sketch, 0.0);
  auto q100 = quantile<double>(sketch, 1.0);
  EXPECT_GE(q0, 0.0);
  EXPECT_LT(q0, 100.0);
  EXPECT_GT(q100, 9900.0);
  EXPECT_LE(q100, 9999.0);
}

TEST_F(KllSketchAggregationTest, repeatedQueries) {
  auto input = buildInput<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto sketch = buildSketch(input);

  // Verify deserialization doesn't corrupt memory across calls
  for (int i = 0; i < 10; i++) {
    auto r = rank(sketch, (int64_t)5);
    EXPECT_GT(r, 0.3);
    EXPECT_LT(r, 0.7);
  }

  // Same for quantile
  for (int i = 0; i < 10; i++) {
    auto q = quantile<int64_t>(sketch, 0.5);
    EXPECT_GE(q, 3);
    EXPECT_LE(q, 7);
  }
}

TEST_F(KllSketchAggregationTest, multiStageMerge) {
  std::vector<RowVectorPtr> batches;

  // Create 5 batches of 100 elements each (0-499)
  for (int b = 0; b < 5; b++) {
    std::vector<std::optional<int64_t>> values;
    for (int i = 0; i < 100; i++) {
      values.push_back(b * 100 + i);
    }
    batches.push_back(makeRowVector({makeNullableFlatVector(values)}));
  }

  // Test multi-stage merge in distributed execution
  auto plan = PlanBuilder()
                  .values(batches)
                  .partialAggregation({}, {"sketch_kll(c0)"})
                  .finalAggregation()
                  .planNode();

  auto sketch = AssertQueryBuilder(plan).copyResults(pool());
  EXPECT_EQ(sketch->size(), 1);

  // Verify merged sketch accuracy
  EXPECT_NEAR(rank(sketch, (int64_t)250), 0.5, 0.05);
  EXPECT_NEAR(quantile<int64_t>(sketch, 0.5), 250, 10);
}

} // namespace
} // namespace facebook::presto::functions::aggregate::test
