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
#include <numeric>
#include <random>

#include "DataSketches/kll_sketch.hpp"

#include "presto_cpp/main/functions/kll_sketch/KllSketchRegistration.h"
#include "presto_cpp/main/types/KllSketchType.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::presto::functions::aggregate::test {
namespace {

class KllSketchTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    folly::SingletonVault::singleton()->registrationComplete();
    AggregationTestBase::SetUp();
    presto::functions::registerAllKllSketchFunctions("");
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

  // Run a partial → final aggregation over the given batches and return the
  // resulting sketch row. Asserts the result is non-null.
  RowVectorPtr buildPartialFinalSketch(
      const std::vector<RowVectorPtr>& batches) {
    auto plan = PlanBuilder()
                    .values(batches)
                    .partialAggregation({}, {"sketch_kll(c0)"})
                    .finalAggregation()
                    .planNode();

    auto sketch = AssertQueryBuilder(plan).copyResults(pool());
    EXPECT_EQ(sketch->size(), 1);
    EXPECT_FALSE(sketch->childAt(0)->isNullAt(0));
    return sketch;
  }

  // Run the plan, expect a VeloxException whose message contains `substr`.
  void expectVeloxUserError(
      const core::PlanNodePtr& plan,
      const std::string& substr) {
    try {
      AssertQueryBuilder(plan).copyResults(pool());
      FAIL() << "Expected a VeloxException containing: " << substr;
    } catch (const VeloxException& e) {
      EXPECT_NE(std::string(e.message()).find(substr), std::string::npos);
    }
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

// sketch_kll — basic correctness per type (single-stage aggregation)

// DOUBLE

TEST_F(KllSketchTest, rankDouble) {
  std::vector<double> values(100);
  std::iota(values.begin(), values.end(), 0);

  auto sketch = buildSketch(buildInput(values));

  EXPECT_NEAR(rank(sketch, -1.0), 0.0, 0.01);
  EXPECT_NEAR(rank(sketch, 49.0), 0.5, 0.02);
  EXPECT_NEAR(rank(sketch, 50.0, false), 0.5, 0.02);
  EXPECT_NEAR(rank(sketch, 99.0), 1.0, 0.01);
}

TEST_F(KllSketchTest, quantileDouble) {
  std::vector<double> values(100);
  std::iota(values.begin(), values.end(), 0);

  auto sketch = buildSketch(buildInput(values));

  EXPECT_NEAR(quantile<double>(sketch, 0.0), 0.0, 1.0);
  EXPECT_NEAR(quantile<double>(sketch, 0.5), 49.0, 2.0);
  EXPECT_NEAR(quantile<double>(sketch, 0.5, false), 49.0, 2.0);
  EXPECT_NEAR(quantile<double>(sketch, 1.0), 99.0, 1.0);
}

// BIGINT

TEST_F(KllSketchTest, rankBigint) {
  std::vector<int64_t> values(100);
  std::iota(values.begin(), values.end(), 0);

  auto sketch = buildSketch(buildInput(values));

  EXPECT_NEAR(rank<int64_t>(sketch, -1), 0.0, 0.01);
  EXPECT_NEAR(rank<int64_t>(sketch, 49), 0.5, 0.02);
  EXPECT_NEAR(rank<int64_t>(sketch, 99), 1.0, 0.01);
}

TEST_F(KllSketchTest, quantileBigint) {
  std::vector<int64_t> values(100);
  std::iota(values.begin(), values.end(), 0);

  auto sketch = buildSketch(buildInput(values));

  EXPECT_NEAR(quantile<int64_t>(sketch, 0.0), 0, 1);
  EXPECT_NEAR(quantile<int64_t>(sketch, 0.5), 49, 2);
  EXPECT_NEAR(quantile<int64_t>(sketch, 0.5, false), 49, 2);
  EXPECT_NEAR(quantile<int64_t>(sketch, 1.0), 99, 1);
}

// VARCHAR

TEST_F(KllSketchTest, rankString) {
  std::vector<std::string> values;
  for (char c = 'a'; c <= 'z'; ++c) {
    values.emplace_back(1, c);
  }

  auto sketch = buildSketch(buildInput(values));

  EXPECT_LT(rank(sketch, std::string("a")), 0.1);
  EXPECT_NEAR(rank(sketch, std::string("m")), 0.5, 0.05);
  EXPECT_NEAR(rank(sketch, std::string("z")), 1.0, 0.01);
}

TEST_F(KllSketchTest, quantileString) {
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

  auto q50_excl = quantile<std::string>(sketch, 0.5, false);
  EXPECT_TRUE(q50_excl == "m" || q50_excl == "n");
}

// BOOLEAN

TEST_F(KllSketchTest, rankBoolean) {
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

TEST_F(KllSketchTest, quantileBoolean) {
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

// sketch_kll — null / edge case handling

TEST_F(KllSketchTest, emptyInput) {
  auto input = buildInput<int64_t>({});
  auto sketch = buildSketch(input);

  EXPECT_TRUE(sketch->childAt(0)->isNullAt(0));
}

TEST_F(KllSketchTest, nullInput) {
  auto vector = makeNullableFlatVector<int64_t>({1, std::nullopt, 2, 3});

  auto input = makeRowVector({vector});
  auto sketch = buildSketch(input);

  EXPECT_EQ(sketch->size(), 1);

  EXPECT_NEAR(rank<int64_t>(sketch, 2), 2.0 / 3.0, 0.05);
  EXPECT_EQ(quantile<int64_t>(sketch, 0.0), 1);
  EXPECT_EQ(quantile<int64_t>(sketch, 1.0), 3);
}

TEST_F(KllSketchTest, allNullInput) {
  auto vector = makeNullableFlatVector<int64_t>(
      {std::nullopt, std::nullopt, std::nullopt});

  auto input = makeRowVector({vector});
  auto sketch = buildSketch(input);

  EXPECT_TRUE(sketch->childAt(0)->isNullAt(0));
}

TEST_F(KllSketchTest, singleElement) {
  auto input = buildInput<int64_t>({42});
  auto sketch = buildSketch(input);

  EXPECT_EQ(quantile<int64_t>(sketch, 0.0), 42);
  EXPECT_EQ(quantile<int64_t>(sketch, 0.5), 42);
  EXPECT_EQ(quantile<int64_t>(sketch, 1.0), 42);

  EXPECT_NEAR(rank<int64_t>(sketch, 42), 1.0, 0.01);
  EXPECT_NEAR(rank<int64_t>(sketch, 41), 0.0, 0.01);
}

TEST_F(KllSketchTest, rankWithNullSketch) {
  auto input = buildInput<int64_t>({});
  auto sketch = buildSketch(input);

  EXPECT_TRUE(sketch->childAt(0)->isNullAt(0));

  auto plan = PlanBuilder()
                  .values({sketch})
                  .project({"sketch_kll_rank(a0, CAST(5 AS BIGINT))"})
                  .planNode();

  auto result = AssertQueryBuilder(plan).copyResults(pool());
  EXPECT_EQ(result->size(), 1);
  EXPECT_TRUE(result->childAt(0)->isNullAt(0));
}

TEST_F(KllSketchTest, quantileWithNullSketch) {
  auto input = buildInput<double>({});
  auto sketch = buildSketch(input);

  EXPECT_TRUE(sketch->childAt(0)->isNullAt(0));

  auto plan = PlanBuilder()
                  .values({sketch})
                  .project({"sketch_kll_quantile(a0, CAST(0.5 AS DOUBLE))"})
                  .planNode();

  auto result = AssertQueryBuilder(plan).copyResults(pool());
  EXPECT_EQ(result->size(), 1);
  EXPECT_TRUE(result->childAt(0)->isNullAt(0));
}

// sketch_kll — error / validation

TEST_F(KllSketchTest, invalidRankNegative) {
  auto input = buildInput<double>({1.0, 2.0, 3.0, 4.0, 5.0});
  auto sketch = buildSketch(input);

  auto plan = PlanBuilder()
                  .values({sketch})
                  .project({"sketch_kll_quantile(a0, CAST(-0.5 AS DOUBLE))"})
                  .planNode();

  EXPECT_THROW(AssertQueryBuilder(plan).copyResults(pool()), VeloxException);
}

TEST_F(KllSketchTest, invalidRankTooLarge) {
  auto input = buildInput<double>({1.0, 2.0, 3.0, 4.0, 5.0});
  auto sketch = buildSketch(input);

  auto plan = PlanBuilder()
                  .values({sketch})
                  .project({"sketch_kll_quantile(a0, CAST(1.5 AS DOUBLE))"})
                  .planNode();

  EXPECT_THROW(AssertQueryBuilder(plan).copyResults(pool()), VeloxException);
}

TEST_F(KllSketchTest, invalidSketchBytesRank) {
  auto garbage =
      makeFlatVector<std::string>({"not_a_sketch"}, KLLSKETCH(BIGINT()));
  auto plan = PlanBuilder()
                  .values({makeRowVector({garbage})})
                  .project({"sketch_kll_rank(c0, CAST(1 AS BIGINT))"})
                  .planNode();
  expectVeloxUserError(plan, "Failed to deserialize KLL sketch");
}

TEST_F(KllSketchTest, invalidSketchBytesQuantile) {
  auto garbage =
      makeFlatVector<std::string>({"not_a_sketch"}, KLLSKETCH(BIGINT()));
  auto plan = PlanBuilder()
                  .values({makeRowVector({garbage})})
                  .project({"sketch_kll_quantile(c0, CAST(0.5 AS DOUBLE))"})
                  .planNode();
  expectVeloxUserError(plan, "Failed to deserialize KLL sketch");
}

// sketch_kll_with_k — custom k parameter

TEST_F(KllSketchTest, customK) {
  auto input = buildInput<double>({1.0, 2.0, 3.0, 4.0, 5.0});

  auto plan = PlanBuilder()
                  .values({input})
                  .singleAggregation({}, {"sketch_kll_with_k(c0, 400)"})
                  .planNode();

  auto sketch = AssertQueryBuilder(plan).copyResults(pool());
  EXPECT_EQ(sketch->size(), 1);

  EXPECT_NEAR(quantile<double>(sketch, 0.0), 1.0, 0.01);
  EXPECT_NEAR(quantile<double>(sketch, 0.5), 3.0, 0.01);
  EXPECT_NEAR(quantile<double>(sketch, 1.0), 5.0, 0.01);
  EXPECT_NEAR(rank(sketch, 3.0), 0.6, 0.1);
}

TEST_F(KllSketchTest, customKWithNullK) {
  auto data = makeNullableFlatVector<double>({1.0, 2.0, 3.0});
  auto kValues = makeNullableFlatVector<int64_t>(
      {std::nullopt, std::nullopt, std::nullopt});
  auto plan = PlanBuilder()
                  .values({makeRowVector({data, kValues})})
                  .singleAggregation({}, {"sketch_kll_with_k(c0, c1)"})
                  .planNode();
  expectVeloxUserError(plan, "k parameter cannot be NULL");
}

TEST_F(KllSketchTest, customKWithNullData) {
  auto data = makeNullableFlatVector<double>(
      {std::nullopt, std::nullopt, std::nullopt});
  auto kValues = makeNullableFlatVector<int64_t>({200, 200, 200});
  auto input = makeRowVector({data, kValues});

  auto plan = PlanBuilder()
                  .values({input})
                  .singleAggregation({}, {"sketch_kll_with_k(c0, c1)"})
                  .planNode();

  auto result = AssertQueryBuilder(plan).copyResults(pool());
  EXPECT_EQ(result->size(), 1);
  EXPECT_TRUE(result->childAt(0)->isNullAt(0));
}

TEST_F(KllSketchTest, customKInconsistentValues) {
  auto data = makeNullableFlatVector<int64_t>({1, 2, 3});
  auto kValues = makeNullableFlatVector<int64_t>({100, 200, 100});
  auto plan = PlanBuilder()
                  .values({makeRowVector({data, kValues})})
                  .singleAggregation({}, {"sketch_kll_with_k(c0, c1)"})
                  .planNode();
  expectVeloxUserError(plan, "k parameter must be constant");
}

// sketch_kll — distributed aggregation (partial → final round-trip)

TEST_F(KllSketchTest, partialAggregationDouble) {
  std::vector<std::optional<double>> batch1(50);
  std::generate(
      batch1.begin(), batch1.end(), [n = 0.0]() mutable { return n++; });
  std::vector<std::optional<double>> batch2(50);
  std::generate(
      batch2.begin(), batch2.end(), [n = 50.0]() mutable { return n++; });

  auto sketch = buildPartialFinalSketch(
      {makeRowVector({makeNullableFlatVector(batch1)}),
       makeRowVector({makeNullableFlatVector(batch2)})});

  EXPECT_NEAR(rank<double>(sketch, 49.0), 0.5, 0.02);
  EXPECT_NEAR(quantile<double>(sketch, 0.5), 49.0, 2.0);
}

TEST_F(KllSketchTest, partialAggregationVarchar) {
  auto sketch = buildPartialFinalSketch(
      {makeRowVector(
           {makeNullableFlatVector<std::string>({"a", "b", "c", "d", "e"})}),
       makeRowVector(
           {makeNullableFlatVector<std::string>({"f", "g", "h", "i", "j"})})});

  EXPECT_NEAR(rank(sketch, std::string("e")), 0.5, 0.1);
  EXPECT_EQ(quantile<std::string>(sketch, 0.0), "a");
  EXPECT_EQ(quantile<std::string>(sketch, 1.0), "j");
}

TEST_F(KllSketchTest, partialAggregationBoolean) {
  auto sketch = buildPartialFinalSketch(
      {makeRowVector(
           {makeNullableFlatVector<bool>({false, false, false, false})}),
       makeRowVector({makeNullableFlatVector<bool>(
           {false, false, false, true, true, true})})});

  EXPECT_EQ(quantile<bool>(sketch, 0.0), false);
  EXPECT_EQ(quantile<bool>(sketch, 1.0), true);
  EXPECT_NEAR(rank(sketch, false), 0.7, 0.05);
  EXPECT_NEAR(rank(sketch, true), 1.0, 0.01);
}

TEST_F(KllSketchTest, partialAggregationBigint) {
  std::vector<std::optional<int64_t>> batch1(50);
  std::generate(
      batch1.begin(), batch1.end(), [n = int64_t{0}]() mutable { return n++; });
  std::vector<std::optional<int64_t>> batch2(50);
  std::generate(batch2.begin(), batch2.end(), [n = int64_t{50}]() mutable {
    return n++;
  });

  auto sketch = buildPartialFinalSketch(
      {makeRowVector({makeNullableFlatVector(batch1)}),
       makeRowVector({makeNullableFlatVector(batch2)})});

  EXPECT_NEAR(rank<int64_t>(sketch, 49), 0.5, 0.02);
  EXPECT_NEAR(quantile<int64_t>(sketch, 0.5), 49, 2);
}

TEST_F(KllSketchTest, multiStageMergeBigint) {
  std::vector<RowVectorPtr> batches;
  for (int b = 0; b < 5; b++) {
    std::vector<std::optional<int64_t>> values(100);
    std::generate(
        values.begin(), values.end(), [n = b * 100]() mutable { return n++; });
    batches.push_back(makeRowVector({makeNullableFlatVector(values)}));
  }

  auto sketch = buildPartialFinalSketch(batches);

  EXPECT_NEAR(rank<int64_t>(sketch, 250), 0.5, 0.05);
  EXPECT_NEAR(quantile<int64_t>(sketch, 0.5), 250, 10);
}

TEST_F(KllSketchTest, multiStageMergeDouble) {
  std::vector<RowVectorPtr> batches;
  for (int b = 0; b < 5; b++) {
    std::vector<std::optional<double>> values(100);
    std::generate(values.begin(), values.end(), [n = b * 100.0]() mutable {
      return n++;
    });
    batches.push_back(makeRowVector({makeNullableFlatVector(values)}));
  }

  auto sketch = buildPartialFinalSketch(batches);

  EXPECT_NEAR(rank<double>(sketch, 250.0), 0.5, 0.05);
  EXPECT_NEAR(quantile<double>(sketch, 0.5), 250.0, 10.0);
}

TEST_F(KllSketchTest, multiStageMergeVarchar) {
  std::vector<RowVectorPtr> batches;
  char base = 'a';
  for (int b = 0; b < 5; b++) {
    std::vector<std::optional<std::string>> values;
    for (int i = 0; i < 5; i++) {
      values.push_back(std::string(1, base++));
    }
    batches.push_back(makeRowVector({makeNullableFlatVector(values)}));
  }

  auto sketch = buildPartialFinalSketch(batches);

  EXPECT_NEAR(rank(sketch, std::string("m")), 0.5, 0.1);
  EXPECT_EQ(quantile<std::string>(sketch, 0.0), "a");
  EXPECT_EQ(quantile<std::string>(sketch, 1.0), "y");
}

TEST_F(KllSketchTest, multiStageMergeBoolean) {
  std::vector<RowVectorPtr> batches;
  for (int b = 0; b < 2; b++) {
    batches.push_back(makeRowVector(
        {makeNullableFlatVector<bool>({false, false, false, false, false})}));
  }
  for (int b = 0; b < 2; b++) {
    batches.push_back(makeRowVector(
        {makeNullableFlatVector<bool>({true, true, true, true, true})}));
  }

  auto sketch = buildPartialFinalSketch(batches);

  EXPECT_EQ(quantile<bool>(sketch, 0.0), false);
  EXPECT_EQ(quantile<bool>(sketch, 1.0), true);
  EXPECT_NEAR(rank(sketch, false), 0.5, 0.05);
  EXPECT_NEAR(rank(sketch, true), 1.0, 0.01);
}

// sketch_kll — scale and stability

TEST_F(KllSketchTest, largeDataset) {
  std::vector<double> values;
  for (int i = 0; i < 10000; i++) {
    values.push_back(static_cast<double>(i));
  }

  std::shuffle(values.begin(), values.end(), std::mt19937(1));

  auto sketch = buildSketch(buildInput(values));

  EXPECT_NEAR(quantile<double>(sketch, 0.5), 5000.0, 150.0);
  EXPECT_NEAR(rank(sketch, 5000.0), 0.5, 0.03);

  auto q0 = quantile<double>(sketch, 0.0);
  auto q100 = quantile<double>(sketch, 1.0);
  EXPECT_GE(q0, 0.0);
  EXPECT_LT(q0, 100.0);
  EXPECT_GT(q100, 9900.0);
  EXPECT_LE(q100, 9999.0);
}

TEST_F(KllSketchTest, repeatedQueries) {
  std::vector<int64_t> data = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  auto input = buildInput<int64_t>(data);
  auto sketch = buildSketch(input);

  for (size_t i = 0; i < data.size(); i++) {
    auto r = rank<int64_t>(sketch, 5);
    EXPECT_NEAR(r, 5.0 / 9.0, 0.1);
  }

  for (size_t i = 0; i < data.size(); i++) {
    auto q = quantile<int64_t>(sketch, 0.5);
    EXPECT_EQ(q, 5);
  }
}

} // namespace
} // namespace facebook::presto::functions::aggregate::test
