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
#include <sstream>

#include "DataSketches/kll_sketch.hpp"

#include "presto_cpp/main/functions/kll_sketch/KllSketchRegistration.h"
#include "presto_cpp/main/functions/kll_sketch/KllSketchTypeTraits.h"
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

// Cross-engine serialization tests: verify Java-serialized KLL sketches are
// correctly consumed by the native engine and vice versa.

class KllSketchCrossEngineTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    folly::SingletonVault::singleton()->registrationComplete();
    AggregationTestBase::SetUp();
    presto::functions::registerAllKllSketchFunctions("");
  }

  template <typename T>
  RowVectorPtr sketchFromBytes(const std::vector<uint8_t>& bytes) {
    std::string rawBytes(
        reinterpret_cast<const char*>(bytes.data()), bytes.size());
    TypePtr elementType;
    if constexpr (std::is_same_v<T, std::string>) {
      elementType = VARCHAR();
    } else {
      elementType = CppToType<T>::create();
    }
    return makeRowVector(
        {makeFlatVector<std::string>({rawBytes}, KLLSKETCH(elementType))});
  }

  template <typename T>
  double rank(
      const std::vector<uint8_t>& sketchBytes,
      T value,
      bool inclusive = true) {
    auto sketch = sketchFromBytes<T>(sketchBytes);
    std::string query;
    if constexpr (std::is_same_v<T, std::string>) {
      query = fmt::format(
          "sketch_kll_rank(c0, '{}'{})", value, inclusive ? "" : ", false");
    } else if constexpr (std::is_same_v<T, bool>) {
      query = fmt::format(
          "sketch_kll_rank(c0, {}{})",
          value ? "true" : "false",
          inclusive ? "" : ", false");
    } else if constexpr (std::is_same_v<T, double>) {
      query = fmt::format(
          "sketch_kll_rank(c0, CAST({} AS DOUBLE){})",
          value,
          inclusive ? "" : ", false");
    } else {
      query = fmt::format(
          "sketch_kll_rank(c0, CAST({} AS BIGINT){})",
          value,
          inclusive ? "" : ", false");
    }
    auto plan = PlanBuilder().values({sketch}).project({query}).planNode();
    return readSingleValue(plan).template value<TypeKind::DOUBLE>();
  }

  template <typename T>
  T quantile(
      const std::vector<uint8_t>& sketchBytes,
      double rankValue,
      bool inclusive = true) {
    auto sketch = sketchFromBytes<T>(sketchBytes);
    auto query = fmt::format(
        "sketch_kll_quantile(c0, CAST({} AS DOUBLE){})",
        rankValue,
        inclusive ? "" : ", false");
    auto plan = PlanBuilder().values({sketch}).project({query}).planNode();
    if constexpr (std::is_same_v<T, std::string>) {
      return readSingleValue(plan).template value<TypeKind::VARCHAR>();
    } else if constexpr (std::is_same_v<T, bool>) {
      return readSingleValue(plan).template value<TypeKind::BOOLEAN>();
    } else if constexpr (std::is_same_v<T, double>) {
      return readSingleValue(plan).template value<TypeKind::DOUBLE>();
    } else {
      return readSingleValue(plan).template value<TypeKind::BIGINT>();
    }
  }

  static std::vector<uint8_t> fromHex(const std::string& hex) {
    VELOX_CHECK_EQ(hex.size() % 2, 0, "hex string must have even length");
    std::vector<uint8_t> out(hex.size() / 2);
    for (size_t i = 0; i < out.size(); ++i) {
      out[i] =
          static_cast<uint8_t>(std::stoul(hex.substr(i * 2, 2), nullptr, 16));
    }
    return out;
  }

  static std::string toHex(const std::vector<uint8_t>& bytes) {
    std::ostringstream oss;
    for (auto b : bytes) {
      oss << fmt::format("{:02x}", b);
    }
    return oss.str();
  }
};

// Java golden bytes (TestKllSketchFunctions.printGolden*Bytes()).
// To regenerate: mvn -pl presto-main-base -Dtest=TestKllSketchFunctions test

// BIGINT, values 0-99, k=200
static const char* kJavaBigintGoldenHex =
    "05010f00c80008006400000000000000c8000100640000000000000000000000630000000000000063000000000000006200000000000000610000000000000060000000000000005f000000000000005e000000000000005d000000000000005c000000000000005b000000000000005a0000000000000059000000000000005800000000000000570000000000000056000000000000005500000000000000540000000000000053000000000000005200000000000000510000000000000050000000000000004f000000000000004e000000000000004d000000000000004c000000000000004b000000000000004a0000000000000049000000000000004800000000000000470000000000000046000000000000004500000000000000440000000000000043000000000000004200000000000000410000000000000040000000000000003f000000000000003e000000000000003d000000000000003c000000000000003b000000000000003a0000000000000039000000000000003800000000000000370000000000000036000000000000003500000000000000340000000000000033000000000000003200000000000000310000000000000030000000000000002f000000000000002e000000000000002d000000000000002c000000000000002b000000000000002a0000000000000029000000000000002800000000000000270000000000000026000000000000002500000000000000240000000000000023000000000000002200000000000000210000000000000020000000000000001f000000000000001e000000000000001d000000000000001c000000000000001b000000000000001a0000000000000019000000000000001800000000000000170000000000000016000000000000001500000000000000140000000000000013000000000000001200000000000000110000000000000010000000000000000f000000000000000e000000000000000d000000000000000c000000000000000b000000000000000a000000000000000900000000000000080000000000000007000000000000000600000000000000050000000000000004000000000000000300000000000000020000000000000001000000000000000000000000000000"; // NOLINT

// DOUBLE, values 0.0-99.0, k=200
static const char* kJavaDoubleGoldenHex =
    "05010f00c80008006400000000000000c80001006400000000000000000000000000000000c058400000000000c058400000000000805840000000000040584000000000000058400000000000c057400000000000805740000000000040574000000000000057400000000000c056400000000000805640000000000040564000000000000056400000000000c055400000000000805540000000000040554000000000000055400000000000c054400000000000805440000000000040544000000000000054400000000000c053400000000000805340000000000040534000000000000053400000000000c052400000000000805240000000000040524000000000000052400000000000c051400000000000805140000000000040514000000000000051400000000000c050400000000000805040000000000040504000000000000050400000000000804f400000000000004f400000000000804e400000000000004e400000000000804d400000000000004d400000000000804c400000000000004c400000000000804b400000000000004b400000000000804a400000000000004a40000000000080494000000000000049400000000000804840000000000000484000000000008047400000000000004740000000000080464000000000000046400000000000804540000000000000454000000000008044400000000000004440000000000080434000000000000043400000000000804240000000000000424000000000008041400000000000004140000000000080404000000000000040400000000000003f400000000000003e400000000000003d400000000000003c400000000000003b400000000000003a4000000000000039400000000000003840000000000000374000000000000036400000000000003540000000000000344000000000000033400000000000003240000000000000314000000000000030400000000000002e400000000000002c400000000000002a40000000000000284000000000000026400000000000002440000000000000224000000000000020400000000000001c4000000000000018400000000000001440000000000000104000000000000008400000000000000040000000000000f03f0000000000000000"; // NOLINT

// VARCHAR, 'a'-'z', k=200
static const char* kJavaVarcharGoldenHex =
    "05010f00c80008001a00000000000000c8000100ae0000000100000061010000007a010000007a0100000079010000007801000000770100000076010000007501000000740100000073010000007201000000710100000070010000006f010000006e010000006d010000006c010000006b010000006a010000006901000000680100000067010000006601000000650100000064010000006301000000620100000061"; // NOLINT

// BOOLEAN, i%3==0, k=200, bit-packed (ArrayOfBooleansSerDe)
static const char* kJavaBooleanGoldenHex =
    "05010f00c80008006400000000000000c800010064000000000149922449922449922449922409"; // NOLINT

TEST_F(KllSketchCrossEngineTest, javaGoldenBytesBigint) {
  auto bytes = fromHex(kJavaBigintGoldenHex);
  EXPECT_NEAR(rank<int64_t>(bytes, -1), 0.0, 0.01);
  EXPECT_NEAR(rank<int64_t>(bytes, 49), 0.5, 0.02);
  EXPECT_NEAR(rank<int64_t>(bytes, 99), 1.0, 0.01);
  EXPECT_EQ(quantile<int64_t>(bytes, 0.0), 0);
  EXPECT_EQ(quantile<int64_t>(bytes, 1.0), 99);
}

TEST_F(KllSketchCrossEngineTest, javaGoldenBytesDouble) {
  auto bytes = fromHex(kJavaDoubleGoldenHex);
  EXPECT_NEAR(rank<double>(bytes, -1.0), 0.0, 0.01);
  EXPECT_NEAR(rank<double>(bytes, 49.0), 0.5, 0.02);
  EXPECT_NEAR(rank<double>(bytes, 99.0), 1.0, 0.01);
  EXPECT_NEAR(quantile<double>(bytes, 0.0), 0.0, 1.0);
  EXPECT_NEAR(quantile<double>(bytes, 1.0), 99.0, 1.0);
}

TEST_F(KllSketchCrossEngineTest, javaGoldenBytesVarchar) {
  auto bytes = fromHex(kJavaVarcharGoldenHex);
  EXPECT_EQ(quantile<std::string>(bytes, 0.0), "a");
  EXPECT_EQ(quantile<std::string>(bytes, 1.0), "z");
  EXPECT_NEAR(rank<std::string>(bytes, std::string("m")), 0.5, 0.05);
}

// BOOLEAN: Java bit-packs booleans (ArrayOfBooleansSerDe); native must decode
// correctly.
TEST_F(KllSketchCrossEngineTest, javaGoldenBytesBoolean) {
  auto bytes = fromHex(kJavaBooleanGoldenHex);
  // ~34 trues, ~66 falses
  EXPECT_NEAR(rank<bool>(bytes, false), 0.66, 0.05);
  EXPECT_NEAR(rank<bool>(bytes, true), 1.0, 0.01);
  EXPECT_EQ(quantile<bool>(bytes, 0.0), false);
  EXPECT_EQ(quantile<bool>(bytes, 1.0), true);
}

// Native → Java: print C++ golden bytes for embedding in
// TestKllSketchFunctions.java. Run with --gtest_also_run_disabled_tests to see
// output.

TEST_F(KllSketchCrossEngineTest, DISABLED_printNativeBigintGoldenBytes) {
  datasketches::kll_sketch<int64_t> sketch(200);
  for (int64_t i = 0; i < 100; ++i) {
    sketch.update(i);
  }
  auto bytes = sketch.serialize();
  std::cout << "NATIVE_BIGINT_GOLDEN_HEX: "
            << toHex(std::vector<uint8_t>(bytes.begin(), bytes.end()))
            << std::endl;
}

TEST_F(KllSketchCrossEngineTest, DISABLED_printNativeDoubleGoldenBytes) {
  datasketches::kll_sketch<double> sketch(200);
  for (double i = 0; i < 100; ++i) {
    sketch.update(i);
  }
  auto bytes = sketch.serialize();
  std::cout << "NATIVE_DOUBLE_GOLDEN_HEX: "
            << toHex(std::vector<uint8_t>(bytes.begin(), bytes.end()))
            << std::endl;
}

TEST_F(KllSketchCrossEngineTest, DISABLED_printNativeVarcharGoldenBytes) {
  datasketches::kll_sketch<std::string> sketch(200);
  for (char c = 'a'; c <= 'z'; ++c) {
    sketch.update(std::string(1, c));
  }
  auto bytes = sketch.serialize();
  std::cout << "NATIVE_VARCHAR_GOLDEN_HEX: "
            << toHex(std::vector<uint8_t>(bytes.begin(), bytes.end()))
            << std::endl;
}

TEST_F(KllSketchCrossEngineTest, DISABLED_printNativeBooleanGoldenBytes) {
  datasketches::kll_sketch<bool> sketch(200);
  for (int i = 0; i < 100; ++i) {
    sketch.update(i % 3 == 0);
  }
  auto bytes =
      facebook::presto::functions::kll_sketch::serializeBoolSketch(sketch);
  std::cout << "NATIVE_BOOLEAN_GOLDEN_HEX (Java-compatible bit-packed): "
            << toHex(bytes) << std::endl;
}

TEST_F(
    KllSketchCrossEngineTest,
    boolSketchSerializeTranscodesItemsToBitPacked) {
  datasketches::kll_sketch<bool> sketch(200);
  for (int i = 0; i < 8; ++i) {
    sketch.update(i % 2 != 0); // false,true,false,true,...
  }

  auto javaBytes =
      facebook::presto::functions::kll_sketch::serializeBoolSketch(sketch);
  auto nativeBytes = sketch.serialize();

  EXPECT_LT(
      javaBytes.size(), nativeBytes.size()); // ceil(8/8)=1 byte vs 8 bytes
  ASSERT_GE(javaBytes.size(), 20u);
  const uint8_t numLevels = javaBytes[18];
  const size_t itemsStart = 20 + static_cast<size_t>(numLevels) * 4;
  ASSERT_GE(javaBytes.size(), itemsStart + 3u);
  ASSERT_GE(nativeBytes.size(), itemsStart + 2u + 8u);
  for (size_t i = 0; i < itemsStart + 2; ++i) {
    EXPECT_EQ(javaBytes[i], nativeBytes[i]) << "header differs at byte " << i;
  }
  ASSERT_EQ(javaBytes.size(), itemsStart + 2 + 1);
  const uint8_t packedByte = javaBytes[itemsStart + 2];
  for (int i = 0; i < 8; ++i) {
    const bool nativeItem = nativeBytes[itemsStart + 2 + i] != 0;
    const bool packedBit = ((packedByte >> i) & 1u) != 0;
    EXPECT_EQ(packedBit, nativeItem)
        << "bit " << i << " mismatch: packed=" << packedBit
        << " native=" << nativeItem;
  }
}

TEST_F(KllSketchCrossEngineTest, boolSketchBitPackingMatchesJavaContract) {
  // 1 true + 8 false at k=200 (no compaction). After sort: false(x8), true(x1).
  // Packed: byte 0 = 0x00, byte 1 = 0x01.
  datasketches::kll_sketch<bool> sketch(200);
  const bool vals[9] = {
      true, false, false, false, false, false, false, false, false};
  for (bool v : vals) {
    sketch.update(v);
  }

  auto javaBytes =
      facebook::presto::functions::kll_sketch::serializeBoolSketch(sketch);
  const uint8_t numLevels = javaBytes[18];
  const size_t itemsStart = 20 + static_cast<size_t>(numLevels) * 4;
  ASSERT_GE(javaBytes.size(), itemsStart + 2u + 2u);
  const uint8_t* packed = javaBytes.data() + itemsStart + 2;
  EXPECT_EQ(packed[0], 0x00u);
  EXPECT_EQ(packed[1], 0x01u);
}

TEST_F(KllSketchCrossEngineTest, boolSketchEmptyAndSingleItemPassthrough) {
  datasketches::kll_sketch<bool> empty(200);
  auto javaEmpty =
      facebook::presto::functions::kll_sketch::serializeBoolSketch(empty);
  auto nativeEmpty = empty.serialize();
  EXPECT_EQ(
      std::vector<uint8_t>(nativeEmpty.begin(), nativeEmpty.end()), javaEmpty);

  datasketches::kll_sketch<bool> single(200);
  single.update(true);
  auto javaSingle =
      facebook::presto::functions::kll_sketch::serializeBoolSketch(single);
  auto nativeSingle = single.serialize();
  EXPECT_EQ(
      std::vector<uint8_t>(nativeSingle.begin(), nativeSingle.end()),
      javaSingle);
}

} // namespace
} // namespace facebook::presto::functions::aggregate::test
