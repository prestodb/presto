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

#include "DataSketches/kll_sketch.hpp"

#include "presto_cpp/main/functions/kll_sketch/KllSketchRegistration.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;
using namespace datasketches;

namespace facebook::presto::functions::aggregate::test {
namespace {
class KllSketchRankTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    folly::SingletonVault::singleton()->registrationComplete();
    AggregationTestBase::SetUp();
    presto::functions::aggregate::kll_sketch::registerAllKllSketchFunctions("");
  }

  // Helper to create a serialized KLL sketch from a vector of values
  template <typename T>
  std::string createSerializedSketch(const std::vector<T>& values) {
    datasketches::kll_sketch<T> sketch;
    for (const auto& value : values) {
      sketch.update(value);
    }

    auto serialized = sketch.serialize();
    return std::string(
        reinterpret_cast<const char*>(serialized.data()), serialized.size());
  }

  // Helper to test rank function
  template <typename T>
  double
  testRank(const std::vector<T>& values, T queryValue, bool inclusive = true) {
    auto sketch = createSerializedSketch(values);
    auto input =
        makeRowVector({makeFlatVector<std::string>({sketch}, VARBINARY())});

    std::string query;
    if constexpr (std::is_same_v<T, std::string>) {
      query = fmt::format(
          "sketch_kll_rank(c0, '{}'{})",
          queryValue,
          inclusive ? "" : ", false");
    } else if constexpr (std::is_same_v<T, bool>) {
      query = fmt::format(
          "sketch_kll_rank(c0, {}{})",
          queryValue ? "true" : "false",
          inclusive ? "" : ", false");
    } else if constexpr (std::is_same_v<T, double>) {
      query = fmt::format(
          "sketch_kll_rank(c0, CAST({} AS DOUBLE){})",
          queryValue,
          inclusive ? "" : ", false");
    } else {
      query = fmt::format(
          "sketch_kll_rank(c0, CAST({} AS BIGINT){})",
          queryValue,
          inclusive ? "" : ", false");
    }

    auto op = PlanBuilder().values({input}).project({query}).planNode();

    return readSingleValue(op).template value<TypeKind::DOUBLE>();
  }
};

// ============================================================================
// Test Doubles - sketch_kll_rank for DOUBLE type
// ============================================================================
TEST_F(KllSketchRankTest, testDoubles) {
  // Create sketch with values 0-99 (matches Java test)
  std::vector<double> values;
  for (int i = 0; i < 100; i++) {
    values.push_back(static_cast<double>(i));
  }

  // Test sketch_kll_rank
  EXPECT_NEAR(testRank(values, -1.0), 0.0, 0.01);
  EXPECT_NEAR(testRank(values, 49.0), 0.5, 0.02);
  EXPECT_NEAR(testRank(values, 50.0, false), 0.5, 0.02);
  EXPECT_NEAR(testRank(values, 99.0), 1.0, 0.01);
}

// ============================================================================
// Test Ints - sketch_kll_rank for BIGINT type
// ============================================================================
TEST_F(KllSketchRankTest, testInts) {
  // Create sketch with values 0-99 (matches Java test)
  std::vector<int64_t> values;
  for (int64_t i = 0; i < 100; i++) {
    values.push_back(i);
  }

  // Test sketch_kll_rank
  EXPECT_NEAR(testRank(values, (int64_t)-1), 0.0, 0.01);
  EXPECT_NEAR(testRank(values, (int64_t)49), 0.5, 0.02);
  EXPECT_NEAR(testRank(values, (int64_t)50, false), 0.5, 0.02);
  EXPECT_NEAR(testRank(values, (int64_t)99), 1.0, 0.01);
}

// ============================================================================
// Test Strings - sketch_kll_rank for VARCHAR type
// ============================================================================
TEST_F(KllSketchRankTest, testStrings) {
  // Create sketch with letters a-z (matches Java test)
  std::vector<std::string> values;
  for (char c = 'a'; c <= 'z'; c++) {
    values.push_back(std::string(1, c));
  }

  // Test sketch_kll_rank
  EXPECT_NEAR(testRank(values, std::string("1")), 0.0, 0.01);
  EXPECT_NEAR(testRank(values, std::string("m")), 0.5, 0.05);
  EXPECT_NEAR(testRank(values, std::string("n"), false), 0.5, 0.05);
  EXPECT_NEAR(testRank(values, std::string("z")), 1.0, 0.01);
}

// ============================================================================
// Test Booleans - sketch_kll_rank for BOOLEAN type
// ============================================================================
TEST_F(KllSketchRankTest, testBooleans) {
  // Create sketch with pattern: every 3rd value is true (matches Java test)
  std::vector<bool> values;
  for (int i = 0; i < 100; i++) {
    values.push_back(i % 3 == 0);
  }

  // Test sketch_kll_rank
  EXPECT_NEAR(testRank(values, false, false), 0.0, 0.01);
  EXPECT_NEAR(testRank(values, true, false), 0.66, 0.05);
  EXPECT_NEAR(testRank(values, false), 0.66, 0.05);
  EXPECT_NEAR(testRank(values, true), 1.0, 0.01);
}

} // namespace
} // namespace facebook::presto::functions::aggregate::test
