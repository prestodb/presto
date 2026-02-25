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

#include "DataSketches/theta_sketch.hpp"
#include "DataSketches/theta_union.hpp"

#include "presto_cpp/main/functions/theta_sketch/ThetaSketchRegistration.h"
#include "velox/common/hyperloglog/HllUtils.h"
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
class ThetaSketchAggregationTest : public AggregationTestBase {
 protected:
  static const std::vector<std::string> kFruits;
  static const std::vector<std::string> kVegetables;

  void SetUp() override {
    folly::SingletonVault::singleton()->registrationComplete();
    AggregationTestBase::SetUp();
    presto::functions::aggregate::theta_sketch::registerAllThetaSketchFunctions(
        "");
  }

  template <typename T>
  void testGlobalAgg(const VectorPtr& values) {
    auto vectors = makeRowVector({values});
    auto expected = makeRowVector({makeFlatVector<std::string>(
        {getExpectedResult<T>(values)}, VARBINARY())});

    testAggregations({vectors}, {}, {"sketch_theta(c0)"}, {expected});
  }

  template <typename T>
  const std::string getExpectedResult(const VectorPtr& values) {
    auto updateSketch = update_theta_sketch::builder().build();
    FlatVector<T>* flatVector = values->asFlatVector<T>();
    for (auto i = 0; i < flatVector->size(); i++) {
      if (!flatVector->isNullAt(i)) {
        if constexpr (
            std::is_same_v<T, std::string> || std::is_same_v<T, StringView>) {
          const auto& strView = flatVector->valueAt(i);
          updateSketch.update(std::string(strView.data(), strView.size()));
        } else if constexpr (std::is_same_v<T, velox::int128_t>) {
          updateSketch.update(std::to_string(flatVector->valueAt(i)));
        } else {
          updateSketch.update(flatVector->valueAt(i));
        }
      }
    }
    auto thetaUnion = theta_union::builder().build();
    thetaUnion.update(updateSketch);

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    thetaUnion.get_result().serialize(s);
    return s.str();
  }

  template <typename T>
  const RowVectorPtr getExpectedResultForGroupBy(
      const VectorPtr& keys,
      const VectorPtr& values) {
    VELOX_CHECK_EQ(keys->size(), values->size());
    struct thetaUnionStruct {
      theta_union thetaUnion = theta_union::builder().build();
      update_theta_sketch updateSketch = update_theta_sketch::builder().build();
      bool hasNull = false;
    };

    std::unordered_map<int32_t, thetaUnionStruct> groupedTheta;
    FlatVector<int32_t>* keysVector = keys->asFlatVector<int32_t>();
    FlatVector<T>* valuesVector = values->asFlatVector<T>();

    for (auto i = 0; i < keysVector->size(); ++i) {
      auto key = keysVector->valueAt(i);
      if (!valuesVector->isNullAt(i)) {
        auto value = valuesVector->valueAt(i);
        if constexpr (
            std::is_same_v<T, std::string> || std::is_same_v<T, StringView>) {
          groupedTheta[key].updateSketch.update(
              std::string(value.data(), value.size()));
        } else if constexpr (std::is_same_v<T, velox::int128_t>) {
          groupedTheta[key].updateSketch.update(std::to_string(value));
        } else {
          groupedTheta[key].updateSketch.update(value);
        }
      } else {
        groupedTheta[keysVector->valueAt(i)].hasNull = true;
      }
    }

    std::unordered_map<int32_t, std::string> results;

    for (auto& iter : groupedTheta) {
      groupedTheta[iter.first].thetaUnion.update(
          groupedTheta[iter.first].updateSketch);
      std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
      groupedTheta[iter.first].thetaUnion.get_result().serialize(s);
      results[iter.first] = s.str();
    }

    return toRowVector(results);
  }

  template <typename T, typename U>
  RowVectorPtr toRowVector(const std::unordered_map<T, U>& data) {
    std::vector<T> keys(data.size());
    transform(data.begin(), data.end(), keys.begin(), [](auto pair) {
      return pair.first;
    });

    std::vector<U> values(data.size());
    transform(data.begin(), data.end(), values.begin(), [](auto pair) {
      return pair.second;
    });

    return makeRowVector(
        {makeFlatVector(keys), makeFlatVector(values, VARBINARY())});
  }

  template <typename T>
  void testGroupByAgg(const VectorPtr& keys, const VectorPtr& values) {
    auto vectors = makeRowVector({keys, values});
    auto expectedResults = getExpectedResultForGroupBy<T>(keys, values);

    testAggregations(
        {vectors}, {"c0"}, {"sketch_theta(c1)"}, {expectedResults});
  }

  template <typename T>
  void runNumericTest(int64_t minValue) {
    vector_size_t size = 50'000;
    auto keys = makeFlatVector<int32_t>(
        size, [&minValue](auto row) { return (minValue + row) % 2; });
    auto values = makeFlatVector<T>(
        size, [&minValue](auto row) { return minValue + row; });
    testGroupByAgg<T>(keys, values);
    testGlobalAgg<T>(values);
  }
};

TEST_F(ThetaSketchAggregationTest, numericTest) {
  runNumericTest<int8_t>(0);
  runNumericTest<int16_t>(std::numeric_limits<int8_t>::max());
  runNumericTest<int32_t>(std::numeric_limits<int16_t>::max());
  runNumericTest<int64_t>(std::numeric_limits<int32_t>::max());
  runNumericTest<int128_t>(std::numeric_limits<int64_t>::max());
}

const std::vector<std::string> ThetaSketchAggregationTest::kFruits = {
    "apple",
    "banana",
    "cherry",
    "dragonfruit",
    "grapefruit",
    "melon",
    "orange",
    "pear",
    "pineapple",
    "unknown fruit with a very long name",
    "watermelon"};

const std::vector<std::string> ThetaSketchAggregationTest::kVegetables = {
    "cucumber",
    "tomato",
    "potato",
    "squash",
    "unknown vegetable with a very long name"};

TEST_F(ThetaSketchAggregationTest, varcharTest) {
  vector_size_t size = 50'000;

  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<StringView>(size, [&](auto row) {
    return StringView(
        row % 2 == 0 ? kFruits[row % kFruits.size()]
                     : kVegetables[row % kVegetables.size()]);
  });

  testGroupByAgg<StringView>(keys, values);
  testGlobalAgg<StringView>(values);
}

TEST_F(ThetaSketchAggregationTest, floatingPointTest) {
  vector_size_t size = 50'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });

  {
    auto values = makeFlatVector<float>(size, [](auto row) {
      return static_cast<float>(rand()) /
          (static_cast<float>(RAND_MAX / 50000));
    });
    testGroupByAgg<float>(keys, values);
    testGlobalAgg<float>(values);
  }

  {
    auto values = makeFlatVector<double>(50000, [](auto row) {
      return static_cast<double>(rand()) /
          (static_cast<double>(RAND_MAX / 50000));
    });
    testGroupByAgg<double>(keys, values);
    testGlobalAgg<double>(values);
  }
}

TEST_F(ThetaSketchAggregationTest, timestampTest) {
  vector_size_t size = 50'000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values = makeFlatVector<Timestamp>(
      size, [](auto row) { return Timestamp(row, row); });
  testGroupByAgg<Timestamp>(keys, values);
  testGlobalAgg<Timestamp>(values);
}

TEST_F(ThetaSketchAggregationTest, allNullsTest) {
  vector_size_t size = 5000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values =
      makeFlatVector<int32_t>(size, [](auto row) { return row; }, nullEvery(1));
  testGroupByAgg<int32_t>(keys, values);
  testGlobalAgg<int32_t>(values);
}

TEST_F(ThetaSketchAggregationTest, mixedNullsTest) {
  vector_size_t size = 5000;
  auto keys = makeFlatVector<int32_t>(size, [](auto row) { return row % 2; });
  auto values =
      makeFlatVector<int64_t>(size, [](auto row) { return row; }, nullEvery(2));
  testGroupByAgg<int64_t>(keys, values);
  testGlobalAgg<int64_t>(values);
}

TEST_F(ThetaSketchAggregationTest, streaming) {
  auto rawInput1 = makeFlatVector<int64_t>({1, 2, 3});
  auto rawInput2 = makeFlatVector<int64_t>(1000, folly::identity);
  auto combinedInput = makeFlatVector<int64_t>({1, 2, 3});
  combinedInput->append(rawInput2->wrappedVector());
  auto result = testStreaming("sketch_theta", true, {rawInput1}, {rawInput2});
  auto expectedResult = getExpectedResult<int64_t>(combinedInput);
  ASSERT_EQ(result->size(), 1);
  ASSERT_EQ(result->asFlatVector<StringView>()->valueAt(0), expectedResult);

  result = testStreaming("sketch_theta", false, {rawInput1}, {rawInput2});
  ASSERT_EQ(result->size(), 1);
  ASSERT_EQ(result->asFlatVector<StringView>()->valueAt(0), expectedResult);
}

TEST_F(ThetaSketchAggregationTest, testSketchThetaEstimate) {
  auto sketch_theta_estimate = [this](auto input) {
    auto op = PlanBuilder()
                  .values({makeRowVector({input})})
                  .singleAggregation({}, {"sketch_theta(c0)"})
                  .project({"sketch_theta_estimate(a0)"})
                  .planNode();

    return readSingleValue(op);
  };

  // Empty sketch
  auto input = makeFlatVector<int64_t>({});
  ASSERT_EQ(sketch_theta_estimate(input).value<TypeKind::DOUBLE>(), 0.0);

  // Single value sketch
  input = makeFlatVector<int64_t>(1);
  ASSERT_EQ(sketch_theta_estimate(input).value<TypeKind::DOUBLE>(), 1.0);

  // Many value sketch
  input = makeFlatVector<int64_t>(100, [](auto row) { return row; });
  update_theta_sketch updateSketch = update_theta_sketch::builder().build();
  for (auto i = 0; i < input->size(); ++i) {
    updateSketch.update(input->valueAt(i));
  }
  theta_union thetaUnion = theta_union::builder().build();
  thetaUnion.update(updateSketch);

  ASSERT_EQ(
      sketch_theta_estimate(input).value<TypeKind::DOUBLE>(),
      thetaUnion.get_result().get_estimate());
}

void assertSummaryMatches(
    compact_theta_sketch compactSketch,
    variant sketchSummary) {
  auto row =
      sketchSummary.value<Row<double, double, double, double, int32_t>>();
  ASSERT_EQ(row.at(0).value<TypeKind::DOUBLE>(), compactSketch.get_estimate());
  ASSERT_EQ(row.at(1).value<TypeKind::DOUBLE>(), compactSketch.get_theta());
  ASSERT_EQ(
      row.at(2).value<TypeKind::DOUBLE>(), compactSketch.get_upper_bound(1));
  ASSERT_EQ(
      row.at(3).value<TypeKind::DOUBLE>(), compactSketch.get_lower_bound(1));
  ASSERT_EQ(
      row.at(4).value<TypeKind::INTEGER>(), compactSketch.get_num_retained());
}

TEST_F(ThetaSketchAggregationTest, testSketchThetaSummary) {
  auto sketch_theta_summary = [this](auto input) {
    auto op = PlanBuilder()
                  .values({makeRowVector({input})})
                  .singleAggregation({}, {"sketch_theta(c0)"})
                  .project({"sketch_theta_summary(a0)"})
                  .planNode();

    return readSingleValue(op);
  };

  // Empty sketch
  auto input = makeFlatVector<int64_t>({});
  update_theta_sketch updateSketch = update_theta_sketch::builder().build();
  theta_union thetaUnion = theta_union::builder().build();
  thetaUnion.update(updateSketch);
  assertSummaryMatches(thetaUnion.get_result(), sketch_theta_summary(input));

  // Single value sketch
  input = makeFlatVector<int64_t>(1);
  updateSketch = update_theta_sketch::builder().build();
  updateSketch.update(1);
  thetaUnion = theta_union::builder().build();
  thetaUnion.update(updateSketch);
  assertSummaryMatches(thetaUnion.get_result(), sketch_theta_summary(input));

  // Many value sketch
  input = makeFlatVector<int64_t>(100, [](auto row) { return row; });
  updateSketch = update_theta_sketch::builder().build();
  for (auto i = 0; i < input->size(); ++i) {
    updateSketch.update(input->valueAt(i));
  }
  thetaUnion = theta_union::builder().build();
  thetaUnion.update(updateSketch);
  assertSummaryMatches(thetaUnion.get_result(), sketch_theta_summary(input));
}
} // namespace
} // namespace facebook::presto::functions::aggregate::test
