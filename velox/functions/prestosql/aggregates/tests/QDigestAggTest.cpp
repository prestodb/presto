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
#include <folly/base64.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/Aggregate.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/prestosql/types/QDigestType.h"

// using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {
namespace {
class QDigestAggTest : public AggregationTestBase {
 protected:
  const std::vector<bool> kGroupByKeys{false, false, false, true, true, true};
  const std::vector<int64_t> kWeights{1, 2, 3, 4, 5, 6};
  const double kAccuracy = 0.75;

  template <typename T>
  void testThrow(
      const std::vector<T>& inputs,
      const std::vector<double>& accuracies) {
    auto type = CppToType<T>::create();
    auto inputVector = makeRowVector(
        {makeFlatVector<T>(inputs), makeFlatVector<double>(accuracies)});
    auto dummyExpected = makeRowVector(
        {makeExpectedVector({inputs.size(), std::nullopt}, type)});
    testAggregations(
        {inputVector}, {}, {"qdigest_agg(c0, 1, c1)"}, {dummyExpected});
  }

  template <typename T>
  void testQDigestAgg(
      const std::vector<T>& inputs,
      const std::optional<std::vector<bool>>& groupByKeys,
      const std::vector<std::optional<std::string>>& expected,
      const std::optional<std::vector<int64_t>>& weights = std::nullopt,
      std::optional<double> accuracy = std::nullopt) {
    auto type = CppToType<T>::create();
    std::vector<std::string> inputNames{"c0"};
    std::vector<VectorPtr> inputVectors{makeFlatVector<T>(inputs)};
    std::vector<VectorPtr> expectedVectors{};
    std::vector<std::string> groupByNames{};
    auto call = "qdigest_agg(c0)";
    if (groupByKeys.has_value()) {
      inputNames.emplace_back("g0");
      inputVectors.emplace_back(makeFlatVector<bool>(groupByKeys.value()));
      expectedVectors.emplace_back(makeFlatVector<bool>({false, true}));
      groupByNames.emplace_back("g0");
    }
    if (weights.has_value()) {
      inputNames.emplace_back("c1");
      inputVectors.emplace_back(makeFlatVector<int64_t>(weights.value()));
      call = "qdigest_agg(c0, c1)";
    }
    if (accuracy.has_value()) {
      inputNames.emplace_back("c2");
      inputVectors.emplace_back(
          makeConstant<double>(accuracy.value(), inputs.size()));
      call = "qdigest_agg(c0, c1, c2)";
    }

    auto inputVector = makeRowVector(inputNames, inputVectors);
    expectedVectors.emplace_back(makeExpectedVector(expected, type));
    auto expectedRowVector = makeRowVector(expectedVectors);
    testAggregations({inputVector}, groupByNames, {call}, {expectedRowVector});
  }

 private:
  VectorPtr makeExpectedVector(
      const std::vector<std::optional<std::string>>& expected,
      const TypePtr& type) {
    std::vector<std::optional<std::string>> decodedExpected;
    decodedExpected.reserve(expected.size());
    for (const auto& value : expected) {
      decodedExpected.emplace_back(decodeBase64(value));
    }
    return makeNullableFlatVector<std::string>(
        decodedExpected, facebook::velox::QDigestType::get(type));
  }

  std::optional<std::string> decodeBase64(
      const std::optional<std::string>& encoded) {
    if (!encoded.has_value()) {
      return std::nullopt;
    }
    std::string decoded(folly::base64DecodedSize(encoded.value()), '\0');
    folly::base64Decode(encoded.value(), decoded.data());
    return decoded;
  }
};

TEST_F(QDigestAggTest, metadata) {
  core::QueryConfig config{{}};
  auto function = exec::Aggregate::create(
      "qdigest_agg",
      core::AggregationNode::Step::kSingle,
      {BIGINT()},
      QDIGEST(BIGINT()),
      config);
  EXPECT_FALSE(function->isFixedSize());
  EXPECT_FALSE(function->accumulatorUsesExternalMemory());
  EXPECT_EQ(function->accumulatorAlignmentSize(), 8);
}

TEST_F(QDigestAggTest, basic) {
  testQDigestAgg<int64_t>(
      {1, 2, 3, 4, 5, 6},
      std::nullopt,
      {"AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAABgAAAAAAAAALAAAAAAAAAAAAAPA/AQAAAAAAAIAAAAAAAAAA8D8CAAAAAAAAgAAAAAAAAADwPwMAAAAAAACAAwAAAAAAAAAAAgAAAAAAAIAHAAAAAAAAAAABAAAAAAAAgAAAAAAAAADwPwQAAAAAAACAAAAAAAAAAPA/BQAAAAAAAIADAAAAAAAAAAAEAAAAAAAAgAAAAAAAAADwPwYAAAAAAACABwAAAAAAAAAABAAAAAAAAIALAAAAAAAAAAABAAAAAAAAgA=="});
  testQDigestAgg<float>(
      {1.0, 2.0, 3.0, 4.0, 5.0, 6.0},
      std::nullopt,
      {"AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAAAAgD8AAAAAAADAQAAAAAALAAAAAAAAAAAAAPA/AACAPwAAAIAAAAAAAAAA8D8AAABAAAAAgAAAAAAAAADwPwAAQEAAAACAWwAAAAAAAAAAAAAAQAAAAIAAAAAAAAAA8D8AAIBAAAAAgAAAAAAAAADwPwAAoEAAAACAVwAAAAAAAAAAAACAQAAAAIAAAAAAAAAA8D8AAMBAAAAAgFsAAAAAAAAAAAAAgEAAAACAXwAAAAAAAAAAAAAAQAAAAIB7AAAAAAAAAAAAAIA/AAAAgA=="});
  testQDigestAgg<double>(
      {1.0, 2.0, 3.0, 4.0, 5.0, 6.0},
      std::nullopt,
      {"AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAPA/AAAAAAAAGEALAAAAAAAAAAAAAPA/AAAAAAAA8L8AAAAAAAAA8D8AAAAAAAAAwAAAAAAAAADwPwAAAAAAAAjAzwAAAAAAAAAAAAAAAAAAAMAAAAAAAAAA8D8AAAAAAAAQwAAAAAAAAADwPwAAAAAAABTAywAAAAAAAAAAAAAAAAAAEMAAAAAAAAAA8D8AAAAAAAAYwM8AAAAAAAAAAAAAAAAAABDA0wAAAAAAAAAAAAAAAAAAAMD7AAAAAAAAAAAAAAAAAADwvw=="});

  testQDigestAgg<int64_t>(
      {-1, -2, 3, -4, 5, -6},
      kGroupByKeys,
      {"AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAP7/////////AwAAAAAAAAAFAAAAAAAAAAAAAPA//v///////38AAAAAAAAA8D//////////fwMAAAAAAAAAAP////////9/AAAAAAAAAPA/AwAAAAAAAID/AAAAAAAAAAD/////////fw==",
       "AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAPr/////////BQAAAAAAAAAFAAAAAAAAAAAAAPA/+v///////38AAAAAAAAA8D/8////////fwsAAAAAAAAAAPz///////9/AAAAAAAAAPA/BQAAAAAAAID/AAAAAAAAAAD8////////fw=="});
  testQDigestAgg<float>(
      {-1.0, -2.0, 3.0, -4.0, 5.0, -6.0},
      kGroupByKeys,
      {"AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAP///7//////AABAQAAAAAAFAAAAAAAAAAAAAPA/////v////38AAAAAAAAA8D///3/A////f3sAAAAAAAAAAP//f8D///9/AAAAAAAAAPA/AABAQAAAAID/AAAAAAAAAAD//3/A////fw==",
       "AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAP//P7//////AACgQAAAAAAFAAAAAAAAAAAAAPA///8/v////38AAAAAAAAA8D///3+/////f1sAAAAAAAAAAP//f7////9/AAAAAAAAAPA/AACgQAAAAID/AAAAAAAAAAD//3+/////fw=="});
  testQDigestAgg<double>(
      {-1.0, -2.0, 3.0, -4.0, 5.0, -6.0},
      kGroupByKeys,
      {"AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAP////////+/AAAAAAAACEAFAAAAAAAAAAAAAPA//////////z8AAAAAAAAA8D////////8PQPsAAAAAAAAAAP///////w9AAAAAAAAAAPA/AAAAAAAACMD/AAAAAAAAAAD///////8PQA==",
       "AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAP///////+e/AAAAAAAAFEAFAAAAAAAAAAAAAPA/////////5z8AAAAAAAAA8D/////////vP88AAAAAAAAAAP///////+8/AAAAAAAAAPA/AAAAAAAAFMD/AAAAAAAAAAD////////vPw=="});
}

TEST_F(QDigestAggTest, withWeights) {
  testQDigestAgg<int64_t>(
      {1, 2, 3, 4, 5, 6},
      std::nullopt,
      {"AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAABgAAAAAAAAALAAAAAAAAAAAAAPA/AQAAAAAAAIAAAAAAAAAAAEACAAAAAAAAgAAAAAAAAAAIQAMAAAAAAACAAwAAAAAAAAAAAgAAAAAAAIAHAAAAAAAAAAABAAAAAAAAgAAAAAAAAAAQQAQAAAAAAACAAAAAAAAAABRABQAAAAAAAIADAAAAAAAAAAAEAAAAAAAAgAAAAAAAAAAYQAYAAAAAAACABwAAAAAAAAAABAAAAAAAAIALAAAAAAAAAAABAAAAAAAAgA=="},
      kWeights);
  testQDigestAgg<float>(
      {1.0, 2.0, 3.0, 4.0, 5.0, 6.0},
      std::nullopt,
      {"AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAAAAgD8AAAAAAADAQAAAAAALAAAAAAAAAAAAAPA/AACAPwAAAIAAAAAAAAAAAEAAAABAAAAAgAAAAAAAAAAIQAAAQEAAAACAWwAAAAAAAAAAAAAAQAAAAIAAAAAAAAAAEEAAAIBAAAAAgAAAAAAAAAAUQAAAoEAAAACAVwAAAAAAAAAAAACAQAAAAIAAAAAAAAAAGEAAAMBAAAAAgFsAAAAAAAAAAAAAgEAAAACAXwAAAAAAAAAAAAAAQAAAAIB7AAAAAAAAAAAAAIA/AAAAgA=="},
      kWeights);
  testQDigestAgg<double>(
      {1.0, 2.0, 3.0, 4.0, 5.0, 6.0},
      std::nullopt,
      {"AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAPA/AAAAAAAAGEALAAAAAAAAAAAAAPA/AAAAAAAA8L8AAAAAAAAAAEAAAAAAAAAAwAAAAAAAAAAIQAAAAAAAAAjAzwAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAEEAAAAAAAAAQwAAAAAAAAAAUQAAAAAAAABTAywAAAAAAAAAAAAAAAAAAEMAAAAAAAAAAGEAAAAAAAAAYwM8AAAAAAAAAAAAAAAAAABDA0wAAAAAAAAAAAAAAAAAAAMD7AAAAAAAAAAAAAAAAAADwvw=="},
      kWeights);

  testQDigestAgg<int64_t>(
      {-1, -2, 3, -4, 5, -6},
      kGroupByKeys,
      {"AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAP7/////////AwAAAAAAAAAFAAAAAAAAAAAAAABA/v///////38AAAAAAAAA8D//////////fwMAAAAAAAAAAP////////9/AAAAAAAAAAhAAwAAAAAAAID/AAAAAAAAAAD/////////fw==",
       "AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAPr/////////BQAAAAAAAAAFAAAAAAAAAAAAABhA+v///////38AAAAAAAAAEED8////////fwsAAAAAAAAAAPz///////9/AAAAAAAAABRABQAAAAAAAID/AAAAAAAAAAD8////////fw=="},
      kWeights);
  testQDigestAgg<float>(
      {-1, -2, 3, -4, 5, -6},
      kGroupByKeys,
      {"AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAP///7//////AABAQAAAAAAFAAAAAAAAAAAAAABA////v////38AAAAAAAAA8D///3/A////f3sAAAAAAAAAAP//f8D///9/AAAAAAAAAAhAAABAQAAAAID/AAAAAAAAAAD//3/A////fw==",
       "AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAP//P7//////AACgQAAAAAAFAAAAAAAAAAAAABhA//8/v////38AAAAAAAAAEED//3+/////f1sAAAAAAAAAAP//f7////9/AAAAAAAAABRAAACgQAAAAID/AAAAAAAAAAD//3+/////fw=="},
      kWeights);
  testQDigestAgg<double>(
      {-1, -2, 3, -4, 5, -6},
      kGroupByKeys,
      {"AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAP////////+/AAAAAAAACEAFAAAAAAAAAAAAAABA/////////z8AAAAAAAAA8D////////8PQPsAAAAAAAAAAP///////w9AAAAAAAAAAAhAAAAAAAAACMD/AAAAAAAAAAD///////8PQA==",
       "AHsUrkfheoQ/AAAAAAAAAAAAAAAAAAAAAP///////+e/AAAAAAAAFEAFAAAAAAAAAAAAABhA////////5z8AAAAAAAAAEED////////vP88AAAAAAAAAAP///////+8/AAAAAAAAABRAAAAAAAAAFMD/AAAAAAAAAAD////////vPw=="},
      kWeights);
}

TEST_F(QDigestAggTest, withAccuracy) {
  testQDigestAgg<int64_t>(
      {1, 2, 3, 4, 5, 6},
      std::nullopt,
      {"AAAAAAAAAOg/AAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAABgAAAAAAAAAJAAAAAAAAAAAAAABAAgAAAAAAAIAAAAAAAAAACEADAAAAAAAAgAMAAAAAAAAAAAIAAAAAAACAAAAAAAAAABBABAAAAAAAAIAAAAAAAAAAFEAFAAAAAAAAgAMAAAAAAAAAAAQAAAAAAACAAAAAAAAAABhABgAAAAAAAIAHAAAAAAAAAAAEAAAAAAAAgAsAAAAAAADwPwEAAAAAAACA"},
      kWeights,
      kAccuracy);
  testQDigestAgg<float>(
      {1, 2, 3, 4, 5, 6},
      std::nullopt,
      {"AAAAAAAAAOg/AAAAAAAAAAAAAAAAAAAAAAAAgD8AAAAAAADAQAAAAAALAAAAAAAAAAAAAPA/AACAPwAAAIAAAAAAAAAAAEAAAABAAAAAgAAAAAAAAAAIQAAAQEAAAACAWwAAAAAAAAAAAAAAQAAAAIAAAAAAAAAAEEAAAIBAAAAAgAAAAAAAAAAUQAAAoEAAAACAVwAAAAAAAAAAAACAQAAAAIAAAAAAAAAAGEAAAMBAAAAAgFsAAAAAAAAAAAAAgEAAAACAXwAAAAAAAAAAAAAAQAAAAIB7AAAAAAAAAAAAAIA/AAAAgA=="},
      kWeights,
      kAccuracy);
  testQDigestAgg<double>(
      {1, 2, 3, 4, 5, 6},
      std::nullopt,
      {"AAAAAAAAAOg/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAPA/AAAAAAAAGEALAAAAAAAAAAAAAPA/AAAAAAAA8L8AAAAAAAAAAEAAAAAAAAAAwAAAAAAAAAAIQAAAAAAAAAjAzwAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAEEAAAAAAAAAQwAAAAAAAAAAUQAAAAAAAABTAywAAAAAAAAAAAAAAAAAAEMAAAAAAAAAAGEAAAAAAAAAYwM8AAAAAAAAAAAAAAAAAABDA0wAAAAAAAAAAAAAAAAAAAMD7AAAAAAAAAAAAAAAAAADwvw=="},
      kWeights,
      kAccuracy);

  testQDigestAgg<int64_t>(
      {-1, -2, 3, -4, 5, -6},
      kGroupByKeys,
      {"AAAAAAAAAOg/AAAAAAAAAAAAAAAAAAAAAP7/////////AwAAAAAAAAAFAAAAAAAAAAAAAABA/v///////38AAAAAAAAA8D//////////fwMAAAAAAAAAAP////////9/AAAAAAAAAAhAAwAAAAAAAID/AAAAAAAAAAD/////////fw==",
       "AAAAAAAAAOg/AAAAAAAAAAAAAAAAAAAAAPr/////////BQAAAAAAAAAFAAAAAAAAAAAAABhA+v///////38AAAAAAAAAEED8////////fwsAAAAAAAAAAPz///////9/AAAAAAAAABRABQAAAAAAAID/AAAAAAAAAAD8////////fw=="},
      kWeights,
      kAccuracy);
  testQDigestAgg<float>(
      {-1, -2, 3, -4, 5, -6},
      kGroupByKeys,
      {"AAAAAAAAAOg/AAAAAAAAAAAAAAAAAAAAAP///7//////AABAQAAAAAAFAAAAAAAAAAAAAABA////v////38AAAAAAAAA8D///3/A////f3sAAAAAAAAAAP//f8D///9/AAAAAAAAAAhAAABAQAAAAID/AAAAAAAAAAD//3/A////fw==",
       "AAAAAAAAAOg/AAAAAAAAAAAAAAAAAAAAAP//P7//////AACgQAAAAAAFAAAAAAAAAAAAABhA//8/v////38AAAAAAAAAEED//3+/////f1sAAAAAAAAAAP//f7////9/AAAAAAAAABRAAACgQAAAAID/AAAAAAAAAAD//3+/////fw=="},
      kWeights,
      kAccuracy);
  testQDigestAgg<double>(
      {-1, -2, 3, -4, 5, -6},
      kGroupByKeys,
      {"AAAAAAAAAOg/AAAAAAAAAAAAAAAAAAAAAP////////+/AAAAAAAACEAFAAAAAAAAAAAAAABA/////////z8AAAAAAAAA8D////////8PQPsAAAAAAAAAAP///////w9AAAAAAAAAAAhAAAAAAAAACMD/AAAAAAAAAAD///////8PQA==",
       "AAAAAAAAAOg/AAAAAAAAAAAAAAAAAAAAAP///////+e/AAAAAAAAFEAFAAAAAAAAAAAAABhA////////5z8AAAAAAAAAEED////////vP88AAAAAAAAAAP///////+8/AAAAAAAAABRAAAAAAAAAFMD/AAAAAAAAAAD////////vPw=="},
      kWeights,
      kAccuracy);
}

TEST_F(QDigestAggTest, emptyInput) {
  testQDigestAgg<int64_t>({}, std::nullopt, {std::nullopt});
  testQDigestAgg<float>({}, std::nullopt, {std::nullopt});
  testQDigestAgg<double>({}, std::nullopt, {std::nullopt});
}

TEST_F(QDigestAggTest, invalid) {
  VELOX_ASSERT_THROW(
      testThrow<int64_t>({1, 2}, {0.1, 0.2}),
      "accuracy must be the same for all rows in the group");
  VELOX_ASSERT_THROW(
      testThrow<float>({1, 2}, {0.1, 0.2}),
      "accuracy must be the same for all rows in the group");
  VELOX_ASSERT_THROW(
      testThrow<double>({1, 2}, {0.1, 0.2}),
      "accuracy must be the same for all rows in the group");
}

} // namespace
} // namespace facebook::velox::aggregate::test
