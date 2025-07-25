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
#include <cmath>
#include <limits>
#include <optional>

#include <gmock/gmock.h>

#include <velox/common/base/VeloxException.h>
#include <velox/vector/SimpleVector.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox {
namespace {

constexpr double kInf = std::numeric_limits<double>::infinity();
constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
constexpr float kInfFloat = std::numeric_limits<float>::infinity();
constexpr float kNanFloat = std::numeric_limits<float>::quiet_NaN();

MATCHER(IsNan, "is NaN") {
  return arg && std::isnan(*arg);
}
MATCHER(IsInf, "is Infinity") {
  return arg && std::isinf(*arg);
}

class DistanceFunctionsTest : public functions::test::FunctionBaseTest {};

TEST_F(DistanceFunctionsTest, cosineSimilarity) {
  const auto cosineSimilarity =
      [&](const std::vector<std::pair<std::string, std::optional<double>>>&
              left,
          const std::vector<std::pair<std::string, std::optional<double>>>&
              right) {
        auto leftMap = makeMapVector<std::string, double>({left});
        auto rightMap = makeMapVector<std::string, double>({right});
        return evaluateOnce<double>(
                   "cosine_similarity(c0,c1)",
                   makeRowVector({leftMap, rightMap}))
            .value();
      };

  EXPECT_NEAR(
      (2.0 * 3.0) / (std::sqrt(5.0) * std::sqrt(10.0)),
      cosineSimilarity({{"a", 1}, {"b", 2}}, {{"c", 1}, {"b", 3}}),
      1e-6);

  EXPECT_NEAR(
      (2.0 * 3.0 + (-1) * 1) / (std::sqrt(1 + 4 + 1) * std::sqrt(1 + 9)),
      cosineSimilarity({{"a", 1}, {"b", 2}, {"c", -1}}, {{"c", 1}, {"b", 3}}),
      1e-6);

  EXPECT_NEAR(
      ((-1) * 1) / (std::sqrt(1 + 4 + 1) * std::sqrt(1 + 9)),
      cosineSimilarity({{"a", 1}, {"b", 2}, {"c", -1}}, {{"c", 1}, {"d", 3}}),
      1e-6);
  // Two maps have no common keys.
  EXPECT_DOUBLE_EQ(
      0.0,
      cosineSimilarity({{"a", 1}, {"b", 2}, {"c", -1}}, {{"d", 1}, {"e", 3}}));

  EXPECT_TRUE(std::isnan(cosineSimilarity({}, {})));
  EXPECT_TRUE(std::isnan(cosineSimilarity({{"d", 1}, {"e", 3}}, {})));
  EXPECT_TRUE(
      std::isnan(cosineSimilarity({{"a", 1}, {"b", 3}}, {{"a", 0}, {"b", 0}})));

  auto nullableLeftMap = makeNullableMapVector<StringView, double>(
      {{{{"a"_sv, 1}, {"b"_sv, std::nullopt}}}});
  auto rightMap =
      makeMapVector<StringView, double>({{{{"c"_sv, 1}, {"b"_sv, 3}}}});

  EXPECT_FALSE(evaluateOnce<double>(
                   "cosine_similarity(c0,c1)",
                   makeRowVector({nullableLeftMap, rightMap}))
                   .has_value());
}

TEST_F(DistanceFunctionsTest, cosineSimilarityArray) {
  const auto cosineSimilarity = [&](const std::vector<double>& left,
                                    const std::vector<double>& right) {
    auto leftMap = makeArrayVector<double>({left});
    auto rightMap = makeArrayVector<double>({right});
    return evaluateOnce<double>(
               "cosine_similarity(c0,c1)", makeRowVector({leftMap, rightMap}))
        .value();
  };
  EXPECT_NEAR(
      (1 * 1 * 1 + 2 * 3) / (std::sqrt(5.0) * std::sqrt(10.0)),
      cosineSimilarity({{1, 2}}, {{1, 3}}),
      1e-6);

  EXPECT_NEAR(
      (1 * 1 + 2 * 3 + (-1) * 5) /
          (std::sqrt(1 + 4 + 1) * std::sqrt(1 + 9 + 25)),
      cosineSimilarity({{1, 2, -1}}, {{1, 3, 5}}),
      1e-6);

  EXPECT_TRUE(std::isnan(cosineSimilarity({}, {})));
  VELOX_ASSERT_THROW(
      cosineSimilarity({1, 3}, {}), "Both arrays need to have identical size");
  EXPECT_TRUE(std::isnan(cosineSimilarity({1, 3}, {0, 0})));
  EXPECT_TRUE(std::isnan(cosineSimilarity({1, 3}, {kNan, 1})));
  EXPECT_TRUE(std::isnan(cosineSimilarity({1, 3}, {kInf, 1})));
}

TEST_F(DistanceFunctionsTest, dotProductArray) {
  const auto dotProduct = [&](const std::vector<double>& left,
                              const std::vector<double>& right) {
    auto leftArray = makeArrayVector<double>({left});
    auto rightArray = makeArrayVector<double>({right});
    return evaluateOnce<double>(
               "dot_product(c0,c1)", makeRowVector({leftArray, rightArray}))
        .value();
  };
  EXPECT_NEAR((1 * 1 + 2 * 3), dotProduct({{1, 2}}, {{1, 3}}), 1e-6);

  EXPECT_NEAR(
      (1 * 1 + 2 * 3 + (-1) * 5), dotProduct({{1, 2, -1}}, {{1, 3, 5}}), 1e-6);

  EXPECT_TRUE(std::isnan(dotProduct({}, {})));
  VELOX_ASSERT_THROW(
      dotProduct({1, 3}, {}), "Both array arguments must have identical sizes");
}

#ifdef VELOX_ENABLE_FAISS

TEST_F(DistanceFunctionsTest, cosineSimilarityFloatArray) {
  const auto cosineSimilarity = [&](const std::vector<float>& left,
                                    const std::vector<float>& right) {
    auto leftMap = makeArrayVector<float>({left});
    auto rightMap = makeArrayVector<float>({right});
    return evaluateOnce<float>(
               "cosine_similarity(c0,c1)", makeRowVector({leftMap, rightMap}))
        .value();
  };
  EXPECT_NEAR(
      (1 * 1 * 1 + 2 * 3) / (std::sqrt(5.0) * std::sqrt(10.0)),
      cosineSimilarity({{1, 2}}, {{1, 3}}),
      1e-6);

  EXPECT_NEAR(
      (1 * 1 + 2 * 3 + (-1) * 5) /
          (std::sqrt(1 + 4 + 1) * std::sqrt(1 + 9 + 25)),
      cosineSimilarity({{1, 2, -1}}, {{1, 3, 5}}),
      1e-6);

  EXPECT_TRUE(std::isnan(cosineSimilarity({}, {})));
  VELOX_ASSERT_THROW(
      cosineSimilarity({1, 3}, {}), "Both arrays need to have identical size");
  EXPECT_TRUE(std::isnan(cosineSimilarity({1, 3}, {0, 0})));
  EXPECT_TRUE(std::isnan(cosineSimilarity({1, 3}, {kNanFloat, 1})));
  EXPECT_TRUE(std::isnan(cosineSimilarity({1, 3}, {kInfFloat, 1})));
}

TEST_F(DistanceFunctionsTest, l2SquaredFunctionFloatArray) {
  const auto l2Squared = [&](const std::vector<float>& left,
                             const std::vector<float>& right) {
    auto leftArray = makeArrayVector<float>({left});
    auto rightArray = makeArrayVector<float>({right});
    return evaluateOnce<float>(
               "l2_squared(c0,c1)", makeRowVector({leftArray, rightArray}))
        .value();
  };

  EXPECT_NEAR(
      (1.234 - 2.345) * (1.234 - 2.345) + (2.456 - 3.567) * (2.456 - 3.567),
      l2Squared({1.234, 2.456}, {2.345, 3.567}),
      1e-6);
  EXPECT_NEAR(
      (1.789 - 4.012) * (1.789 - 4.012) + (2.345 * 2.345) +
          (-1.678 - 5.901) * (-1.678 - 5.901),
      l2Squared({1.789, 2.345, -1.678}, {4.012, 0.0, 5.901}),
      1e-5);
  EXPECT_TRUE(std::isnan(l2Squared({}, {})));
  VELOX_ASSERT_THROW(
      l2Squared({1.234, 3.456}, {}), "Both arrays need to have identical size");
}

TEST_F(DistanceFunctionsTest, l2SquaredFunctionDoubleArray) {
  const auto l2Squared = [&](const std::vector<double>& left,
                             const std::vector<double>& right) {
    auto leftArray = makeArrayVector<double>({left});
    auto rightArray = makeArrayVector<double>({right});
    return evaluateOnce<double>(
               "l2_squared(c0,c1)", makeRowVector({leftArray, rightArray}))
        .value();
  };

  EXPECT_NEAR(
      (1.5 - 2.3) * (1.5 - 2.3) + (2.7 - 3.8) * (2.7 - 3.8),
      l2Squared({1.5, 2.7}, {2.3, 3.8}),
      1e-6);
  EXPECT_NEAR(
      (1.1 - 4.2) * (1.1 - 4.2) + (2.5 * 2.5) + (-1.3 - 5.6) * (-1.3 - 5.6),
      l2Squared({1.1, 2.5, -1.3}, {4.2, 0.0, 5.6}),
      1e-5);
  EXPECT_TRUE(std::isnan(l2Squared({}, {})));
  VELOX_ASSERT_THROW(
      l2Squared({1.0, 3.0}, {}), "Both arrays need to have identical size");
}

TEST_F(DistanceFunctionsTest, dotProductFloatArray) {
  const auto dotProduct = [&](const std::vector<float>& left,
                              const std::vector<float>& right) {
    auto leftArray = makeArrayVector<float>({left});
    auto rightArray = makeArrayVector<float>({right});
    return evaluateOnce<float>(
               "dot_product(c0,c1)", makeRowVector({leftArray, rightArray}))
        .value();
  };
  EXPECT_NEAR((1 * 1 + 2 * 3), dotProduct({{1, 2}}, {{1, 3}}), 1e-6);

  EXPECT_NEAR(
      (1 * 1 + 2 * 3 + (-1) * 5), dotProduct({{1, 2, -1}}, {{1, 3, 5}}), 1e-6);

  EXPECT_TRUE(std::isnan(dotProduct({}, {})));
  VELOX_ASSERT_THROW(
      dotProduct({1, 3}, {}), "Both array arguments must have identical sizes");
}
#endif // VELOX_ENABLE_FAISS

} // namespace
} // namespace facebook::velox
