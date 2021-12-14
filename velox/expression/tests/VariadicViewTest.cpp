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

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

namespace {

using namespace facebook::velox;
using namespace facebook::velox::exec;

class VariadicViewTest : public functions::test::FunctionBaseTest {
 protected:
  std::vector<std::vector<std::optional<int64_t>>> bigIntVectors = {
      {std::nullopt, std::nullopt, std::nullopt},
      {0, 1, 2},
      {99, 98, std::nullopt},
      {101, std::nullopt, 102},
      {std::nullopt, 10001, 12345676},
      {std::nullopt, std::nullopt, 3},
      {std::nullopt, 4, std::nullopt},
      {5, std::nullopt, std::nullopt},
  };

  void testVariadicView(const std::vector<VectorPtr>& additionalVectors = {}) {
    std::vector<VectorPtr> vectors(
        additionalVectors.begin(), additionalVectors.end());
    for (const auto& vector : bigIntVectors) {
      vectors.emplace_back(makeNullableFlatVector(vector));
    }
    SelectivityVector rows(vectors[0]->size());
    EvalCtx ctx(&execCtx_, nullptr, nullptr);
    DecodedArgs args(rows, vectors, &ctx);

    size_t startIndex = additionalVectors.size();
    VectorReader<Variadic<int64_t>> reader(args, startIndex);

    auto testItem = [&](int row, int arg, auto item) {
      // Test has_value.
      ASSERT_EQ(bigIntVectors[arg][row].has_value(), item.has_value());

      // Test bool implicit cast.
      ASSERT_EQ(bigIntVectors[arg][row].has_value(), static_cast<bool>(item));

      if (bigIntVectors[arg][row].has_value()) {
        // Test * operator.
        ASSERT_EQ(bigIntVectors[arg][row].value(), *item);

        // Test value().
        ASSERT_EQ(bigIntVectors[arg][row].value(), item.value());
      }
      // Test == with std::optional
      ASSERT_EQ(item, bigIntVectors[arg][row]);
    };

    for (auto row = 0; row < vectors[0]->size(); ++row) {
      auto variadicView = reader[row];
      auto arg = 0;

      // Test iterate loop.
      for (auto item : variadicView) {
        testItem(row, arg, item);
        arg++;
      }
      ASSERT_EQ(arg, bigIntVectors.size());

      // Test iterate loop explicit begin & end.
      auto it = variadicView.begin();
      arg = 0;
      while (it != variadicView.end()) {
        testItem(row, arg, *it);
        arg++;
        ++it;
      }
      ASSERT_EQ(arg, bigIntVectors.size());

      // Test index based loop.
      for (arg = 0; arg < variadicView.size(); arg++) {
        testItem(row, arg, variadicView[arg]);
      }
      ASSERT_EQ(arg, bigIntVectors.size());

      // Test loop iterator with <.
      arg = 0;
      for (auto it2 = variadicView.begin(); it2 < variadicView.end(); it2++) {
        testItem(row, arg, *it2);
        arg++;
      }
      ASSERT_EQ(arg, bigIntVectors.size());
    }
  }
};

TEST_F(VariadicViewTest, variadicInt) {
  testVariadicView();
}

TEST_F(VariadicViewTest, variadicIntMoreArgs) {
  // Test accessing Variadic args when there are other args before it.
  testVariadicView(
      {makeNullableFlatVector(std::vector<std::optional<int64_t>>{-1, -2, -3}),
       makeNullableFlatVector(
           std::vector<std::optional<int64_t>>{-4, std::nullopt, -6}),
       makeNullableFlatVector(std::vector<std::optional<int64_t>>{
           std::nullopt, std::nullopt, std::nullopt})});
}

TEST_F(VariadicViewTest, notNullContainer) {
  std::vector<VectorPtr> vectors;
  for (const auto& vector : bigIntVectors) {
    vectors.emplace_back(makeNullableFlatVector(vector));
  }
  SelectivityVector rows(vectors[0]->size());
  EvalCtx ctx(&execCtx_, nullptr, nullptr);
  DecodedArgs args(rows, vectors, &ctx);

  VectorReader<Variadic<int64_t>> reader(args, 0);

  for (auto row = 0; row < vectors[0]->size(); ++row) {
    auto variadicView = reader[row];
    int arg = 0;
    for (auto value : variadicView.skipNulls()) {
      while (arg < bigIntVectors.size() &&
             bigIntVectors[arg][row] == std::nullopt) {
        arg++;
      }
      ASSERT_EQ(value, bigIntVectors[arg][row].value());
      arg++;
    }
  }
}

} // namespace
