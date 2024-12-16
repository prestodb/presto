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
#include "velox/vector/VectorPrinter.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gmock/gmock.h>

namespace facebook::velox::test {

class VectorPrinterTest : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  static std::vector<std::string> summarizeToLines(
      const BaseVector& vector,
      const VectorPrinter::Options& options = {}) {
    std::vector<std::string> lines;
    folly::split('\n', VectorPrinter::summarizeToText(vector, options), lines);
    if (lines.back().empty()) {
      lines.pop_back();
    }
    return lines;
  }
};

// Sanity check that printVector doesn't fail or crash.
TEST_F(VectorPrinterTest, printVectorFuzz) {
  VectorFuzzer::Options options;
  options.vectorSize = 100;
  options.nullRatio = 0.1;

  VectorFuzzer fuzzer(options, pool());
  SelectivityVector rows(options.vectorSize);
  for (auto i = 0; i < rows.size(); i += 2) {
    rows.setValid(i, true);
  }
  rows.updateBounds();

  for (auto i = 0; i < 50; ++i) {
    auto data = fuzzer.fuzz(fuzzer.randType());
    ASSERT_NO_THROW(printVector(*data));

    ASSERT_NO_THROW(printVector(*data, 0, 1'000));
    ASSERT_NO_THROW(printVector(*data, 34, 10));

    ASSERT_NO_THROW(printVector(*data, rows));
  }
}

TEST_F(VectorPrinterTest, printVectorMap) {
  auto data = makeMapVector<int64_t, int64_t>({
      {},
      {{1, 10}},
  });
  ASSERT_NO_THROW(printVector(*data));
}

TEST_F(VectorPrinterTest, summarizeToText) {
  auto data = makeRowVector(
      {"a", "b", "c", "d"},
      {
          makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8}),
          makeArrayVectorFromJson<int64_t>({
              "[1, 2, 3]",
              "[4, 5]",
              "null",
              "[6, 7, 8, 9]",
              "null",
              "null",
              "[10, 11, 12]",
              "[]",
          }),
          wrapInDictionary(
              makeIndices({0, 0, 0, 1, 2, 2, 3, 3}),
              makeMapVectorFromJson<int32_t, float>({
                  "{1: 1.0}",
                  "{3: 3.0, 4: 4.0, 5: 5.0}",
                  "{}",
                  "{6: 6.0, 7: 7.0, 8: 8.0, 9: 9.0}",
              })),
          makeConstant("hello", 8),
      });

  EXPECT_THAT(
      summarizeToLines(*data),
      ::testing::ElementsAre(
          ::testing::MatchesRegex("ROW\\(4\\) 8 rows ROW [0-9]+.*"),
          ::testing::MatchesRegex("   INTEGER 8 rows FLAT [0-9]+.*"),
          ::testing::MatchesRegex("   ARRAY 8 rows ARRAY [0-9]+.*"),
          "         Stats: 3 nulls, 1 empty, sizes: [2...4, avg 3]",
          ::testing::MatchesRegex("      BIGINT 12 rows FLAT [0-9]+.*"),
          ::testing::MatchesRegex("   MAP 8 rows DICTIONARY [0-9]+.*"),
          "         Stats: 0 nulls, 4 unique",
          ::testing::MatchesRegex("      MAP 4 rows MAP [0-9]+.*"),
          "            Stats: 0 nulls, 1 empty, sizes: [1...4, avg 2]",
          ::testing::MatchesRegex("         INTEGER 8 rows FLAT [0-9]+.*"),
          ::testing::MatchesRegex("         REAL 8 rows FLAT [0-9]+.*"),
          ::testing::MatchesRegex("   VARCHAR 8 rows CONSTANT [0-9]+.*")));

  EXPECT_THAT(
      summarizeToLines(
          *data,
          {.types = {}, .includeChildNames = true, .includeNodeIds = true}),
      ::testing::ElementsAre(
          ::testing::MatchesRegex("0 ROW\\(4\\) 8 rows ROW [0-9]+.*"),
          ::testing::MatchesRegex("   0.0 INTEGER 8 rows FLAT [0-9]+.* a"),
          ::testing::MatchesRegex("   0.1 ARRAY 8 rows ARRAY [0-9]+.* b"),
          "         Stats: 3 nulls, 1 empty, sizes: [2...4, avg 3]",
          ::testing::MatchesRegex("      0.1.0 BIGINT 12 rows FLAT [0-9]+.*"),
          ::testing::MatchesRegex("   0.2 MAP 8 rows DICTIONARY [0-9]+.* c"),
          "         Stats: 0 nulls, 4 unique",
          ::testing::MatchesRegex("      0.2.0 MAP 4 rows MAP [0-9]+.*"),
          "            Stats: 0 nulls, 1 empty, sizes: [1...4, avg 2]",
          ::testing::MatchesRegex(
              "         0.2.0.0 INTEGER 8 rows FLAT [0-9]+.*"),
          ::testing::MatchesRegex("         0.2.0.1 REAL 8 rows FLAT [0-9]+.*"),
          ::testing::MatchesRegex(
              "   0.3 VARCHAR 8 rows CONSTANT [0-9]+.* d")));

  EXPECT_THAT(
      summarizeToLines(*data, {.types = {}, .maxChildren = 2}),
      ::testing::ElementsAre(
          ::testing::MatchesRegex("ROW\\(4\\) 8 rows ROW [0-9]+.*"),
          ::testing::MatchesRegex("   INTEGER 8 rows FLAT [0-9]+.*"),
          ::testing::MatchesRegex("   ARRAY 8 rows ARRAY [0-9]+.*"),
          "         Stats: 3 nulls, 1 empty, sizes: [2...4, avg 3]",
          ::testing::MatchesRegex("      BIGINT 12 rows FLAT [0-9]+.*"),
          "   ...2 more"));

  EXPECT_THAT(
      summarizeToLines(
          *data,
          {.types = {}, .maxChildren = 2, .indent = 2, .skipTopSummary = true}),
      ::testing::ElementsAre(
          ::testing::MatchesRegex("         INTEGER 8 rows FLAT [0-9]+.*"),
          ::testing::MatchesRegex("         ARRAY 8 rows ARRAY [0-9]+.*"),
          "               Stats: 3 nulls, 1 empty, sizes: [2...4, avg 3]",
          ::testing::MatchesRegex("            BIGINT 12 rows FLAT [0-9]+.*"),
          "         ...2 more"));
}

} // namespace facebook::velox::test
