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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/window/tests/WindowTestBase.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

class RankTest : public WindowTestBase {
 protected:
  explicit RankTest(const std::string& rankFunction)
      : rankFunction_(rankFunction) {}

  void testTwoColumnInput(const RowVectorPtr& vectors) {
    WindowTestBase::testTwoColumnInput({vectors}, rankFunction_);
  }

  void testWindowFunction(
      const std::vector<RowVectorPtr>& vectors,
      const std::vector<std::string>& overClauses) {
    WindowTestBase::testWindowFunction(vectors, rankFunction_, overClauses);
  }

  const std::string rankFunction_;
};

class MultiRankTest : public RankTest,
                      public testing::WithParamInterface<std::string> {
 public:
  MultiRankTest() : RankTest(GetParam()) {}
};

TEST_P(MultiRankTest, basic) {
  vector_size_t size = 1000;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 10; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
  });

  testTwoColumnInput({vectors});
}

TEST_P(MultiRankTest, singlePartition) {
  // Test all input rows in a single partition.
  vector_size_t size = 1'000;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(
          size, [](auto row) { return row % 50; }, nullEvery(7)),
  });

  testTwoColumnInput({vectors});
}

TEST_P(MultiRankTest, singleRowPartitions) {
  vector_size_t size = 1000;
  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
  });

  testTwoColumnInput({vectors});
}

TEST_P(MultiRankTest, randomInput) {
  auto vectors = makeVectors(
      ROW({"c0", "c1", "c2", "c3"},
          {BIGINT(), SMALLINT(), INTEGER(), BIGINT()}),
      10,
      2,
      0.3);
  createDuckDbTable(vectors);

  std::vector<std::string> overClauses = {
      "partition by c0 order by c1, c2, c3",
      "partition by c1 order by c0, c2, c3",
      "partition by c0 order by c1 desc, c2, c3",
      "partition by c1 order by c0 desc, c2, c3",
      "partition by c0 order by c1 desc nulls first, c2, c3",
      "partition by c1 order by c0 nulls first, c2, c3",
      "partition by c0 order by c1",
      "partition by c0 order by c2",
      "partition by c0 order by c3",
      "partition by c1 order by c0 desc",
      "partition by c0, c1 order by c2, c3",
      "partition by c0, c1 order by c2, c3 nulls first",
      "partition by c0, c1 order by c2",
      "partition by c0, c1 order by c2 nulls first",
      "partition by c0, c1 order by c2 desc",
      "partition by c0, c1 order by c2 desc nulls first",
      "order by c0, c1, c2, c3",
      "order by c0, c1 nulls first, c2, c3",
      "order by c0, c1 desc nulls first, c2, c3",
      "order by c0 nulls first, c1 nulls first, c2, c3",
      "order by c0 nulls first, c1 desc nulls first, c2, c3",
      "order by c0 desc nulls first, c1 nulls first, c2, c3",
      "order by c0 desc nulls first, c1 desc nulls first, c2, c3",
      "partition by c0, c1, c2, c3",
  };

  testWindowFunction(vectors, overClauses);
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    RankTest,
    MultiRankTest,
    testing::ValuesIn(
        {std::string("rank()"),
         std::string("dense_rank()"),
         std::string("percent_rank()"),
         std::string("cume_dist()")}));

}; // namespace
}; // namespace facebook::velox::window::test
