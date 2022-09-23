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
#include "velox/functions/prestosql/window/tests/WindowTestBase.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

class RankTest : public WindowTestBase {};

TEST_F(RankTest, basic) {
  vector_size_t size = 1000;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(
          size, [](auto row) -> int32_t { return row % 10; }),
      makeFlatVector<int32_t>(
          size, [](auto row) -> int32_t { return row % 7; }),
  });

  testTwoColumnInput({vectors}, "rank");
}

TEST_F(RankTest, singlePartition) {
  // Test all input rows in a single partition.
  vector_size_t size = 1'000;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
  });

  testTwoColumnInput({vectors}, "rank");
}

TEST_F(RankTest, randomInput) {
  auto vectors = makeVectors(
      ROW({"c0", "c1", "c2", "c3"},
          {BIGINT(), SMALLINT(), INTEGER(), BIGINT()}),
      10,
      2);
  createDuckDbTable(vectors);

  std::vector<std::string> overClauses = {
      "partition by c0 order by c1, c2, c3",
      "partition by c1 order by c0, c2, c3",
      "partition by c0 order by c1 desc, c2, c3",
      "partition by c1 order by c0 desc, c2, c3",
      "partition by c0 order by c1",
      "partition by c0 order by c2",
      "partition by c0 order by c3",
      "partition by c1 order by c0 desc",
      "partition by c0, c1 order by c2, c3",
      "partition by c0, c1 order by c2",
      "partition by c0, c1 order by c2 desc",
      "order by c0, c1, c2, c3",
      "partition by c0, c1, c2, c3",
  };

  testWindowFunction(vectors, "rank", overClauses);
}

}; // namespace
}; // namespace facebook::velox::window::test
