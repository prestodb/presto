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
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::functions::test;

class MapEntriesTest : public FunctionBaseTest {};

TEST_F(MapEntriesTest, basic) {
  vector_size_t size = 1'000;

  auto map = makeMapVector<int64_t, int64_t>(
      size,
      [](vector_size_t row) { return row % 5; },
      [](vector_size_t row) { return row % 7; },
      [](vector_size_t row) { return row % 11; },
      nullEvery(13));

  auto result = evaluate<ArrayVector>("map_entries(C0)", makeRowVector({map}));
  ASSERT_EQ(size, result->size());

  auto resultKeys = result->elements()->as<RowVector>()->childAt(0);
  auto resultValues = result->elements()->as<RowVector>()->childAt(1);
  for (auto i = 0; i < size; i++) {
    auto isNull = map->isNullAt(i);
    ASSERT_EQ(isNull, result->isNullAt(i)) << "at " << i;
    if (!isNull) {
      auto mapSize = map->sizeAt(i);
      ASSERT_EQ(mapSize, result->sizeAt(i)) << "at " << i;
      for (auto j = 0; j < mapSize; j++) {
        ASSERT_TRUE(map->mapKeys()->equalValueAt(
            resultKeys.get(), map->offsetAt(i) + j, result->offsetAt(i) + j));
        ASSERT_TRUE(map->mapValues()->equalValueAt(
            resultValues.get(), map->offsetAt(i) + j, result->offsetAt(i) + j));
      }
    }
  }
}
