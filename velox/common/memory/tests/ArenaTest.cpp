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
#include "velox/common/memory/Arena.h"

#include <string>

#include "gtest/gtest.h"

using namespace facebook::velox;

TEST(ArenaTest, smallAllocations) {
  Arena arena;
  char* pos1 = arena.reserve(10);
  memset(pos1, 'x', 10);
  char* pos2 = arena.reserve(7);
  memset(pos2, 'y', 7);
  arena.reserve(1);
  for (int i = 0; i < 10; ++i)
    ASSERT_EQ('x', pos1[i]);
  for (int i = 0; i < 7; ++i)
    ASSERT_EQ('y', pos2[i]);
}

TEST(ArenaTest, largeAllocations) {
  Arena arena(123);
  constexpr int kBufSize1 = 5 * 1000 * 1000;
  char* pos1 = arena.reserve(kBufSize1);
  memset(pos1, 'x', kBufSize1);
  constexpr int kBufSize2 = 3;
  char* pos2 = arena.reserve(kBufSize2);
  memset(pos2, 'y', kBufSize2);
  constexpr int kBufSize3 = 11 * 1000 * 1000;
  char* pos3 = arena.reserve(kBufSize3);
  memset(pos3, 'z', kBufSize3);
  for (int i = 0; i < kBufSize1; ++i)
    ASSERT_EQ('x', pos1[i]);
  for (int i = 0; i < kBufSize2; ++i)
    ASSERT_EQ('y', pos2[i]);
  for (int i = 0; i < kBufSize3; ++i)
    ASSERT_EQ('z', pos3[i]);
}

TEST(ArenaTest, writeString) {
  Arena arena;
  std::vector<std::string_view> arena_fruits;
  // Add a scope so the strings we define within are destroyed  before the end
  // of the test, letting us know that our string views are pointing into the
  // arena.
  {
    const std::string apple = "apple";
    const std::string banana = "banana";
    const std::string grape = "grape";
    std::string big_pear(4 * 1000 * 1000, 0);
    for (int i = 0; i < 1000 * 1000; ++i) {
      big_pear[4 * i] = 'p';
      big_pear[4 * i + 1] = 'e';
      big_pear[4 * i + 2] = 'a';
      big_pear[4 * i + 3] = 'r';
    }
    std::vector<std::string_view> fruits = {apple, banana, big_pear, grape};
    for (auto fruit : fruits)
      arena_fruits.push_back(arena.writeString(fruit));
  }
  ASSERT_EQ(arena_fruits[0], "apple");
  ASSERT_EQ(arena_fruits[1], "banana");
  ASSERT_EQ(arena_fruits[2].size(), 4 * 1000 * 1000);
  ASSERT_EQ(arena_fruits[2].find("pear"), 0);
  ASSERT_EQ(arena_fruits[3], "grape");
}
