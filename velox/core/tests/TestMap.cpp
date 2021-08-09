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
#include <gtest/gtest.h>
#include "velox/core/CoreTypeSystem.h"

using namespace facebook::velox::core;

namespace {
template <typename MAP>
void mapInsertionTest(MAP& container) {
  // Create a zero initialized entry.
  container.append(0);
  container.append(1) = 2;
  container.append(2) = 3;

  // Create a null entry.
  container.appendNullable(3);
  container.emplace(4, 5);
}

template <typename MAP>
void mapAccessTest(const MAP& container) {
  EXPECT_EQ(container.size(), 5);
  EXPECT_EQ(container.at(2).value(), 3);
  EXPECT_THROW(container.at(5), std::out_of_range);
  EXPECT_TRUE(container.at(0).has_value());
  EXPECT_EQ(container.at(0).value(), typename MAP::val_type{});
  EXPECT_EQ(container.find(1)->second.value(), 2);
  EXPECT_FALSE(container.at(3).has_value());
  EXPECT_TRUE(container.contains(3));
  EXPECT_FALSE(container.contains(5));
}
} // namespace

TEST(Map, MapVal) {
  SlowMapVal<size_t, size_t> val;
  EXPECT_EQ(val.size(), 0);

  mapInsertionTest(val);
  mapAccessTest(val);
}

TEST(Map, MapWriter) {
  SlowMapWriter<size_t, size_t> writer;
  EXPECT_EQ(writer.size(), 0);

  mapInsertionTest(writer);
  mapAccessTest(writer);

  writer.clear();
  EXPECT_EQ(writer.size(), 0);
}

TEST(Map, MapCopy) {
  SlowMapVal<size_t, size_t> val;
  mapInsertionTest(val);

  SlowMapWriter<size_t, size_t> writer;

  // Support entire copy from a reader.
  writer = val;

  mapAccessTest(writer);
}
