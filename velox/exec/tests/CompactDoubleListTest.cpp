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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "velox/exec/CompactDoubleList.h"

using namespace facebook::velox;

namespace {
int32_t listSize(const CompactDoubleList& list) {
  int32_t count = 0;
  for (auto element = list.next(); element != &list;
       element = element->next()) {
    ++count;
  }
  return count;
}
} // namespace

TEST(CompactDoubleListTest, basic) {
  constexpr int32_t kNumElements = 10;
  CompactDoubleList list;
  std::array<CompactDoubleList, kNumElements> elements;
  EXPECT_TRUE(list.empty());
  for (auto i = 0; i < kNumElements; ++i) {
    EXPECT_EQ(i, listSize(list));
    list.insert(&elements[i]);
  }
  for (auto i = 0; i < kNumElements; ++i) {
    EXPECT_EQ(kNumElements - i, listSize(list));
    elements[i].remove();
  }
  EXPECT_TRUE(list.empty());
}
