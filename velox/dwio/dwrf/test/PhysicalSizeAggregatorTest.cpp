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

#include <gtest/gtest.h>

#include "velox/dwio/dwrf/writer/PhysicalSizeAggregator.h"

using namespace ::testing;

namespace facebook::velox::dwrf {
TEST(PhysicalSizeAggregatorTest, UpdateLeaf) {
  auto leafOne = std::make_unique<PhysicalSizeAggregator>(nullptr);
  auto leafTwo = std::make_unique<PhysicalSizeAggregator>(nullptr);
  ASSERT_EQ(0, leafOne->getResult());
  ASSERT_EQ(0, leafTwo->getResult());

  leafOne->recordSize(
      DwrfStreamIdentifier{0, 0, 0, StreamKind::StreamKind_DATA}, 1);
  EXPECT_EQ(1, leafOne->getResult());
  EXPECT_EQ(0, leafTwo->getResult());

  leafTwo->recordSize(
      DwrfStreamIdentifier{1, 0, 0, StreamKind::StreamKind_DATA}, 2);
  EXPECT_EQ(1, leafOne->getResult());
  EXPECT_EQ(2, leafTwo->getResult());

  leafOne->recordSize(
      DwrfStreamIdentifier{2, 0, 0, StreamKind::StreamKind_DATA}, 4);
  EXPECT_EQ(5, leafOne->getResult());
  EXPECT_EQ(2, leafTwo->getResult());
}

TEST(PhysicalSizeAggregatorTest, UpdateParent) {
  auto parent = std::make_unique<PhysicalSizeAggregator>(nullptr);
  auto childOne = std::make_unique<PhysicalSizeAggregator>(parent.get());
  auto childTwo = std::make_unique<PhysicalSizeAggregator>(parent.get());
  ASSERT_EQ(0, parent->getResult());
  ASSERT_EQ(0, childOne->getResult());
  ASSERT_EQ(0, childTwo->getResult());

  parent->recordSize(
      DwrfStreamIdentifier{0, 0, 0, StreamKind::StreamKind_DATA}, 1);
  ASSERT_EQ(1, parent->getResult());
  ASSERT_EQ(0, childOne->getResult());
  ASSERT_EQ(0, childTwo->getResult());

  childOne->recordSize(
      DwrfStreamIdentifier{1, 0, 0, StreamKind::StreamKind_DATA}, 2);
  EXPECT_EQ(3, parent->getResult());
  EXPECT_EQ(2, childOne->getResult());
  EXPECT_EQ(0, childTwo->getResult());

  childTwo->recordSize(
      DwrfStreamIdentifier{2, 0, 0, StreamKind::StreamKind_DATA}, 4);
  EXPECT_EQ(7, parent->getResult());
  EXPECT_EQ(2, childOne->getResult());
  EXPECT_EQ(4, childTwo->getResult());
}
} // namespace facebook::velox::dwrf
