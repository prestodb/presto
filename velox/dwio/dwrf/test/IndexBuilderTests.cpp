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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/dwio/dwrf/writer/IndexBuilder.h"

using namespace testing;

namespace facebook::velox::dwrf {

class IndexBuilderTest : public testing::Test {
 protected:
  static const proto::RowIndexEntry& getEntry(
      IndexBuilder& builder,
      size_t index) {
    return *builder.getEntry(index);
  }

  static std::vector<uint64_t> getPositions(
      IndexBuilder& builder,
      size_t index) {
    auto& positions = builder.getEntry(index)->positions();
    return std::vector<uint64_t>{positions.begin(), positions.end()};
  }

  StatisticsBuilderOptions options_{16};
};

TEST_F(IndexBuilderTest, Constructor) {
  IndexBuilder builder{nullptr};
  EXPECT_EQ(1, builder.getEntrySize());
  // Ensure a clean start.
  EXPECT_EQ(0, getEntry(builder, 0).positions_size());
}

TEST_F(IndexBuilderTest, AddEntry) {
  IndexBuilder builder{nullptr};
  ASSERT_EQ(1, builder.getEntrySize());
  builder.add(0uL);
  builder.add(42uL);
  ASSERT_THAT(getPositions(builder, 0), ElementsAreArray({0uL, 42uL}));

  StatisticsBuilder sb{options_};
  for (size_t i = 0; i != 50; ++i) {
    builder.addEntry(sb);
  }

  // The existing entry should be intact.
  EXPECT_THAT(getPositions(builder, 0), ElementsAreArray({0uL, 42uL}));
  ASSERT_EQ(51, builder.getEntrySize());
  for (size_t i = 0; i != 50; ++i) {
    // The newly added entries should be empty.
    EXPECT_EQ(0, getEntry(builder, i + 1).positions_size());
    EXPECT_TRUE(getEntry(builder, i).has_statistics());
  }
}

TEST_F(IndexBuilderTest, Add) {
  IndexBuilder builder{nullptr};
  builder.add(0uL);
  EXPECT_THAT(getPositions(builder, 0), ElementsAreArray({0uL}));
  builder.add(42uL);
  EXPECT_THAT(getPositions(builder, 0), ElementsAreArray({0uL, 42uL}));

  StatisticsBuilder sb{options_};
  builder.addEntry(sb);
  builder.add(1uL);
  EXPECT_THAT(getPositions(builder, 0), ElementsAreArray({0uL, 42uL}));
  EXPECT_THAT(getPositions(builder, 1), ElementsAreArray({1uL}));

  builder.addEntry(sb);
  builder.add(144uL);
  EXPECT_THAT(getPositions(builder, 0), ElementsAreArray({0uL, 42uL}));
  EXPECT_THAT(getPositions(builder, 1), ElementsAreArray({1uL}));
  EXPECT_THAT(getPositions(builder, 2), ElementsAreArray({144uL}));
}

TEST_F(IndexBuilderTest, Backfill) {
  IndexBuilder builder{nullptr};
  StatisticsBuilder sb{options_};
  builder.addEntry(sb);
  ASSERT_EQ(0, getEntry(builder, 0).positions_size());
  builder.add(0uL);
  ASSERT_EQ(0, getEntry(builder, 0).positions_size());
  ASSERT_THAT(getPositions(builder, 1), ElementsAreArray({0uL}));

  builder.add(42uL, 0);
  // Welp, this is a paranoid test that the two versions of add can
  // work together properly.
  builder.add(0uL);
  builder.add(7uL, 1);
  EXPECT_THAT(getPositions(builder, 0), ElementsAreArray({42uL}));
  EXPECT_THAT(getPositions(builder, 1), ElementsAreArray({0uL, 0uL, 7uL}));

  // Insert randomly in the middle.
  for (size_t i = 0; i != 5; ++i) {
    builder.addEntry(sb);
  }
  for (size_t i = 2; i != 7; ++i) {
    ASSERT_EQ(0, getEntry(builder, i).positions_size());
  }
  builder.add(144uL, 4);
  EXPECT_THAT(getPositions(builder, 0), ElementsAreArray({42uL}));
  EXPECT_THAT(getPositions(builder, 1), ElementsAreArray({0uL, 0uL, 7uL}));
  ASSERT_EQ(0, getEntry(builder, 2).positions_size());
  ASSERT_EQ(0, getEntry(builder, 3).positions_size());
  EXPECT_THAT(getPositions(builder, 4), ElementsAreArray({144uL}));
  ASSERT_EQ(0, getEntry(builder, 5).positions_size());
  ASSERT_EQ(0, getEntry(builder, 6).positions_size());
}

TEST_F(IndexBuilderTest, RemovePresentStreamPositions) {
  IndexBuilder builder{nullptr};
  auto indexCount = 3;
  StatisticsBuilder sb{options_};
  for (auto i = 0; i < indexCount; ++i) {
    builder.add(1);
    builder.capturePresentStreamOffset();
    for (auto j = 2; j < 10; ++j) {
      builder.add(j);
    }
    builder.addEntry(sb);
  }

  builder.removePresentStreamPositions(false);
  for (auto i = 0; i < indexCount; ++i) {
    EXPECT_THAT(getPositions(builder, i), ElementsAreArray({1, 5, 6, 7, 8, 9}));
  }
  builder.removePresentStreamPositions(true);
  for (auto i = 0; i < indexCount; ++i) {
    EXPECT_THAT(getPositions(builder, i), ElementsAreArray({1, 9}));
  }
}

} // namespace facebook::velox::dwrf
