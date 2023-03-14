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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <string>

#include "velox/buffer/StringViewBufferHolder.h"
#include "velox/common/memory/Memory.h"
#include "velox/type/StringView.h"

namespace facebook::velox {
namespace {
std::string nonInlinedString() {
  return std::string(13, 'a');
}

std::string inlinedString() {
  return "a";
}
} // namespace

class StringViewBufferHolderTest : public testing::Test {
 protected:
  StringViewBufferHolder makeHolder() {
    return StringViewBufferHolder(pool_.get());
  }

  std::shared_ptr<memory::MemoryPool> pool_{memory::getDefaultMemoryPool()};
};

TEST_F(StringViewBufferHolderTest, inlinedStringViewDoesNotCopyToBuffer) {
  auto holder = makeHolder();

  std::string value = inlinedString();
  StringView originalStringViewValue(value);
  ASSERT_TRUE(originalStringViewValue.isInline());

  auto ownedStringView = holder.getOwnedValue(originalStringViewValue);
  ASSERT_TRUE(ownedStringView.isInline());

  ASSERT_EQ(originalStringViewValue, ownedStringView);

  auto buffers = holder.moveBuffers();
  ASSERT_EQ(0, buffers.size());
}

TEST_F(StringViewBufferHolderTest, nonInlinedStringViewCopiesToBuffer) {
  auto holder = makeHolder();

  std::string value = nonInlinedString();
  StringView originalStringViewValue(value);
  ASSERT_FALSE(originalStringViewValue.isInline());

  auto ownedStringView = holder.getOwnedValue(originalStringViewValue);
  ASSERT_FALSE(ownedStringView.isInline());

  ASSERT_EQ(originalStringViewValue, ownedStringView);

  auto buffers = holder.moveBuffers();
  ASSERT_EQ(1, buffers.size());

  ASSERT_EQ(ownedStringView.data(), buffers.at(0)->as<char>());
}

TEST_F(StringViewBufferHolderTest, moreBuffersAreUsedWhenCurrentBufferFills) {
  // We're setting this high enough such that a new buffer allocation should be
  // needed by the StringViewBufferHolder. The min allocation is set to 8KB, but
  // the memory allocator allocates more than the requested size, so just
  // setting to much higher to guarantee the first buffer allocated is filled.
  size_t approxBytesToAdd = 1024 * 20;

  size_t nonInlinedBytesAdded = 0;
  auto holder = makeHolder();
  std::vector<std::string> addedStrings;

  std::vector<StringView> ownedStringViews;

  auto inlineIndex = [](size_t index) { return index % 2 == 0; };

  while (nonInlinedBytesAdded < approxBytesToAdd) {
    if (inlineIndex(addedStrings.size())) {
      addedStrings.push_back(inlinedString());
    } else {
      // Just to make sure the tests don't pass if only the memory location at
      // offset 0 was being used, change the suffix of the string each time
      addedStrings.push_back(
          nonInlinedString() + std::to_string(addedStrings.size()));
      nonInlinedBytesAdded += addedStrings.back().size();
    }

    ownedStringViews.push_back(
        holder.getOwnedValue(StringView(addedStrings.back())));
  }

  auto buffers = holder.moveBuffers();

  // Double checking that approxBytesToAdd was large enough to cause more than
  // one buffer to be allocated.
  ASSERT_GT(buffers.size(), 1);

  // Intentionally doing the comparison at the end in case memory was getting
  // overridden
  for (size_t i = 0; i < ownedStringViews.size(); ++i) {
    ASSERT_EQ(inlineIndex(i), ownedStringViews.at(i).isInline());
    ASSERT_EQ(addedStrings.at(i), ownedStringViews.at(i).str());
    ASSERT_NE(addedStrings.at(i).data(), ownedStringViews.at(i).data());
  }
}

TEST_F(StringViewBufferHolderTest, stateIsClearedAfterStringsAreMoved) {
  auto holder = makeHolder();
  std::string value = nonInlinedString();

  holder.getOwnedValue(StringView(value));

  auto firstMoved = holder.moveBuffers();
  ASSERT_EQ(1, firstMoved.size());

  auto secondMoved = holder.moveBuffers();
  ASSERT_EQ(0, secondMoved.size());
}

TEST_F(StringViewBufferHolderTest, getOwnedValueCanBeCalledWithIntegerType) {
  auto holder = makeHolder();
  ASSERT_EQ(42, holder.getOwnedValue(42));
  ASSERT_EQ(0, holder.moveBuffers().size());
}

TEST_F(StringViewBufferHolderTest, getOwnedValueCanBeCalledWithStringType) {
  const char* buf = "abcdefghijklmnopqrstuvxz";
  StringView result;

  auto holder = makeHolder();
  ASSERT_EQ(0, holder.buffers().size());

  {
    std::string str = buf;
    result = holder.getOwnedValue(str);
  }
  // Make sure `str` is already destructed at this point.
  ASSERT_EQ(StringView(buf), result);
  ASSERT_EQ(1, holder.buffers().size());
}

TEST_F(
    StringViewBufferHolderTest,
    getOwnedValueCanBeCalledWithStringPieceType) {
  const char* buf = "abcdefghijklmnopqrstuvxz";
  StringView result;
  folly::StringPiece piece;

  auto holder = makeHolder();
  ASSERT_EQ(0, holder.buffers().size());

  {
    std::string str = buf;
    piece = str;
    result = holder.getOwnedValue(piece);
  }

  // `str` is already destructed and piece is invalid.
  ASSERT_EQ(StringView(buf), result);
  ASSERT_EQ(1, holder.buffers().size());
}

TEST_F(StringViewBufferHolderTest, buffersCopy) {
  auto holder = makeHolder();
  holder.getOwnedValue(nonInlinedString());

  auto buffers = holder.buffers();
  EXPECT_EQ(1, buffers.size());

  auto firstMoved = holder.moveBuffers();
  EXPECT_EQ(1, firstMoved.size());
  EXPECT_EQ(1, buffers.size());

  auto secondMoved = holder.moveBuffers();
  EXPECT_EQ(0, secondMoved.size());
  EXPECT_EQ(1, firstMoved.size());
  EXPECT_EQ(1, buffers.size());

  auto secondBuffers = holder.buffers();
  EXPECT_EQ(0, secondBuffers.size());
  EXPECT_EQ(0, secondMoved.size());
  EXPECT_EQ(1, firstMoved.size());
  EXPECT_EQ(1, buffers.size());

  EXPECT_EQ(buffers.back()->as<uint8_t>(), firstMoved.back()->as<uint8_t>());
  EXPECT_NE(&buffers, &firstMoved);
}

} // namespace facebook::velox
