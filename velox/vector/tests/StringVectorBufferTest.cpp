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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/vector/StringVectorBuffer.h"

namespace facebook::velox::test {

class StringVectorBufferTest : public ::testing::Test {
 protected:
  void testAppendAndFlush(
      const std::vector<std::string>& stringVec,
      int32_t initialCapacity,
      int32_t maxCapacity) {
    auto batchSize = 10;
    // Create a FlatVector to use with StringVectorBuffer.
    auto vector = BaseVector::create<FlatVector<StringView>>(
        VARBINARY(), batchSize, pool_.get());
    // Create a StringVectorBuffer with initial and max capacity.
    StringVectorBuffer buffer(vector.get(), initialCapacity, maxCapacity);
    // Append each character of the string to the buffer.
    int32_t offset = 0;
    for (const std::string& str : stringVec) {
      for (char c : str) {
        buffer.appendByte(c);
      }
      buffer.flushRow(offset++);
    }
    // Check that the vector has the expected data.
    for (int i = 0; i < stringVec.size(); ++i) {
      EXPECT_EQ(vector->valueAt(i), StringView(stringVec[i]));
    }
  }

  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_ =
      velox::memory::deprecatedAddDefaultLeafMemoryPool();
};

TEST_F(StringVectorBufferTest, resize) {
  auto batchSize = 10;
  auto vector = BaseVector::create<FlatVector<StringView>>(
      VARBINARY(), batchSize, pool_.get());

  StringVectorBuffer buffer(vector.get(), 10, 100);

  // Append data to trigger resize.
  // This will trigger resize twice. The first time it will allocate additional
  // (10 + 1) bytes. Because the buffer is already at 10 bytes and is not
  // flushed, it will be copied to the new buffer.
  // At the end of writing the first 15 bytes, the buffer should be at a
  // total capacity of 42 bytes. (10 + 11 + 21)
  for (int8_t i = 0; i < 15; ++i) {
    buffer.appendByte(i);
  }

  // Check that the buffer resized properly to fit 15 bytes.
  buffer.flushRow(0);
  EXPECT_EQ(vector->valueAt(0).size(), 15);

  // fill exisitng buffer of 21 bytes
  for (int8_t i = 15; i < 21; ++i) {
    buffer.appendByte(i);
  }
  buffer.flushRow(1);

  // resize to fit additional 42 bytes
  // total capacity = 84 bytes
  for (int8_t i = 0; i < 42; ++i) {
    buffer.appendByte(i);
  }
  buffer.flushRow(2);

  // resize to max capacity of 100 bytes
  for (int8_t i = 84; i < 100; ++i) {
    buffer.appendByte(i);
  }
  buffer.flushRow(3);

  VELOX_ASSERT_THROW(
      buffer.appendByte(100),
      "Cannot grow buffer with totalCapacity:100B to meet minRequiredCapacity:101B");
  // Check that the buffer resized properly to fit 42 bytes.
  EXPECT_EQ(vector->valueAt(0).size(), 15);
  EXPECT_EQ(vector->valueAt(1).size(), 6);
  EXPECT_EQ(vector->valueAt(2).size(), 42);
  EXPECT_EQ(vector->valueAt(3).size(), 16);
}

TEST_F(StringVectorBufferTest, maxCapacity) {
  auto batchSize = 10;
  auto vector = BaseVector::create<FlatVector<StringView>>(
      VARBINARY(), batchSize, pool_.get());

  StringVectorBuffer buffer(vector.get(), 10, 20);

  // Append data to reach max capacity.
  for (int8_t i = 0; i < 10; ++i) {
    buffer.appendByte(i);
  }
  buffer.flushRow(0);
  for (int8_t i = 10; i < 20; ++i) {
    buffer.appendByte(i);
  }

  VELOX_ASSERT_THROW(
      buffer.appendByte(21),
      "Cannot grow buffer with totalCapacity:20B to meet minRequiredCapacity:31B");
}

TEST_F(StringVectorBufferTest, appendAndFlushBasic) {
  std::vector<std::string> stringVec = {"example", "strings", "to", "append"};
  testAppendAndFlush(stringVec, 10, 100);
}

TEST_F(StringVectorBufferTest, appendAndFlushLongString) {
  // Define a long string that will trigger a resize.
  std::vector<std::string> stringVec = {"I am a very very very long string"};
  testAppendAndFlush(stringVec, 10, 100);
}

TEST_F(StringVectorBufferTest, appendAndFlushMaxCapacity) {
  std::vector<std::string> stringVec = {"I am a very very very long string"};
  VELOX_ASSERT_THROW(
      testAppendAndFlush(stringVec, 20, 20),
      "Cannot grow buffer with totalCapacity:20B to meet minRequiredCapacity:41B");
}
} // namespace facebook::velox::test
