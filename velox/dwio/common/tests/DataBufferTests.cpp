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
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/DataBuffer.h"

namespace facebook {
namespace velox {
namespace dwio {
namespace common {
using namespace facebook::velox::memory;
using namespace testing;
using MemoryPool = facebook::velox::memory::MemoryPool;

class DataBufferTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    MemoryManager::testingSetInstance({});
  }

  const std::shared_ptr<MemoryPool> pool_ = memoryManager()->addLeafPool();
};

TEST_F(DataBufferTest, ZeroOut) {
  const uint8_t VALUE = 13;
  DataBuffer<uint8_t> buffer(*pool_, 16);
  for (auto i = 0; i < buffer.size(); i++) {
    auto data = buffer.data();
    ASSERT_EQ(data[i], 0);
    data[i] = VALUE;
  }

  buffer.resize(8);
  for (auto i = 0; i < buffer.size(); i++) {
    auto data = buffer.data();
    ASSERT_EQ(data[i], VALUE);
  }

  auto currentSize = buffer.size();
  ASSERT_EQ(currentSize, 8);
  buffer.resize(32);
  for (auto i = 0; i < buffer.size(); i++) {
    auto data = buffer.data();
    if (i < currentSize) {
      ASSERT_EQ(data[i], VALUE);
    } else {
      ASSERT_EQ(data[i], 0);
    }
  }
}

TEST_F(DataBufferTest, At) {
  DataBuffer<uint8_t> buffer{*pool_};
  for (auto i = 0; i != 15; ++i) {
    buffer.append(i);
  }
  ASSERT_EQ(15, buffer.size());

  for (auto i = 0; i != 15; ++i) {
    EXPECT_EQ(i, buffer.at(i));
  }

  buffer.resize(8);
  for (auto i = 0; i != 8; ++i) {
    EXPECT_EQ(i, buffer.at(i));
  }
  for (auto i = 8; i != 42; ++i) {
    EXPECT_THROW(buffer.at(i), exception::LoggedException);
  }
}

TEST_F(DataBufferTest, Reset) {
  DataBuffer<uint8_t> buffer{*pool_};
  buffer.reserve(16);
  for (auto i = 0; i != 15; ++i) {
    buffer.append(i);
  }
  ASSERT_EQ(15, buffer.size());
  ASSERT_EQ(16, buffer.capacity());
  {
    buffer.clear();
    EXPECT_EQ(0, buffer.size());
    EXPECT_EQ(0, buffer.capacity());

    buffer.reserve(12);
    EXPECT_EQ(0, buffer.size());
    EXPECT_EQ(12, buffer.capacity());
    for (auto i = 0; i != 11; ++i) {
      buffer.append(i);
    }
    EXPECT_EQ(11, buffer.size());
    EXPECT_EQ(12, buffer.capacity());
  }

  {
    buffer.clear();
    EXPECT_EQ(0, buffer.size());
    EXPECT_EQ(0, buffer.capacity());

    buffer.reserve(16);
    EXPECT_EQ(0, buffer.size());
    EXPECT_EQ(16, buffer.capacity());
    for (auto i = 0; i != 15; ++i) {
      buffer.append(i);
    }
    EXPECT_EQ(15, buffer.size());
    EXPECT_EQ(16, buffer.capacity());
  }

  {
    buffer.clear();
    EXPECT_EQ(0, buffer.size());
    EXPECT_EQ(0, buffer.capacity());

    buffer.reserve(32);
    EXPECT_EQ(0, buffer.size());
    EXPECT_EQ(32, buffer.capacity());
    for (auto i = 0; i != 31; ++i) {
      buffer.append(i);
    }
    EXPECT_EQ(31, buffer.size());
    EXPECT_EQ(32, buffer.capacity());
  }
}

TEST_F(DataBufferTest, Wrap) {
  auto size = 26;
  auto buffer = velox::AlignedBuffer::allocate<char>(size, pool_.get());
  auto raw = buffer->asMutable<char>();
  for (size_t i = 0; i < size; ++i) {
    raw[i] = 'a' + i;
  }
  auto dataBuffer = DataBuffer<char>::wrap(buffer);
  buffer = nullptr;
  ASSERT_EQ(size, dataBuffer->size());
  ASSERT_EQ(size, dataBuffer->capacity());
  for (size_t i = 0; i < size; ++i) {
    ASSERT_EQ((*dataBuffer)[i], 'a' + i);
  }
}

TEST_F(DataBufferTest, Move) {
  {
    DataBuffer<uint8_t> buffer{*pool_};
    buffer.reserve(16);
    for (auto i = 0; i != 15; ++i) {
      buffer.append(i);
    }
    ASSERT_EQ(15, buffer.size());
    ASSERT_EQ(16, buffer.capacity());
    const auto usedBytes = pool_->usedBytes();

    // Expect no double freeing from memory pool.
    DataBuffer<uint8_t> newBuffer{std::move(buffer)};
    ASSERT_EQ(15, newBuffer.size());
    ASSERT_EQ(16, newBuffer.capacity());
    ASSERT_EQ(usedBytes, pool_->usedBytes());
  }
  ASSERT_EQ(0, pool_->usedBytes());
}
} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
