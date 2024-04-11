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

#include "velox/dwio/common/FileSink.h"

#include <gtest/gtest.h>

namespace facebook::velox::dwio::common {

class MemorySinkTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

TEST_F(MemorySinkTest, create) {
  std::string chars("abcdefghijklmnopqrst");
  std::vector<DataBuffer<char>> buffers;

  // Add 'abcdefghij' to first buffer
  buffers.emplace_back(*pool_);
  buffers.back().append(0, chars.data(), 10);

  // Add 'klmnopqrst' to second buffer
  buffers.emplace_back(*pool_);
  buffers.back().append(0, chars.data() + 10, 10);

  ASSERT_EQ(buffers.size(), 2);

  auto memorySink = std::make_unique<MemorySink>(
      1024, dwio::common::FileSink::Options{.pool = pool_.get()});

  ASSERT_TRUE(memorySink->isBuffered());
  // Write data to MemorySink.
  memorySink->write(buffers);
  ASSERT_EQ(memorySink->size(), chars.length());
  ASSERT_EQ(memorySink->data(), chars);
  memorySink->close();
}
} // namespace facebook::velox::dwio::common
