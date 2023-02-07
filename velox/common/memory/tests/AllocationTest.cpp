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
#include "velox/common/memory/Memory.h"

using namespace ::testing;
using namespace facebook::velox::memory;

namespace facebook::velox::exec::test {

class AllocationTest : public testing::Test {};

// This test is to verify that Allocation doesn't merge different append buffers
// into the same PageRun even if two buffers are contiguous in memory space.
TEST_F(AllocationTest, append) {
  Allocation allocation;
  const uint64_t startBufAddrValue = 4096;
  uint8_t* const firstBufAddr = reinterpret_cast<uint8_t*>(startBufAddrValue);
  const int32_t kNumPages = 10;
  allocation.append(firstBufAddr, kNumPages);
  ASSERT_EQ(allocation.numPages(), kNumPages);
  ASSERT_EQ(allocation.numRuns(), 1);
  uint8_t* const secondBufAddr = reinterpret_cast<uint8_t*>(
      startBufAddrValue + kNumPages * AllocationTraits::kPageSize);
  allocation.append(secondBufAddr, kNumPages - 1);
  ASSERT_EQ(allocation.numPages(), kNumPages * 2 - 1);
  ASSERT_EQ(allocation.numRuns(), 2);
  uint8_t* const thirdBufAddr = reinterpret_cast<uint8_t*>(
      firstBufAddr + 4 * kNumPages * AllocationTraits::kPageSize);
  allocation.append(thirdBufAddr, kNumPages * 2);
  ASSERT_EQ(allocation.numPages(), kNumPages * 4 - 1);
  ASSERT_EQ(allocation.numRuns(), 3);
  VELOX_ASSERT_THROW(allocation.append(thirdBufAddr, kNumPages), "");
  allocation.clear();
}
} // namespace facebook::velox::exec::test
