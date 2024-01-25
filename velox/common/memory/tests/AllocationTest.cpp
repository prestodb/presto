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

namespace facebook::velox::memory {

class AllocationTest : public testing::Test {};

TEST_F(AllocationTest, basic) {
  ASSERT_EQ(AllocationTraits::numPagesInHugePage(), 512);
  ASSERT_EQ(AllocationTraits::roundUpPageBytes(0), 0);
  ASSERT_EQ(AllocationTraits::roundUpPageBytes(1), AllocationTraits::kPageSize);
  ASSERT_EQ(
      AllocationTraits::roundUpPageBytes(4093), AllocationTraits::kPageSize);
  ASSERT_EQ(
      AllocationTraits::roundUpPageBytes(4094), AllocationTraits::kPageSize);
}

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

TEST_F(AllocationTest, appendMove) {
  const uint64_t startBufAddrValue = 4096;
  uint8_t* const firstBufAddr = reinterpret_cast<uint8_t*>(startBufAddrValue);
  const int32_t kNumPages = 10;
  Allocation allocation;
  allocation.append(firstBufAddr, kNumPages);
  ASSERT_EQ(allocation.numPages(), kNumPages);
  ASSERT_EQ(allocation.numRuns(), 1);

  Allocation otherAllocation;
  uint8_t* const secondBufAddr = reinterpret_cast<uint8_t*>(
      startBufAddrValue + kNumPages * AllocationTraits::kPageSize);
  otherAllocation.append(secondBufAddr, kNumPages);
  ASSERT_EQ(otherAllocation.numPages(), kNumPages);

  // 'allocation' gets all the runs of 'otherAllocation' and 'otherAllocation'
  // is left empty.
  allocation.appendMove(otherAllocation);
  ASSERT_EQ(kNumPages * 2, allocation.numPages());
  ASSERT_EQ(0, otherAllocation.numPages());
  ASSERT_EQ(2, allocation.numRuns());
  ASSERT_EQ(0, otherAllocation.numRuns());
  allocation.clear();
}

TEST_F(AllocationTest, maxPageRunLimit) {
  Allocation allocation;
  const uint64_t vaildBufAddrValue = 4096;
  uint8_t* validBufAddr = reinterpret_cast<uint8_t*>(vaildBufAddrValue);
  allocation.append(validBufAddr, Allocation::PageRun::kMaxPagesInRun);
  ASSERT_EQ(allocation.numPages(), Allocation::PageRun::kMaxPagesInRun);
  ASSERT_EQ(allocation.numRuns(), 1);

  const uint64_t invaildBufAddrValue = 4096 * 1024;
  uint8_t* invalidBufAddr = reinterpret_cast<uint8_t*>(invaildBufAddrValue);
  VELOX_ASSERT_THROW(
      allocation.append(
          invalidBufAddr, Allocation::PageRun::kMaxPagesInRun + 1),
      "The number of pages to append 65536 exceeds the PageRun limit 65535");
  VELOX_ASSERT_THROW(
      allocation.append(
          invalidBufAddr, Allocation::PageRun::kMaxPagesInRun * 2),
      "The number of pages to append 131070 exceeds the PageRun limit 65535");
  ASSERT_EQ(allocation.numPages(), Allocation::PageRun::kMaxPagesInRun);
  ASSERT_EQ(allocation.numRuns(), 1);
  LOG(ERROR) << "here";
  allocation.clear();
}

} // namespace facebook::velox::memory
