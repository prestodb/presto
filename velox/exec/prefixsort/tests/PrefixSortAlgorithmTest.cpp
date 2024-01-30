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

#include <folly/Random.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "velox/exec/prefixsort/PrefixSortAlgorithm.h"
#include "velox/exec/prefixsort/PrefixSortEncoder.h"
#include "velox/exec/prefixsort/tests/utils/EncoderTestUtils.h"
#include "velox/vector/tests/VectorTestUtils.h"

namespace facebook::velox::exec::prefixsort::test {

class PrefixSortAlgorithmTest : public testing::Test,
                                public velox::test::VectorTestBase {
 public:
  void testQuickSort(size_t size) {
    // Data1 will be sorted by quickSort.
    std::vector<int64_t> data1(size);
    std::generate(
        data1.begin(), data1.end(), [&]() { return folly::Random::rand64(); });

    // Data2 will be sorted by std::sort.
    std::vector<int64_t> data2 = data1;

    // Sort data1 with quick-sort.
    {
      char* start = (char*)data1.data();
      char* end = start + sizeof(int64_t) * data1.size();
      uint32_t entrySize = sizeof(int64_t);
      auto swapBuffer = AlignedBuffer::allocate<char>(entrySize, pool());
      PrefixSortRunner sortRunner(entrySize, swapBuffer->asMutable<char>());
      encodeInPlace(data1);
      sortRunner.quickSort(
          start, end, [&](char* a, char* b) { return memcmp(a, b, 8); });
    }

    // Sort data2 with std::sort.
    std::sort(data2.begin(), data2.end());
    decodeInPlace(data1);
    ASSERT_EQ(data1, data2);
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(PrefixSortAlgorithmTest, quickSort) {
  testQuickSort(PrefixSortRunner::kSmallSort - 1);
  testQuickSort(PrefixSortRunner::kSmallSort);
  testQuickSort(PrefixSortRunner::kSmallSort + 1);
  testQuickSort(PrefixSortRunner::kMediumSort);
  // Any number bigger than kMediumSort is sufficient for testing.
  testQuickSort(PrefixSortRunner::kMediumSort + 1000);
}

TEST_F(PrefixSortAlgorithmTest, testingMedian3) {
  // Generate 3 elements randomly as input data.
  std::vector<int64_t> data1(3);
  std::generate(
      data1.begin(), data1.end(), [&]() { return folly::Random::rand64(); });
  std::vector<int64_t> data2(data1);
  encodeInPlace(data1);

  size_t entrySize = sizeof(int64_t);
  auto ptr1 = (char*)data1.data();
  auto ptr2 = ptr1 + entrySize;
  auto ptr3 = ptr2 + entrySize;
  auto medianPtr = PrefixSortRunner::testingMedian3(
      ptr1, ptr2, ptr3, entrySize, [&](char* a, char* b) {
        return memcmp(a, b, entrySize);
      });
  decodeInPlace(data1);
  auto median = *(reinterpret_cast<int64_t*>(medianPtr));
  // Sort the input vector data, the middle element must be equal to the
  // median we calculated.
  std::sort(data2.begin(), data2.end());
  ASSERT_EQ(median, data2[1]);
}

} // namespace facebook::velox::exec::prefixsort::test
