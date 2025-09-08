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

#include "velox/dwio/common/SortingWriter.h"
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/SortBuffer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::memory;

namespace facebook::velox::dwio::common::test {

class MockWriter : public Writer {
 public:
  MockWriter() {
    setState(State::kRunning);
  }

  void write(const VectorPtr& data) override {
    writtenData_.push_back(data);
    totalRowsWritten_ += data->size();
  }

  bool finish() override {
    return true;
  }

  void flush() override {}

  void close() override {
    setState(State::kClosed);
  }

  void abort() override {
    setState(State::kAborted);
  }

  uint64_t getTotalRowsWritten() const {
    return totalRowsWritten_;
  }

 private:
  std::vector<VectorPtr> writtenData_;
  uint64_t totalRowsWritten_ = 0;
};

class SortingWriterTest : public testing::Test,
                          public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  std::unique_ptr<SortBuffer> createSortBuffer() {
    const RowTypePtr inputType =
        ROW({{"c0", BIGINT()}, {"c1", INTEGER()}, {"c2", VARCHAR()}});

    const std::vector<column_index_t> sortColumnIndices{1};
    const std::vector<CompareFlags> sortCompareFlags{
        {true, true, false, CompareFlags::NullHandlingMode::kNullAsValue}};

    const velox::common::PrefixSortConfig prefixSortConfig{
        std::numeric_limits<uint32_t>::max(),
        std::numeric_limits<uint32_t>::max(),
        12};

    return std::make_unique<SortBuffer>(
        inputType,
        sortColumnIndices,
        sortCompareFlags,
        pool_.get(),
        &nonReclaimableSection_,
        prefixSortConfig);
  }

  RowVectorPtr createTestData(uint64_t numRows = 1000) {
    return makeRowVector(
        {makeFlatVector<int64_t>(
             numRows, [](vector_size_t row) { return row; }),
         makeFlatVector<int32_t>(
             numRows, [numRows](vector_size_t row) { return numRows - row; }),
         makeFlatVector<std::string>(numRows, [](vector_size_t row) {
           return fmt::format("row_{}", row);
         })});
  }

  tsan_atomic<bool> nonReclaimableSection_{false};
};

TEST_F(SortingWriterTest, largeRowSizeExceedsMaxOutputBytes) {
  auto mockWriter = std::make_unique<MockWriter>();
  auto mockWriterPtr = mockWriter.get();

  auto sortBuffer = createSortBuffer();

  const vector_size_t maxOutputRowsConfig = 1000;
  const uint64_t maxOutputBytesConfig = 1;
  const uint64_t outputTimeSliceLimitMs = 1000;

  SortingWriter sortingWriter(
      std::move(mockWriter),
      std::move(sortBuffer),
      maxOutputRowsConfig,
      maxOutputBytesConfig,
      outputTimeSliceLimitMs);

  RowVectorPtr testData = createTestData(10);
  sortingWriter.write(testData);
  ASSERT_TRUE(sortingWriter.finish());
  ASSERT_GT(mockWriterPtr->getTotalRowsWritten(), 0);
}

} // namespace facebook::velox::dwio::common::test
