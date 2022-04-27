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

#include "velox/dwio/dwrf/test/utils/E2EWriterTestUtil.h"

#include <gtest/gtest.h>
#include "velox/dwio/common/MemoryInputStream.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::memory;

namespace facebook::velox::dwrf {

/* static */ std::unique_ptr<Writer> E2EWriterTestUtil::writeData(
    std::unique_ptr<DataSink> sink,
    const std::shared_ptr<const Type>& type,
    const std::vector<VectorPtr>& batches,
    const std::shared_ptr<Config>& config,
    std::function<std::unique_ptr<DWRFFlushPolicy>()> flushPolicyFactory,
    std::function<
        std::unique_ptr<LayoutPlanner>(StreamList, const EncodingContainer&)>
        layoutPlannerFactory,
    const int64_t writerMemoryCap) {
  // write file to memory
  WriterOptions options;
  options.config = config;
  options.schema = type;
  options.memoryBudget = writerMemoryCap;
  options.flushPolicyFactory = flushPolicyFactory;
  options.layoutPlannerFactory = layoutPlannerFactory;

  auto writer = std::make_unique<Writer>(
      options,
      std::move(sink),
      velox::memory::getProcessDefaultMemoryManager().getRoot());

  for (size_t i = 0; i < batches.size(); ++i) {
    writer->write(batches[i]);
  }

  writer->close();
  return writer;
}

/* static */ void E2EWriterTestUtil::testWriter(
    MemoryPool& pool,
    const std::shared_ptr<const Type>& type,
    const std::vector<VectorPtr>& batches,
    // Memory footprint for F14 map under different compilers could be
    // different.
    size_t numStripesLower,
    size_t numStripesUpper,
    const std::shared_ptr<Config>& config,
    std::function<std::unique_ptr<DWRFFlushPolicy>()> flushPolicyFactory,
    std::function<
        std::unique_ptr<LayoutPlanner>(StreamList, const EncodingContainer&)>
        layoutPlannerFactory,
    const int64_t writerMemoryCap,
    const bool verifyContent) {
  // write file to memory
  auto sink = std::make_unique<MemorySink>(pool, 200 * 1024 * 1024);
  auto sinkPtr = sink.get();

  // Writer owns sink. Keeping writer alive to avoid deleting the sink.
  auto writer = writeData(
      std::move(sink),
      type,
      batches,
      config,
      flushPolicyFactory,
      layoutPlannerFactory,
      writerMemoryCap);
  // read it back and compare
  auto input =
      std::make_unique<MemoryInputStream>(sinkPtr->getData(), sinkPtr->size());

  ReaderOptions readerOpts;
  RowReaderOptions rowReaderOpts;
  auto reader = std::make_unique<DwrfReader>(readerOpts, std::move(input));
  EXPECT_GE(numStripesUpper, reader->getNumberOfStripes());
  EXPECT_LE(numStripesLower, reader->getNumberOfStripes());
  if (!verifyContent) {
    return;
  }

  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto dwrfRowReader = dynamic_cast<DwrfRowReader*>(rowReader.get());

  auto batchIndex = 0;
  auto rowIndex = 0;
  VectorPtr batch;
  while (dwrfRowReader->next(1000, batch)) {
    for (int32_t i = 0; i < batch->size(); ++i) {
      ASSERT_TRUE(batches[batchIndex]->equalValueAt(batch.get(), rowIndex, i))
          << "Content mismatch at batch " << batchIndex << " at index "
          << rowIndex
          << "Reference: " << batches[batchIndex]->toString(rowIndex)
          << " read: " << batch->toString(i);

      if (++rowIndex == batches[batchIndex]->size()) {
        rowIndex = 0;
        ++batchIndex;
      }
    }
  }
  ASSERT_EQ(batchIndex, batches.size());
  ASSERT_EQ(rowIndex, 0);
}

/* static */ std::vector<VectorPtr> E2EWriterTestUtil::generateBatches(
    const std::shared_ptr<const Type>& type,
    size_t batchCount,
    size_t size,
    uint32_t seed,
    MemoryPool& pool) {
  std::vector<VectorPtr> batches;
  std::mt19937 gen{};
  gen.seed(seed);
  for (size_t i = 0; i < batchCount; ++i) {
    batches.push_back(BatchMaker::createBatch(type, size, pool, gen));
  }
  return batches;
}

/* static */ std::vector<VectorPtr> E2EWriterTestUtil::generateBatches(
    VectorPtr batch) {
  std::vector<VectorPtr> batches;
  batches.push_back(std::move(batch));
  return batches;
}

} // namespace facebook::velox::dwrf
