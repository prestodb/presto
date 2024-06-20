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
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::memory;

namespace facebook::velox::dwrf {

/* static */ std::unique_ptr<Writer> E2EWriterTestUtil::createWriter(
    std::unique_ptr<FileSink> sink,
    const std::shared_ptr<const Type>& type,
    const std::shared_ptr<Config>& config,
    std::function<std::unique_ptr<DWRFFlushPolicy>()> flushPolicyFactory,
    std::function<std::unique_ptr<LayoutPlanner>(const TypeWithId&)>
        layoutPlannerFactory,
    const int64_t writerMemoryCap) {
  // write file to memory
  dwrf::WriterOptions options;
  options.config = config;
  options.schema = type;
  options.memoryBudget = writerMemoryCap;
  options.flushPolicyFactory = flushPolicyFactory;
  options.layoutPlannerFactory = layoutPlannerFactory;

  return std::make_unique<dwrf::Writer>(
      std::move(sink), options, velox::memory::memoryManager()->addRootPool());
}

/* static */ std::unique_ptr<Writer> E2EWriterTestUtil::writeData(
    std::unique_ptr<Writer> writer,
    const std::vector<VectorPtr>& batches) {
  for (size_t i = 0; i < batches.size(); ++i) {
    writer->write(batches[i]);
  }

  writer->close();
  LOG(INFO) << "writer root pool usage: "
            << writer->getContext().testingGetWriterMemoryStats();
  return writer;
}

/* static */ std::unique_ptr<Writer> E2EWriterTestUtil::writeData(
    std::unique_ptr<FileSink> sink,
    const std::shared_ptr<const Type>& type,
    const std::vector<VectorPtr>& batches,
    const std::shared_ptr<Config>& config,
    std::function<std::unique_ptr<DWRFFlushPolicy>()> flushPolicyFactory,
    std::function<std::unique_ptr<LayoutPlanner>(const TypeWithId&)>
        layoutPlannerFactory,
    const int64_t writerMemoryCap) {
  auto writer = createWriter(
      std::move(sink),
      type,
      config,
      std::move(flushPolicyFactory),
      std::move(layoutPlannerFactory),
      writerMemoryCap);
  return writeData(std::move(writer), batches);
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
    std::function<std::unique_ptr<LayoutPlanner>(const TypeWithId&)>
        layoutPlannerFactory,
    const int64_t writerMemoryCap,
    const bool verifyContent) {
  // write file to memory
  auto sink = std::make_unique<MemorySink>(
      200 * 1024 * 1024, FileSink::Options{.pool = &pool});
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
  auto readFile = std::make_shared<InMemoryReadFile>(
      std::string(sinkPtr->data(), sinkPtr->size()));
  auto input = std::make_unique<BufferedInput>(readFile, pool);

  dwio::common::ReaderOptions readerOpts{&pool};
  RowReaderOptions rowReaderOpts;
  auto reader = std::make_unique<DwrfReader>(readerOpts, std::move(input));
  EXPECT_GE(numStripesUpper, reader->getNumberOfStripes());
  EXPECT_LE(numStripesLower, reader->getNumberOfStripes());

  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto dwrfRowReader = dynamic_cast<DwrfRowReader*>(rowReader.get());

  size_t dictEncodingCount = 0;
  size_t totalEncodingCount = 0;
  for (size_t i = 0; i < reader->getNumberOfStripes(); ++i) {
    bool preload{false};
    auto stripeMetadata = dwrfRowReader->fetchStripe(i, preload);
    const auto& stripeFooter = *stripeMetadata->footer;
    totalEncodingCount += stripeFooter.encoding_size();
    for (const auto& encoding : stripeFooter.encoding()) {
      if (encoding.kind() == proto::ColumnEncoding_Kind_DICTIONARY) {
        ++dictEncodingCount;
      }
    }
  }
  LOG(INFO) << fmt::format(
      "dict encoding distribution: {}/{}",
      dictEncodingCount,
      totalEncodingCount);

  if (!verifyContent) {
    return;
  }

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
