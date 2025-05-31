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

#include "velox/common/file/FileSystems.h"
#include "velox/exec/Merge.h"
#include "velox/exec/SortBuffer.h"
#include "velox/exec/Spill.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/type/Type.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox;
using namespace facebook::velox::memory;

namespace facebook::velox::exec::test {

class ConcatFilesSpillMergeStreamTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
  }

  std::vector<RowVectorPtr> generateSortedVectors(
      const int32_t numVectors,
      const size_t maxOutputRows) {
    const VectorFuzzer::Options fuzzerOpts{.vectorSize = maxOutputRows};
    const auto vectors = createVectors(numVectors, inputType_, fuzzerOpts);
    const auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        common::PrefixSortConfig{},
        nullptr,
        nullptr);
    for (const auto& vector : vectors) {
      sortBuffer->addInput(vector);
    }
    sortBuffer->noMoreInput();
    std::vector<RowVectorPtr> sortedVectors;
    sortedVectors.reserve(numVectors);
    for (auto i = 0; i < numVectors; ++i) {
      sortedVectors.emplace_back(sortBuffer->getOutput(maxOutputRows));
    }
    return sortedVectors;
  }

  SpillFiles generateSortedSpillFiles(
      const std::vector<RowVectorPtr>& sortedVectors) {
    const auto spiller = std::make_unique<MergeSpiller>(
        inputType_, HashBitRange{}, sortingKeys_, &spillConfig_, &spillStats_);
    for (const auto& vector : sortedVectors) {
      spiller->spill(SpillPartitionId(0), vector);
    }
    SpillPartitionSet spillPartitionSet;
    spiller->finishSpill(spillPartitionSet);
    EXPECT_EQ(spillPartitionSet.size(), 1);
    return spillPartitionSet.cbegin()->second->files();
  }

  std::pair<
      std::vector<RowVectorPtr>,
      std::vector<std::unique_ptr<SpillMergeStream>>>
  generateInputs(size_t numStreams, size_t maxOutputRows) {
    std::vector<RowVectorPtr> totalVectors;
    std::vector<std::unique_ptr<SpillMergeStream>> spillStreams;
    for (auto i = 1; i <= numStreams; ++i) {
      const auto vectors = generateSortedVectors(i * 3 + 1, maxOutputRows);
      for (const auto& vector : vectors) {
        totalVectors.push_back(vector);
      }
      const auto spillFiles = generateSortedSpillFiles(vectors);
      EXPECT_EQ(spillFiles.size(), vectors.size());
      std::vector<std::unique_ptr<SpillReadFile>> spillReadFiles;
      spillReadFiles.reserve(spillFiles.size());
      for (const auto& spillFile : spillFiles) {
        spillReadFiles.emplace_back(SpillReadFile::create(
            spillFile, spillConfig_.readBufferSize, pool_.get(), &spillStats_));
      }
      auto stream =
          ConcatFilesSpillMergeStream::create(i - 1, std::move(spillReadFiles));
      spillStreams.push_back(std::move(stream));
    }
    return std::make_pair(std::move(totalVectors), std::move(spillStreams));
  }

  std::vector<RowVectorPtr> mergeSpillStreams(
      std::vector<std::unique_ptr<SpillMergeStream>> spillStreams,
      size_t numVectors,
      size_t maxOutputRows) const {
    std::vector<RowVectorPtr> results;
    const auto spillMerger = std::make_unique<TreeOfLosers<SpillMergeStream>>(
        std::move(spillStreams));
    for (auto i = 0; i < numVectors; ++i) {
      auto output = std::static_pointer_cast<RowVector>(
          BaseVector::create(inputType_, maxOutputRows, pool_.get()));
      for (auto& child : output->children()) {
        child->resize(maxOutputRows);
      }
      // Records the source rows to copy to 'output_' in order.
      std::vector<const RowVector*> spillSources(maxOutputRows);
      std::vector<vector_size_t> spillSourceRows(maxOutputRows);
      int32_t outputRow = 0;
      int32_t outputSize = 0;
      bool isEndOfBatch = false;
      while (outputRow + outputSize < output->size()) {
        SpillMergeStream* stream = spillMerger->next();
        spillSources[outputSize] = &stream->current();
        spillSourceRows[outputSize] = stream->currentIndex(&isEndOfBatch);
        ++outputSize;
        if (FOLLY_UNLIKELY(isEndOfBatch)) {
          // The stream is at end of input batch. Need to copy out the rows
          // before fetching next batch in 'pop'.
          gatherCopy(
              output.get(),
              outputRow,
              outputSize,
              spillSources,
              spillSourceRows,
              {});
          outputRow += outputSize;
          outputSize = 0;
        }
        // Advance the stream.
        stream->pop();
      }
      if (FOLLY_LIKELY(outputSize != 0)) {
        gatherCopy(
            output.get(),
            outputRow,
            outputSize,
            spillSources,
            spillSourceRows,
            {});
      }
      results.push_back(output);
    }
    EXPECT_EQ(spillMerger->next(), nullptr);
    return results;
  }

  std::vector<RowVectorPtr> makeExpectedResults(
      const std::vector<RowVectorPtr>& vectors,
      size_t maxOutputRows) {
    const auto sortBuffer = std::make_unique<SortBuffer>(
        inputType_,
        sortColumnIndices_,
        sortCompareFlags_,
        pool_.get(),
        &nonReclaimableSection_,
        common::PrefixSortConfig{},
        nullptr,
        nullptr);
    for (const auto& vector : vectors) {
      sortBuffer->addInput(vector);
    }
    sortBuffer->noMoreInput();
    std::vector<RowVectorPtr> sortedVectors;
    sortedVectors.reserve(vectors.size());
    for (auto i = 0; i < vectors.size(); ++i) {
      sortedVectors.emplace_back(sortBuffer->getOutput(maxOutputRows));
    }
    return sortedVectors;
  }

 private:
  const RowTypePtr inputType_ = ROW(
      {{"c0", BIGINT()},
       {"c1", INTEGER()},
       {"c2", SMALLINT()},
       {"c3", VARCHAR()}});
  const std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency())};
  const std::vector<column_index_t> sortColumnIndices_{0, 2};
  const std::vector<CompareFlags> sortCompareFlags_{
      CompareFlags{},
      CompareFlags{.ascending = false}};
  const std::vector<SpillSortKey> sortingKeys_ =
      SpillState::makeSortingKeys(sortColumnIndices_, sortCompareFlags_);
  const std::shared_ptr<TempDirectoryPath> spillDirectory_ =
      exec::test::TempDirectoryPath::create();
  const common::SpillConfig spillConfig_{
      [&]() -> const std::string& { return spillDirectory_->getPath(); },
      [&](uint64_t) {},
      "0.0.0",
      10, // Force to create a file per spill to mock multiple files per stream
      0,
      1 << 20,
      executor_.get(),
      100,
      100,
      0,
      0,
      0,
      0,
      0,
      "none",
      std::nullopt};

  folly::Synchronized<common::SpillStats> spillStats_;
  tsan_atomic<bool> nonReclaimableSection_{false};
};
} // namespace facebook::velox::exec::test

TEST_F(ConcatFilesSpillMergeStreamTest, basic) {
  struct {
    size_t maxOutputRows;
    size_t numStreams;

    std::string debugString() const {
      return fmt::format(
          "maxOutputRows:{} numStreams:{}", maxOutputRows, numStreams);
    }
  } testSettings[] = {
      {1, 1},
      {1, 3},
      {1, 8},
      {7, 1},
      {7, 3},
      {7, 8},
      {16, 1},
      {16, 3},
      {16, 8}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    auto [totalVectors, spillStreams] =
        generateInputs(testData.numStreams, testData.maxOutputRows);
    std::vector<RowVectorPtr> results = mergeSpillStreams(
        std::move(spillStreams), totalVectors.size(), testData.maxOutputRows);
    ASSERT_EQ(totalVectors.size(), results.size());
    const auto expectedResults =
        makeExpectedResults(totalVectors, testData.maxOutputRows);
    ASSERT_EQ(expectedResults.size(), results.size());
    ASSERT_TRUE(assertEqualResults(expectedResults, results));
  }
}
