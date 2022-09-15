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

#include "velox/exec/UnorderedStreamReader.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

class UnorderedStreamReaderTest : public testing::Test,
                                  public test::VectorTestBase {
 protected:
  void SetUp() override {
    rowType_ = ROW(
        {"c0", "c1", "c2", "c3"}, {BIGINT(), VARCHAR(), INTEGER(), BIGINT()});
  }

  class MockBatchStream : public BatchStream {
   public:
    MockBatchStream(std::vector<RowVectorPtr> rowVectors)
        : rowVectors_(std::move(rowVectors)) {}

    bool nextBatch(RowVectorPtr& batch) final {
      if (currentVector_ >= rowVectors_.size()) {
        return false;
      }
      batch = std::move(rowVectors_[currentVector_++]);
      return true;
    }

   private:
    int currentVector_{0};
    std::vector<RowVectorPtr> rowVectors_;
  };

  std::vector<RowVectorPtr> makeVectors(vector_size_t size, int numVectors) {
    std::vector<RowVectorPtr> vectors;
    VectorFuzzer::Options options;
    options.vectorSize = size;
    VectorFuzzer fuzzer(options, pool_.get(), 0);
    for (int32_t i = 0; i < numVectors; ++i) {
      auto vector =
          std::dynamic_pointer_cast<RowVector>(fuzzer.fuzzRow(rowType_));
      vectors.push_back(vector);
    }
    return vectors;
  }

  void runTest(int numStreams) {
    folly::Random::DefaultGenerator rng;
    rng.seed(numStreams);
    std::vector<std::unique_ptr<MockBatchStream>> batchStreams;
    batchStreams.reserve(numStreams);
    std::vector<RowVectorPtr> sourceVectors;
    for (int i = 0; i < numStreams; ++i) {
      auto vectors = makeVectors(
          std::max<vector_size_t>(1, folly::Random::rand32(1024, rng)),
          std::max<int>(1, folly::Random::rand32(32, rng)));
      std::copy(
          vectors.begin(), vectors.end(), std::back_inserter(sourceVectors));
      batchStreams.push_back(
          std::make_unique<MockBatchStream>(std::move(vectors)));
    }
    UnorderedStreamReader reader(std::move(batchStreams));
    RowVectorPtr readBatch;
    for (int i = 0; i < sourceVectors.size(); ++i) {
      ASSERT_TRUE(reader.nextBatch(readBatch));
      for (int j = 0; j < sourceVectors[i]->size(); ++j) {
        ASSERT_EQ(
            sourceVectors[i]->compare(readBatch.get(), j, j, CompareFlags{}),
            0);
      }
    }
    ASSERT_FALSE(reader.nextBatch(readBatch));
    ASSERT_FALSE(reader.nextBatch(readBatch));
  }

  RowTypePtr rowType_;
};

TEST_F(UnorderedStreamReaderTest, readBatch) {
  runTest(0);
  runTest(1);
  runTest(128);
}
