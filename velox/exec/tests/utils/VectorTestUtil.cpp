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

#include "velox/exec/tests/utils/VectorTestUtil.h"

using namespace facebook::velox;

namespace facebook::velox::exec::test {

void shuffleBatches(std::vector<RowVectorPtr>& batches) {
  std::default_random_engine rng(1234);
  std::shuffle(std::begin(batches), std::end(batches), rng);
}

std::vector<RowVectorPtr> makeBatches(
    int32_t batchSize,
    int32_t numBatches,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool,
    double nullRatio,
    bool shuffle) {
  VELOX_CHECK_GE(batchSize, 0);
  VELOX_CHECK_GT(numBatches, 0);

  std::vector<RowVectorPtr> batches;
  batches.reserve(numBatches);
  if (batchSize != 0) {
    VectorFuzzer::Options options;
    options.vectorSize = batchSize;
    options.nullRatio = nullRatio;
    VectorFuzzer fuzzer(options, pool);
    for (int32_t i = 0; i < numBatches; ++i) {
      batches.push_back(fuzzer.fuzzInputRow(rowType));
    }
  } else {
    batches.push_back(RowVector::createEmpty(rowType, pool));
  }
  // NOTE: we generate a number of vectors with a fresh new fuzzer init with
  // the same fix seed. The purpose is to ensure we have sufficient match if
  // we use the row type for both build and probe inputs. Here we shuffle the
  // built vectors to introduce some randomness during the join execution.
  if (shuffle) {
    shuffleBatches(batches);
  }
  return batches;
}

std::vector<RowVectorPtr> makeBatches(
    vector_size_t numBatches,
    std::function<RowVectorPtr(int32_t)> makeVector,
    bool shuffle) {
  std::vector<RowVectorPtr> batches;
  batches.reserve(numBatches);
  for (int32_t i = 0; i < numBatches; ++i) {
    batches.push_back(makeVector(i));
  }
  if (shuffle) {
    shuffleBatches(batches);
  }
  return batches;
}

std::vector<RowVectorPtr> mergeBatches(
    std::vector<RowVectorPtr>&& lhs,
    std::vector<RowVectorPtr>&& rhs,
    bool shuffle) {
  std::vector<RowVectorPtr> mergedBatches;
  mergedBatches.reserve(lhs.size() + rhs.size());
  std::move(lhs.begin(), lhs.end(), std::back_inserter(mergedBatches));
  std::move(rhs.begin(), rhs.end(), std::back_inserter(mergedBatches));
  if (shuffle) {
    shuffleBatches(mergedBatches);
  }
  return mergedBatches;
}

std::vector<std::string> concat(
    const std::vector<std::string>& a,
    const std::vector<std::string>& b) {
  std::vector<std::string> result;
  result.insert(result.end(), a.begin(), a.end());
  result.insert(result.end(), b.begin(), b.end());
  return result;
}

std::vector<RowVectorPtr> makeCopies(
    const std::vector<RowVectorPtr>& source,
    int32_t numCopies) {
  std::vector<RowVectorPtr> res;
  res.reserve(source.size() * numCopies);
  for (auto i = 0; i < numCopies; ++i) {
    std::copy(source.begin(), source.end(), std::back_inserter(res));
  }
  return res;
}

} // namespace facebook::velox::exec::test
