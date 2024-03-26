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

#include "velox/connectors/fuzzer/FuzzerConnector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::connector::fuzzer {

FuzzerDataSource::FuzzerDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    velox::memory::MemoryPool* pool)
    : outputType_(outputType), pool_(pool) {
  auto fuzzerTableHandle =
      std::dynamic_pointer_cast<FuzzerTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      fuzzerTableHandle,
      "TableHandle must be an instance of FuzzerTableHandle");

  vectorFuzzer_ = std::make_unique<VectorFuzzer>(
      fuzzerTableHandle->fuzzerOptions, pool_, fuzzerTableHandle->fuzzerSeed);
}

void FuzzerDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_EQ(
      currentSplit_,
      nullptr,
      "Previous split has not been processed yet. Call next() to process the split.");
  currentSplit_ = std::dynamic_pointer_cast<FuzzerConnectorSplit>(split);
  VELOX_CHECK(currentSplit_, "Wrong type of split for FuzzerDataSource.");

  splitOffset_ = 0;
  splitEnd_ = currentSplit_->numRows;
}

std::optional<RowVectorPtr> FuzzerDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK_NOT_NULL(
      currentSplit_, "No split to process. Call addSplit() first.");

  // Split exhausted.
  if (splitOffset_ >= splitEnd_) {
    currentSplit_ = nullptr;
    splitOffset_ = 0;
    splitEnd_ = 0;
    return nullptr;
  }

  const size_t outputRows = std::min(size, (splitEnd_ - splitOffset_));
  splitOffset_ += outputRows;

  auto outputVector = vectorFuzzer_->fuzzRow(outputType_, outputRows);
  completedRows_ += outputVector->size();
  completedBytes_ += outputVector->retainedSize();
  return outputVector;
}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<FuzzerConnectorFactory>())

} // namespace facebook::velox::connector::fuzzer
