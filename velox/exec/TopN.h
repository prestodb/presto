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
#pragma once

#include "velox/exec/Operator.h"
#include "velox/exec/RowContainer.h"

namespace facebook::velox::exec {

class TopN : public Operator {
 public:
  TopN(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::TopNNode>& topNNode);

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

 private:
  const int32_t count_;

  bool finished_ = false;
  uint32_t numRowsReturned_ = 0;

  // As the inputs are added to TopN operator, we use topRows_ (a priority
  // queue) to keep track of the pointers to rows stored in the
  // RowContainer (data_). We only update the RowContainer if a row is a
  // candidate for top rows. Otherwise, we will discard the row.
  // Since we use a priority queue for TopN, we perform
  // O(total_rows * logN) comparisons and require O(N) space.
  // Once all inputs are available, we copy the final set of rows to the
  // vector (rows_) in correct order. We use this vector along with the
  // RowContainer to generate the TopN's output.
  std::unique_ptr<RowContainer> data_;
  RowComparator comparator_;
  std::priority_queue<char*, std::vector<char*>, RowComparator> topRows_;
  std::vector<char*> rows_;

  std::vector<DecodedVector> decodedVectors_;
  vector_size_t outputBatchSize_;
};
} // namespace facebook::velox::exec
