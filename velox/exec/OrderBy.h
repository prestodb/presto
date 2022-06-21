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

#include "velox/exec/ContainerRowSerde.h"
#include "velox/exec/Operator.h"
#include "velox/exec/RowContainer.h"

namespace facebook::velox::exec {

// OrderBy operator implementation: OrderBy stores all its inputs in a
// RowContainer as the inputs are added. Until all inputs are available,
// it blocks the pipeline. Once all inputs are available, it sorts pointers
// to the rows using the RowContainer's compare() function. And finally it
// constructs and returns the sorted output RowVector using the data in the
// RowContainer.
// Limitations:
// * It memcopies twice: 1) input to RowContainer and 2) RowContainer to
// output.
// * It does not support spilling. For now, if memory limit exceeds, it will
// throw an exception: VeloxMemoryCapExceeded.
class OrderBy : public Operator {
 public:
  OrderBy(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::OrderByNode>& orderByNode);

  bool needsInput() const override {
    return !finished_;
  }

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

 private:
  static const int32_t kBatchSizeInBytes{2 * 1024 * 1024};

  std::unique_ptr<RowContainer> data_;
  std::vector<std::pair<column_index_t, core::SortOrder>> keyInfo_;

  size_t numRows_ = 0;
  size_t numRowsReturned_ = 0;
  std::vector<char*> returningRows_;

  bool finished_ = false;
};
} // namespace facebook::velox::exec
