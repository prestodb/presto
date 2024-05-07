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
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

class Values : public SourceOperator {
 public:
  Values(
      int32_t operatorId,
      DriverCtx* driverCtx,
      std::shared_ptr<const core::ValuesNode> values);

  void initialize() override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  void noMoreInput() override {
    Operator::noMoreInput();
    close();
  }

  bool isFinished() override;

  void close() override;

  /// Returns the current processing vector index in 'values_'. This method is
  /// only used for test.
  int32_t testingCurrent() const {
    return current_;
  }

  const std::vector<RowVectorPtr> values() const {
    return values_;
  }

  int32_t roundsLeft() const {
    return roundsLeft_;
  }

 private:
  std::shared_ptr<const core::ValuesNode> valueNodes_;
  std::vector<RowVectorPtr> values_;
  int32_t current_ = 0;
  size_t roundsLeft_ = 1;
};

} // namespace facebook::velox::exec
