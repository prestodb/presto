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

#include "velox/vector/arrow/Abi.h"

namespace facebook::velox::exec {

class ArrowStream : public SourceOperator {
 public:
  ArrowStream(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::ArrowStreamNode>& arrowStreamNode);

  virtual ~ArrowStream();

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override;

  void close() override;

 private:
  /// Return last error in Arrow array stream.
  const char* getError() const;

  bool finished_ = false;
  std::shared_ptr<ArrowArrayStream> arrowStream_;
};

} // namespace facebook::velox::exec
