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

#include <folly/io/IOBuf.h>
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::exec {

/// This file contains the plan node and operators that can be used for
/// off-process expression evaluation in a plan. The operator serializes input
/// batches and sends them to a remote process along with the expressions
/// specified in `expressions`.

/// Off-process expression eval plan node. `expressions` control the expressions
/// that will be remotely executed.
class OffProcessExpressionEvalNode : public core::PlanNode {
 public:
  OffProcessExpressionEvalNode(
      core::PlanNodeId id,
      std::vector<core::TypedExprPtr> expressions,
      core::PlanNodePtr source)
      : PlanNode(std::move(id)),
        expressions_{std::move(expressions)},
        sources_{std::move(source)} {
    VELOX_USER_CHECK_EQ(1, sources_.size());
  }

  const RowTypePtr& outputType() const override {
    return sources_.front()->outputType();
  }

  const std::vector<core::TypedExprPtr>& expressions() const {
    return expressions_;
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "OffProcessExpressionEval";
  }

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<core::TypedExprPtr> expressions_;
  const std::vector<core::PlanNodePtr> sources_;
};

class OffProcessExpressionEvalOperator : public exec::Operator {
 public:
  OffProcessExpressionEvalOperator(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      const std::shared_ptr<const OffProcessExpressionEvalNode>& planNode);

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  bool needsInput() const override {
    return !noMoreInput_;
  }

  RowVectorPtr getOutput() override;

  exec::BlockingReason isBlocked(ContinueFuture* /* future */) override {
    return exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }

 private:
  // Sends the current IOBuf off-process, returning the result IOBuf received
  // from the remote process.
  //
  // TODO: this function will need to return a future.
  std::unique_ptr<folly::IOBuf> sendOffProcess(
      const std::vector<core::TypedExprPtr>& expressions,
      std::unique_ptr<folly::IOBuf>&& ioBuf);

  // Flushes the current stream group contents.
  void flushStreamGroup();

  RowVectorPtr deserializeIOBuf(const folly::IOBuf& ioBuf);

  std::unique_ptr<VectorStreamGroup> streamGroup_;
  ByteStream byteStream_;

  std::unique_ptr<folly::IOBuf> ioBuf_;

  RowTypePtr inputType_;
  std::vector<core::TypedExprPtr> expressions_;
};

class OffProcessExpressionEvalTranslator
    : public exec::Operator::PlanNodeTranslator {
  std::unique_ptr<exec::Operator> toOperator(
      exec::DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto offProcessNode =
            std::dynamic_pointer_cast<const OffProcessExpressionEvalNode>(
                node)) {
      return std::make_unique<OffProcessExpressionEvalOperator>(
          id, ctx, offProcessNode);
    }
    return nullptr;
  }
};

} // namespace facebook::velox::exec
