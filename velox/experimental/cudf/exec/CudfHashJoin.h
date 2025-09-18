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

#include "velox/experimental/cudf/exec/NvtxHelper.h"
#include "velox/experimental/cudf/vector/CudfVector.h"

#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/JoinBridge.h"
#include "velox/exec/Operator.h"
#include "velox/vector/ComplexVector.h"

#include <cudf/ast/expressions.hpp>
#include <cudf/join/hash_join.hpp>
#include <cudf/table/table.hpp>

namespace facebook::velox::cudf_velox {

class CudfHashJoinBridge : public exec::JoinBridge {
 public:
  using hash_type =
      std::pair<std::shared_ptr<cudf::table>, std::shared_ptr<cudf::hash_join>>;

  void setHashTable(std::optional<hash_type> hashObject);

  std::optional<hash_type> hashOrFuture(ContinueFuture* future);

 private:
  std::optional<hash_type> hashObject_;
};

class CudfHashJoinBuild : public exec::Operator, public NvtxHelper {
 public:
  CudfHashJoinBuild(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      std::shared_ptr<const core::HashJoinNode> joinNode);

  void addInput(RowVectorPtr input) override;

  bool needsInput() const override;

  RowVectorPtr getOutput() override;

  void noMoreInput() override;

  exec::BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

 private:
  std::shared_ptr<const core::HashJoinNode> joinNode_;
  std::vector<CudfVectorPtr> inputs_;
  ContinueFuture future_{ContinueFuture::makeEmpty()};
};

class CudfHashJoinProbe : public exec::Operator, public NvtxHelper {
 public:
  using hash_type = CudfHashJoinBridge::hash_type;

  CudfHashJoinProbe(
      int32_t operatorId,
      exec::DriverCtx* driverCtx,
      std::shared_ptr<const core::HashJoinNode> joinNode);

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  RowVectorPtr getOutput() override;

  bool skipProbeOnEmptyBuild() const;

  exec::BlockingReason isBlocked(ContinueFuture* future) override;

  static bool isSupportedJoinType(core::JoinType joinType) {
    return joinType == core::JoinType::kInner ||
        joinType == core::JoinType::kLeft ||
        joinType == core::JoinType::kAnti ||
        joinType == core::JoinType::kLeftSemiFilter ||
        joinType == core::JoinType::kRight ||
        joinType == core::JoinType::kRightSemiFilter;
  }

  bool isFinished() override;

 private:
  std::shared_ptr<const core::HashJoinNode> joinNode_;
  std::optional<hash_type> hashObject_;

  // Filter related members
  cudf::ast::tree tree_;
  std::vector<std::unique_ptr<cudf::scalar>> scalars_;

  bool rightPrecomputed_{false};

  // Batched probe inputs needed for right join
  std::vector<CudfVectorPtr> inputs_;
  ContinueFuture future_{ContinueFuture::makeEmpty()};

  std::vector<cudf::size_type> leftKeyIndices_;
  std::vector<cudf::size_type> rightKeyIndices_;
  std::vector<cudf::size_type> leftColumnIndicesToGather_;
  std::vector<cudf::size_type> rightColumnIndicesToGather_;
  std::vector<size_t> leftColumnOutputIndices_;
  std::vector<size_t> rightColumnOutputIndices_;
  bool finished_{false};

  // Copied from HashProbe.h
  // Indicates whether to skip probe input data processing or not. It only
  // applies for a specific set of join types (see skipProbeOnEmptyBuild()), and
  // the build table is empty and the probe input is read from non-spilled
  // source. This ensures the hash probe operator keeps running until all the
  // probe input from the sources have been processed. It prevents the exchange
  // hanging problem at the producer side caused by the early query finish.
  bool skipInput_{false};
};

class CudfHashJoinBridgeTranslator : public exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<exec::Operator>
  toOperator(exec::DriverCtx* ctx, int32_t id, const core::PlanNodePtr& node);

  std::unique_ptr<exec::JoinBridge> toJoinBridge(const core::PlanNodePtr& node);

  exec::OperatorSupplier toOperatorSupplier(const core::PlanNodePtr& node);
};

} // namespace facebook::velox::cudf_velox
