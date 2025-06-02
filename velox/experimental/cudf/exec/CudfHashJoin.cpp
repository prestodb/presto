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

#include "velox/experimental/cudf/exec/CudfHashJoin.h"
#include "velox/experimental/cudf/exec/ExpressionEvaluator.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/exec/Utilities.h"

#include "velox/exec/Task.h"

#include <cudf/copying.hpp>
#include <cudf/join.hpp>

#include <nvtx3/nvtx3.hpp>

namespace facebook::velox::cudf_velox {

void CudfHashJoinBridge::setHashTable(
    std::optional<CudfHashJoinBridge::hash_type> hashObject) {
  if (cudfDebugEnabled()) {
    std::cout << "Calling CudfHashJoinBridge::setHashTable" << std::endl;
  }
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(
        !hashObject_.has_value(),
        "CudfHashJoinBridge already has a hash table");
    hashObject_ = std::move(hashObject);
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

std::optional<CudfHashJoinBridge::hash_type> CudfHashJoinBridge::hashOrFuture(
    ContinueFuture* future) {
  if (cudfDebugEnabled()) {
    std::cout << "Calling CudfHashJoinBridge::hashOrFuture" << std::endl;
  }
  std::lock_guard<std::mutex> l(mutex_);
  if (hashObject_.has_value()) {
    return hashObject_;
  }
  if (cudfDebugEnabled()) {
    std::cout << "Calling CudfHashJoinBridge::hashOrFuture constructing promise"
              << std::endl;
  }
  promises_.emplace_back("CudfHashJoinBridge::hashOrFuture");
  if (cudfDebugEnabled()) {
    std::cout << "Calling CudfHashJoinBridge::hashOrFuture getSemiFuture"
              << std::endl;
  }
  *future = promises_.back().getSemiFuture();
  if (cudfDebugEnabled()) {
    std::cout << "Calling CudfHashJoinBridge::hashOrFuture returning nullopt"
              << std::endl;
  }
  return std::nullopt;
}

CudfHashJoinBuild::CudfHashJoinBuild(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    std::shared_ptr<const core::HashJoinNode> joinNode)
    // TODO check outputType should be set or not?
    : exec::Operator(
          driverCtx,
          nullptr, // joinNode->sources(),
          operatorId,
          joinNode->id(),
          "CudfHashJoinBuild"),
      NvtxHelper(
          nvtx3::rgb{65, 105, 225}, // Royal Blue
          operatorId,
          fmt::format("[{}]", joinNode->id())),
      joinNode_(joinNode) {
  if (cudfDebugEnabled()) {
    std::cout << "CudfHashJoinBuild constructor" << std::endl;
  }
}

void CudfHashJoinBuild::addInput(RowVectorPtr input) {
  if (cudfDebugEnabled()) {
    std::cout << "Calling CudfHashJoinBuild::addInput" << std::endl;
  }
  // Queue inputs, process all at once.
  if (input->size() > 0) {
    auto cudfInput = std::dynamic_pointer_cast<CudfVector>(input);
    VELOX_CHECK_NOT_NULL(cudfInput);
    inputs_.push_back(std::move(cudfInput));
  }
}

bool CudfHashJoinBuild::needsInput() const {
  if (cudfDebugEnabled()) {
    std::cout << "Calling CudfHashJoinBuild::needsInput" << std::endl;
  }
  return !noMoreInput_;
}

RowVectorPtr CudfHashJoinBuild::getOutput() {
  return nullptr;
}

void CudfHashJoinBuild::noMoreInput() {
  if (cudfDebugEnabled()) {
    std::cout << "Calling CudfHashJoinBuild::noMoreInput" << std::endl;
  }
  VELOX_NVTX_OPERATOR_FUNC_RANGE();
  Operator::noMoreInput();
  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<exec::Driver>> peers;
  // Only last driver collects all answers
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(), operatorCtx_->driver(), &future_, promises, peers)) {
    return;
  }
  // Collect results from peers
  for (auto& peer : peers) {
    auto op = peer->findOperator(planNodeId());
    auto* build = dynamic_cast<CudfHashJoinBuild*>(op);
    VELOX_CHECK_NOT_NULL(build);
    inputs_.insert(inputs_.end(), build->inputs_.begin(), build->inputs_.end());
  }

  SCOPE_EXIT {
    // Realize the promises so that the other Drivers (which were not
    // the last to finish) can continue from the barrier and finish.
    peers.clear();
    for (auto& promise : promises) {
      promise.setValue();
    }
  };

  auto stream = cudfGlobalStreamPool().get_stream();
  auto tbl = getConcatenatedTable(inputs_, stream);

  // Release input data after synchronizing
  stream.synchronize();
  inputs_.clear();

  VELOX_CHECK_NOT_NULL(tbl);
  if (cudfDebugEnabled()) {
    std::cout << "Build table number of columns: " << tbl->num_columns()
              << std::endl;
    std::cout << "Build table number of rows: " << tbl->num_rows() << std::endl;
  }

  auto buildType = joinNode_->sources()[1]->outputType();
  auto rightKeys = joinNode_->rightKeys();

  auto buildKeyIndices = std::vector<cudf::size_type>(rightKeys.size());
  for (size_t i = 0; i < buildKeyIndices.size(); i++) {
    buildKeyIndices[i] = static_cast<cudf::size_type>(
        buildType->getChildIdx(rightKeys[i]->name()));
  }

  // Only need to construct hash_join object if it's an inner join or left join
  // and doesn't have a filter. All other cases use a standalone function in
  // cudf
  bool buildHashJoin = (joinNode_->isInnerJoin() || joinNode_->isLeftJoin()) &&
      !joinNode_->filter();
  auto hashObject = (buildHashJoin) ? std::make_shared<cudf::hash_join>(
                                          tbl->view().select(buildKeyIndices),
                                          cudf::null_equality::EQUAL,
                                          stream)
                                    : nullptr;
  if (buildHashJoin) {
    VELOX_CHECK_NOT_NULL(hashObject);
  }

  if (cudfDebugEnabled()) {
    if (hashObject != nullptr) {
      printf("hashObject is not nullptr %p\n", hashObject.get());
    } else {
      printf("hashObject is *** nullptr\n");
    }
  }

  // set hash table to CudfHashJoinBridge
  auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
      operatorCtx_->driverCtx()->splitGroupId, planNodeId());
  auto cudfHashJoinBridge =
      std::dynamic_pointer_cast<CudfHashJoinBridge>(joinBridge);
  cudfHashJoinBridge->setHashTable(std::make_optional(
      std::make_pair(std::shared_ptr(std::move(tbl)), std::move(hashObject))));
}

exec::BlockingReason CudfHashJoinBuild::isBlocked(ContinueFuture* future) {
  if (!future_.valid()) {
    return exec::BlockingReason::kNotBlocked;
  }
  *future = std::move(future_);
  return exec::BlockingReason::kWaitForJoinBuild;
}

bool CudfHashJoinBuild::isFinished() {
  return !future_.valid() && noMoreInput_;
}

CudfHashJoinProbe::CudfHashJoinProbe(
    int32_t operatorId,
    exec::DriverCtx* driverCtx,
    std::shared_ptr<const core::HashJoinNode> joinNode)
    : exec::Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          "CudfHashJoinProbe"),
      NvtxHelper(
          nvtx3::rgb{0, 128, 128}, // Teal
          operatorId,
          fmt::format("[{}]", joinNode->id())),
      joinNode_(joinNode) {
  if (cudfDebugEnabled()) {
    std::cout << "CudfHashJoinProbe constructor" << std::endl;
  }
  auto probeType = joinNode_->sources()[0]->outputType();
  auto buildType = joinNode_->sources()[1]->outputType();
  auto const& leftKeys = joinNode_->leftKeys(); // probe keys
  auto const& rightKeys = joinNode_->rightKeys(); // build keys

  if (cudfDebugEnabled()) {
    for (int i = 0; i < probeType->names().size(); i++) {
      std::cout << "Left column " << i << ": " << probeType->names()[i]
                << std::endl;
    }

    for (int i = 0; i < buildType->names().size(); i++) {
      std::cout << "Right column " << i << ": " << buildType->names()[i]
                << std::endl;
    }

    for (int i = 0; i < leftKeys.size(); i++) {
      std::cout << "Left key " << i << ": " << leftKeys[i]->name() << " "
                << leftKeys[i]->type()->kind() << std::endl;
    }

    for (int i = 0; i < rightKeys.size(); i++) {
      std::cout << "Right key " << i << ": " << rightKeys[i]->name() << " "
                << rightKeys[i]->type()->kind() << std::endl;
    }
  }

  auto const probeTableNumColumns = probeType->size();
  leftKeyIndices_ = std::vector<cudf::size_type>(leftKeys.size());
  for (size_t i = 0; i < leftKeyIndices_.size(); i++) {
    leftKeyIndices_[i] = static_cast<cudf::size_type>(
        probeType->getChildIdx(leftKeys[i]->name()));
    VELOX_CHECK_LT(leftKeyIndices_[i], probeTableNumColumns);
  }
  auto const buildTableNumColumns = buildType->size();
  rightKeyIndices_ = std::vector<cudf::size_type>(rightKeys.size());
  for (size_t i = 0; i < rightKeyIndices_.size(); i++) {
    rightKeyIndices_[i] = static_cast<cudf::size_type>(
        buildType->getChildIdx(rightKeys[i]->name()));
    VELOX_CHECK_LT(rightKeyIndices_[i], buildTableNumColumns);
  }

  auto outputType = joinNode_->outputType();
  leftColumnIndicesToGather_ = std::vector<cudf::size_type>();
  rightColumnIndicesToGather_ = std::vector<cudf::size_type>();
  leftColumnOutputIndices_ = std::vector<size_t>();
  rightColumnOutputIndices_ = std::vector<size_t>();
  for (int i = 0; i < outputType->names().size(); i++) {
    auto const outputName = outputType->names()[i];
    if (cudfDebugEnabled()) {
      std::cout << "Output column " << i << ": " << outputName << std::endl;
    }
    auto channel = probeType->getChildIdxIfExists(outputName);
    if (channel.has_value()) {
      leftColumnIndicesToGather_.push_back(
          static_cast<cudf::size_type>(channel.value()));
      leftColumnOutputIndices_.push_back(i);
      continue;
    }
    channel = buildType->getChildIdxIfExists(outputName);
    if (channel.has_value()) {
      rightColumnIndicesToGather_.push_back(
          static_cast<cudf::size_type>(channel.value()));
      rightColumnOutputIndices_.push_back(i);
      continue;
    }
    VELOX_FAIL(
        "Join field {} not in probe or build input", outputType->children()[i]);
  }

  if (cudfDebugEnabled()) {
    for (int i = 0; i < leftColumnIndicesToGather_.size(); i++) {
      std::cout << "Left index to gather " << i << ": "
                << leftColumnIndicesToGather_[i] << std::endl;
    }

    for (int i = 0; i < rightColumnIndicesToGather_.size(); i++) {
      std::cout << "Right index to gather " << i << ": "
                << rightColumnIndicesToGather_[i] << std::endl;
    }
  }

  // Setup filter in case it exists
  if (joinNode_->filter()) {
    // simplify expression
    exec::ExprSet exprs({joinNode_->filter()}, operatorCtx_->execCtx());
    VELOX_CHECK_EQ(exprs.exprs().size(), 1);

    // We don't need to get tables that contain conditional comparison columns
    // We'll pass the entire table. The ast will handle finding the required
    // columns. This is required because we build the ast with whole row schema
    // and the column locations in that schema translate to column locations
    // in whole tables

    // create ast tree
    std::vector<PrecomputeInstruction> rightPrecomputeInstructions;
    std::vector<PrecomputeInstruction> leftPrecomputeInstructions;
    if (joinNode_->isRightJoin() || joinNode_->isRightSemiFilterJoin()) {
      createAstTree(
          exprs.exprs()[0],
          tree_,
          scalars_,
          buildType,
          probeType,
          rightPrecomputeInstructions,
          leftPrecomputeInstructions);
    } else {
      createAstTree(
          exprs.exprs()[0],
          tree_,
          scalars_,
          probeType,
          buildType,
          leftPrecomputeInstructions,
          rightPrecomputeInstructions);
    }
    if (leftPrecomputeInstructions.size() > 0 ||
        rightPrecomputeInstructions.size() > 0) {
      VELOX_NYI("Filters that require precomputation are not yet supported");
    }
  }
}

bool CudfHashJoinProbe::needsInput() const {
  return !finished_ && input_ == nullptr;
}

void CudfHashJoinProbe::addInput(RowVectorPtr input) {
  input_ = std::move(input);
}

RowVectorPtr CudfHashJoinProbe::getOutput() {
  if (cudfDebugEnabled()) {
    std::cout << "Calling CudfHashJoinProbe::getOutput" << std::endl;
  }
  VELOX_NVTX_OPERATOR_FUNC_RANGE();

  if (!input_) {
    return nullptr;
  }
  if (!hashObject_.has_value()) {
    return nullptr;
  }
  auto cudfInput = std::dynamic_pointer_cast<CudfVector>(input_);
  VELOX_CHECK_NOT_NULL(cudfInput);
  auto stream = cudfInput->stream();
  auto leftTable = cudfInput->release(); // probe table
  if (cudfDebugEnabled()) {
    std::cout << "Probe table number of columns: " << leftTable->num_columns()
              << std::endl;
    std::cout << "Probe table number of rows: " << leftTable->num_rows()
              << std::endl;
  }

  // TODO pass the input pool !!!
  // TODO: We should probably subset columns before calling to_cudf_table?
  // Maybe that isn't a problem if we fuse operators together.
  auto& rightTable = hashObject_.value().first;
  auto& hb = hashObject_.value().second;
  VELOX_CHECK_NOT_NULL(rightTable);
  if (cudfDebugEnabled()) {
    if (rightTable != nullptr)
      printf(
          "right_table is not nullptr %p hasValue(%d)\n",
          rightTable.get(),
          hashObject_.has_value());
    if (hb != nullptr)
      printf(
          "hb is not nullptr %p hasValue(%d)\n",
          hb.get(),
          hashObject_.has_value());
  }

  std::unique_ptr<rmm::device_uvector<cudf::size_type>> leftJoinIndices;
  std::unique_ptr<rmm::device_uvector<cudf::size_type>> rightJoinIndices;

  auto leftTableView = leftTable->view();
  auto rightTableView = rightTable->view();

  if (joinNode_->isInnerJoin()) {
    // left = probe, right = build
    if (joinNode_->filter()) {
      std::tie(leftJoinIndices, rightJoinIndices) = cudf::mixed_inner_join(
          leftTableView.select(leftKeyIndices_),
          rightTableView.select(rightKeyIndices_),
          leftTableView,
          rightTableView,
          tree_.back(),
          cudf::null_equality::EQUAL,
          std::nullopt,
          stream);
    } else {
      VELOX_CHECK_NOT_NULL(hb);
      std::tie(leftJoinIndices, rightJoinIndices) = hb->inner_join(
          leftTableView.select(leftKeyIndices_), std::nullopt, stream);
    }
  } else if (joinNode_->isLeftJoin()) {
    if (joinNode_->filter()) {
      std::tie(leftJoinIndices, rightJoinIndices) = cudf::mixed_left_join(
          leftTableView.select(leftKeyIndices_),
          rightTableView.select(rightKeyIndices_),
          leftTableView,
          rightTableView,
          tree_.back(),
          cudf::null_equality::EQUAL,
          std::nullopt,
          stream);
    } else {
      VELOX_CHECK_NOT_NULL(hb);
      std::tie(leftJoinIndices, rightJoinIndices) = hb->left_join(
          leftTableView.select(leftKeyIndices_), std::nullopt, stream);
    }
  } else if (joinNode_->isRightJoin()) {
    if (joinNode_->filter()) {
      std::tie(rightJoinIndices, leftJoinIndices) = cudf::mixed_left_join(
          rightTableView.select(rightKeyIndices_),
          leftTableView.select(leftKeyIndices_),
          rightTableView,
          leftTableView,
          tree_.back(),
          cudf::null_equality::EQUAL,
          std::nullopt,
          stream);
    } else {
      std::tie(rightJoinIndices, leftJoinIndices) = cudf::left_join(
          rightTableView.select(rightKeyIndices_),
          leftTableView.select(leftKeyIndices_),
          cudf::null_equality::EQUAL,
          stream,
          cudf::get_current_device_resource_ref());
    }
  } else if (joinNode_->isAntiJoin()) {
    if (joinNode_->filter()) {
      leftJoinIndices = cudf::mixed_left_anti_join(
          leftTableView.select(leftKeyIndices_),
          rightTableView.select(rightKeyIndices_),
          leftTableView,
          rightTableView,
          tree_.back(),
          cudf::null_equality::EQUAL,
          stream,
          cudf::get_current_device_resource_ref());
    } else {
      leftJoinIndices = cudf::left_anti_join(
          leftTableView.select(leftKeyIndices_),
          rightTableView.select(rightKeyIndices_),
          cudf::null_equality::EQUAL,
          stream,
          cudf::get_current_device_resource_ref());
    }
  } else if (joinNode_->isLeftSemiFilterJoin()) {
    if (joinNode_->filter()) {
      leftJoinIndices = cudf::mixed_left_semi_join(
          leftTableView.select(leftKeyIndices_),
          rightTableView.select(rightKeyIndices_),
          leftTableView,
          rightTableView,
          tree_.back(),
          cudf::null_equality::EQUAL,
          stream,
          cudf::get_current_device_resource_ref());
    } else {
      leftJoinIndices = cudf::left_semi_join(
          leftTableView.select(leftKeyIndices_),
          rightTableView.select(rightKeyIndices_),
          cudf::null_equality::EQUAL,
          stream,
          cudf::get_current_device_resource_ref());
    }
  } else if (joinNode_->isRightSemiFilterJoin()) {
    if (joinNode_->filter()) {
      rightJoinIndices = cudf::mixed_left_semi_join(
          rightTableView.select(rightKeyIndices_),
          leftTableView.select(leftKeyIndices_),
          rightTableView,
          leftTableView,
          tree_.back(),
          cudf::null_equality::EQUAL,
          stream,
          cudf::get_current_device_resource_ref());
    } else {
      rightJoinIndices = cudf::left_semi_join(
          rightTableView.select(rightKeyIndices_),
          leftTableView.select(leftKeyIndices_),
          cudf::null_equality::EQUAL,
          stream,
          cudf::get_current_device_resource_ref());
    }
  } else {
    VELOX_FAIL("Unsupported join type: ", joinNode_->joinType());
  }
  auto leftIndicesSpan = leftJoinIndices
      ? cudf::device_span<cudf::size_type const>{*leftJoinIndices}
      : cudf::device_span<cudf::size_type const>{};
  auto rightIndicesSpan = rightJoinIndices
      ? cudf::device_span<cudf::size_type const>{*rightJoinIndices}
      : cudf::device_span<cudf::size_type const>{};

  auto leftInput = leftTableView.select(leftColumnIndicesToGather_);
  auto rightInput = rightTableView.select(rightColumnIndicesToGather_);

  auto leftIndicesCol = cudf::column_view{leftIndicesSpan};
  auto rightIndicesCol = cudf::column_view{rightIndicesSpan};
  auto constexpr oobPolicy = cudf::out_of_bounds_policy::NULLIFY;
  auto leftResult = cudf::gather(leftInput, leftIndicesCol, oobPolicy, stream);
  auto rightResult =
      cudf::gather(rightInput, rightIndicesCol, oobPolicy, stream);

  if (cudfDebugEnabled()) {
    std::cout << "Left result number of columns: " << leftResult->num_columns()
              << std::endl;
    std::cout << "Right result number of columns: "
              << rightResult->num_columns() << std::endl;
  }

  auto leftCols = leftResult->release();
  auto rightCols = rightResult->release();
  auto joinedCols =
      std::vector<std::unique_ptr<cudf::column>>(outputType_->names().size());
  for (int i = 0; i < leftColumnOutputIndices_.size(); i++) {
    joinedCols[leftColumnOutputIndices_[i]] = std::move(leftCols[i]);
  }
  for (int i = 0; i < rightColumnOutputIndices_.size(); i++) {
    joinedCols[rightColumnOutputIndices_[i]] = std::move(rightCols[i]);
  }
  auto cudfOutput = std::make_unique<cudf::table>(std::move(joinedCols));
  stream.synchronize();

  input_.reset();
  finished_ = noMoreInput_;

  auto const size = cudfOutput->num_rows();
  if (cudfOutput->num_columns() == 0 or size == 0) {
    return nullptr;
  }
  return std::make_shared<CudfVector>(
      pool(), outputType_, size, std::move(cudfOutput), stream);
}

exec::BlockingReason CudfHashJoinProbe::isBlocked(ContinueFuture* future) {
  if (hashObject_.has_value()) {
    return exec::BlockingReason::kNotBlocked;
  }

  auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
      operatorCtx_->driverCtx()->splitGroupId, planNodeId());
  auto cudfJoinBridge =
      std::dynamic_pointer_cast<CudfHashJoinBridge>(joinBridge);
  VELOX_CHECK_NOT_NULL(cudfJoinBridge);
  VELOX_CHECK_NOT_NULL(future);
  auto hashObject = cudfJoinBridge->hashOrFuture(future);

  if (!hashObject.has_value()) {
    if (cudfDebugEnabled()) {
      std::cout << "CudfHashJoinProbe is blocked, waiting for join build"
                << std::endl;
    }
    return exec::BlockingReason::kWaitForJoinBuild;
  }
  hashObject_ = std::move(hashObject);

  return exec::BlockingReason::kNotBlocked;
}

bool CudfHashJoinProbe::isFinished() {
  auto const isFinished = finished_ || (noMoreInput_ && input_ == nullptr);

  // Release hashObject_ if finished
  if (isFinished) {
    hashObject_.reset();
  }
  return isFinished;
}

std::unique_ptr<exec::Operator> CudfHashJoinBridgeTranslator::toOperator(
    exec::DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  if (cudfDebugEnabled()) {
    std::cout << "Calling CudfHashJoinBridgeTranslator::toOperator"
              << std::endl;
  }
  if (auto joinNode =
          std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
    return std::make_unique<CudfHashJoinProbe>(id, ctx, joinNode);
  }
  return nullptr;
}

std::unique_ptr<exec::JoinBridge> CudfHashJoinBridgeTranslator::toJoinBridge(
    const core::PlanNodePtr& node) {
  if (cudfDebugEnabled()) {
    std::cout << "Calling CudfHashJoinBridgeTranslator::toJoinBridge"
              << std::endl;
  }
  if (auto joinNode =
          std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
    auto joinBridge = std::make_unique<CudfHashJoinBridge>();
    return joinBridge;
  }
  return nullptr;
}

exec::OperatorSupplier CudfHashJoinBridgeTranslator::toOperatorSupplier(
    const core::PlanNodePtr& node) {
  if (cudfDebugEnabled()) {
    std::cout << "Calling CudfHashJoinBridgeTranslator::toOperatorSupplier"
              << std::endl;
  }
  if (auto joinNode =
          std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
    return [joinNode](int32_t operatorId, exec::DriverCtx* ctx) {
      return std::make_unique<CudfHashJoinBuild>(operatorId, ctx, joinNode);
    };
  }
  return nullptr;
}

} // namespace facebook::velox::cudf_velox
