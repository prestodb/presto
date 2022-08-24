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
#include "velox/exec/LocalPlanner.h"
#include "velox/core/PlanFragment.h"
#include "velox/exec/AssignUniqueId.h"
#include "velox/exec/CallbackSink.h"
#include "velox/exec/CrossJoinBuild.h"
#include "velox/exec/CrossJoinProbe.h"
#include "velox/exec/EnforceSingleRow.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/FilterProject.h"
#include "velox/exec/GroupId.h"
#include "velox/exec/HashAggregation.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/HashProbe.h"
#include "velox/exec/Limit.h"
#include "velox/exec/Merge.h"
#include "velox/exec/MergeJoin.h"
#include "velox/exec/OrderBy.h"
#include "velox/exec/PartitionedOutput.h"
#include "velox/exec/StreamingAggregation.h"
#include "velox/exec/TableScan.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/TopN.h"
#include "velox/exec/Unnest.h"
#include "velox/exec/Values.h"
#include "velox/exec/Window.h"

namespace facebook::velox::exec {

namespace detail {

/// Returns true if source nodes must run in a separate pipeline.
bool mustStartNewPipeline(
    std::shared_ptr<const core::PlanNode> planNode,
    int sourceId) {
  if (auto localMerge =
          std::dynamic_pointer_cast<const core::LocalMergeNode>(planNode)) {
    // LocalMerge's source runs on its own pipeline.
    return true;
  }

  if (std::dynamic_pointer_cast<const core::LocalPartitionNode>(planNode)) {
    return true;
  }

  // Non-first sources always run in their own pipeline.
  return sourceId != 0;
}

OperatorSupplier makeConsumerSupplier(ConsumerSupplier consumerSupplier) {
  if (consumerSupplier) {
    return [consumerSupplier](int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<CallbackSink>(
          operatorId, ctx, consumerSupplier());
    };
  }
  return nullptr;
}

OperatorSupplier makeConsumerSupplier(
    const std::shared_ptr<const core::PlanNode>& planNode) {
  if (auto localMerge =
          std::dynamic_pointer_cast<const core::LocalMergeNode>(planNode)) {
    return [localMerge](int32_t operatorId, DriverCtx* ctx) {
      auto mergeSource = ctx->task->addLocalMergeSource(
          ctx->splitGroupId, localMerge->id(), localMerge->outputType());

      auto consumer = [mergeSource](
                          RowVectorPtr input, ContinueFuture* future) {
        return mergeSource->enqueue(input, future);
      };
      return std::make_unique<CallbackSink>(operatorId, ctx, consumer);
    };
  }

  if (auto localPartitionNode =
          std::dynamic_pointer_cast<const core::LocalPartitionNode>(planNode)) {
    return [localPartitionNode](int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<LocalPartition>(
          operatorId, ctx, localPartitionNode);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
    return [join](int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<HashBuild>(operatorId, ctx, join);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::CrossJoinNode>(planNode)) {
    return [join](int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<CrossJoinBuild>(operatorId, ctx, join);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::MergeJoinNode>(planNode)) {
    auto planNodeId = planNode->id();
    return [planNodeId](int32_t operatorId, DriverCtx* ctx) {
      auto source =
          ctx->task->getMergeJoinSource(ctx->splitGroupId, planNodeId);
      auto consumer = [source](RowVectorPtr input, ContinueFuture* future) {
        return source->enqueue(input, future);
      };
      return std::make_unique<CallbackSink>(operatorId, ctx, consumer);
    };
  }

  return Operator::operatorSupplierFromPlanNode(planNode);
}

void plan(
    const std::shared_ptr<const core::PlanNode>& planNode,
    std::vector<std::shared_ptr<const core::PlanNode>>* currentPlanNodes,
    const std::shared_ptr<const core::PlanNode>& consumerNode,
    OperatorSupplier consumerSupplier,
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories) {
  if (!currentPlanNodes) {
    driverFactories->push_back(std::make_unique<DriverFactory>());
    currentPlanNodes = &driverFactories->back()->planNodes;
    driverFactories->back()->consumerSupplier = consumerSupplier;
    driverFactories->back()->consumerNode = consumerNode;
  }

  auto sources = planNode->sources();
  if (sources.empty()) {
    driverFactories->back()->inputDriver = true;
  } else {
    for (int32_t i = 0; i < sources.size(); ++i) {
      plan(
          sources[i],
          mustStartNewPipeline(planNode, i) ? nullptr : currentPlanNodes,
          planNode,
          makeConsumerSupplier(planNode),
          driverFactories);
    }
  }

  currentPlanNodes->push_back(planNode);
}

// Sometimes consumer limits the number of drivers its producer can run.
uint32_t maxDriversForConsumer(
    const std::shared_ptr<const core::PlanNode>& node) {
  if (std::dynamic_pointer_cast<const core::MergeJoinNode>(node)) {
    // MergeJoinNode must run single-threaded.
    return 1;
  }
  return std::numeric_limits<uint32_t>::max();
}

uint32_t maxDrivers(const DriverFactory& driverFactory) {
  uint32_t count = maxDriversForConsumer(driverFactory.consumerNode);
  if (count == 1) {
    return count;
  }
  for (auto& node : driverFactory.planNodes) {
    if (auto topN = std::dynamic_pointer_cast<const core::TopNNode>(node)) {
      if (!topN->isPartial()) {
        // final topN must run single-threaded
        return 1;
      }
    } else if (
        auto values = std::dynamic_pointer_cast<const core::ValuesNode>(node)) {
      // values node must run single-threaded, unless in test context
      if (!values->isParallelizable()) {
        return 1;
      }
    } else if (
        auto limit = std::dynamic_pointer_cast<const core::LimitNode>(node)) {
      // final limit must run single-threaded
      if (!limit->isPartial()) {
        return 1;
      }
    } else if (
        auto orderBy =
            std::dynamic_pointer_cast<const core::OrderByNode>(node)) {
      // final orderby must run single-threaded
      if (!orderBy->isPartial()) {
        return 1;
      }
    } else if (
        auto localExchange =
            std::dynamic_pointer_cast<const core::LocalPartitionNode>(node)) {
      // Local gather must run single-threaded.
      if (localExchange->type() == core::LocalPartitionNode::Type::kGather) {
        return 1;
      }
    } else if (std::dynamic_pointer_cast<const core::LocalMergeNode>(node)) {
      // Local merge must run single-threaded.
      return 1;
    } else if (std::dynamic_pointer_cast<const core::MergeExchangeNode>(node)) {
      // Merge exchange must run single-threaded.
      return 1;
    } else if (std::dynamic_pointer_cast<const core::MergeJoinNode>(node)) {
      // Merge join must run single-threaded.
      return 1;
    } else if (
        auto tableWrite =
            std::dynamic_pointer_cast<const core::TableWriteNode>(node)) {
      if (!tableWrite->insertTableHandle()
               ->connectorInsertTableHandle()
               ->supportsMultiThreading()) {
        return 1;
      }
    } else {
      auto result = Operator::maxDrivers(node);
      if (result) {
        VELOX_CHECK_GT(
            *result,
            0,
            "maxDrivers must be greater than 0. Plan node: {}",
            node->toString())
        if (*result == 1) {
          return 1;
        }
        count = std::min(*result, count);
      }
    }
  }
  return count;
}
} // namespace detail

// static
void LocalPlanner::plan(
    const core::PlanFragment& planFragment,
    ConsumerSupplier consumerSupplier,
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories,
    uint32_t maxDrivers) {
  detail::plan(
      planFragment.planNode,
      nullptr,
      nullptr,
      detail::makeConsumerSupplier(consumerSupplier),
      driverFactories);

  (*driverFactories)[0]->outputDriver = true;

  for (auto& factory : *driverFactories) {
    factory->maxDrivers = detail::maxDrivers(*factory);
    factory->numDrivers = std::min(factory->maxDrivers, maxDrivers);
    // For grouped/bucketed execution we would have separate groups of drivers
    // dealing with separate split groups (one driver can access splits from
    // only one designated split group), hence we will have total number of
    // drivers multiplied by the number of split groups.
    if (planFragment.isGroupedExecution()) {
      factory->numTotalDrivers =
          factory->numDrivers * planFragment.numSplitGroups;
    } else {
      factory->numTotalDrivers = factory->numDrivers;
    }
  }
}

std::shared_ptr<Driver> DriverFactory::createDriver(
    std::unique_ptr<DriverCtx> ctx,
    std::shared_ptr<ExchangeClient> exchangeClient,
    std::function<int(int pipelineId)> numDrivers) {
  std::vector<std::unique_ptr<Operator>> operators;
  operators.reserve(planNodes.size());

  for (int32_t i = 0; i < planNodes.size(); i++) {
    // Id of the Operator being made. This is not the same as 'i'
    // because some PlanNodes may get fused.
    auto id = operators.size();
    auto planNode = planNodes[i];
    if (auto filterNode =
            std::dynamic_pointer_cast<const core::FilterNode>(planNode)) {
      if (i < planNodes.size() - 1) {
        auto next = planNodes[i + 1];
        if (auto projectNode =
                std::dynamic_pointer_cast<const core::ProjectNode>(next)) {
          operators.push_back(std::make_unique<FilterProject>(
              id, ctx.get(), filterNode, projectNode));
          i++;
          continue;
        }
      }
      operators.push_back(
          std::make_unique<FilterProject>(id, ctx.get(), filterNode, nullptr));
    } else if (
        auto projectNode =
            std::dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
      operators.push_back(
          std::make_unique<FilterProject>(id, ctx.get(), nullptr, projectNode));
    } else if (
        auto valuesNode =
            std::dynamic_pointer_cast<const core::ValuesNode>(planNode)) {
      operators.push_back(std::make_unique<Values>(id, ctx.get(), valuesNode));
    } else if (
        auto tableScanNode =
            std::dynamic_pointer_cast<const core::TableScanNode>(planNode)) {
      operators.push_back(
          std::make_unique<TableScan>(id, ctx.get(), tableScanNode));
    } else if (
        auto tableWriteNode =
            std::dynamic_pointer_cast<const core::TableWriteNode>(planNode)) {
      operators.push_back(
          std::make_unique<TableWriter>(id, ctx.get(), tableWriteNode));
    } else if (
        auto mergeExchangeNode =
            std::dynamic_pointer_cast<const core::MergeExchangeNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<MergeExchange>(i, ctx.get(), mergeExchangeNode));
    } else if (
        auto exchangeNode =
            std::dynamic_pointer_cast<const core::ExchangeNode>(planNode)) {
      operators.push_back(std::make_unique<Exchange>(
          id, ctx.get(), exchangeNode, exchangeClient));
    } else if (
        auto partitionedOutputNode =
            std::dynamic_pointer_cast<const core::PartitionedOutputNode>(
                planNode)) {
      operators.push_back(std::make_unique<PartitionedOutput>(
          id, ctx.get(), partitionedOutputNode));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
      operators.push_back(std::make_unique<HashProbe>(id, ctx.get(), joinNode));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::CrossJoinNode>(planNode)) {
      operators.push_back(
          std::make_unique<CrossJoinProbe>(id, ctx.get(), joinNode));
    } else if (
        auto aggregationNode =
            std::dynamic_pointer_cast<const core::AggregationNode>(planNode)) {
      if (!aggregationNode->preGroupedKeys().empty() &&
          aggregationNode->preGroupedKeys().size() ==
              aggregationNode->groupingKeys().size()) {
        operators.push_back(std::make_unique<StreamingAggregation>(
            id, ctx.get(), aggregationNode));
      } else {
        operators.push_back(
            std::make_unique<HashAggregation>(id, ctx.get(), aggregationNode));
      }
    } else if (
        auto groupIdNode =
            std::dynamic_pointer_cast<const core::GroupIdNode>(planNode)) {
      operators.push_back(
          std::make_unique<GroupId>(id, ctx.get(), groupIdNode));
    } else if (
        auto topNNode =
            std::dynamic_pointer_cast<const core::TopNNode>(planNode)) {
      operators.push_back(std::make_unique<TopN>(id, ctx.get(), topNNode));
    } else if (
        auto limitNode =
            std::dynamic_pointer_cast<const core::LimitNode>(planNode)) {
      operators.push_back(std::make_unique<Limit>(id, ctx.get(), limitNode));
    } else if (
        auto orderByNode =
            std::dynamic_pointer_cast<const core::OrderByNode>(planNode)) {
      operators.push_back(
          std::make_unique<OrderBy>(id, ctx.get(), orderByNode));
    } else if (
        auto windowNode =
            std::dynamic_pointer_cast<const core::WindowNode>(planNode)) {
      operators.push_back(std::make_unique<Window>(id, ctx.get(), windowNode));
    } else if (
        auto localMerge =
            std::dynamic_pointer_cast<const core::LocalMergeNode>(planNode)) {
      auto localMergeOp =
          std::make_unique<LocalMerge>(id, ctx.get(), localMerge);
      operators.push_back(std::move(localMergeOp));
    } else if (
        auto mergeJoin =
            std::dynamic_pointer_cast<const core::MergeJoinNode>(planNode)) {
      auto mergeJoinOp = std::make_unique<MergeJoin>(id, ctx.get(), mergeJoin);
      ctx->task->createMergeJoinSource(ctx->splitGroupId, mergeJoin->id());
      operators.push_back(std::move(mergeJoinOp));
    } else if (
        auto localPartitionNode =
            std::dynamic_pointer_cast<const core::LocalPartitionNode>(
                planNode)) {
      operators.push_back(std::make_unique<LocalExchange>(
          id,
          ctx.get(),
          localPartitionNode->outputType(),
          localPartitionNode->id(),
          ctx->partitionId));
    } else if (
        auto unnest =
            std::dynamic_pointer_cast<const core::UnnestNode>(planNode)) {
      operators.push_back(std::make_unique<Unnest>(id, ctx.get(), unnest));
    } else if (
        auto enforceSingleRow =
            std::dynamic_pointer_cast<const core::EnforceSingleRowNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<EnforceSingleRow>(id, ctx.get(), enforceSingleRow));
    } else if (
        auto assignUniqueIdNode =
            std::dynamic_pointer_cast<const core::AssignUniqueIdNode>(
                planNode)) {
      operators.push_back(std::make_unique<AssignUniqueId>(
          id,
          ctx.get(),
          assignUniqueIdNode,
          assignUniqueIdNode->taskUniqueId(),
          assignUniqueIdNode->uniqueIdCounter()));
    } else {
      auto extended = Operator::fromPlanNode(ctx.get(), id, planNode);
      VELOX_CHECK(extended, "Unsupported plan node: {}", planNode->toString());
      operators.push_back(std::move(extended));
    }
  }
  if (consumerSupplier) {
    operators.push_back(consumerSupplier(operators.size(), ctx.get()));
  }

  return std::make_shared<Driver>(std::move(ctx), std::move(operators));
}
} // namespace facebook::velox::exec
