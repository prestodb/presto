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
#include <set>

#include "velox/core/PlanFragment.h"
#include "velox/exec/ArrowStream.h"
#include "velox/exec/AssignUniqueId.h"
#include "velox/exec/CallbackSink.h"
#include "velox/exec/EnforceSingleRow.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Expand.h"
#include "velox/exec/FilterProject.h"
#include "velox/exec/GroupId.h"
#include "velox/exec/HashAggregation.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/HashProbe.h"
#include "velox/exec/IndexLookupJoin.h"
#include "velox/exec/Limit.h"
#include "velox/exec/LocalPlanner.h"
#include "velox/exec/MarkDistinct.h"
#include "velox/exec/Merge.h"
#include "velox/exec/MergeJoin.h"
#include "velox/exec/NestedLoopJoinBuild.h"
#include "velox/exec/NestedLoopJoinProbe.h"
#include "velox/exec/OperatorTraceScan.h"
#include "velox/exec/OrderBy.h"
#include "velox/exec/ParallelProject.h"
#include "velox/exec/PartitionedOutput.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/exec/RowNumber.h"
#include "velox/exec/ScaleWriterLocalPartition.h"
#include "velox/exec/SpatialJoinBuild.h"
#include "velox/exec/SpatialJoinProbe.h"
#include "velox/exec/StreamingAggregation.h"
#include "velox/exec/TableScan.h"
#include "velox/exec/TableWriteMerge.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/Task.h"
#include "velox/exec/TopN.h"
#include "velox/exec/TopNRowNumber.h"
#include "velox/exec/Unnest.h"
#include "velox/exec/Values.h"
#include "velox/exec/Window.h"

namespace facebook::velox::exec {

namespace {

// If the upstream is partial limit, downstream is final limit and we want to
// flush as soon as we can to reach the limit and do as little work as possible.
bool eagerFlush(const core::PlanNode& node) {
  if (auto* limit = dynamic_cast<const core::LimitNode*>(&node)) {
    return limit->isPartial() && limit->offset() + limit->count() < 10'000;
  }
  if (node.sources().empty()) {
    return false;
  }
  // Follow the first source, which is driving the output.
  return eagerFlush(*node.sources()[0]);
}

} // namespace

namespace detail {

using PlanNodePtr = std::shared_ptr<const core::PlanNode>;

/// Returns true if source nodes must run in a separate pipeline.
bool mustStartNewPipeline(const PlanNodePtr& planNode, int sourceId) {
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

// Creates the customized local partition operator for table writer scaling.
std::unique_ptr<Operator> createScaleWriterLocalPartition(
    const std::shared_ptr<const core::LocalPartitionNode>& localPartitionNode,
    int32_t operatorId,
    DriverCtx* ctx) {
  if (dynamic_cast<const RoundRobinPartitionFunctionSpec*>(
          &localPartitionNode->partitionFunctionSpec())) {
    return std::make_unique<ScaleWriterLocalPartition>(
        operatorId, ctx, localPartitionNode);
  }

  return std::make_unique<ScaleWriterPartitioningLocalPartition>(
      operatorId, ctx, localPartitionNode);
}

OperatorSupplier makeOperatorSupplier(ConsumerSupplier consumerSupplier) {
  if (consumerSupplier) {
    return [consumerSupplier = std::move(consumerSupplier)](
               int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<CallbackSink>(
          operatorId, ctx, consumerSupplier());
    };
  }
  return nullptr;
}

OperatorSupplier makeOperatorSupplier(const PlanNodePtr& planNode) {
  if (auto localMerge =
          std::dynamic_pointer_cast<const core::LocalMergeNode>(planNode)) {
    return [localMerge](int32_t operatorId, DriverCtx* ctx) {
      auto mergeSource = ctx->task->addLocalMergeSource(
          ctx->splitGroupId,
          localMerge->id(),
          localMerge->outputType(),
          ctx->queryConfig().localMergeSourceQueueSize());
      auto consumerCb =
          [mergeSource](
              RowVectorPtr input, bool drained, ContinueFuture* future) {
            VELOX_CHECK(!drained);
            return mergeSource->enqueue(std::move(input), future);
          };
      auto startCb = [mergeSource](ContinueFuture* future) {
        return mergeSource->started(future);
      };
      return std::make_unique<CallbackSink>(
          operatorId, ctx, std::move(consumerCb), std::move(startCb));
    };
  }

  if (auto localPartitionNode =
          std::dynamic_pointer_cast<const core::LocalPartitionNode>(planNode)) {
    if (localPartitionNode->scaleWriter()) {
      return [localPartitionNode](int32_t operatorId, DriverCtx* ctx) {
        return createScaleWriterLocalPartition(
            localPartitionNode, operatorId, ctx);
      };
    }
    bool useEagerFlush = eagerFlush(*planNode);
    return [localPartitionNode, useEagerFlush](
               int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<LocalPartition>(
          operatorId, ctx, localPartitionNode, useEagerFlush);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
    return [join](int32_t operatorId, DriverCtx* ctx) {
      if (ctx->task->hasMixedExecutionGroupJoin(join.get()) &&
          needRightSideJoin(join->joinType())) {
        VELOX_UNSUPPORTED(
            "Hash join currently does not support mixed grouped execution for join "
            "type {}",
            core::JoinTypeName::toName(join->joinType()));
      }
      return std::make_unique<HashBuild>(operatorId, ctx, join);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(planNode)) {
    return [join](int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<NestedLoopJoinBuild>(operatorId, ctx, join);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::SpatialJoinNode>(planNode)) {
    return [join](int32_t operatorId, DriverCtx* ctx) {
      return std::make_unique<SpatialJoinBuild>(operatorId, ctx, join);
    };
  }

  if (auto join =
          std::dynamic_pointer_cast<const core::MergeJoinNode>(planNode)) {
    auto planNodeId = planNode->id();
    return [planNodeId](int32_t operatorId, DriverCtx* ctx) {
      auto source =
          ctx->task->getMergeJoinSource(ctx->splitGroupId, planNodeId);
      auto consumer =
          [source](RowVectorPtr input, bool drained, ContinueFuture* future) {
            if (drained) {
              VELOX_CHECK_NULL(input);
              source->drain();
              return BlockingReason::kNotBlocked;
            } else {
              VELOX_CHECK(!drained);
              return source->enqueue(std::move(input), future);
            }
          };
      return std::make_unique<CallbackSink>(operatorId, ctx, consumer);
    };
  }

  return Operator::operatorSupplierFromPlanNode(planNode);
}

// void removeOutputFromHashKeys(
//     const RowTypePtr& outputRowType,
//     std::set<std::string>& hashKeys) {
//   for (const auto& outputFieldName : outputRowType->names()) {
//     hashKeys.erase(outputFieldName);
//   }
//
//   if (type->isPrimitiveType()) {
//     hashKeys.erase(std::string(name));
//     return;
//   }
//
//   if (type->isMap()) {
//     const auto& mapType = type->as<TypeKind::MAP>();
//     removeOutputFromHashKeys(mapType.keyType(), hashKeys);
//     removeOutputFromHashKeys(mapType.valueType(), hashKeys);
//   } else if (type->isArray()) {
//     const auto& arrayType = type->as<TypeKind::ARRAY>();
//     removeOutputFromHashKeys(arrayType.elementType(), hashKeys);
//   } else if (type->isRow()) {
//     const auto& rowType = type->as<TypeKind::ROW>();
//     for (const auto& childType : rowType.children()) {
//       removeOutputFromHashKeys(childType, hashKeys);
//     }
//   } else {
//     VELOX_UNREACHABLE(
//         "removeOutputFromHashKeys: Unsupported complex type: {}",
//         type->toString());
//   }
// }

bool isIntegral(const TypePtr& type) {
  return type->isBigint() || type->isInteger() || type->isSmallint() ||
      type->isTinyint();
}

bool isWideningIntegralCast(const core::TypedExprPtr& expr) {
  auto castExpr = std::dynamic_pointer_cast<const core::CastTypedExpr>(expr);
  if (!castExpr) {
    return false;
  }

  auto outputType = castExpr->type();
  auto inputType = castExpr->inputs()[0]->type();
  if (!isIntegral(outputType) || !isIntegral(inputType)) {
    return false;
  }
  if (outputType->cppSizeInBytes() > inputType->cppSizeInBytes()) {
    return true;
  }
  return false;
}
//
// std::map<std::string, std::pair<std::string, TypePtr>> plan(
//    const PlanNodePtr& planNode,
//    std::vector<PlanNodePtr>* currentPlanNodes,
//    const PlanNodePtr& consumerNode,
//    OperatorSupplier operatorSupplier,
//    std::vector<std::unique_ptr<DriverFactory>>* driverFactories,
//    std::set<std::string>& hashOnlyKeys) {
//  if (!currentPlanNodes) {
//    auto driverFactory = std::make_unique<DriverFactory>();
//    currentPlanNodes = &driverFactory->planNodes;
//    driverFactory->operatorSupplier = std::move(operatorSupplier);
//    driverFactory->consumerNode = consumerNode;
//    driverFactories->push_back(std::move(driverFactory));
//  }
//
//  std::map<std::string, std::pair<std::string, TypePtr>> updatedOutputTypes;
//  if (auto joinNode =
//          std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
//    // Collect all hash keys from both sides of the join that are not output.
//    std::set<std::string> currentHashKeys;
//    auto& leftKeys = joinNode->leftKeys();
//    for (int i = 0; i < leftKeys.size(); ++i) {
//      currentHashKeys.insert(leftKeys[i]->name());
//    }
//    auto& rightKeys = joinNode->rightKeys();
//    for (int i = 0; i < rightKeys.size(); ++i) {
//      currentHashKeys.insert(rightKeys[i]->name());
//    }
//
//    // Traverse the output type and remove the hash keys that are also output
//    removeOutputFromHashKeys(planNode->outputType(), currentHashKeys);
//
//    // Merge with the hash keys from the upper level.
//    for (const auto& key : currentHashKeys) {
//      hashOnlyKeys.insert(key);
//    }
//  } else if (
//      auto projectNode = std::const_pointer_cast<core::ProjectNode>(
//          dynamic_pointer_cast<const core::ProjectNode>(planNode))) {
//    // Remove cast expressions on hash keys if they are widening integral
//    casts. for (auto i = 0; i < projectNode->names().size(); i++) {
//      auto name = projectNode->names()[i];
//      if (hashOnlyKeys.find(name) != hashOnlyKeys.end()) {
//        auto projection = projectNode->projections()[i];
//        if (isWideningIntegralCast(projection)) {
//          hashOnlyKeys.erase(name);
//          VELOX_CHECK_EQ(projection->inputs().size(), 1);
//          auto newProjection = projection->inputs()[0];
//
//          // This implementation only handles field access expressions.
//          if (newProjection->isFieldAccessKind()) {
//            auto fieldAccessExpr =
//                std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
//                    newProjection);
//            auto newName = fieldAccessExpr->name();
//            projectNode->setProjection(i, newProjection);
//            updatedOutputTypes[name] =
//                std::make_pair(newName, newProjection->type());
//          }
//        }
//      }
//    }
//    if (!updatedOutputTypes.empty()) {
//      projectNode->updateNewTypes(updatedOutputTypes);
//    }
//  } else if (
//      auto tableScanNode = std::const_pointer_cast<core::TableScanNode>(
//          dynamic_pointer_cast<const core::TableScanNode>(planNode))) {
//    // Remove cast expressions on hash keys if they are widening integral
//    casts. for (auto& column : tableScanNode->assignments()) {
//      printf("Column: %s", column.first.c_str());
//    }
//  }
//
//  const auto& sources = planNode->sources();
//  if (sources.empty()) {
//    driverFactories->back()->inputDriver = true;
//  } else {
//    const auto numSourcesToPlan =
//        isIndexLookupJoin(planNode.get()) ? 1 : sources.size();
//    for (int32_t i = 0; i < numSourcesToPlan; ++i) {
//      auto updatedTypesBySource = plan(
//          sources[i],
//          mustStartNewPipeline(planNode, i) ? nullptr : currentPlanNodes,
//          planNode,
//          makeOperatorSupplier(planNode),
//          driverFactories,
//          hashOnlyKeys);
//
//      // Backtrack and propagate the updated types to the current plan node.
//      if (!updatedTypesBySource.empty()) {
//        //        auto updatedOutputTypesBySource =
//        auto mutablePlanNode =
//            std::const_pointer_cast<core::PlanNode>(planNode);
//        mutablePlanNode->updateNewTypes(updatedTypesBySource);
//        updatedOutputTypes.insert(
//            updatedTypesBySource.begin(), updatedTypesBySource.end());
//        //        if (auto joinNode =
//        //        std::const_pointer_cast<core::HashJoinNode>(
//        //                std::dynamic_pointer_cast<const
//        //                core::HashJoinNode>(planNode))) {
//        //          joinNode->updateJoinKeys(updatedTypesBySource);
//        //        }
//      }
//    }
//  }
//
//  currentPlanNodes->push_back(planNode);
//  return updatedOutputTypes;
//}
//

void plan(
    const std::shared_ptr<const core::PlanNode>& planNode,
    std::vector<std::shared_ptr<const core::PlanNode>>* currentPlanNodes,
    const std::shared_ptr<const core::PlanNode>& consumerNode,
    OperatorSupplier operatorSupplier,
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories) {
  if (!currentPlanNodes) {
    auto driverFactory = std::make_unique<DriverFactory>();
    currentPlanNodes = &driverFactory->planNodes;
    driverFactory->operatorSupplier = std::move(operatorSupplier);
    driverFactory->consumerNode = consumerNode;
    driverFactories->push_back(std::move(driverFactory));
  }

  const auto& sources = planNode->sources();
  if (sources.empty()) {
    driverFactories->back()->inputDriver = true;
  } else {
    const auto numSourcesToPlan =
        isIndexLookupJoin(planNode.get()) ? 1 : sources.size();
    for (int32_t i = 0; i < numSourcesToPlan; ++i) {
      plan(
          sources[i],
          mustStartNewPipeline(planNode, i) ? nullptr : currentPlanNodes,
          planNode,
          makeOperatorSupplier(planNode),
          driverFactories);
    }
  }

  currentPlanNodes->push_back(planNode);
}

void updateHashKeysForHashJoinNode(
    const std::shared_ptr<const core::HashJoinNode>& joinNode,
    int8_t childIndex,
    std::set<std::string>& hashOnlyKeys) {
  std::vector<core::FieldAccessTypedExprPtr>* keys;

  if (childIndex == 0) {
    auto leftKeys = joinNode->leftKeys();
    for (int i = 0; i < leftKeys.size(); ++i) {
      hashOnlyKeys.insert(leftKeys[i]->name());
    }
  } else if (childIndex == 1) {
    auto rightKeys = joinNode->rightKeys();
    for (int i = 0; i < rightKeys.size(); ++i) {
      hashOnlyKeys.insert(rightKeys[i]->name());
    }
  } else {
    VELOX_UNREACHABLE("Invalid child index: {}", childIndex);
  }

  // Traverse the output type and remove the hash keys that are also output
  for (const auto& outputFieldName : joinNode->outputType()->names()) {
    hashOnlyKeys.erase(outputFieldName);
  }
  //  removeOutputFromHashKeys(joinNode->outputType(), hashOnlyKeys);
}

PlanNodePtr plan2(
    PlanNodePtr planNode,
    std::vector<PlanNodePtr>* currentPlanNodes,
    const PlanNodePtr& consumerNode,
    OperatorSupplier operatorSupplier,
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories,
    std::set<std::string>& hashOnlyKeys) {
  LOG(INFO) << "Planning node: " << planNode->toString() << " from consumer: "
            << (consumerNode.get() ? consumerNode->toString() : "null");
  // 0) Start a new pipeline if needed.
  if (!currentPlanNodes) {
    // Leaf node, start a new driver factory.
    auto driverFactory = std::make_unique<DriverFactory>();
    currentPlanNodes = &driverFactory->planNodes;
    driverFactory->operatorSupplier = std::move(operatorSupplier);
    driverFactory->consumerNode = consumerNode;
    driverFactories->push_back(std::move(driverFactory));
  }

  auto& sources = planNode->sources();
  if (sources.empty()) {
    // Leaf: fall through and add this node below.
    driverFactories->back()->inputDriver = true;
  } else {
    bool needNewPlanNode = false;
    std::set<int> projectionsNeedUpdate;
    std::vector<PlanNodePtr> newSources;

    // 1) Plan children
    const auto numSourcesToPlan =
        isIndexLookupJoin(planNode.get()) ? 1 : sources.size();
    for (int32_t i = 0; i < numSourcesToPlan; ++i) {
      // 1.1 Prepare for planning, update the hash keys.
      // For each source, we may need to update the hash keys based on the
      // current node type and whether it's a widening cast.
      if (auto joinNode =
              std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
        // 1.1.1 For hash join node, we need to update the hash keys for each
        // side of the join.
        updateHashKeysForHashJoinNode(joinNode, i, hashOnlyKeys);
        for (const auto& key : hashOnlyKeys) {
          LOG(INFO) << "Hash Join key: " << key;
        }
      } else if (
          auto projectNode =
              dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
        // 1.1.2 For ProjectNode, remove cast expressions on hash keys if they
        // are widening integral casts.
        for (auto i = 0; i < projectNode->names().size(); i++) {
          const auto& name = projectNode->names()[i];
          if (hashOnlyKeys.find(name) != hashOnlyKeys.end()) {
            if (isWideningIntegralCast(projectNode->projections()[i])) {
              hashOnlyKeys.erase(name);
              projectionsNeedUpdate.insert(i);
              needNewPlanNode = true;
              LOG(INFO) << "Remove widening integral cast on hash key: "
                        << name;
            }
          }
        }
      }

      // 1.2 Recurse on the i-th source
      auto source = plan2(
          sources[i],
          mustStartNewPipeline(planNode, i) ? nullptr : currentPlanNodes,
          planNode,
          makeOperatorSupplier(planNode),
          driverFactories,
          hashOnlyKeys);

      // Backtrack and propagate the updated types to the current plan node.
      if (source != sources[i]) {
        needNewPlanNode = true;
      }
      newSources.push_back(source); // TODO: Do not push into vector if not
    }

    // 2. Create a new plan node with updated sources and/or projections.
    if (needNewPlanNode) {
      VELOX_CHECK_GT(newSources.size(), 0);
      VELOX_CHECK_EQ(newSources.size(), sources.size());
      if (const auto& projectNode =
              std::dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
        // 2.1 Special handling for ProjectNode to remove widening integral
        // casts. Create new projections with the updated expressions.
        auto newProjections = projectNode->projections(); // copy
        if (!projectionsNeedUpdate.empty()) {
          for (auto index : projectionsNeedUpdate) {
            const auto& projection = projectNode->projections()[index];
            VELOX_CHECK(
                isWideningIntegralCast(projection),
                "Expect widening integral cast, got {}",
                projection->toString());
            VELOX_CHECK_EQ(projection->inputs().size(), 1);
            // Replace CAST(int->bigint) by its input expr (identity on base
            // type).
            newProjections[index] = projection->inputs()[0];
          }
        }

        // Update the projections from newSources[0] if needed.
        for (auto i = 0; i < newProjections.size(); i++) {
          auto newProjection = newProjections[i];
          if (newProjection->isFieldAccessKind()) {
            auto fieldAccessExpr =
                std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                    newProjection);
            auto fieldName = fieldAccessExpr->name();

            // It's guranteed that the field name exists in the new source type.
            auto newType = newSources[0]->outputType()->findChild(fieldName);

            // Create a new FieldAccessTypedExpr with the updated type from
            // newSources[0].
            newProjections[i] = std::make_shared<core::FieldAccessTypedExpr>(
                newType, fieldName);
          }
        }

        core::ProjectNode::Builder builder;
        planNode = builder.id(planNode->id())
                       .source(newSources[0])
                       .projections(newProjections)
                       .names(projectNode->names())
                       .build();
        LOG(INFO) << "Created new ProjectNode: " << planNode->toString();
      } else {
        // 2.2 Generic path: same node with new children.
        planNode = planNode->copyWithNewSources(newSources);
        LOG(INFO) << "Created new generic PlanNode: " << planNode->toString();
      }
    }
  }

  // 3. Add the current plan node to the current pipeline.
  currentPlanNodes->push_back(planNode);
  return planNode;
}

PlanNodePtr plan3(
    PlanNodePtr planNode,
    std::vector<std::shared_ptr<const core::PlanNode>>* currentPlanNodes,
    const std::shared_ptr<const core::PlanNode>& consumerNode,
    OperatorSupplier operatorSupplier,
    std::vector<std::unique_ptr<DriverFactory>>* driverFactories,
    std::map<std::string, TypePtr>& deesiredOutputTypes) {
  if (!currentPlanNodes) {
    auto driverFactory = std::make_unique<DriverFactory>();
    currentPlanNodes = &driverFactory->planNodes;
    driverFactory->operatorSupplier = std::move(operatorSupplier);
    driverFactory->consumerNode = consumerNode;
    driverFactories->push_back(std::move(driverFactory));
  }

  auto& sources = planNode->sources();
  if (sources.empty()) {
    // Leaf: new driver, and update TableScan output type if needed.
    driverFactories->back()->inputDriver = true;

    if (
        // TableScan Node: update the output type to avoid upcasting in
        // the upcoming FilterProject
        const auto& tableScanNode =
            std::dynamic_pointer_cast<const core::TableScanNode>(planNode)) {
      const auto& outputType = tableScanNode->outputType();
      std::vector<std::string> names;
      std::vector<TypePtr> types;
      names.reserve(outputType->size());
      types.reserve(outputType->size());
      for (int i = 0; i < outputType->size(); i++) {
        std::string name = outputType->nameOf(i);

        auto iter = deesiredOutputTypes.find(name);
        if (iter != deesiredOutputTypes.end()) {
          types.push_back(iter->second);
          names.push_back(std::move(name));
          LOG(INFO) << "Update TableScanNode output type for column: "
                    << names.back() << " from " << outputType->childAt(i)
                    << " to " << types.back();
        } else {
          //          auto& childType = outputType->childAt(i);
          types.push_back(outputType->childAt(i));
        }
      }
      auto newOutputType =
          std::make_shared<RowType>(std::move(names), std::move(types));

      core::TableScanNode::Builder builder(*tableScanNode);
      planNode = builder.outputType(std::move(newOutputType)).build();
      LOG(INFO) << "Created new TableScanNode: " << planNode->toString();
    }
  } else {
    // Non-leaf node.
    bool needNewPlanNode = false;
    std::set<int> projectionsNeedUpdate;
    std::vector<PlanNodePtr> newSources;

    // For ProjectNode that is the immediate consumer of TableScan, remove
    // cast expressions if they are widening integer casts. We want the
    // TableScan to return the upcasted type by type coercion.
    if (const auto& projectNode =
            dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
      VELOX_CHECK_EQ(projectNode->sources().size(), 1);
      if (auto tableScanNode =
              std::dynamic_pointer_cast<const core::TableScanNode>(
                  projectNode->sources()[0])) {
        // Only handle the case where ProjectNode is the immediate consumer of
        // TableScan.
        for (auto i = 0; i < projectNode->names().size(); i++) {
          auto& projection = projectNode->projections()[i];
          auto newProjection = projection;

          if (isWideningIntegralCast(projection)) {
            const auto& name = projectNode->names()[i];
            TypePtr type = projectNode->outputType()->childAt(i);

            // Find the new name from the input of the cast expression, which
            // must be a FieldAccessTypedExpr.
            VELOX_CHECK_EQ(projection->inputs().size(), 1);
            const auto& inputExpr = projection->inputs()[0];
            VELOX_CHECK(inputExpr->isFieldAccessKind());
            auto childExpr =
                std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                    inputExpr);
            auto childName = childExpr->name();

            // Update the desired output type for its source to avoid upcasting.
            deesiredOutputTypes[childName] = type;
            projectionsNeedUpdate.insert(i);
            needNewPlanNode = true;
            LOG(INFO) << "Remove widening integral cast on key: " << name;
          }
        }
      }
    }

    const auto numSourcesToPlan =
        isIndexLookupJoin(planNode.get()) ? 1 : sources.size();
    for (int32_t i = 0; i < numSourcesToPlan; ++i) {
      auto source = plan3(
          sources[i],
          mustStartNewPipeline(planNode, i) ? nullptr : currentPlanNodes,
          planNode,
          makeOperatorSupplier(planNode),
          driverFactories,
          deesiredOutputTypes);

      // Backtrack and propagate the updated types to the current plan node.
      if (source != sources[i]) {
        needNewPlanNode = true;
      }
      newSources.push_back(source);
    }

    if (needNewPlanNode) {
      if (const auto& projectNode =
              std::dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
        // Special handling for ProjectNode to remove widening integral
        // casts. Create new projections with the updated expressions.
        auto newProjections = projectNode->projections(); // copy
        if (!projectionsNeedUpdate.empty()) {
          for (auto index : projectionsNeedUpdate) {
            const auto& projection = projectNode->projections()[index];
            VELOX_CHECK(
                isWideningIntegralCast(projection),
                "Expect widening integral cast, got {}",
                projection->toString());
            VELOX_CHECK_EQ(projection->inputs().size(), 1);

            // Replace CAST(int->bigint) by its input expr (identity on base
            // type).
            const auto& inputExpr = projection->inputs()[0];
            VELOX_CHECK(inputExpr->isFieldAccessKind());
            auto fieldAccessExpr =
                std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                    inputExpr);
            auto fieldName = fieldAccessExpr->name();
            TypePtr type = projectNode->outputType()->childAt(index);
            newProjections[index] =
                std::make_shared<core::FieldAccessTypedExpr>(type, fieldName);
          }
        }

        core::ProjectNode::Builder builder;
        planNode = builder.id(planNode->id())
                       .source(newSources[0])
                       .projections(newProjections)
                       .names(projectNode->names())
                       .build();
        LOG(INFO) << "Created new ProjectNode: " << planNode->toString();
      } else {
        // 2.2 Generic path: same node with new children.
        planNode = planNode->copyWithNewSources(newSources);
        LOG(INFO) << "Created new generic PlanNode: " << planNode->toString();
      }
    }
  }
  // 3. Add the current (updated) plan node to the current pipeline.
  currentPlanNodes->push_back(planNode);
  return planNode;
}

// Sometimes consumer limits the number of drivers its producer can run.
uint32_t maxDriversForConsumer(const PlanNodePtr& node) {
  if (std::dynamic_pointer_cast<const core::MergeJoinNode>(node)) {
    // MergeJoinNode must run single-threaded.
    return 1;
  }
  return std::numeric_limits<uint32_t>::max();
}

uint32_t maxDrivers(
    const DriverFactory& driverFactory,
    const core::QueryConfig& queryConfig) {
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
      if (!values->testingIsParallelizable()) {
        return 1;
      }
    } else if (std::dynamic_pointer_cast<const core::ArrowStreamNode>(node)) {
      // ArrowStream node must run single-threaded.
      return 1;
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
      switch (localExchange->type()) {
        case core::LocalPartitionNode::Type::kGather:
          return 1;
        case core::LocalPartitionNode::Type::kRepartition:
          count = std::min(queryConfig.maxLocalExchangePartitionCount(), count);
          break;
        default:
          VELOX_UNREACHABLE("Unexpected local exchange type");
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
        auto join = std::dynamic_pointer_cast<const core::HashJoinNode>(node)) {
      // Right semi project doesn't support multi-threaded execution.
      if (join->isRightSemiProjectJoin()) {
        return 1;
      }
    } else if (
        auto tableWrite =
            std::dynamic_pointer_cast<const core::TableWriteNode>(node)) {
      const auto& connectorInsertHandle =
          tableWrite->insertTableHandle()->connectorInsertTableHandle();
      if (!connectorInsertHandle->supportsMultiThreading()) {
        return 1;
      } else {
        if (tableWrite->hasPartitioningScheme()) {
          return queryConfig.taskPartitionedWriterCount();
        } else {
          return queryConfig.taskWriterCount();
        }
      }
    } else {
      auto result = Operator::maxDrivers(node);
      if (result) {
        VELOX_CHECK_GT(
            *result,
            0,
            "maxDrivers must be greater than 0. Plan node: {}",
            node->toString());
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
    const core::QueryConfig& queryConfig,
    uint32_t maxDrivers) {
  for (auto& adapter : DriverFactory::adapters) {
    if (adapter.inspect) {
      adapter.inspect(planFragment);
    }
  }

  if (queryConfig.pushdownIntegerUpcastsToScan()) {
    std::map<std::string, TypePtr> desiredOutputTypes;
    detail::plan3(
        planFragment.planNode,
        nullptr,
        nullptr,
        detail::makeOperatorSupplier(std::move(consumerSupplier)),
        driverFactories,
        desiredOutputTypes);
  } else {
    detail::plan(
        planFragment.planNode,
        nullptr,
        nullptr,
        detail::makeOperatorSupplier(std::move(consumerSupplier)),
        driverFactories);
  }

  (*driverFactories)[0]->outputDriver = true;

  if (planFragment.isGroupedExecution()) {
    determineGroupedExecutionPipelines(planFragment, *driverFactories);
    markMixedJoinBridges(*driverFactories);
  }

  // Determine number of drivers for each pipeline.
  for (auto& factory : *driverFactories) {
    factory->maxDrivers = detail::maxDrivers(*factory, queryConfig);
    factory->numDrivers = std::min(factory->maxDrivers, maxDrivers);

    // Pipelines running grouped/bucketed execution would have separate groups
    // of drivers dealing with separate split groups (one driver can access
    // splits from only one designated split group), hence we will have total
    // number of drivers multiplied by the number of split groups.
    if (factory->groupedExecution) {
      factory->numTotalDrivers =
          factory->numDrivers * planFragment.numSplitGroups;
    } else {
      factory->numTotalDrivers = factory->numDrivers;
    }
  }
}

// static
void LocalPlanner::determineGroupedExecutionPipelines(
    const core::PlanFragment& planFragment,
    std::vector<std::unique_ptr<DriverFactory>>& driverFactories) {
  // We run backwards - from leaf pipelines to the root pipeline.
  for (auto it = driverFactories.rbegin(); it != driverFactories.rend(); ++it) {
    auto& factory = *it;

    // See if pipelines have leaf nodes that use grouped execution strategy.
    if (planFragment.leafNodeRunsGroupedExecution(factory->leafNodeId())) {
      factory->groupedExecution = true;
    }

    // If a pipeline's leaf node is Local Partition, which has all sources
    // belonging to pipelines that run Grouped Execution, then our pipeline
    // should run Grouped Execution as well.
    if (auto localPartitionNode =
            std::dynamic_pointer_cast<const core::LocalPartitionNode>(
                factory->planNodes.front())) {
      size_t numGroupedExecutionSources{0};
      for (const auto& sourceNode : localPartitionNode->sources()) {
        for (auto& anotherFactory : driverFactories) {
          if (sourceNode == anotherFactory->planNodes.back() &&
              anotherFactory->groupedExecution) {
            ++numGroupedExecutionSources;
            break;
          }
        }
      }
      if (numGroupedExecutionSources > 0 &&
          numGroupedExecutionSources == localPartitionNode->sources().size()) {
        factory->groupedExecution = true;
      }
    }
  }
}

// static
void LocalPlanner::markMixedJoinBridges(
    std::vector<std::unique_ptr<DriverFactory>>& driverFactories) {
  for (auto& factory : driverFactories) {
    // We are interested in grouped execution pipelines only.
    if (!factory->groupedExecution) {
      continue;
    }

    // See if we have any join nodes.
    for (const auto& planNode : factory->planNodes) {
      if (auto joinNode =
              std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
        // See if the build source (2nd) belongs to an ungrouped execution.
        auto& buildSourceNode = planNode->sources()[1];
        for (auto& factoryOther : driverFactories) {
          if (!factoryOther->groupedExecution &&
              buildSourceNode->id() == factoryOther->outputNodeId()) {
            factoryOther->mixedExecutionModeHashJoinNodeIds.emplace(
                planNode->id());
            factory->mixedExecutionModeHashJoinNodeIds.emplace(planNode->id());
            break;
          }
        }
      } else if (
          auto joinNode =
              std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(
                  planNode)) {
        // See if the build source (2nd) belongs to an ungrouped execution.
        auto& buildSourceNode = planNode->sources()[1];
        for (auto& factoryOther : driverFactories) {
          if (!factoryOther->groupedExecution &&
              buildSourceNode->id() == factoryOther->outputNodeId()) {
            factoryOther->mixedExecutionModeNestedLoopJoinNodeIds.emplace(
                planNode->id());
            factory->mixedExecutionModeNestedLoopJoinNodeIds.emplace(
                planNode->id());
            break;
          }
        }
      } else if (
          auto spatialJoinNode =
              std::dynamic_pointer_cast<const core::SpatialJoinNode>(
                  planNode)) {
        VELOX_FAIL("Spatial joins do not support grouped execution.");
      }
    }
  }
}

std::shared_ptr<Driver> DriverFactory::createDriver(
    std::unique_ptr<DriverCtx> ctx,
    std::shared_ptr<ExchangeClient> exchangeClient,
    std::shared_ptr<PipelinePushdownFilters> filters,
    std::function<int(int pipelineId)> numDrivers) {
  auto driver = std::shared_ptr<Driver>(new Driver());
  ctx->driver = driver.get();
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
          operators.push_back(
              std::make_unique<FilterProject>(
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
        auto projectNode =
            std::dynamic_pointer_cast<const core::ParallelProjectNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<ParallelProject>(id, ctx.get(), projectNode));
    } else if (
        auto valuesNode =
            std::dynamic_pointer_cast<const core::ValuesNode>(planNode)) {
      operators.push_back(std::make_unique<Values>(id, ctx.get(), valuesNode));
    } else if (
        auto arrowStreamNode =
            std::dynamic_pointer_cast<const core::ArrowStreamNode>(planNode)) {
      operators.push_back(
          std::make_unique<ArrowStream>(id, ctx.get(), arrowStreamNode));
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
        auto tableWriteMergeNode =
            std::dynamic_pointer_cast<const core::TableWriteMergeNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<TableWriteMerge>(
              id, ctx.get(), tableWriteMergeNode));
    } else if (
        auto mergeExchangeNode =
            std::dynamic_pointer_cast<const core::MergeExchangeNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<MergeExchange>(i, ctx.get(), mergeExchangeNode));
    } else if (
        auto exchangeNode =
            std::dynamic_pointer_cast<const core::ExchangeNode>(planNode)) {
      // NOTE: the exchange client can only be used by one operator in a
      // driver.
      VELOX_CHECK_NOT_NULL(exchangeClient);
      operators.push_back(
          std::make_unique<Exchange>(
              id, ctx.get(), exchangeNode, std::move(exchangeClient)));
    } else if (
        auto partitionedOutputNode =
            std::dynamic_pointer_cast<const core::PartitionedOutputNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<PartitionedOutput>(
              id, ctx.get(), partitionedOutputNode, eagerFlush(*planNode)));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
      operators.push_back(std::make_unique<HashProbe>(id, ctx.get(), joinNode));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<NestedLoopJoinProbe>(id, ctx.get(), joinNode));
    } else if (
        auto spatialJoinNode =
            std::dynamic_pointer_cast<const core::SpatialJoinNode>(planNode)) {
      operators.push_back(
          std::make_unique<SpatialJoinProbe>(id, ctx.get(), spatialJoinNode));
    } else if (
        auto joinNode =
            std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<IndexLookupJoin>(id, ctx.get(), joinNode));
    } else if (
        auto aggregationNode =
            std::dynamic_pointer_cast<const core::AggregationNode>(planNode)) {
      if (aggregationNode->isPreGrouped()) {
        operators.push_back(
            std::make_unique<StreamingAggregation>(
                id, ctx.get(), aggregationNode));
      } else {
        operators.push_back(
            std::make_unique<HashAggregation>(id, ctx.get(), aggregationNode));
      }
    } else if (
        auto expandNode =
            std::dynamic_pointer_cast<const core::ExpandNode>(planNode)) {
      operators.push_back(std::make_unique<Expand>(id, ctx.get(), expandNode));
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
        auto rowNumberNode =
            std::dynamic_pointer_cast<const core::RowNumberNode>(planNode)) {
      operators.push_back(
          std::make_unique<RowNumber>(id, ctx.get(), rowNumberNode));
    } else if (
        auto topNRowNumberNode =
            std::dynamic_pointer_cast<const core::TopNRowNumberNode>(
                planNode)) {
      operators.push_back(
          std::make_unique<TopNRowNumber>(id, ctx.get(), topNRowNumberNode));
    } else if (
        auto markDistinctNode =
            std::dynamic_pointer_cast<const core::MarkDistinctNode>(planNode)) {
      operators.push_back(
          std::make_unique<MarkDistinct>(id, ctx.get(), markDistinctNode));
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
      operators.push_back(
          std::make_unique<LocalExchange>(
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
      operators.push_back(
          std::make_unique<AssignUniqueId>(
              id,
              ctx.get(),
              assignUniqueIdNode,
              assignUniqueIdNode->taskUniqueId(),
              assignUniqueIdNode->uniqueIdCounter()));
    } else if (
        const auto traceScanNode =
            std::dynamic_pointer_cast<const core::TraceScanNode>(planNode)) {
      operators.push_back(
          std::make_unique<trace::OperatorTraceScan>(
              id, ctx.get(), traceScanNode));
    } else {
      std::unique_ptr<Operator> extended;
      if (planNode->requiresExchangeClient()) {
        // NOTE: the exchange client can only be used by one operator in a
        // driver.
        VELOX_CHECK_NOT_NULL(exchangeClient);
        extended = Operator::fromPlanNode(
            ctx.get(), id, planNode, std::move(exchangeClient));
      } else {
        extended = Operator::fromPlanNode(ctx.get(), id, planNode);
      }
      VELOX_CHECK(extended, "Unsupported plan node: {}", planNode->toString());
      operators.push_back(std::move(extended));
    }
  }
  if (operatorSupplier) {
    operators.push_back(operatorSupplier(operators.size(), ctx.get()));
  }

  if (filters->empty()) {
    filters->resize(operators.size());
  } else {
    VELOX_CHECK_EQ(filters->size(), operators.size());
  }
  driver->init(std::move(ctx), std::move(operators));
  for (auto& adapter : adapters) {
    if (adapter.adapt(*this, *driver)) {
      break;
    }
  }
  driver->isAdaptable_ = false;
  driver->pushdownFilters_ = std::move(filters);
  return driver;
}

std::vector<std::unique_ptr<Operator>> DriverFactory::replaceOperators(
    Driver& driver,
    int32_t begin,
    int32_t end,
    std::vector<std::unique_ptr<Operator>> replaceWith) const {
  VELOX_CHECK(driver.isAdaptable_);
  std::vector<std::unique_ptr<exec::Operator>> replaced;
  for (auto i = begin; i < end; ++i) {
    replaced.push_back(std::move(driver.operators_[i]));
  }

  driver.operators_.erase(
      driver.operators_.begin() + begin, driver.operators_.begin() + end);

  // Insert the replacement at the place of the erase. Do manually because
  // insert() is not good with unique pointers.
  driver.operators_.resize(driver.operators_.size() + replaceWith.size());
  for (int32_t i = driver.operators_.size() - 1;
       i >= begin + replaceWith.size();
       --i) {
    driver.operators_[i] = std::move(driver.operators_[i - replaceWith.size()]);
  }
  for (auto i = 0; i < replaceWith.size(); ++i) {
    driver.operators_[i + begin] = std::move(replaceWith[i]);
  }

  // Set the ids to be consecutive.
  for (auto i = 0; i < driver.operators_.size(); ++i) {
    driver.operators_[i]->setOperatorIdFromAdapter(i);
  }
  return replaced;
}

std::vector<core::PlanNodeId> DriverFactory::needsHashJoinBridges() const {
  std::vector<core::PlanNodeId> planNodeIds;
  // Ungrouped execution pipelines need to take care of cross-mode bridges.
  if (!groupedExecution && !mixedExecutionModeHashJoinNodeIds.empty()) {
    planNodeIds.insert(
        planNodeIds.end(),
        mixedExecutionModeHashJoinNodeIds.begin(),
        mixedExecutionModeHashJoinNodeIds.end());
  }
  for (const auto& planNode : planNodes) {
    if (auto joinNode =
            std::dynamic_pointer_cast<const core::HashJoinNode>(planNode)) {
      // Grouped execution pipelines should not create cross-mode bridges.
      if (!groupedExecution ||
          !mixedExecutionModeHashJoinNodeIds.contains(joinNode->id())) {
        planNodeIds.emplace_back(joinNode->id());
      }
    }
  }
  return planNodeIds;
}

std::vector<core::PlanNodeId> DriverFactory::needsNestedLoopJoinBridges()
    const {
  std::vector<core::PlanNodeId> planNodeIds;
  // Ungrouped execution pipelines need to take care of cross-mode bridges.
  if (!groupedExecution && !mixedExecutionModeNestedLoopJoinNodeIds.empty()) {
    planNodeIds.insert(
        planNodeIds.end(),
        mixedExecutionModeNestedLoopJoinNodeIds.begin(),
        mixedExecutionModeNestedLoopJoinNodeIds.end());
  }
  for (const auto& planNode : planNodes) {
    if (auto joinNode =
            std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(
                planNode)) {
      // Grouped execution pipelines should not create cross-mode bridges.
      if (!groupedExecution ||
          !mixedExecutionModeNestedLoopJoinNodeIds.contains(joinNode->id())) {
        planNodeIds.emplace_back(joinNode->id());
      }
    }
  }

  return planNodeIds;
}

std::vector<core::PlanNodeId> DriverFactory::needsSpatialJoinBridges() const {
  std::vector<core::PlanNodeId> planNodeIds;
  for (const auto& planNode : planNodes) {
    if (auto joinNode =
            std::dynamic_pointer_cast<const core::SpatialJoinNode>(planNode)) {
      // Grouped execution pipelines should not create cross-mode bridges.
      planNodeIds.emplace_back(joinNode->id());
    }
  }

  return planNodeIds;
}

// static
void DriverFactory::registerAdapter(DriverAdapter adapter) {
  adapters.push_back(std::move(adapter));
}

// static
std::vector<DriverAdapter> DriverFactory::adapters;

} // namespace facebook::velox::exec
