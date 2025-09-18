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

#include "velox/experimental/cudf/connectors/hive/CudfHiveConnector.h"
#include "velox/experimental/cudf/connectors/hive/CudfHiveDataSource.h"
#include "velox/experimental/cudf/exec/CudfConversion.h"
#include "velox/experimental/cudf/exec/CudfFilterProject.h"
#include "velox/experimental/cudf/exec/CudfHashAggregation.h"
#include "velox/experimental/cudf/exec/CudfHashJoin.h"
#include "velox/experimental/cudf/exec/CudfLimit.h"
#include "velox/experimental/cudf/exec/CudfLocalPartition.h"
#include "velox/experimental/cudf/exec/CudfOrderBy.h"
#include "velox/experimental/cudf/exec/ExpressionEvaluator.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/exec/Utilities.h"

#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/exec/Driver.h"
#include "velox/exec/FilterProject.h"
#include "velox/exec/HashAggregation.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/HashProbe.h"
#include "velox/exec/Limit.h"
#include "velox/exec/Operator.h"
#include "velox/exec/OrderBy.h"
#include "velox/exec/TableScan.h"

#include <cudf/detail/nvtx/ranges.hpp>

#include <cuda.h>

#include <iostream>

DEFINE_bool(velox_cudf_enabled, true, "Enable cuDF-Velox acceleration");
DEFINE_string(velox_cudf_memory_resource, "async", "Memory resource for cuDF");
DEFINE_bool(velox_cudf_debug, false, "Enable debug printing");

namespace facebook::velox::cudf_velox {

namespace {

template <class... Deriveds, class Base>
bool isAnyOf(const Base* p) {
  return ((dynamic_cast<const Deriveds*>(p) != nullptr) || ...);
}

} // namespace

bool CompileState::compile(bool force_replace) {
  auto operators = driver_.operators();

  if (FLAGS_velox_cudf_debug) {
    std::cout << "Operators before adapting for cuDF: count ["
              << operators.size() << "]" << std::endl;
    for (auto& op : operators) {
      std::cout << "  Operator: ID " << op->operatorId() << ": "
                << op->toString() << std::endl;
    }
  }

  // Make sure operator states are initialized.  We will need to inspect some of
  // them during the transformation.
  driver_.initializeOperators();

  bool replacementsMade = false;
  auto ctx = driver_.driverCtx();

  // Get plan node by id lookup.
  auto getPlanNode = [&](const core::PlanNodeId& id) {
    auto& nodes = driverFactory_.planNodes;
    auto it =
        std::find_if(nodes.cbegin(), nodes.cend(), [&id](const auto& node) {
          return node->id() == id;
        });
    if (it != nodes.end()) {
      return *it;
    }
    VELOX_CHECK(driverFactory_.consumerNode->id() == id);
    return driverFactory_.consumerNode;
  };

  auto isTableScanSupported = [getPlanNode](const exec::Operator* op) {
    if (!isAnyOf<exec::TableScan>(op)) {
      return false;
    }
    auto tableScanNode = std::dynamic_pointer_cast<const core::TableScanNode>(
        getPlanNode(op->planNodeId()));
    VELOX_CHECK(tableScanNode != nullptr);
    auto const& connector = velox::connector::getConnector(
        tableScanNode->tableHandle()->connectorId());
    auto cudfHiveConnector = std::dynamic_pointer_cast<
        facebook::velox::cudf_velox::connector::hive::CudfHiveConnector>(
        connector);
    if (!cudfHiveConnector) {
      return false;
    }
    // TODO (dm): we need to ask CudfHiveConnector whether this table handle is
    // supported by it. It may choose to produce a HiveDatasource.
    return true;
  };

  auto isFilterProjectSupported = [](const exec::Operator* op) {
    if (auto filterProjectOp = dynamic_cast<const exec::FilterProject*>(op)) {
      auto info = filterProjectOp->exprsAndProjection();
      return ExpressionEvaluator::canBeEvaluated(info.exprs->exprs());
    }
    return false;
  };

  auto isJoinSupported = [getPlanNode](const exec::Operator* op) {
    if (!isAnyOf<exec::HashBuild, exec::HashProbe>(op)) {
      return false;
    }
    auto planNode = std::dynamic_pointer_cast<const core::HashJoinNode>(
        getPlanNode(op->planNodeId()));
    if (!planNode) {
      return false;
    }
    if (!CudfHashJoinProbe::isSupportedJoinType(planNode->joinType())) {
      return false;
    }
    // disabling null-aware anti join with filter until we implement it right
    if (planNode->joinType() == core::JoinType::kAnti and
        planNode->isNullAware() and planNode->filter()) {
      return false;
    }
    return true;
  };

  auto isSupportedGpuOperator =
      [isFilterProjectSupported, isJoinSupported, isTableScanSupported](
          const exec::Operator* op) {
        return isAnyOf<
                   exec::OrderBy,
                   exec::HashAggregation,
                   exec::Limit,
                   exec::LocalPartition,
                   exec::LocalExchange>(op) ||
            isFilterProjectSupported(op) || isJoinSupported(op) ||
            isTableScanSupported(op);
      };

  std::vector<bool> isSupportedGpuOperators(operators.size());
  std::transform(
      operators.begin(),
      operators.end(),
      isSupportedGpuOperators.begin(),
      isSupportedGpuOperator);
  auto acceptsGpuInput = [isFilterProjectSupported,
                          isJoinSupported](const exec::Operator* op) {
    return isAnyOf<
               exec::OrderBy,
               exec::HashAggregation,
               exec::Limit,
               exec::LocalPartition>(op) ||
        isFilterProjectSupported(op) || isJoinSupported(op);
  };
  auto producesGpuOutput = [isFilterProjectSupported,
                            isJoinSupported,
                            isTableScanSupported](const exec::Operator* op) {
    return isAnyOf<
               exec::OrderBy,
               exec::HashAggregation,
               exec::Limit,
               exec::LocalExchange>(op) ||
        isFilterProjectSupported(op) ||
        (isAnyOf<exec::HashProbe>(op) && isJoinSupported(op)) ||
        (isTableScanSupported(op));
  };

  int32_t operatorsOffset = 0;
  for (int32_t operatorIndex = 0; operatorIndex < operators.size();
       ++operatorIndex) {
    std::vector<std::unique_ptr<exec::Operator>> replaceOp;

    exec::Operator* oper = operators[operatorIndex];
    auto replacingOperatorIndex = operatorIndex + operatorsOffset;
    VELOX_CHECK(oper);

    const bool previousOperatorIsNotGpu =
        (operatorIndex > 0 and !isSupportedGpuOperators[operatorIndex - 1]);
    const bool nextOperatorIsNotGpu =
        (operatorIndex < operators.size() - 1 and
         !isSupportedGpuOperators[operatorIndex + 1]);
    const bool isLastOperatorOfTask =
        driverFactory_.outputDriver and operatorIndex == operators.size() - 1;

    auto id = oper->operatorId();
    if (previousOperatorIsNotGpu and acceptsGpuInput(oper)) {
      auto planNode = getPlanNode(oper->planNodeId());
      replaceOp.push_back(std::make_unique<CudfFromVelox>(
          id, planNode->outputType(), ctx, planNode->id() + "-from-velox"));
      replaceOp.back()->initialize();
    }

    // This is used to denote if the current operator is kept or replaced.
    auto keepOperator = 0;
    // TableScan
    if (isTableScanSupported(oper)) {
      auto planNode = std::dynamic_pointer_cast<const core::TableScanNode>(
          getPlanNode(oper->planNodeId()));
      VELOX_CHECK(planNode != nullptr);
      keepOperator = 1;
    } else if (isJoinSupported(oper)) {
      if (auto joinBuildOp = dynamic_cast<exec::HashBuild*>(oper)) {
        auto planNode = std::dynamic_pointer_cast<const core::HashJoinNode>(
            getPlanNode(joinBuildOp->planNodeId()));
        VELOX_CHECK(planNode != nullptr);
        // From-Velox (optional)
        replaceOp.push_back(
            std::make_unique<CudfHashJoinBuild>(id, ctx, planNode));
        replaceOp.back()->initialize();
      } else if (auto joinProbeOp = dynamic_cast<exec::HashProbe*>(oper)) {
        auto planNode = std::dynamic_pointer_cast<const core::HashJoinNode>(
            getPlanNode(joinProbeOp->planNodeId()));
        VELOX_CHECK(planNode != nullptr);
        // From-Velox (optional)
        replaceOp.push_back(
            std::make_unique<CudfHashJoinProbe>(id, ctx, planNode));
        replaceOp.back()->initialize();
        // To-Velox (optional)
      }
    } else if (auto orderByOp = dynamic_cast<exec::OrderBy*>(oper)) {
      auto id = orderByOp->operatorId();
      auto planNode = std::dynamic_pointer_cast<const core::OrderByNode>(
          getPlanNode(orderByOp->planNodeId()));
      VELOX_CHECK(planNode != nullptr);
      replaceOp.push_back(std::make_unique<CudfOrderBy>(id, ctx, planNode));
      replaceOp.back()->initialize();
    } else if (auto hashAggOp = dynamic_cast<exec::HashAggregation*>(oper)) {
      auto planNode = std::dynamic_pointer_cast<const core::AggregationNode>(
          getPlanNode(hashAggOp->planNodeId()));
      VELOX_CHECK(planNode != nullptr);
      replaceOp.push_back(
          std::make_unique<CudfHashAggregation>(id, ctx, planNode));
      replaceOp.back()->initialize();
    } else if (isFilterProjectSupported(oper)) {
      auto filterProjectOp = dynamic_cast<exec::FilterProject*>(oper);
      auto info = filterProjectOp->exprsAndProjection();
      auto& idProjections = filterProjectOp->identityProjections();
      auto projectPlanNode = std::dynamic_pointer_cast<const core::ProjectNode>(
          getPlanNode(filterProjectOp->planNodeId()));
      auto filterPlanNode = std::dynamic_pointer_cast<const core::FilterNode>(
          getPlanNode(filterProjectOp->planNodeId()));
      // If filter only, filter node only exists.
      // If project only, or filter and project, project node only exists.
      VELOX_CHECK(projectPlanNode != nullptr or filterPlanNode != nullptr);
      replaceOp.push_back(std::make_unique<CudfFilterProject>(
          id, ctx, info, idProjections, filterPlanNode, projectPlanNode));
      replaceOp.back()->initialize();
    } else if (auto limitOp = dynamic_cast<exec::Limit*>(oper)) {
      auto planNode = std::dynamic_pointer_cast<const core::LimitNode>(
          getPlanNode(limitOp->planNodeId()));
      VELOX_CHECK(planNode != nullptr);
      replaceOp.push_back(std::make_unique<CudfLimit>(id, ctx, planNode));
      replaceOp.back()->initialize();
    } else if (
        auto localPartitionOp = dynamic_cast<exec::LocalPartition*>(oper)) {
      auto planNode = std::dynamic_pointer_cast<const core::LocalPartitionNode>(
          getPlanNode(localPartitionOp->planNodeId()));
      VELOX_CHECK(planNode != nullptr);
      replaceOp.push_back(
          std::make_unique<CudfLocalPartition>(id, ctx, planNode));
      replaceOp.back()->initialize();
    } else if (
        auto localExchangeOp = dynamic_cast<exec::LocalExchange*>(oper)) {
      keepOperator = 1;
    }

    if (producesGpuOutput(oper) and
        (nextOperatorIsNotGpu or isLastOperatorOfTask)) {
      auto planNode = getPlanNode(oper->planNodeId());
      replaceOp.push_back(std::make_unique<CudfToVelox>(
          id, planNode->outputType(), ctx, planNode->id() + "-to-velox"));
      replaceOp.back()->initialize();
    }

    if (force_replace) {
      if (FLAGS_velox_cudf_debug) {
        std::printf(
            "Operator: ID %d: %s, keepOperator = %d, replaceOp.size() = %ld\n",
            oper->operatorId(),
            oper->toString().c_str(),
            keepOperator,
            replaceOp.size());
      }
      auto shouldSupportTableScan =
          [isParquetConnectorRegistered](const exec::Operator* op) {
            return isAnyOf<exec::TableScan>(op) && isParquetConnectorRegistered;
          };
      auto shouldSupportGpuOperator =
          [isFilterProjectSupported,
           shouldSupportTableScan](const exec::Operator* op) {
            return isAnyOf<
                       exec::OrderBy,
                       exec::HashAggregation,
                       exec::Limit,
                       exec::LocalPartition,
                       exec::LocalExchange,
                       exec::HashBuild,
                       exec::HashProbe>(op) ||
                isFilterProjectSupported(op) || shouldSupportTableScan(op);
          };
      VELOX_CHECK(
          !(keepOperator == 0 && shouldSupportGpuOperator(oper) &&
            replaceOp.empty()),
          "Replacement with cuDF operator failed");
    }

    if (not replaceOp.empty()) {
      operatorsOffset += replaceOp.size() - 1 + keepOperator;
      [[maybe_unused]] auto replaced = driverFactory_.replaceOperators(
          driver_,
          replacingOperatorIndex + keepOperator,
          replacingOperatorIndex + 1,
          std::move(replaceOp));
      replacementsMade = true;
    }
  }

  if (FLAGS_velox_cudf_debug) {
    operators = driver_.operators();
    std::cout << "Operators after adapting for cuDF: count ["
              << operators.size() << "]" << std::endl;
    for (auto& op : operators) {
      std::cout << "  Operator: ID " << op->operatorId() << ": "
                << op->toString() << std::endl;
    }
  }

  return replacementsMade;
}

struct CudfDriverAdapter {
  std::shared_ptr<rmm::mr::device_memory_resource> mr_;
  bool force_replace_;

  CudfDriverAdapter(
      std::shared_ptr<rmm::mr::device_memory_resource> mr,
      bool force_replace)
      : mr_(mr), force_replace_{force_replace} {}

  // Call operator needed by DriverAdapter
  bool operator()(const exec::DriverFactory& factory, exec::Driver& driver) {
    if (!driver.driverCtx()->queryConfig().get<bool>(kCudfEnabled, true)) {
      return false;
    }
    auto state = CompileState(factory, driver);
    auto res = state.compile(force_replace_);
    return res;
  }
};

static bool isCudfRegistered = false;

void registerCudf(const CudfOptions& options) {
  if (cudfIsRegistered()) {
    return;
  }
  if (!options.cudfEnabled) {
    return;
  }

  registerBuiltinFunctions(options.prefix());

  CUDF_FUNC_RANGE();
  cudaFree(nullptr); // Initialize CUDA context at startup

  const std::string mrMode = options.cudfMemoryResource;
  auto mr = cudf_velox::createMemoryResource(mrMode, options.memoryPercent);
  cudf::set_current_device_resource(mr.get());

  exec::Operator::registerOperator(
      std::make_unique<CudfHashJoinBridgeTranslator>());
  CudfDriverAdapter cda{mr, options.force_replace};
  exec::DriverAdapter cudfAdapter{kCudfAdapterName, {}, cda};
  exec::DriverFactory::registerAdapter(cudfAdapter);
  isCudfRegistered = true;
}

void unregisterCudf() {
  exec::DriverFactory::adapters.erase(
      std::remove_if(
          exec::DriverFactory::adapters.begin(),
          exec::DriverFactory::adapters.end(),
          [](const exec::DriverAdapter& adapter) {
            return adapter.label == kCudfAdapterName;
          }),
      exec::DriverFactory::adapters.end());

  isCudfRegistered = false;
}

bool cudfIsRegistered() {
  return isCudfRegistered;
}

bool cudfDebugEnabled() {
  return FLAGS_velox_cudf_debug;
}

} // namespace facebook::velox::cudf_velox
