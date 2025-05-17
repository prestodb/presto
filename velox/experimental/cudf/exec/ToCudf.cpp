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

#include "velox/experimental/cudf/exec/CudfConversion.h"
#include "velox/experimental/cudf/exec/CudfHashAggregation.h"
#include "velox/experimental/cudf/exec/CudfOrderBy.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/exec/Utilities.h"

#include "velox/exec/Driver.h"
#include "velox/exec/FilterProject.h"
#include "velox/exec/HashAggregation.h"
#include "velox/exec/HashBuild.h"
#include "velox/exec/HashProbe.h"
#include "velox/exec/Operator.h"
#include "velox/exec/OrderBy.h"

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

bool CompileState::compile() {
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

  auto isSupportedGpuOperator = [](const exec::Operator* op) {
    return isAnyOf<exec::OrderBy, exec::HashAggregation>(op);
  };

  std::vector<bool> isSupportedGpuOperators(operators.size());
  std::transform(
      operators.begin(),
      operators.end(),
      isSupportedGpuOperators.begin(),
      isSupportedGpuOperator);

  auto acceptsGpuInput = [](const exec::Operator* op) {
    return isAnyOf<exec::OrderBy, exec::HashAggregation>(op);
  };

  auto producesGpuOutput = [](const exec::Operator* op) {
    return isAnyOf<exec::OrderBy, exec::HashAggregation>(op);
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

    if (auto* orderByOp = dynamic_cast<exec::OrderBy*>(oper)) {
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
    }

    if (producesGpuOutput(oper) and
        (nextOperatorIsNotGpu or isLastOperatorOfTask)) {
      auto planNode = getPlanNode(oper->planNodeId());
      replaceOp.push_back(std::make_unique<CudfToVelox>(
          id, planNode->outputType(), ctx, planNode->id() + "-to-velox"));
      replaceOp.back()->initialize();
    }

    if (not replaceOp.empty()) {
      operatorsOffset += replaceOp.size() - 1;
      [[maybe_unused]] auto replaced = driverFactory_.replaceOperators(
          driver_,
          replacingOperatorIndex,
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

  CudfDriverAdapter(std::shared_ptr<rmm::mr::device_memory_resource> mr)
      : mr_(mr) {}

  // Call operator needed by DriverAdapter
  bool operator()(const exec::DriverFactory& factory, exec::Driver& driver) {
    auto state = CompileState(factory, driver);
    auto res = state.compile();
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

  CUDF_FUNC_RANGE();
  cudaFree(nullptr); // Initialize CUDA context at startup

  const std::string mrMode = options.cudfMemoryResource;
  auto mr = cudf_velox::createMemoryResource(mrMode);
  cudf::set_current_device_resource(mr.get());
  CudfDriverAdapter cda{mr};
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

} // namespace facebook::velox::cudf_velox
