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

#include "velox/exec/Operator.h"

namespace facebook::velox::core {
struct PlanFragment;
} // namespace facebook::velox::core

namespace facebook::velox::exec {

class LocalPlanner {
 public:
  static void plan(
      const core::PlanFragment& planFragment,
      ConsumerSupplier consumerSupplier,
      std::vector<std::unique_ptr<DriverFactory>>* driverFactories,
      uint32_t maxDrivers);

  // Determine which pipelines should run Grouped Execution.
  static void determineGroupedExecutionPipelines(
      const core::PlanFragment& planFragment,
      std::vector<std::unique_ptr<DriverFactory>>& driverFactories);

  // Detect joins running in grouped execution mode having sources running in
  // ungrouped execution mode. In such case the join bridge would be between
  // grouped execution and ungrouped execution and needs special care to be
  // detected by both pipelines.
  static void markMixedJoinBridges(
      std::vector<std::unique_ptr<DriverFactory>>& driverFactories);
};
} // namespace facebook::velox::exec
