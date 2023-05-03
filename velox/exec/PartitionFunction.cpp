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
#include <velox/exec/HashPartitionFunction.h>
#include <velox/exec/RoundRobinPartitionFunction.h>
#include "velox/core/PlanNode.h"

namespace facebook::velox::exec {

void registerPartitionFunctionSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();

  registry.Register(
      "HashPartitionFunctionSpec", HashPartitionFunctionSpec::deserialize);
  registry.Register(
      "RoundRobinPartitionFunctionSpec",
      RoundRobinPartitionFunctionSpec::deserialize);
}

} // namespace facebook::velox::exec
