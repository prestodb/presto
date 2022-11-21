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
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/exec/Aggregate.h"

namespace facebook::velox::window {

extern void registerAggregateWindowFunction(const std::string& name);

namespace prestosql {

extern void registerRowNumber(const std::string& name);
extern void registerRank(const std::string& name);
extern void registerDenseRank(const std::string& name);
extern void registerPercentRank(const std::string& name);
extern void registerCumeDist(const std::string& name);
extern void registerNtile(const std::string& name);
extern void registerNthValue(const std::string& name);

void registerAllWindowFunctions() {
  registerRowNumber("row_number");
  registerRank("rank");
  registerDenseRank("dense_rank");
  registerPercentRank("percent_rank");
  registerCumeDist("cume_dist");
  registerNtile("ntile");
  registerNthValue("nth_value");

  // Register all aggregate functions as window functions.
  const auto& aggregateFunctions = exec::aggregateFunctions();
  for (const auto& aggregateEntry : aggregateFunctions) {
    window::registerAggregateWindowFunction(aggregateEntry.first);
  }
}

} // namespace prestosql

} // namespace facebook::velox::window
