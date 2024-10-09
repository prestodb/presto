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

#include "velox/functions/sparksql/aggregates/Register.h"

#include "velox/functions/sparksql/aggregates/AverageAggregate.h"
#include "velox/functions/sparksql/aggregates/BitwiseXorAggregate.h"
#include "velox/functions/sparksql/aggregates/BloomFilterAggAggregate.h"
#include "velox/functions/sparksql/aggregates/CentralMomentsAggregate.h"
#include "velox/functions/sparksql/aggregates/CollectListAggregate.h"
#include "velox/functions/sparksql/aggregates/RegrReplacementAggregate.h"
#include "velox/functions/sparksql/aggregates/SumAggregate.h"

namespace facebook::velox::functions::aggregate::sparksql {

extern void registerCollectSetAggAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerFirstLastAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMinMaxAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMinMaxByAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerModeAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);

void registerAggregateFunctions(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerFirstLastAggregates(prefix, withCompanionFunctions, overwrite);
  registerMinMaxAggregates(prefix, withCompanionFunctions, overwrite);
  registerMinMaxByAggregates(prefix, withCompanionFunctions, overwrite);
  registerBitwiseXorAggregate(prefix, withCompanionFunctions, overwrite);
  registerBloomFilterAggAggregate(
      prefix + "bloom_filter_agg", withCompanionFunctions, overwrite);
  registerAverage(prefix + "avg", withCompanionFunctions, overwrite);
  registerSum(prefix + "sum", withCompanionFunctions, overwrite);
  registerCentralMomentsAggregate(prefix, withCompanionFunctions, overwrite);
  registerCollectSetAggAggregate(prefix, withCompanionFunctions, overwrite);
  registerCollectListAggregate(prefix, withCompanionFunctions, overwrite);
  registerRegrReplacementAggregate(prefix, withCompanionFunctions, overwrite);
  registerModeAggregate(prefix, withCompanionFunctions, overwrite);
}

std::vector<std::string> listAggregateFunctionNames() {
  std::vector<std::string> names;
  exec::aggregateFunctions().withRLock([&](const auto& map) {
    names.reserve(map.size());
    for (const auto& function : map) {
      names.push_back(function.first);
    }
  });

  return names;
}
} // namespace facebook::velox::functions::aggregate::sparksql
