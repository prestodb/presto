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
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"

namespace facebook::velox::aggregate::prestosql {

extern void registerApproxDistinctAggregates(const std::string& prefix);
extern void registerApproxMostFrequentAggregate(const std::string& prefix);
extern void registerApproxPercentileAggregate(const std::string& prefix);
extern void registerArbitraryAggregate(const std::string& prefix);
extern void registerArrayAggregate(const std::string& prefix);
extern void registerAverageAggregate(const std::string& prefix);
extern void registerBitwiseAggregates(const std::string& prefix);
extern void registerBoolAggregates(const std::string& prefix);
extern void registerChecksumAggregate(const std::string& prefix);
extern void registerCountAggregate(const std::string& prefix);
extern void registerCountIfAggregate(const std::string& prefix);
extern void registerCovarianceAggregates(const std::string& prefix);
extern void registerHistogramAggregate(const std::string& prefix);
extern void registerMapAggAggregate(const std::string& prefix);
extern void registerMapUnionAggregate(const std::string& prefix);
extern void registerMaxSizeForStatsAggregate(const std::string& prefix);
extern void registerMinMaxAggregates(const std::string& prefix);
extern void registerMinMaxByAggregates(const std::string& prefix);
extern void registerSumAggregate(const std::string& prefix);
extern void registerVarianceAggregates(const std::string& prefix);

void registerAllAggregateFunctions(const std::string& prefix) {
  registerApproxDistinctAggregates(prefix);
  registerApproxMostFrequentAggregate(prefix);
  registerApproxPercentileAggregate(prefix);
  registerArbitraryAggregate(prefix);
  registerArrayAggregate(prefix);
  registerAverageAggregate(prefix);
  registerBitwiseAggregates(prefix);
  registerBoolAggregates(prefix);
  registerChecksumAggregate(prefix);
  registerCountAggregate(prefix);
  registerCountIfAggregate(prefix);
  registerCovarianceAggregates(prefix);
  registerHistogramAggregate(prefix);
  registerMapAggAggregate(prefix);
  registerMapUnionAggregate(prefix);
  registerMaxSizeForStatsAggregate(prefix);
  registerMinMaxAggregates(prefix);
  registerMinMaxByAggregates(prefix);
  registerSumAggregate(prefix);
  registerVarianceAggregates(prefix);
}

} // namespace facebook::velox::aggregate::prestosql
