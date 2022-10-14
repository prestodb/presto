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

extern void registerApproxDistinctAggregates();
extern void registerApproxMostFrequentAggregate();
extern void registerApproxPercentileAggregate();
extern void registerArbitraryAggregate();
extern void registerArrayAggregate();
extern void registerAverageAggregate();
extern void registerBitwiseAggregates();
extern void registerBoolAggregates();
extern void registerChecksumAggregate();
extern void registerCountAggregate();
extern void registerCountIfAggregate();
extern void registerCovarianceAggregates();
extern void registerHistogramAggregate();
extern void registerMapAggAggregate();
extern void registerMapUnionAggregate();
extern void registerMaxSizeForStatsAggregate();
extern void registerMinMaxAggregates();
extern void registerMinMaxByAggregates();
extern void registerSumAggregate();
extern void registerVarianceAggregates();

void registerAllAggregateFunctions() {
  registerApproxDistinctAggregates();
  registerApproxMostFrequentAggregate();
  registerApproxPercentileAggregate();
  registerArbitraryAggregate();
  registerArrayAggregate();
  registerAverageAggregate();
  registerBitwiseAggregates();
  registerBoolAggregates();
  registerChecksumAggregate();
  registerCountAggregate();
  registerCountIfAggregate();
  registerCovarianceAggregates();
  registerHistogramAggregate();
  registerMapAggAggregate();
  registerMapUnionAggregate();
  registerMaxSizeForStatsAggregate();
  registerMinMaxAggregates();
  registerMinMaxByAggregates();
  registerSumAggregate();
  registerVarianceAggregates();
}

} // namespace facebook::velox::aggregate::prestosql
