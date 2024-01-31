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
#include "velox/exec/Aggregate.h"

namespace facebook::velox::aggregate::prestosql {

extern void registerApproxMostFrequentAggregate(const std::string& prefix);
extern void registerApproxPercentileAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerArbitraryAggregate(const std::string& prefix);
extern void registerArrayAggAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerAverageAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerBitwiseXorAggregate(
    const std::string& prefix,
    bool onlyPrestoSignatures);
extern void registerChecksumAggregate(const std::string& prefix);
extern void registerCountAggregate(const std::string& prefix);
extern void registerCountIfAggregate(const std::string& prefix);
extern void registerEntropyAggregate(const std::string& prefix);
extern void registerGeometricMeanAggregate(const std::string& prefix);
extern void registerHistogramAggregate(const std::string& prefix);
extern void registerMapAggAggregate(const std::string& prefix);
extern void registerMapUnionAggregate(const std::string& prefix);
extern void registerMapUnionSumAggregate(const std::string& prefix);
extern void registerMaxDataSizeForStatsAggregate(const std::string& prefix);
extern void registerMultiMapAggAggregate(const std::string& prefix);
extern void registerSumDataSizeForStatsAggregate(const std::string& prefix);
extern void registerReduceAgg(const std::string& prefix);
extern void registerSetAggAggregate(const std::string& prefix);
extern void registerSetUnionAggregate(const std::string& prefix);

extern void registerApproxDistinctAggregates(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerBitwiseAggregates(
    const std::string& prefix,
    bool onlyPrestoSignatures);
extern void registerBoolAggregates(const std::string& prefix);
extern void registerCentralMomentsAggregates(const std::string& prefix);
extern void registerCovarianceAggregates(const std::string& prefix);
extern void registerMinMaxAggregates(const std::string& prefix);
extern void registerMinMaxByAggregates(const std::string& prefix);
extern void registerSumAggregate(const std::string& prefix);
extern void registerVarianceAggregates(const std::string& prefix);

void registerAllAggregateFunctions(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool onlyPrestoSignatures) {
  registerApproxDistinctAggregates(prefix, withCompanionFunctions);
  registerApproxMostFrequentAggregate(prefix);
  registerApproxPercentileAggregate(prefix, withCompanionFunctions);
  registerArbitraryAggregate(prefix);
  registerArrayAggAggregate(prefix, withCompanionFunctions);
  registerAverageAggregate(prefix, withCompanionFunctions);
  registerBitwiseAggregates(prefix, onlyPrestoSignatures);
  registerBitwiseXorAggregate(prefix, onlyPrestoSignatures);
  registerBoolAggregates(prefix);
  registerCentralMomentsAggregates(prefix);
  registerChecksumAggregate(prefix);
  registerCountAggregate(prefix);
  registerCountIfAggregate(prefix);
  registerCovarianceAggregates(prefix);
  registerEntropyAggregate(prefix);
  registerGeometricMeanAggregate(prefix);
  registerHistogramAggregate(prefix);
  registerMapAggAggregate(prefix);
  registerMapUnionAggregate(prefix);
  registerMapUnionSumAggregate(prefix);
  registerMaxDataSizeForStatsAggregate(prefix);
  registerMultiMapAggAggregate(prefix);
  registerSumDataSizeForStatsAggregate(prefix);
  registerMinMaxAggregates(prefix);
  registerMinMaxByAggregates(prefix);
  registerReduceAgg(prefix);
  registerSetAggAggregate(prefix);
  registerSetUnionAggregate(prefix);
  registerSumAggregate(prefix);
  registerVarianceAggregates(prefix);
}

} // namespace facebook::velox::aggregate::prestosql
