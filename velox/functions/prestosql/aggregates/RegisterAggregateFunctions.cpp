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

extern void registerApproxMostFrequentAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerApproxPercentileAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerArbitraryAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerArrayAggAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerAverageAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerBitwiseXorAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool onlyPrestoSignatures);
extern void registerChecksumAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerCountAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerCountIfAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerEntropyAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerGeometricMeanAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerHistogramAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerMapAggAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerMapUnionAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerMapUnionSumAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerMaxDataSizeForStatsAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerMultiMapAggAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerSumDataSizeForStatsAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerReduceAgg(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerSetAggAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerSetUnionAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);

extern void registerApproxDistinctAggregates(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerBitwiseAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool onlyPrestoSignatures);
extern void registerBoolAggregates(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerCentralMomentsAggregates(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerCovarianceAggregates(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerMinMaxAggregates(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerMinMaxByAggregates(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerSumAggregate(
    const std::string& prefix,
    bool withCompanionFunctions);
extern void registerVarianceAggregates(
    const std::string& prefix,
    bool withCompanionFunctions);

void registerAllAggregateFunctions(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool onlyPrestoSignatures) {
  registerApproxDistinctAggregates(prefix, withCompanionFunctions);
  registerApproxMostFrequentAggregate(prefix, withCompanionFunctions);
  registerApproxPercentileAggregate(prefix, withCompanionFunctions);
  registerArbitraryAggregate(prefix, withCompanionFunctions);
  registerArrayAggAggregate(prefix, withCompanionFunctions);
  registerAverageAggregate(prefix, withCompanionFunctions);
  registerBitwiseAggregates(
      prefix, withCompanionFunctions, onlyPrestoSignatures);
  registerBitwiseXorAggregate(
      prefix, withCompanionFunctions, onlyPrestoSignatures);
  registerBoolAggregates(prefix, withCompanionFunctions);
  registerCentralMomentsAggregates(prefix, withCompanionFunctions);
  registerChecksumAggregate(prefix, withCompanionFunctions);
  registerCountAggregate(prefix, withCompanionFunctions);
  registerCountIfAggregate(prefix, withCompanionFunctions);
  registerCovarianceAggregates(prefix, withCompanionFunctions);
  registerEntropyAggregate(prefix, withCompanionFunctions);
  registerGeometricMeanAggregate(prefix, withCompanionFunctions);
  registerHistogramAggregate(prefix, withCompanionFunctions);
  registerMapAggAggregate(prefix, withCompanionFunctions);
  registerMapUnionAggregate(prefix, withCompanionFunctions);
  registerMapUnionSumAggregate(prefix, withCompanionFunctions);
  registerMaxDataSizeForStatsAggregate(prefix, withCompanionFunctions);
  registerMultiMapAggAggregate(prefix, withCompanionFunctions);
  registerSumDataSizeForStatsAggregate(prefix, withCompanionFunctions);
  registerMinMaxAggregates(prefix, withCompanionFunctions);
  registerMinMaxByAggregates(prefix, withCompanionFunctions);
  registerReduceAgg(prefix, withCompanionFunctions);
  registerSetAggAggregate(prefix, withCompanionFunctions);
  registerSetUnionAggregate(prefix, withCompanionFunctions);
  registerSumAggregate(prefix, withCompanionFunctions);
  registerVarianceAggregates(prefix, withCompanionFunctions);
}

} // namespace facebook::velox::aggregate::prestosql
