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
#include "velox/functions/prestosql/aggregates/ApproxDistinctAggregates.h"
#include "velox/functions/prestosql/aggregates/ApproxMostFrequentAggregate.h"
#include "velox/functions/prestosql/aggregates/ApproxPercentileAggregate.h"
#include "velox/functions/prestosql/aggregates/ArbitraryAggregate.h"
#include "velox/functions/prestosql/aggregates/ArrayAggAggregate.h"
#include "velox/functions/prestosql/aggregates/AverageAggregate.h"
#include "velox/functions/prestosql/aggregates/BitwiseAggregates.h"
#include "velox/functions/prestosql/aggregates/BitwiseXorAggregate.h"
#include "velox/functions/prestosql/aggregates/BoolAggregates.h"
#include "velox/functions/prestosql/aggregates/CentralMomentsAggregates.h"
#include "velox/functions/prestosql/aggregates/ChecksumAggregate.h"
#include "velox/functions/prestosql/aggregates/ClassificationAggregation.h"
#include "velox/functions/prestosql/aggregates/CountAggregate.h"
#include "velox/functions/prestosql/aggregates/CountIfAggregate.h"
#include "velox/functions/prestosql/aggregates/CovarianceAggregates.h"
#include "velox/functions/prestosql/aggregates/EntropyAggregates.h"
#include "velox/functions/prestosql/aggregates/GeometricMeanAggregate.h"
#include "velox/functions/prestosql/aggregates/HistogramAggregate.h"
#include "velox/functions/prestosql/aggregates/MapAggAggregate.h"
#include "velox/functions/prestosql/aggregates/MapUnionAggregate.h"
#include "velox/functions/prestosql/aggregates/MapUnionSumAggregate.h"
#include "velox/functions/prestosql/aggregates/MaxByAggregate.h"
#include "velox/functions/prestosql/aggregates/MaxSizeForStatsAggregate.h"
#include "velox/functions/prestosql/aggregates/MergeAggregate.h"
#include "velox/functions/prestosql/aggregates/MinByAggregate.h"
#include "velox/functions/prestosql/aggregates/MinMaxAggregates.h"
#include "velox/functions/prestosql/aggregates/MultiMapAggAggregate.h"
#include "velox/functions/prestosql/aggregates/NoisyApproxSfmAggregate.h"
#include "velox/functions/prestosql/aggregates/NoisyAvgGaussianAggregate.h"
#include "velox/functions/prestosql/aggregates/NoisyCountGaussianAggregate.h"
#include "velox/functions/prestosql/aggregates/NoisyCountIfGaussianAggregate.h"
#include "velox/functions/prestosql/aggregates/NoisySumGaussianAggregate.h"
#include "velox/functions/prestosql/aggregates/NumericHistogramAggregate.h"
#include "velox/functions/prestosql/aggregates/QDigestAggAggregate.h"
#include "velox/functions/prestosql/aggregates/ReduceAgg.h"
#include "velox/functions/prestosql/aggregates/SetAggregates.h"
#include "velox/functions/prestosql/aggregates/SumAggregate.h"
#include "velox/functions/prestosql/aggregates/SumDataSizeForStatsAggregate.h"
#include "velox/functions/prestosql/aggregates/VarianceAggregates.h"
#include "velox/functions/prestosql/types/JsonRegistration.h"
#include "velox/functions/prestosql/types/TDigestRegistration.h"

namespace facebook::velox::aggregate::prestosql {

extern void registerApproxMostFrequentAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerApproxPercentileAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerArbitraryAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerArrayAggAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerAverageAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerBitwiseXorAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool onlyPrestoSignatures,
    bool overwrite);
extern void registerChecksumAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerClassificationFunctions(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerCountAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerCountIfAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerEntropyAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerGeometricMeanAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerHistogramAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMapAggAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMapUnionAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMapUnionSumAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMaxDataSizeForStatsAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMultiMapAggAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerSumDataSizeForStatsAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerReduceAgg(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerSetAggAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerSetUnionAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);

extern void registerApproxDistinctAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerBitwiseAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool onlyPrestoSignatures,
    bool overwrite);
extern void registerBoolAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerCentralMomentsAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerCovarianceAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMinMaxAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMaxByAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerMinByAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerSumAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerVarianceAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);
extern void registerTDigestAggregate(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite);

void registerAllAggregateFunctions(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool onlyPrestoSignatures,
    bool overwrite) {
  registerJsonType();
  registerTDigestType();
  registerApproxDistinctAggregates(prefix, withCompanionFunctions, overwrite);
  registerApproxMostFrequentAggregate(
      prefix, withCompanionFunctions, overwrite);
  registerApproxPercentileAggregate(prefix, withCompanionFunctions, overwrite);
  registerQDigestAggAggregate(prefix, overwrite);
  registerArbitraryAggregate(prefix, withCompanionFunctions, overwrite);
  registerArrayAggAggregate(prefix, withCompanionFunctions, overwrite);
  registerAverageAggregate(prefix, withCompanionFunctions, overwrite);
  registerBitwiseAggregates(
      prefix, withCompanionFunctions, onlyPrestoSignatures, overwrite);
  registerBitwiseXorAggregate(
      prefix, withCompanionFunctions, onlyPrestoSignatures, overwrite);
  registerBoolAggregates(prefix, withCompanionFunctions, overwrite);
  registerCentralMomentsAggregates(prefix, withCompanionFunctions, overwrite);
  registerChecksumAggregate(prefix, withCompanionFunctions, overwrite);
  registerClassificationFunctions(prefix, withCompanionFunctions, overwrite);
  registerCountAggregate(prefix, withCompanionFunctions, overwrite);
  registerCountIfAggregate(prefix, withCompanionFunctions, overwrite);
  registerCovarianceAggregates(prefix, withCompanionFunctions, overwrite);
  registerEntropyAggregate(prefix, withCompanionFunctions, overwrite);
  registerGeometricMeanAggregate(prefix, withCompanionFunctions, overwrite);
  registerHistogramAggregate(prefix, withCompanionFunctions, overwrite);
  registerMapAggAggregate(prefix, withCompanionFunctions, overwrite);
  registerMapUnionAggregate(prefix, withCompanionFunctions, overwrite);
  registerMapUnionSumAggregate(prefix, withCompanionFunctions, overwrite);
  registerMaxDataSizeForStatsAggregate(
      prefix, withCompanionFunctions, overwrite);
  registerMultiMapAggAggregate(prefix, withCompanionFunctions, overwrite);
  registerSumDataSizeForStatsAggregate(
      prefix, withCompanionFunctions, overwrite);
  registerMergeAggregate(prefix, withCompanionFunctions, overwrite);
  registerMinMaxAggregates(prefix, withCompanionFunctions, overwrite);
  registerMaxByAggregates(prefix, withCompanionFunctions, overwrite);
  registerMinByAggregates(prefix, withCompanionFunctions, overwrite);
  registerNoisyAvgGaussianAggregate(prefix, withCompanionFunctions, overwrite);
  registerNoisyCountIfGaussianAggregate(
      prefix, withCompanionFunctions, overwrite);
  registerNoisyCountGaussianAggregate(
      prefix, withCompanionFunctions, overwrite);
  registerNoisySumGaussianAggregate(prefix, withCompanionFunctions, overwrite);
  registerReduceAgg(prefix, withCompanionFunctions, overwrite);
  registerSetAggAggregate(prefix, withCompanionFunctions, overwrite);
  registerSetUnionAggregate(prefix, withCompanionFunctions, overwrite);
  registerSumAggregate(prefix, withCompanionFunctions, overwrite);
  registerVarianceAggregates(prefix, withCompanionFunctions, overwrite);
  registerTDigestAggregate(prefix, withCompanionFunctions, overwrite);
  registerNoisyApproxSfmAggregate(prefix, withCompanionFunctions, overwrite);
  registerNumericHistogramAggregate(prefix, withCompanionFunctions, overwrite);
}

void registerInternalAggregateFunctions(const std::string& prefix) {
  bool withCompanionFunctions = false;
  bool overwrite = false;
  registerCountDistinctAggregate(prefix, withCompanionFunctions, overwrite);
  registerInternalArrayAggAggregate(prefix, withCompanionFunctions, overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
