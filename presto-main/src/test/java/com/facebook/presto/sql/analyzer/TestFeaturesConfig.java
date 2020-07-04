/*
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
package com.facebook.presto.sql.analyzer;

import com.facebook.airlift.configuration.ConfigurationFactory;
import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.presto.operator.aggregation.arrayagg.ArrayAggGroupImplementation;
import com.facebook.presto.operator.aggregation.histogram.HistogramGroupImplementation;
import com.facebook.presto.operator.aggregation.multimapagg.MultimapAggGroupImplementation;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartitioningPrecisionStrategy;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.AggregationPartitioningMergingStrategy.LEGACY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.AggregationPartitioningMergingStrategy.TOP_DOWN;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialMergePushdownStrategy.PUSH_THROUGH_LOW_MEMORY_OPERATORS;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.SPILLER_SPILL_PATH;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.SPILL_ENABLED;
import static com.facebook.presto.sql.analyzer.RegexLibrary.JONI;
import static com.facebook.presto.sql.analyzer.RegexLibrary.RE2J;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestFeaturesConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(FeaturesConfig.class)
                .setCpuCostWeight(75)
                .setMemoryCostWeight(10)
                .setNetworkCostWeight(15)
                .setDistributedIndexJoinsEnabled(false)
                .setJoinDistributionType(PARTITIONED)
                .setJoinMaxBroadcastTableSize(null)
                .setGroupedExecutionForAggregationEnabled(false)
                .setGroupedExecutionForJoinEnabled(true)
                .setGroupedExecutionForEligibleTableScansEnabled(false)
                .setDynamicScheduleForGroupedExecutionEnabled(false)
                .setRecoverableGroupedExecutionEnabled(false)
                .setMaxFailedTaskPercentage(0.3)
                .setMaxStageRetries(0)
                .setConcurrentLifespansPerTask(0)
                .setFastInequalityJoins(true)
                .setColocatedJoinsEnabled(true)
                .setSpatialJoinsEnabled(true)
                .setJoinReorderingStrategy(ELIMINATE_CROSS_JOINS)
                .setPartialMergePushdownStrategy(FeaturesConfig.PartialMergePushdownStrategy.NONE)
                .setMaxReorderedJoins(9)
                .setRedistributeWrites(true)
                .setScaleWriters(false)
                .setWriterMinSize(new DataSize(32, MEGABYTE))
                .setOptimizedScaleWriterProducerBuffer(false)
                .setOptimizeMetadataQueries(false)
                .setOptimizeHashGeneration(true)
                .setPushTableWriteThroughUnion(true)
                .setDictionaryAggregation(false)
                .setAggregationPartitioningMergingStrategy(LEGACY)
                .setLegacyArrayAgg(false)
                .setGroupByUsesEqualTo(false)
                .setLegacyMapSubscript(false)
                .setRegexLibrary(JONI)
                .setRe2JDfaStatesLimit(Integer.MAX_VALUE)
                .setRe2JDfaRetries(5)
                .setSpillEnabled(false)
                .setAggregationOperatorUnspillMemoryLimit(DataSize.valueOf("4MB"))
                .setSpillerSpillPaths("")
                .setSpillerThreads(4)
                .setSpillMaxUsedSpaceThreshold(0.9)
                .setMemoryRevokingThreshold(0.9)
                .setMemoryRevokingTarget(0.5)
                .setOptimizeMixedDistinctAggregations(false)
                .setLegacyLogFunction(false)
                .setIterativeOptimizerEnabled(true)
                .setIterativeOptimizerTimeout(new Duration(3, MINUTES))
                .setEnableStatsCalculator(true)
                .setEnableStatsCollectionForTemporaryTable(false)
                .setIgnoreStatsCalculatorFailures(true)
                .setPrintStatsForNonJoinQuery(false)
                .setDefaultFilterFactorEnabled(false)
                .setExchangeCompressionEnabled(false)
                .setLegacyTimestamp(true)
                .setLegacyRowFieldOrdinalAccess(false)
                .setLegacyCharToVarcharCoercion(false)
                .setEnableIntermediateAggregations(false)
                .setPushAggregationThroughJoin(true)
                .setParseDecimalLiteralsAsDouble(false)
                .setForceSingleNodeOutput(true)
                .setPagesIndexEagerCompactionEnabled(false)
                .setFilterAndProjectMinOutputPageSize(new DataSize(500, KILOBYTE))
                .setFilterAndProjectMinOutputPageRowCount(256)
                .setUseMarkDistinct(true)
                .setPreferPartialAggregation(true)
                .setOptimizeTopNRowNumber(true)
                .setHistogramGroupImplementation(HistogramGroupImplementation.NEW)
                .setArrayAggGroupImplementation(ArrayAggGroupImplementation.NEW)
                .setMultimapAggGroupImplementation(MultimapAggGroupImplementation.NEW)
                .setDistributedSortEnabled(true)
                .setMaxGroupingSets(2048)
                .setLegacyUnnestArrayRows(false)
                .setJsonSerdeCodeGenerationEnabled(false)
                .setPushLimitThroughOuterJoin(true)
                .setMaxConcurrentMaterializations(3)
                .setPushdownSubfieldsEnabled(false)
                .setTableWriterMergeOperatorEnabled(true)
                .setOptimizeFullOuterJoinWithCoalesce(true)
                .setIndexLoaderTimeout(new Duration(20, SECONDS))
                .setOptimizedRepartitioningEnabled(false)
                .setListBuiltInFunctionsOnly(true)
                .setPartitioningPrecisionStrategy(PartitioningPrecisionStrategy.AUTOMATIC)
                .setExperimentalFunctionsEnabled(false)
                .setUseLegacyScheduler(true)
                .setOptimizeCommonSubExpressions(true)
                .setPreferDistributedUnion(true)
                .setOptimizeNullsInJoin(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("cpu-cost-weight", "0.4")
                .put("memory-cost-weight", "0.3")
                .put("network-cost-weight", "0.2")
                .put("experimental.iterative-optimizer-enabled", "false")
                .put("experimental.iterative-optimizer-timeout", "10s")
                .put("experimental.enable-stats-calculator", "false")
                .put("experimental.enable-stats-collection-for-temporary-table", "true")
                .put("optimizer.ignore-stats-calculator-failures", "false")
                .put("print-stats-for-non-join-query", "true")
                .put("optimizer.default-filter-factor-enabled", "true")
                .put("deprecated.legacy-array-agg", "true")
                .put("deprecated.legacy-log-function", "true")
                .put("deprecated.group-by-uses-equal", "true")
                .put("deprecated.legacy-map-subscript", "true")
                .put("deprecated.legacy-row-field-ordinal-access", "true")
                .put("deprecated.legacy-char-to-varchar-coercion", "true")
                .put("distributed-index-joins-enabled", "true")
                .put("join-distribution-type", "BROADCAST")
                .put("join-max-broadcast-table-size", "42GB")
                .put("grouped-execution-for-aggregation-enabled", "true")
                .put("grouped-execution-for-join-enabled", "false")
                .put("experimental.grouped-execution-for-eligible-table-scans-enabled", "true")
                .put("dynamic-schedule-for-grouped-execution", "true")
                .put("recoverable-grouped-execution-enabled", "true")
                .put("max-failed-task-percentage", "0.8")
                .put("max-stage-retries", "10")
                .put("concurrent-lifespans-per-task", "1")
                .put("fast-inequality-joins", "false")
                .put("colocated-joins-enabled", "false")
                .put("spatial-joins-enabled", "false")
                .put("optimizer.join-reordering-strategy", "NONE")
                .put("experimental.optimizer.partial-merge-pushdown-strategy", PUSH_THROUGH_LOW_MEMORY_OPERATORS.name())
                .put("optimizer.max-reordered-joins", "5")
                .put("redistribute-writes", "false")
                .put("scale-writers", "true")
                .put("writer-min-size", "42GB")
                .put("optimized-scale-writer-producer-buffer", "true")
                .put("optimizer.optimize-metadata-queries", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("optimizer.optimize-mixed-distinct-aggregations", "true")
                .put("optimizer.push-table-write-through-union", "false")
                .put("optimizer.dictionary-aggregation", "true")
                .put("optimizer.push-aggregation-through-join", "false")
                .put("optimizer.aggregation-partition-merging", "top_down")
                .put("regex-library", "RE2J")
                .put("re2j.dfa-states-limit", "42")
                .put("re2j.dfa-retries", "42")
                .put("experimental.spill-enabled", "true")
                .put("experimental.aggregation-operator-unspill-memory-limit", "100MB")
                .put("experimental.spiller-spill-path", "/tmp/custom/spill/path1,/tmp/custom/spill/path2")
                .put("experimental.spiller-threads", "42")
                .put("experimental.spiller-max-used-space-threshold", "0.8")
                .put("experimental.memory-revoking-threshold", "0.2")
                .put("experimental.memory-revoking-target", "0.8")
                .put("exchange.compression-enabled", "true")
                .put("deprecated.legacy-timestamp", "false")
                .put("optimizer.enable-intermediate-aggregations", "true")
                .put("parse-decimal-literals-as-double", "true")
                .put("optimizer.force-single-node-output", "false")
                .put("pages-index.eager-compaction-enabled", "true")
                .put("experimental.filter-and-project-min-output-page-size", "1MB")
                .put("experimental.filter-and-project-min-output-page-row-count", "2048")
                .put("histogram.implementation", "LEGACY")
                .put("arrayagg.implementation", "LEGACY")
                .put("multimapagg.implementation", "LEGACY")
                .put("optimizer.use-mark-distinct", "false")
                .put("optimizer.prefer-partial-aggregation", "false")
                .put("optimizer.optimize-top-n-row-number", "false")
                .put("distributed-sort", "false")
                .put("analyzer.max-grouping-sets", "2047")
                .put("deprecated.legacy-unnest-array-rows", "true")
                .put("experimental.json-serde-codegen-enabled", "true")
                .put("optimizer.push-limit-through-outer-join", "false")
                .put("max-concurrent-materializations", "5")
                .put("experimental.pushdown-subfields-enabled", "true")
                .put("experimental.table-writer-merge-operator-enabled", "false")
                .put("optimizer.optimize-full-outer-join-with-coalesce", "false")
                .put("index-loader-timeout", "10s")
                .put("experimental.optimized-repartitioning", "true")
                .put("list-built-in-functions-only", "false")
                .put("partitioning-precision-strategy", "PREFER_EXACT_PARTITIONING")
                .put("experimental-functions-enabled", "true")
                .put("use-legacy-scheduler", "false")
                .put("optimize-common-sub-expressions", "false")
                .put("prefer-distributed-union", "false")
                .put("optimize-nulls-in-join", "true")
                .build();

        FeaturesConfig expected = new FeaturesConfig()
                .setCpuCostWeight(0.4)
                .setMemoryCostWeight(0.3)
                .setNetworkCostWeight(0.2)
                .setIterativeOptimizerEnabled(false)
                .setIterativeOptimizerTimeout(new Duration(10, SECONDS))
                .setEnableStatsCalculator(false)
                .setEnableStatsCollectionForTemporaryTable(true)
                .setIgnoreStatsCalculatorFailures(false)
                .setPrintStatsForNonJoinQuery(true)
                .setDistributedIndexJoinsEnabled(true)
                .setJoinDistributionType(BROADCAST)
                .setJoinMaxBroadcastTableSize(new DataSize(42, GIGABYTE))
                .setGroupedExecutionForAggregationEnabled(true)
                .setGroupedExecutionForJoinEnabled(false)
                .setGroupedExecutionForEligibleTableScansEnabled(true)
                .setDynamicScheduleForGroupedExecutionEnabled(true)
                .setRecoverableGroupedExecutionEnabled(true)
                .setMaxFailedTaskPercentage(0.8)
                .setMaxStageRetries(10)
                .setConcurrentLifespansPerTask(1)
                .setFastInequalityJoins(false)
                .setColocatedJoinsEnabled(false)
                .setSpatialJoinsEnabled(false)
                .setJoinReorderingStrategy(NONE)
                .setPartialMergePushdownStrategy(PUSH_THROUGH_LOW_MEMORY_OPERATORS)
                .setMaxReorderedJoins(5)
                .setRedistributeWrites(false)
                .setScaleWriters(true)
                .setWriterMinSize(new DataSize(42, GIGABYTE))
                .setOptimizedScaleWriterProducerBuffer(true)
                .setOptimizeMetadataQueries(true)
                .setOptimizeHashGeneration(false)
                .setOptimizeMixedDistinctAggregations(true)
                .setPushTableWriteThroughUnion(false)
                .setDictionaryAggregation(true)
                .setAggregationPartitioningMergingStrategy(TOP_DOWN)
                .setPushAggregationThroughJoin(false)
                .setLegacyArrayAgg(true)
                .setGroupByUsesEqualTo(true)
                .setLegacyMapSubscript(true)
                .setRegexLibrary(RE2J)
                .setRe2JDfaStatesLimit(42)
                .setRe2JDfaRetries(42)
                .setSpillEnabled(true)
                .setAggregationOperatorUnspillMemoryLimit(DataSize.valueOf("100MB"))
                .setSpillerSpillPaths("/tmp/custom/spill/path1,/tmp/custom/spill/path2")
                .setSpillerThreads(42)
                .setSpillMaxUsedSpaceThreshold(0.8)
                .setMemoryRevokingThreshold(0.2)
                .setMemoryRevokingTarget(0.8)
                .setLegacyLogFunction(true)
                .setExchangeCompressionEnabled(true)
                .setLegacyTimestamp(false)
                .setLegacyRowFieldOrdinalAccess(true)
                .setLegacyCharToVarcharCoercion(true)
                .setEnableIntermediateAggregations(true)
                .setParseDecimalLiteralsAsDouble(true)
                .setForceSingleNodeOutput(false)
                .setPagesIndexEagerCompactionEnabled(true)
                .setFilterAndProjectMinOutputPageSize(new DataSize(1, MEGABYTE))
                .setFilterAndProjectMinOutputPageRowCount(2048)
                .setUseMarkDistinct(false)
                .setPreferPartialAggregation(false)
                .setOptimizeTopNRowNumber(false)
                .setHistogramGroupImplementation(HistogramGroupImplementation.LEGACY)
                .setArrayAggGroupImplementation(ArrayAggGroupImplementation.LEGACY)
                .setMultimapAggGroupImplementation(MultimapAggGroupImplementation.LEGACY)
                .setDistributedSortEnabled(false)
                .setMaxGroupingSets(2047)
                .setLegacyUnnestArrayRows(true)
                .setDefaultFilterFactorEnabled(true)
                .setJsonSerdeCodeGenerationEnabled(true)
                .setPushLimitThroughOuterJoin(false)
                .setMaxConcurrentMaterializations(5)
                .setPushdownSubfieldsEnabled(true)
                .setTableWriterMergeOperatorEnabled(false)
                .setOptimizeFullOuterJoinWithCoalesce(false)
                .setIndexLoaderTimeout(new Duration(10, SECONDS))
                .setOptimizedRepartitioningEnabled(true)
                .setListBuiltInFunctionsOnly(false)
                .setPartitioningPrecisionStrategy(PartitioningPrecisionStrategy.PREFER_EXACT_PARTITIONING)
                .setExperimentalFunctionsEnabled(true)
                .setUseLegacyScheduler(false)
                .setOptimizeCommonSubExpressions(false)
                .setPreferDistributedUnion(false)
                .setOptimizeNullsInJoin(true);
        assertFullMapping(properties, expected);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*\\Q" + SPILLER_SPILL_PATH + " must be configured when " + SPILL_ENABLED + " is set to true\\E.*")
    public void testValidateSpillConfiguredIfEnabled()
    {
        new ConfigurationFactory(ImmutableMap.of(SPILL_ENABLED, "true"))
                .build(FeaturesConfig.class);
    }
}
