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
import com.facebook.presto.sql.analyzer.FeaturesConfig.AggregationIfToFilterRewriteStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartitioningPrecisionStrategy;
import com.facebook.presto.sql.analyzer.FeaturesConfig.SingleStreamSpillerChoice;
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
import static com.facebook.presto.sql.analyzer.FeaturesConfig.TaskSpillingStrategy.ORDER_BY_CREATE_TIME;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.TaskSpillingStrategy.PER_TASK_MEMORY_THRESHOLD;
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
                .setJoinMaxBroadcastTableSize(new DataSize(100, MEGABYTE))
                .setGroupedExecutionEnabled(true)
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
                .setReduceAggForComplexTypesEnabled(true)
                .setRegexLibrary(JONI)
                .setRe2JDfaStatesLimit(Integer.MAX_VALUE)
                .setRe2JDfaRetries(5)
                .setSpillEnabled(false)
                .setJoinSpillingEnabled(true)
                .setAggregationSpillEnabled(true)
                .setDistinctAggregationSpillEnabled(true)
                .setDedupBasedDistinctAggregationSpillEnabled(false)
                .setOrderByAggregationSpillEnabled(true)
                .setWindowSpillEnabled(true)
                .setOrderBySpillEnabled(true)
                .setAggregationOperatorUnspillMemoryLimit(DataSize.valueOf("4MB"))
                .setSpillerSpillPaths("")
                .setSpillerThreads(4)
                .setSpillMaxUsedSpaceThreshold(0.9)
                .setMemoryRevokingThreshold(0.9)
                .setMemoryRevokingTarget(0.5)
                .setTaskSpillingStrategy(ORDER_BY_CREATE_TIME)
                .setQueryLimitSpillEnabled(false)
                .setSingleStreamSpillerChoice(SingleStreamSpillerChoice.LOCAL_FILE)
                .setSpillerTempStorage("local")
                .setMaxRevocableMemoryPerTask(new DataSize(500, MEGABYTE))
                .setOptimizeMixedDistinctAggregations(false)
                .setLegacyLogFunction(false)
                .setIterativeOptimizerEnabled(true)
                .setIterativeOptimizerTimeout(new Duration(3, MINUTES))
                .setRuntimeOptimizerEnabled(false)
                .setEnableDynamicFiltering(false)
                .setDynamicFilteringMaxPerDriverRowCount(100)
                .setDynamicFilteringMaxPerDriverSize(new DataSize(10, KILOBYTE))
                .setDynamicFilteringRangeRowLimitPerDriver(0)
                .setFragmentResultCachingEnabled(false)
                .setEnableStatsCalculator(true)
                .setEnableStatsCollectionForTemporaryTable(false)
                .setIgnoreStatsCalculatorFailures(true)
                .setPrintStatsForNonJoinQuery(false)
                .setDefaultFilterFactorEnabled(false)
                .setExchangeCompressionEnabled(false)
                .setExchangeChecksumEnabled(false)
                .setLegacyTimestamp(true)
                .setLegacyRowFieldOrdinalAccess(false)
                .setLegacyCharToVarcharCoercion(false)
                .setLegacyDateTimestampToVarcharCoercion(false)
                .setEnableIntermediateAggregations(false)
                .setPushAggregationThroughJoin(true)
                .setParseDecimalLiteralsAsDouble(false)
                .setForceSingleNodeOutput(true)
                .setPagesIndexEagerCompactionEnabled(false)
                .setFilterAndProjectMinOutputPageSize(new DataSize(500, KILOBYTE))
                .setFilterAndProjectMinOutputPageRowCount(256)
                .setUseMarkDistinct(true)
                .setPreferPartialAggregation(true)
                .setPartialAggregationStrategy(PartialAggregationStrategy.ALWAYS)
                .setPartialAggregationByteReductionThreshold(0.5)
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
                .setPushdownDereferenceEnabled(false)
                .setTableWriterMergeOperatorEnabled(true)
                .setIndexLoaderTimeout(new Duration(20, SECONDS))
                .setOptimizedRepartitioningEnabled(false)
                .setListBuiltInFunctionsOnly(true)
                .setPartitioningPrecisionStrategy(PartitioningPrecisionStrategy.AUTOMATIC)
                .setExperimentalFunctionsEnabled(false)
                .setUseLegacyScheduler(true)
                .setOptimizeCommonSubExpressions(true)
                .setPreferDistributedUnion(true)
                .setOptimizeNullsInJoin(false)
                .setSkipRedundantSort(true)
                .setWarnOnNoTableLayoutFilter("")
                .setInlineSqlFunctions(true)
                .setCheckAccessControlOnUtilizedColumnsOnly(false)
                .setAllowWindowOrderByLiterals(true)
                .setEnforceFixedDistributionForOutputOperator(false)
                .setEmptyJoinOptimization(false)
                .setLogFormattedQueryEnabled(false)
                .setSpoolingOutputBufferEnabled(false)
                .setSpoolingOutputBufferThreshold(new DataSize(8, MEGABYTE))
                .setSpoolingOutputBufferTempStorage("local")
                .setPrestoSparkAssignBucketToPartitionForPartitionedTableWriteEnabled(false)
                .setPartialResultsEnabled(false)
                .setPartialResultsCompletionRatioThreshold(0.5)
                .setOffsetClauseEnabled(false)
                .setPartialResultsMaxExecutionTimeMultiplier(2.0)
                .setMaterializedViewDataConsistencyEnabled(true)
                .setQueryOptimizationWithMaterializedViewEnabled(false)
                .setAggregationIfToFilterRewriteStrategy(AggregationIfToFilterRewriteStrategy.DISABLED));
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
                .put("experimental.runtime-optimizer-enabled", "true")
                .put("experimental.enable-dynamic-filtering", "true")
                .put("experimental.dynamic-filtering-max-per-driver-row-count", "256")
                .put("experimental.dynamic-filtering-max-per-driver-size", "64kB")
                .put("experimental.dynamic-filtering-range-row-limit-per-driver", "1000")
                .put("experimental.fragment-result-caching-enabled", "true")
                .put("experimental.enable-stats-calculator", "false")
                .put("experimental.enable-stats-collection-for-temporary-table", "true")
                .put("optimizer.ignore-stats-calculator-failures", "false")
                .put("print-stats-for-non-join-query", "true")
                .put("optimizer.default-filter-factor-enabled", "true")
                .put("deprecated.legacy-array-agg", "true")
                .put("deprecated.legacy-log-function", "true")
                .put("deprecated.group-by-uses-equal", "true")
                .put("deprecated.legacy-map-subscript", "true")
                .put("reduce-agg-for-complex-types-enabled", "false")
                .put("deprecated.legacy-row-field-ordinal-access", "true")
                .put("deprecated.legacy-char-to-varchar-coercion", "true")
                .put("deprecated.legacy-date-timestamp-to-varchar-coercion", "true")
                .put("distributed-index-joins-enabled", "true")
                .put("join-distribution-type", "BROADCAST")
                .put("join-max-broadcast-table-size", "42GB")
                .put("grouped-execution-enabled", "false")
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
                .put("experimental.join-spill-enabled", "false")
                .put("experimental.aggregation-spill-enabled", "false")
                .put("experimental.distinct-aggregation-spill-enabled", "false")
                .put("experimental.dedup-based-distinct-aggregation-spill-enabled", "true")
                .put("experimental.order-by-aggregation-spill-enabled", "false")
                .put("experimental.window-spill-enabled", "false")
                .put("experimental.order-by-spill-enabled", "false")
                .put("experimental.aggregation-operator-unspill-memory-limit", "100MB")
                .put("experimental.spiller-spill-path", "/tmp/custom/spill/path1,/tmp/custom/spill/path2")
                .put("experimental.spiller-threads", "42")
                .put("experimental.spiller-max-used-space-threshold", "0.8")
                .put("experimental.memory-revoking-threshold", "0.2")
                .put("experimental.memory-revoking-target", "0.8")
                .put("experimental.spiller.task-spilling-strategy", "PER_TASK_MEMORY_THRESHOLD")
                .put("experimental.query-limit-spill-enabled", "true")
                .put("experimental.spiller.single-stream-spiller-choice", "TEMP_STORAGE")
                .put("experimental.spiller.spiller-temp-storage", "crail")
                .put("experimental.spiller.max-revocable-task-memory", "1GB")
                .put("exchange.compression-enabled", "true")
                .put("exchange.checksum-enabled", "true")
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
                .put("optimizer.partial-aggregation-strategy", "automatic")
                .put("optimizer.partial-aggregation-byte-reduction-threshold", "0.8")
                .put("optimizer.optimize-top-n-row-number", "false")
                .put("distributed-sort", "false")
                .put("analyzer.max-grouping-sets", "2047")
                .put("deprecated.legacy-unnest-array-rows", "true")
                .put("experimental.json-serde-codegen-enabled", "true")
                .put("optimizer.push-limit-through-outer-join", "false")
                .put("max-concurrent-materializations", "5")
                .put("experimental.pushdown-subfields-enabled", "true")
                .put("experimental.pushdown-dereference-enabled", "true")
                .put("experimental.table-writer-merge-operator-enabled", "false")
                .put("index-loader-timeout", "10s")
                .put("experimental.optimized-repartitioning", "true")
                .put("list-built-in-functions-only", "false")
                .put("partitioning-precision-strategy", "PREFER_EXACT_PARTITIONING")
                .put("experimental-functions-enabled", "true")
                .put("use-legacy-scheduler", "false")
                .put("optimize-common-sub-expressions", "false")
                .put("prefer-distributed-union", "false")
                .put("optimize-nulls-in-join", "true")
                .put("warn-on-no-table-layout-filter", "ry@nlikestheyankees,ds")
                .put("inline-sql-functions", "false")
                .put("check-access-control-on-utilized-columns-only", "true")
                .put("optimizer.skip-redundant-sort", "false")
                .put("is-allow-window-order-by-literals", "false")
                .put("enforce-fixed-distribution-for-output-operator", "true")
                .put("optimizer.optimize-joins-with-empty-sources", "true")
                .put("log-formatted-query-enabled", "true")
                .put("spooling-output-buffer-enabled", "true")
                .put("spooling-output-buffer-threshold", "16MB")
                .put("spooling-output-buffer-temp-storage", "tempfs")
                .put("spark.assign-bucket-to-partition-for-partitioned-table-write-enabled", "true")
                .put("partial-results-enabled", "true")
                .put("partial-results-completion-ratio-threshold", "0.9")
                .put("partial-results-max-execution-time-multiplier", "1.5")
                .put("offset-clause-enabled", "true")
                .put("materialized-view-data-consistency-enabled", "false")
                .put("query-optimization-with-materialized-view-enabled", "true")
                .put("optimizer.aggregation-if-to-filter-rewrite-strategy", "filter_with_if")
                .build();

        FeaturesConfig expected = new FeaturesConfig()
                .setCpuCostWeight(0.4)
                .setMemoryCostWeight(0.3)
                .setNetworkCostWeight(0.2)
                .setIterativeOptimizerEnabled(false)
                .setIterativeOptimizerTimeout(new Duration(10, SECONDS))
                .setRuntimeOptimizerEnabled(true)
                .setEnableDynamicFiltering(true)
                .setDynamicFilteringMaxPerDriverRowCount(256)
                .setDynamicFilteringMaxPerDriverSize(new DataSize(64, KILOBYTE))
                .setDynamicFilteringRangeRowLimitPerDriver(1000)
                .setFragmentResultCachingEnabled(true)
                .setEnableStatsCalculator(false)
                .setEnableStatsCollectionForTemporaryTable(true)
                .setIgnoreStatsCalculatorFailures(false)
                .setPrintStatsForNonJoinQuery(true)
                .setDistributedIndexJoinsEnabled(true)
                .setJoinDistributionType(BROADCAST)
                .setJoinMaxBroadcastTableSize(new DataSize(42, GIGABYTE))
                .setGroupedExecutionEnabled(false)
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
                .setReduceAggForComplexTypesEnabled(false)
                .setRegexLibrary(RE2J)
                .setRe2JDfaStatesLimit(42)
                .setRe2JDfaRetries(42)
                .setSpillEnabled(true)
                .setJoinSpillingEnabled(false)
                .setAggregationSpillEnabled(false)
                .setDistinctAggregationSpillEnabled(false)
                .setDedupBasedDistinctAggregationSpillEnabled(true)
                .setOrderByAggregationSpillEnabled(false)
                .setWindowSpillEnabled(false)
                .setOrderBySpillEnabled(false)
                .setAggregationOperatorUnspillMemoryLimit(DataSize.valueOf("100MB"))
                .setSpillerSpillPaths("/tmp/custom/spill/path1,/tmp/custom/spill/path2")
                .setSpillerThreads(42)
                .setSpillMaxUsedSpaceThreshold(0.8)
                .setMemoryRevokingThreshold(0.2)
                .setMemoryRevokingTarget(0.8)
                .setTaskSpillingStrategy(PER_TASK_MEMORY_THRESHOLD)
                .setQueryLimitSpillEnabled(true)
                .setSingleStreamSpillerChoice(SingleStreamSpillerChoice.TEMP_STORAGE)
                .setSpillerTempStorage("crail")
                .setMaxRevocableMemoryPerTask(new DataSize(1, GIGABYTE))
                .setLegacyLogFunction(true)
                .setExchangeCompressionEnabled(true)
                .setExchangeChecksumEnabled(true)
                .setLegacyTimestamp(false)
                .setLegacyRowFieldOrdinalAccess(true)
                .setLegacyCharToVarcharCoercion(true)
                .setLegacyDateTimestampToVarcharCoercion(true)
                .setEnableIntermediateAggregations(true)
                .setParseDecimalLiteralsAsDouble(true)
                .setForceSingleNodeOutput(false)
                .setPagesIndexEagerCompactionEnabled(true)
                .setFilterAndProjectMinOutputPageSize(new DataSize(1, MEGABYTE))
                .setFilterAndProjectMinOutputPageRowCount(2048)
                .setUseMarkDistinct(false)
                .setPreferPartialAggregation(false)
                .setPartialAggregationStrategy(PartialAggregationStrategy.AUTOMATIC)
                .setPartialAggregationByteReductionThreshold(0.8)
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
                .setPushdownDereferenceEnabled(true)
                .setTableWriterMergeOperatorEnabled(false)
                .setIndexLoaderTimeout(new Duration(10, SECONDS))
                .setOptimizedRepartitioningEnabled(true)
                .setListBuiltInFunctionsOnly(false)
                .setPartitioningPrecisionStrategy(PartitioningPrecisionStrategy.PREFER_EXACT_PARTITIONING)
                .setExperimentalFunctionsEnabled(true)
                .setUseLegacyScheduler(false)
                .setOptimizeCommonSubExpressions(false)
                .setPreferDistributedUnion(false)
                .setOptimizeNullsInJoin(true)
                .setSkipRedundantSort(false)
                .setWarnOnNoTableLayoutFilter("ry@nlikestheyankees,ds")
                .setInlineSqlFunctions(false)
                .setCheckAccessControlOnUtilizedColumnsOnly(true)
                .setSkipRedundantSort(false)
                .setAllowWindowOrderByLiterals(false)
                .setEnforceFixedDistributionForOutputOperator(true)
                .setEmptyJoinOptimization(true)
                .setLogFormattedQueryEnabled(true)
                .setSpoolingOutputBufferEnabled(true)
                .setSpoolingOutputBufferThreshold(new DataSize(16, MEGABYTE))
                .setSpoolingOutputBufferTempStorage("tempfs")
                .setPrestoSparkAssignBucketToPartitionForPartitionedTableWriteEnabled(true)
                .setPartialResultsEnabled(true)
                .setPartialResultsCompletionRatioThreshold(0.9)
                .setOffsetClauseEnabled(true)
                .setPartialResultsMaxExecutionTimeMultiplier(1.5)
                .setMaterializedViewDataConsistencyEnabled(false)
                .setQueryOptimizationWithMaterializedViewEnabled(true)
                .setAggregationIfToFilterRewriteStrategy(AggregationIfToFilterRewriteStrategy.FILTER_WITH_IF);
        assertFullMapping(properties, expected);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*\\Q" + SPILLER_SPILL_PATH + " must be configured when " + SPILL_ENABLED + " is set to true\\E.*")
    public void testValidateSpillConfiguredIfEnabled()
    {
        new ConfigurationFactory(ImmutableMap.of(SPILL_ENABLED, "true"))
                .build(FeaturesConfig.class);
    }
}
