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

import com.facebook.presto.operator.aggregation.arrayagg.ArrayAggGroupImplementation;
import com.facebook.presto.operator.aggregation.histogram.HistogramGroupImplementation;
import com.facebook.presto.operator.aggregation.multimapagg.MultimapAggGroupImplementation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static com.facebook.presto.sql.analyzer.RegexLibrary.JONI;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig({
        "resource-group-manager",
        "experimental.resource-groups-enabled",
        "experimental-syntax-enabled",
        "analyzer.experimental-syntax-enabled",
        "optimizer.processing-optimization",
        "deprecated.legacy-order-by",
        "deprecated.legacy-join-using"})
public class FeaturesConfig
{
    @VisibleForTesting
    static final String SPILL_ENABLED = "experimental.spill-enabled";
    @VisibleForTesting
    static final String SPILLER_SPILL_PATH = "experimental.spiller-spill-path";

    private double cpuCostWeight = 75;
    private double memoryCostWeight = 10;
    private double networkCostWeight = 15;
    private boolean distributedIndexJoinsEnabled;
    private JoinDistributionType joinDistributionType = PARTITIONED;
    private DataSize joinMaxBroadcastTableSize;
    private boolean colocatedJoinsEnabled;
    private boolean groupedExecutionForAggregationEnabled;
    private boolean groupedExecutionForEligibleTableScansEnabled;
    private boolean dynamicScheduleForGroupedExecution;
    private boolean recoverableGroupedExecutionEnabled;
    private double maxFailedTaskPercentage = 0.3;
    private int concurrentLifespansPerTask;
    private boolean spatialJoinsEnabled = true;
    private boolean fastInequalityJoins = true;
    private JoinReorderingStrategy joinReorderingStrategy = ELIMINATE_CROSS_JOINS;
    private PartialMergePushdownStrategy partialMergePushdownStrategy = PartialMergePushdownStrategy.NONE;
    private int maxReorderedJoins = 9;
    private boolean redistributeWrites = true;
    private boolean scaleWriters;
    private DataSize writerMinSize = new DataSize(32, DataSize.Unit.MEGABYTE);
    private boolean optimizeMetadataQueries;
    private boolean optimizeHashGeneration = true;
    private boolean enableIntermediateAggregations;
    private boolean pushTableWriteThroughUnion = true;
    private boolean exchangeCompressionEnabled;
    private boolean legacyArrayAgg;
    private boolean legacyLogFunction;
    private boolean groupByUsesEqualTo;
    private boolean legacyTimestamp = true;
    private boolean legacyMapSubscript;
    private boolean legacyRowFieldOrdinalAccess;
    private boolean legacyCharToVarcharCoercion;
    private boolean optimizeMixedDistinctAggregations;
    private boolean forceSingleNodeOutput = true;
    private boolean pagesIndexEagerCompactionEnabled;
    private boolean distributedSort = true;

    private boolean dictionaryAggregation;

    private int re2JDfaStatesLimit = Integer.MAX_VALUE;
    private int re2JDfaRetries = 5;
    private RegexLibrary regexLibrary = JONI;
    private HistogramGroupImplementation histogramGroupImplementation = HistogramGroupImplementation.NEW;
    private ArrayAggGroupImplementation arrayAggGroupImplementation = ArrayAggGroupImplementation.NEW;
    private MultimapAggGroupImplementation multimapAggGroupImplementation = MultimapAggGroupImplementation.NEW;
    private boolean spillEnabled;
    private DataSize aggregationOperatorUnspillMemoryLimit = new DataSize(4, DataSize.Unit.MEGABYTE);
    private List<Path> spillerSpillPaths = ImmutableList.of();
    private int spillerThreads = 4;
    private double spillMaxUsedSpaceThreshold = 0.9;
    private boolean iterativeOptimizerEnabled = true;
    private boolean enableStatsCalculator = true;
    private boolean ignoreStatsCalculatorFailures = true;
    private boolean printStatsForNonJoinQuery;
    private boolean defaultFilterFactorEnabled;
    private boolean pushAggregationThroughJoin = true;
    private double memoryRevokingTarget = 0.5;
    private double memoryRevokingThreshold = 0.9;
    private boolean parseDecimalLiteralsAsDouble;
    private boolean useMarkDistinct = true;
    private boolean preferPartialAggregation = true;
    private boolean optimizeTopNRowNumber = true;
    private boolean pushLimitThroughOuterJoin = true;

    private Duration iterativeOptimizerTimeout = new Duration(3, MINUTES); // by default let optimizer wait a long time in case it retrieves some data from ConnectorMetadata

    private DataSize filterAndProjectMinOutputPageSize = new DataSize(500, KILOBYTE);
    private int filterAndProjectMinOutputPageRowCount = 256;
    private int maxGroupingSets = 2048;
    private boolean legacyUnnestArrayRows;

    private boolean jsonSerdeCodeGenerationEnabled;
    private int maxConcurrentMaterializations = 3;

    private boolean pushdownSubfieldsEnabled;

    private boolean tableWriterMergeOperatorEnabled = true;

    public enum JoinReorderingStrategy
    {
        NONE,
        ELIMINATE_CROSS_JOINS,
        AUTOMATIC,
    }

    public enum JoinDistributionType
    {
        BROADCAST,
        PARTITIONED,
        AUTOMATIC;

        public boolean canPartition()
        {
            return this == PARTITIONED || this == AUTOMATIC;
        }

        public boolean canReplicate()
        {
            return this == BROADCAST || this == AUTOMATIC;
        }
    }

    public enum PartialMergePushdownStrategy
    {
        NONE,
        PUSH_THROUGH_LOW_MEMORY_OPERATORS
    }

    public double getCpuCostWeight()
    {
        return cpuCostWeight;
    }

    @Config("cpu-cost-weight")
    public FeaturesConfig setCpuCostWeight(double cpuCostWeight)
    {
        this.cpuCostWeight = cpuCostWeight;
        return this;
    }

    public double getMemoryCostWeight()
    {
        return memoryCostWeight;
    }

    @Config("memory-cost-weight")
    public FeaturesConfig setMemoryCostWeight(double memoryCostWeight)
    {
        this.memoryCostWeight = memoryCostWeight;
        return this;
    }

    public double getNetworkCostWeight()
    {
        return networkCostWeight;
    }

    @Config("network-cost-weight")
    public FeaturesConfig setNetworkCostWeight(double networkCostWeight)
    {
        this.networkCostWeight = networkCostWeight;
        return this;
    }

    public boolean isDistributedIndexJoinsEnabled()
    {
        return distributedIndexJoinsEnabled;
    }

    @Config("distributed-index-joins-enabled")
    public FeaturesConfig setDistributedIndexJoinsEnabled(boolean distributedIndexJoinsEnabled)
    {
        this.distributedIndexJoinsEnabled = distributedIndexJoinsEnabled;
        return this;
    }

    @Config("deprecated.legacy-row-field-ordinal-access")
    public FeaturesConfig setLegacyRowFieldOrdinalAccess(boolean value)
    {
        this.legacyRowFieldOrdinalAccess = value;
        return this;
    }

    public boolean isLegacyRowFieldOrdinalAccess()
    {
        return legacyRowFieldOrdinalAccess;
    }

    @Config("deprecated.legacy-char-to-varchar-coercion")
    public FeaturesConfig setLegacyCharToVarcharCoercion(boolean value)
    {
        this.legacyCharToVarcharCoercion = value;
        return this;
    }

    public boolean isLegacyCharToVarcharCoercion()
    {
        return legacyCharToVarcharCoercion;
    }

    @Config("deprecated.legacy-array-agg")
    public FeaturesConfig setLegacyArrayAgg(boolean legacyArrayAgg)
    {
        this.legacyArrayAgg = legacyArrayAgg;
        return this;
    }

    public boolean isLegacyArrayAgg()
    {
        return legacyArrayAgg;
    }

    @Config("deprecated.legacy-log-function")
    public FeaturesConfig setLegacyLogFunction(boolean value)
    {
        this.legacyLogFunction = value;
        return this;
    }

    public boolean isLegacyLogFunction()
    {
        return legacyLogFunction;
    }

    @Config("deprecated.group-by-uses-equal")
    public FeaturesConfig setGroupByUsesEqualTo(boolean value)
    {
        this.groupByUsesEqualTo = value;
        return this;
    }

    public boolean isGroupByUsesEqualTo()
    {
        return groupByUsesEqualTo;
    }

    @Config("deprecated.legacy-timestamp")
    public FeaturesConfig setLegacyTimestamp(boolean value)
    {
        this.legacyTimestamp = value;
        return this;
    }

    public boolean isLegacyTimestamp()
    {
        return legacyTimestamp;
    }

    @Config("deprecated.legacy-map-subscript")
    public FeaturesConfig setLegacyMapSubscript(boolean value)
    {
        this.legacyMapSubscript = value;
        return this;
    }

    public boolean isLegacyMapSubscript()
    {
        return legacyMapSubscript;
    }

    public JoinDistributionType getJoinDistributionType()
    {
        return joinDistributionType;
    }

    @Config("join-distribution-type")
    public FeaturesConfig setJoinDistributionType(JoinDistributionType joinDistributionType)
    {
        this.joinDistributionType = requireNonNull(joinDistributionType, "joinDistributionType is null");
        return this;
    }

    public DataSize getJoinMaxBroadcastTableSize()
    {
        return joinMaxBroadcastTableSize;
    }

    @Config("join-max-broadcast-table-size")
    public FeaturesConfig setJoinMaxBroadcastTableSize(DataSize joinMaxBroadcastTableSize)
    {
        this.joinMaxBroadcastTableSize = joinMaxBroadcastTableSize;
        return this;
    }

    public boolean isGroupedExecutionForAggregationEnabled()
    {
        return groupedExecutionForAggregationEnabled;
    }

    @Config("grouped-execution-for-aggregation-enabled")
    @ConfigDescription("Experimental: Use grouped execution for aggregation when possible")
    public FeaturesConfig setGroupedExecutionForAggregationEnabled(boolean groupedExecutionForAggregationEnabled)
    {
        this.groupedExecutionForAggregationEnabled = groupedExecutionForAggregationEnabled;
        return this;
    }

    public boolean isGroupedExecutionForEligibleTableScansEnabled()
    {
        return groupedExecutionForEligibleTableScansEnabled;
    }

    @Config("experimental.grouped-execution-for-eligible-table-scans-enabled")
    @ConfigDescription("Experimental: Use grouped execution for eligible table scans")
    public FeaturesConfig setGroupedExecutionForEligibleTableScansEnabled(boolean groupedExecutionForEligibleTableScansEnabled)
    {
        this.groupedExecutionForEligibleTableScansEnabled = groupedExecutionForEligibleTableScansEnabled;
        return this;
    }

    public boolean isDynamicScheduleForGroupedExecutionEnabled()
    {
        return dynamicScheduleForGroupedExecution;
    }

    @Config("dynamic-schedule-for-grouped-execution")
    @ConfigDescription("Experimental: Use dynamic schedule for grouped execution when possible")
    public FeaturesConfig setDynamicScheduleForGroupedExecutionEnabled(boolean dynamicScheduleForGroupedExecution)
    {
        this.dynamicScheduleForGroupedExecution = dynamicScheduleForGroupedExecution;
        return this;
    }

    public boolean isRecoverableGroupedExecutionEnabled()
    {
        return recoverableGroupedExecutionEnabled;
    }

    @Config("recoverable-grouped-execution-enabled")
    @ConfigDescription("Use recoverable grouped execution when possible")
    public FeaturesConfig setRecoverableGroupedExecutionEnabled(boolean recoverableGroupedExecutionEnabled)
    {
        this.recoverableGroupedExecutionEnabled = recoverableGroupedExecutionEnabled;
        return this;
    }

    public double getMaxFailedTaskPercentage()
    {
        return maxFailedTaskPercentage;
    }

    @Config("max-failed-task-percentage")
    @ConfigDescription("Max percentage of failed tasks that are retryable for recoverable dynamic scheduling")
    public FeaturesConfig setMaxFailedTaskPercentage(double maxFailedTaskPercentage)
    {
        this.maxFailedTaskPercentage = maxFailedTaskPercentage;
        return this;
    }

    public int getConcurrentLifespansPerTask()
    {
        return concurrentLifespansPerTask;
    }

    @Config("concurrent-lifespans-per-task")
    @Min(0)
    @ConfigDescription("Experimental: Default number of lifespans that run in parallel on each task when grouped execution is enabled")
    // When set to zero, a limit is not imposed on the number of lifespans that run in parallel
    public FeaturesConfig setConcurrentLifespansPerTask(int concurrentLifespansPerTask)
    {
        this.concurrentLifespansPerTask = concurrentLifespansPerTask;
        return this;
    }

    public boolean isColocatedJoinsEnabled()
    {
        return colocatedJoinsEnabled;
    }

    @Config("colocated-joins-enabled")
    @ConfigDescription("Experimental: Use a colocated join when possible")
    public FeaturesConfig setColocatedJoinsEnabled(boolean colocatedJoinsEnabled)
    {
        this.colocatedJoinsEnabled = colocatedJoinsEnabled;
        return this;
    }

    public boolean isSpatialJoinsEnabled()
    {
        return spatialJoinsEnabled;
    }

    @Config("spatial-joins-enabled")
    @ConfigDescription("Use spatial index for spatial joins when possible")
    public FeaturesConfig setSpatialJoinsEnabled(boolean spatialJoinsEnabled)
    {
        this.spatialJoinsEnabled = spatialJoinsEnabled;
        return this;
    }

    @Config("fast-inequality-joins")
    @ConfigDescription("Use faster handling of inequality joins if it is possible")
    public FeaturesConfig setFastInequalityJoins(boolean fastInequalityJoins)
    {
        this.fastInequalityJoins = fastInequalityJoins;
        return this;
    }

    public boolean isFastInequalityJoins()
    {
        return fastInequalityJoins;
    }

    public JoinReorderingStrategy getJoinReorderingStrategy()
    {
        return joinReorderingStrategy;
    }

    @Config("optimizer.join-reordering-strategy")
    @ConfigDescription("The strategy to use for reordering joins")
    public FeaturesConfig setJoinReorderingStrategy(JoinReorderingStrategy joinReorderingStrategy)
    {
        this.joinReorderingStrategy = joinReorderingStrategy;
        return this;
    }

    public PartialMergePushdownStrategy getPartialMergePushdownStrategy()
    {
        return partialMergePushdownStrategy;
    }

    @Config("experimental.optimizer.partial-merge-pushdown-strategy")
    public FeaturesConfig setPartialMergePushdownStrategy(PartialMergePushdownStrategy partialMergePushdownStrategy)
    {
        this.partialMergePushdownStrategy = partialMergePushdownStrategy;
        return this;
    }

    @Min(2)
    public int getMaxReorderedJoins()
    {
        return maxReorderedJoins;
    }

    @Config("optimizer.max-reordered-joins")
    @ConfigDescription("The maximum number of tables to reorder in cost-based join reordering")
    public FeaturesConfig setMaxReorderedJoins(int maxReorderedJoins)
    {
        this.maxReorderedJoins = maxReorderedJoins;
        return this;
    }

    public boolean isRedistributeWrites()
    {
        return redistributeWrites;
    }

    @Config("redistribute-writes")
    public FeaturesConfig setRedistributeWrites(boolean redistributeWrites)
    {
        this.redistributeWrites = redistributeWrites;
        return this;
    }

    public boolean isScaleWriters()
    {
        return scaleWriters;
    }

    @Config("scale-writers")
    public FeaturesConfig setScaleWriters(boolean scaleWriters)
    {
        this.scaleWriters = scaleWriters;
        return this;
    }

    @NotNull
    public DataSize getWriterMinSize()
    {
        return writerMinSize;
    }

    @Config("writer-min-size")
    @ConfigDescription("Target minimum size of writer output when scaling writers")
    public FeaturesConfig setWriterMinSize(DataSize writerMinSize)
    {
        this.writerMinSize = writerMinSize;
        return this;
    }

    public boolean isOptimizeMetadataQueries()
    {
        return optimizeMetadataQueries;
    }

    @Config("optimizer.optimize-metadata-queries")
    public FeaturesConfig setOptimizeMetadataQueries(boolean optimizeMetadataQueries)
    {
        this.optimizeMetadataQueries = optimizeMetadataQueries;
        return this;
    }

    public boolean isUseMarkDistinct()
    {
        return useMarkDistinct;
    }

    @Config("optimizer.use-mark-distinct")
    public FeaturesConfig setUseMarkDistinct(boolean value)
    {
        this.useMarkDistinct = value;
        return this;
    }

    public boolean isPreferPartialAggregation()
    {
        return preferPartialAggregation;
    }

    @Config("optimizer.prefer-partial-aggregation")
    public FeaturesConfig setPreferPartialAggregation(boolean value)
    {
        this.preferPartialAggregation = value;
        return this;
    }

    public boolean isOptimizeTopNRowNumber()
    {
        return optimizeTopNRowNumber;
    }

    @Config("optimizer.optimize-top-n-row-number")
    public FeaturesConfig setOptimizeTopNRowNumber(boolean optimizeTopNRowNumber)
    {
        this.optimizeTopNRowNumber = optimizeTopNRowNumber;
        return this;
    }

    public boolean isOptimizeHashGeneration()
    {
        return optimizeHashGeneration;
    }

    @Config("optimizer.optimize-hash-generation")
    public FeaturesConfig setOptimizeHashGeneration(boolean optimizeHashGeneration)
    {
        this.optimizeHashGeneration = optimizeHashGeneration;
        return this;
    }

    public boolean isPushTableWriteThroughUnion()
    {
        return pushTableWriteThroughUnion;
    }

    @Config("optimizer.push-table-write-through-union")
    public FeaturesConfig setPushTableWriteThroughUnion(boolean pushTableWriteThroughUnion)
    {
        this.pushTableWriteThroughUnion = pushTableWriteThroughUnion;
        return this;
    }

    public boolean isDictionaryAggregation()
    {
        return dictionaryAggregation;
    }

    @Config("optimizer.dictionary-aggregation")
    public FeaturesConfig setDictionaryAggregation(boolean dictionaryAggregation)
    {
        this.dictionaryAggregation = dictionaryAggregation;
        return this;
    }

    @Min(2)
    public int getRe2JDfaStatesLimit()
    {
        return re2JDfaStatesLimit;
    }

    @Config("re2j.dfa-states-limit")
    public FeaturesConfig setRe2JDfaStatesLimit(int re2JDfaStatesLimit)
    {
        this.re2JDfaStatesLimit = re2JDfaStatesLimit;
        return this;
    }

    @Min(0)
    public int getRe2JDfaRetries()
    {
        return re2JDfaRetries;
    }

    @Config("re2j.dfa-retries")
    public FeaturesConfig setRe2JDfaRetries(int re2JDfaRetries)
    {
        this.re2JDfaRetries = re2JDfaRetries;
        return this;
    }

    public RegexLibrary getRegexLibrary()
    {
        return regexLibrary;
    }

    @Config("regex-library")
    public FeaturesConfig setRegexLibrary(RegexLibrary regexLibrary)
    {
        this.regexLibrary = regexLibrary;
        return this;
    }

    public boolean isSpillEnabled()
    {
        return spillEnabled;
    }

    @Config(SPILL_ENABLED)
    public FeaturesConfig setSpillEnabled(boolean spillEnabled)
    {
        this.spillEnabled = spillEnabled;
        return this;
    }

    public boolean isIterativeOptimizerEnabled()
    {
        return iterativeOptimizerEnabled;
    }

    @Config("experimental.iterative-optimizer-enabled")
    public FeaturesConfig setIterativeOptimizerEnabled(boolean value)
    {
        this.iterativeOptimizerEnabled = value;
        return this;
    }

    public Duration getIterativeOptimizerTimeout()
    {
        return iterativeOptimizerTimeout;
    }

    @Config("experimental.iterative-optimizer-timeout")
    public FeaturesConfig setIterativeOptimizerTimeout(Duration timeout)
    {
        this.iterativeOptimizerTimeout = timeout;
        return this;
    }

    public boolean isEnableStatsCalculator()
    {
        return enableStatsCalculator;
    }

    @Config("experimental.enable-stats-calculator")
    public FeaturesConfig setEnableStatsCalculator(boolean enableStatsCalculator)
    {
        this.enableStatsCalculator = enableStatsCalculator;
        return this;
    }

    public boolean isIgnoreStatsCalculatorFailures()
    {
        return ignoreStatsCalculatorFailures;
    }

    @Config("optimizer.ignore-stats-calculator-failures")
    @ConfigDescription("Ignore statistics calculator failures")
    public FeaturesConfig setIgnoreStatsCalculatorFailures(boolean ignoreStatsCalculatorFailures)
    {
        this.ignoreStatsCalculatorFailures = ignoreStatsCalculatorFailures;
        return this;
    }

    public boolean isPrintStatsForNonJoinQuery()
    {
        return printStatsForNonJoinQuery;
    }

    @Config("print-stats-for-non-join-query")
    public FeaturesConfig setPrintStatsForNonJoinQuery(boolean printStatsForNonJoinQuery)
    {
        this.printStatsForNonJoinQuery = printStatsForNonJoinQuery;
        return this;
    }

    @Config("optimizer.default-filter-factor-enabled")
    public FeaturesConfig setDefaultFilterFactorEnabled(boolean defaultFilterFactorEnabled)
    {
        this.defaultFilterFactorEnabled = defaultFilterFactorEnabled;
        return this;
    }

    public boolean isDefaultFilterFactorEnabled()
    {
        return defaultFilterFactorEnabled;
    }

    public DataSize getAggregationOperatorUnspillMemoryLimit()
    {
        return aggregationOperatorUnspillMemoryLimit;
    }

    @Config("experimental.aggregation-operator-unspill-memory-limit")
    public FeaturesConfig setAggregationOperatorUnspillMemoryLimit(DataSize aggregationOperatorUnspillMemoryLimit)
    {
        this.aggregationOperatorUnspillMemoryLimit = aggregationOperatorUnspillMemoryLimit;
        return this;
    }

    public List<Path> getSpillerSpillPaths()
    {
        return spillerSpillPaths;
    }

    @Config(SPILLER_SPILL_PATH)
    public FeaturesConfig setSpillerSpillPaths(String spillPaths)
    {
        List<String> spillPathsSplit = ImmutableList.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().split(spillPaths));
        this.spillerSpillPaths = spillPathsSplit.stream().map(Paths::get).collect(toImmutableList());
        return this;
    }

    @AssertTrue(message = SPILLER_SPILL_PATH + " must be configured when " + SPILL_ENABLED + " is set to true")
    public boolean isSpillerSpillPathsConfiguredIfSpillEnabled()
    {
        return !isSpillEnabled() || !spillerSpillPaths.isEmpty();
    }

    @Min(1)
    public int getSpillerThreads()
    {
        return spillerThreads;
    }

    @Config("experimental.spiller-threads")
    public FeaturesConfig setSpillerThreads(int spillerThreads)
    {
        this.spillerThreads = spillerThreads;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getMemoryRevokingThreshold()
    {
        return memoryRevokingThreshold;
    }

    @Config("experimental.memory-revoking-threshold")
    @ConfigDescription("Revoke memory when memory pool is filled over threshold")
    public FeaturesConfig setMemoryRevokingThreshold(double memoryRevokingThreshold)
    {
        this.memoryRevokingThreshold = memoryRevokingThreshold;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getMemoryRevokingTarget()
    {
        return memoryRevokingTarget;
    }

    @Config("experimental.memory-revoking-target")
    @ConfigDescription("When revoking memory, try to revoke so much that pool is filled below target at the end")
    public FeaturesConfig setMemoryRevokingTarget(double memoryRevokingTarget)
    {
        this.memoryRevokingTarget = memoryRevokingTarget;
        return this;
    }

    public double getSpillMaxUsedSpaceThreshold()
    {
        return spillMaxUsedSpaceThreshold;
    }

    @Config("experimental.spiller-max-used-space-threshold")
    public FeaturesConfig setSpillMaxUsedSpaceThreshold(double spillMaxUsedSpaceThreshold)
    {
        this.spillMaxUsedSpaceThreshold = spillMaxUsedSpaceThreshold;
        return this;
    }

    public boolean isOptimizeMixedDistinctAggregations()
    {
        return optimizeMixedDistinctAggregations;
    }

    @Config("optimizer.optimize-mixed-distinct-aggregations")
    public FeaturesConfig setOptimizeMixedDistinctAggregations(boolean value)
    {
        this.optimizeMixedDistinctAggregations = value;
        return this;
    }

    public boolean isExchangeCompressionEnabled()
    {
        return exchangeCompressionEnabled;
    }

    @Config("exchange.compression-enabled")
    public FeaturesConfig setExchangeCompressionEnabled(boolean exchangeCompressionEnabled)
    {
        this.exchangeCompressionEnabled = exchangeCompressionEnabled;
        return this;
    }

    public boolean isEnableIntermediateAggregations()
    {
        return enableIntermediateAggregations;
    }

    @Config("optimizer.enable-intermediate-aggregations")
    public FeaturesConfig setEnableIntermediateAggregations(boolean enableIntermediateAggregations)
    {
        this.enableIntermediateAggregations = enableIntermediateAggregations;
        return this;
    }

    public boolean isPushAggregationThroughJoin()
    {
        return pushAggregationThroughJoin;
    }

    @Config("optimizer.push-aggregation-through-join")
    public FeaturesConfig setPushAggregationThroughJoin(boolean value)
    {
        this.pushAggregationThroughJoin = value;
        return this;
    }

    public boolean isParseDecimalLiteralsAsDouble()
    {
        return parseDecimalLiteralsAsDouble;
    }

    @Config("parse-decimal-literals-as-double")
    public FeaturesConfig setParseDecimalLiteralsAsDouble(boolean parseDecimalLiteralsAsDouble)
    {
        this.parseDecimalLiteralsAsDouble = parseDecimalLiteralsAsDouble;
        return this;
    }

    public boolean isForceSingleNodeOutput()
    {
        return forceSingleNodeOutput;
    }

    @Config("optimizer.force-single-node-output")
    public FeaturesConfig setForceSingleNodeOutput(boolean value)
    {
        this.forceSingleNodeOutput = value;
        return this;
    }

    public boolean isPagesIndexEagerCompactionEnabled()
    {
        return pagesIndexEagerCompactionEnabled;
    }

    @Config("pages-index.eager-compaction-enabled")
    public FeaturesConfig setPagesIndexEagerCompactionEnabled(boolean pagesIndexEagerCompactionEnabled)
    {
        this.pagesIndexEagerCompactionEnabled = pagesIndexEagerCompactionEnabled;
        return this;
    }

    @MaxDataSize("1MB")
    public DataSize getFilterAndProjectMinOutputPageSize()
    {
        return filterAndProjectMinOutputPageSize;
    }

    @Config("experimental.filter-and-project-min-output-page-size")
    public FeaturesConfig setFilterAndProjectMinOutputPageSize(DataSize filterAndProjectMinOutputPageSize)
    {
        this.filterAndProjectMinOutputPageSize = filterAndProjectMinOutputPageSize;
        return this;
    }

    @Min(0)
    public int getFilterAndProjectMinOutputPageRowCount()
    {
        return filterAndProjectMinOutputPageRowCount;
    }

    @Config("experimental.filter-and-project-min-output-page-row-count")
    public FeaturesConfig setFilterAndProjectMinOutputPageRowCount(int filterAndProjectMinOutputPageRowCount)
    {
        this.filterAndProjectMinOutputPageRowCount = filterAndProjectMinOutputPageRowCount;
        return this;
    }

    @Config("histogram.implementation")
    public FeaturesConfig setHistogramGroupImplementation(HistogramGroupImplementation groupByMode)
    {
        this.histogramGroupImplementation = groupByMode;
        return this;
    }

    public HistogramGroupImplementation getHistogramGroupImplementation()
    {
        return histogramGroupImplementation;
    }

    public ArrayAggGroupImplementation getArrayAggGroupImplementation()
    {
        return arrayAggGroupImplementation;
    }

    @Config("arrayagg.implementation")
    public FeaturesConfig setArrayAggGroupImplementation(ArrayAggGroupImplementation groupByMode)
    {
        this.arrayAggGroupImplementation = groupByMode;
        return this;
    }

    public MultimapAggGroupImplementation getMultimapAggGroupImplementation()
    {
        return multimapAggGroupImplementation;
    }

    @Config("multimapagg.implementation")
    public FeaturesConfig setMultimapAggGroupImplementation(MultimapAggGroupImplementation groupByMode)
    {
        this.multimapAggGroupImplementation = groupByMode;
        return this;
    }

    public boolean isDistributedSortEnabled()
    {
        return distributedSort;
    }

    @Config("distributed-sort")
    public FeaturesConfig setDistributedSortEnabled(boolean enabled)
    {
        distributedSort = enabled;
        return this;
    }

    public int getMaxGroupingSets()
    {
        return maxGroupingSets;
    }

    @Config("analyzer.max-grouping-sets")
    public FeaturesConfig setMaxGroupingSets(int maxGroupingSets)
    {
        this.maxGroupingSets = maxGroupingSets;
        return this;
    }

    public boolean isLegacyUnnestArrayRows()
    {
        return legacyUnnestArrayRows;
    }

    @Config("deprecated.legacy-unnest-array-rows")
    public FeaturesConfig setLegacyUnnestArrayRows(boolean legacyUnnestArrayRows)
    {
        this.legacyUnnestArrayRows = legacyUnnestArrayRows;
        return this;
    }

    @Config("experimental.json-serde-codegen-enabled")
    @ConfigDescription("Enable code generation for JSON serialization and deserialization")
    public FeaturesConfig setJsonSerdeCodeGenerationEnabled(boolean jsonSerdeCodeGenerationEnabled)
    {
        this.jsonSerdeCodeGenerationEnabled = jsonSerdeCodeGenerationEnabled;
        return this;
    }

    public boolean isJsonSerdeCodeGenerationEnabled()
    {
        return jsonSerdeCodeGenerationEnabled;
    }

    @Config("optimizer.push-limit-through-outer-join")
    public FeaturesConfig setPushLimitThroughOuterJoin(boolean pushLimitThroughOuterJoin)
    {
        this.pushLimitThroughOuterJoin = pushLimitThroughOuterJoin;
        return this;
    }

    public boolean isPushLimitThroughOuterJoin()
    {
        return pushLimitThroughOuterJoin;
    }

    @Config("max-concurrent-materializations")
    @Min(1)
    @ConfigDescription("The maximum number of materializing plan sections that can run concurrently")
    public FeaturesConfig setMaxConcurrentMaterializations(int maxConcurrentMaterializations)
    {
        this.maxConcurrentMaterializations = maxConcurrentMaterializations;
        return this;
    }

    public int getMaxConcurrentMaterializations()
    {
        return maxConcurrentMaterializations;
    }

    @Config("experimental.pushdown-subfields-enabled")
    @ConfigDescription("Experimental: enable subfield pruning")
    public FeaturesConfig setPushdownSubfieldsEnabled(boolean pushdownSubfieldsEnabled)
    {
        this.pushdownSubfieldsEnabled = pushdownSubfieldsEnabled;
        return this;
    }

    public boolean isPushdownSubfieldsEnabled()
    {
        return pushdownSubfieldsEnabled;
    }

    public boolean isTableWriterMergeOperatorEnabled()
    {
        return tableWriterMergeOperatorEnabled;
    }

    @Config("experimental.table-writer-merge-operator-enabled")
    public FeaturesConfig setTableWriterMergeOperatorEnabled(boolean tableWriterMergeOperatorEnabled)
    {
        this.tableWriterMergeOperatorEnabled = tableWriterMergeOperatorEnabled;
        return this;
    }
}
