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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.DefunctConfig;
import com.facebook.presto.operator.aggregation.arrayagg.ArrayAggGroupImplementation;
import com.facebook.presto.operator.aggregation.histogram.HistogramGroupImplementation;
import com.facebook.presto.operator.aggregation.multimapagg.MultimapAggGroupImplementation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
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

import static com.facebook.presto.sql.analyzer.FeaturesConfig.AggregationPartitioningMergingStrategy.LEGACY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.TaskSpillingStrategy.ORDER_BY_CREATE_TIME;
import static com.facebook.presto.sql.analyzer.RegexLibrary.JONI;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

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

    private static final String JOIN_SPILL_ENABLED = "experimental.join-spill-enabled";
    private static final String SINGLE_STREAM_SPILLER_CHOICE = "experimental.spiller.single-stream-spiller-choice";

    private double cpuCostWeight = 75;
    private double memoryCostWeight = 10;
    private double networkCostWeight = 15;
    private boolean distributedIndexJoinsEnabled;
    private JoinDistributionType joinDistributionType = PARTITIONED;
    private DataSize joinMaxBroadcastTableSize = new DataSize(100, MEGABYTE);
    private boolean colocatedJoinsEnabled = true;
    private boolean groupedExecutionEnabled = true;
    private boolean recoverableGroupedExecutionEnabled;
    private double maxFailedTaskPercentage = 0.3;
    private int maxStageRetries;
    private int concurrentLifespansPerTask;
    private boolean spatialJoinsEnabled = true;
    private boolean fastInequalityJoins = true;
    private TaskSpillingStrategy taskSpillingStrategy = ORDER_BY_CREATE_TIME;
    private boolean queryLimitSpillEnabled;
    private SingleStreamSpillerChoice singleStreamSpillerChoice = SingleStreamSpillerChoice.LOCAL_FILE;
    private String spillerTempStorage = "local";
    private DataSize maxRevocableMemoryPerTask = new DataSize(500, MEGABYTE);
    private JoinReorderingStrategy joinReorderingStrategy = ELIMINATE_CROSS_JOINS;
    private PartialMergePushdownStrategy partialMergePushdownStrategy = PartialMergePushdownStrategy.NONE;
    private int maxReorderedJoins = 9;
    private boolean redistributeWrites = true;
    private boolean scaleWriters;
    private DataSize writerMinSize = new DataSize(32, DataSize.Unit.MEGABYTE);
    private boolean optimizedScaleWriterProducerBuffer;
    private boolean optimizeMetadataQueries;
    private boolean optimizeMetadataQueriesIgnoreStats;
    private boolean optimizeHashGeneration = true;
    private boolean enableIntermediateAggregations;
    private boolean pushTableWriteThroughUnion = true;
    private boolean exchangeCompressionEnabled;
    private boolean exchangeChecksumEnabled;
    private boolean legacyArrayAgg;
    private boolean reduceAggForComplexTypesEnabled = true;
    private boolean legacyLogFunction;
    private boolean groupByUsesEqualTo;
    private boolean legacyTimestamp = true;
    private boolean legacyMapSubscript;
    private boolean legacyRowFieldOrdinalAccess;
    private boolean legacyCharToVarcharCoercion;
    private boolean legacyDateTimestampToVarcharCoercion;
    private boolean optimizeMixedDistinctAggregations;
    private boolean forceSingleNodeOutput = true;
    private boolean pagesIndexEagerCompactionEnabled;
    private boolean distributedSort = true;
    private boolean optimizeJoinsWithEmptySources;
    private boolean logFormattedQueryEnabled;

    private boolean dictionaryAggregation;

    private int re2JDfaStatesLimit = Integer.MAX_VALUE;
    private int re2JDfaRetries = 5;
    private RegexLibrary regexLibrary = JONI;
    private HistogramGroupImplementation histogramGroupImplementation = HistogramGroupImplementation.NEW;
    private ArrayAggGroupImplementation arrayAggGroupImplementation = ArrayAggGroupImplementation.NEW;
    private MultimapAggGroupImplementation multimapAggGroupImplementation = MultimapAggGroupImplementation.NEW;
    private boolean spillEnabled;
    private boolean joinSpillingEnabled = true;
    private boolean aggregationSpillEnabled = true;
    private boolean distinctAggregationSpillEnabled = true;
    private boolean dedupBasedDistinctAggregationSpillEnabled;
    private boolean orderByAggregationSpillEnabled = true;
    private boolean windowSpillEnabled = true;
    private boolean orderBySpillEnabled = true;
    private DataSize aggregationOperatorUnspillMemoryLimit = new DataSize(4, DataSize.Unit.MEGABYTE);
    private List<Path> spillerSpillPaths = ImmutableList.of();
    private int spillerThreads = 4;
    private double spillMaxUsedSpaceThreshold = 0.9;
    private boolean iterativeOptimizerEnabled = true;
    private boolean runtimeOptimizerEnabled;
    private boolean enableStatsCalculator = true;
    private boolean enableStatsCollectionForTemporaryTable;
    private boolean ignoreStatsCalculatorFailures = true;
    private boolean printStatsForNonJoinQuery;
    private boolean defaultFilterFactorEnabled;
    private boolean pushAggregationThroughJoin = true;
    private double memoryRevokingTarget = 0.5;
    private double memoryRevokingThreshold = 0.9;
    private boolean parseDecimalLiteralsAsDouble;
    private boolean useMarkDistinct = true;
    private boolean preferPartialAggregation = true;
    private PartialAggregationStrategy partialAggregationStrategy = PartialAggregationStrategy.ALWAYS;
    private double partialAggregationByteReductionThreshold = 0.5;
    private boolean optimizeTopNRowNumber = true;
    private boolean pushLimitThroughOuterJoin = true;

    private Duration iterativeOptimizerTimeout = new Duration(3, MINUTES); // by default let optimizer wait a long time in case it retrieves some data from ConnectorMetadata
    private boolean enableDynamicFiltering;
    private int dynamicFilteringMaxPerDriverRowCount = 100;
    private DataSize dynamicFilteringMaxPerDriverSize = new DataSize(10, KILOBYTE);
    private int dynamicFilteringRangeRowLimitPerDriver;

    private boolean fragmentResultCachingEnabled;

    private DataSize filterAndProjectMinOutputPageSize = new DataSize(500, KILOBYTE);
    private int filterAndProjectMinOutputPageRowCount = 256;
    private int maxGroupingSets = 2048;
    private boolean legacyUnnestArrayRows;
    private AggregationPartitioningMergingStrategy aggregationPartitioningMergingStrategy = LEGACY;

    private boolean jsonSerdeCodeGenerationEnabled;
    private int maxConcurrentMaterializations = 3;
    private boolean optimizedRepartitioningEnabled;

    private boolean pushdownSubfieldsEnabled;

    private boolean tableWriterMergeOperatorEnabled = true;

    private Duration indexLoaderTimeout = new Duration(20, SECONDS);

    private boolean listBuiltInFunctionsOnly = true;
    private boolean experimentalFunctionsEnabled;
    private boolean useLegacyScheduler = true;
    private boolean optimizeCommonSubExpressions = true;
    private boolean preferDistributedUnion = true;
    private boolean optimizeNullsInJoin;
    private boolean pushdownDereferenceEnabled;
    private boolean inlineSqlFunctions = true;
    private boolean checkAccessControlOnUtilizedColumnsOnly;
    private boolean skipRedundantSort = true;
    private boolean isAllowWindowOrderByLiterals = true;

    private boolean spoolingOutputBufferEnabled;
    private DataSize spoolingOutputBufferThreshold = new DataSize(8, MEGABYTE);
    private String spoolingOutputBufferTempStorage = "local";

    private String warnOnNoTableLayoutFilter = "";

    private PartitioningPrecisionStrategy partitioningPrecisionStrategy = PartitioningPrecisionStrategy.AUTOMATIC;

    private boolean enforceFixedDistributionForOutputOperator;
    private boolean prestoSparkAssignBucketToPartitionForPartitionedTableWriteEnabled;

    private boolean partialResultsEnabled;
    private double partialResultsCompletionRatioThreshold = 0.5;
    private double partialResultsMaxExecutionTimeMultiplier = 2.0;

    private boolean offsetClauseEnabled;
    private boolean materializedViewDataConsistencyEnabled = true;

    private boolean queryOptimizationWithMaterializedViewEnabled;
    private AggregationIfToFilterRewriteStrategy aggregationIfToFilterRewriteStrategy = AggregationIfToFilterRewriteStrategy.DISABLED;
    private boolean verboseRuntimeStatsEnabled;

    public enum PartitioningPrecisionStrategy
    {
        // Let Presto decide when to repartition
        AUTOMATIC,
        // Use exact partitioning until Presto becomes smarter WRT to picking when to repartition
        PREFER_EXACT_PARTITIONING
    }

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

    public enum AggregationPartitioningMergingStrategy
    {
        LEGACY, // merge partition preference with parent but apply current partition preference
        TOP_DOWN, // merge partition preference with parent and apply the merged partition preference
        BOTTOM_UP; // don't merge partition preference and apply current partition preference only

        public boolean isMergingWithParent()
        {
            return this == LEGACY || this == TOP_DOWN;
        }

        public boolean isAdoptingMergedPreference()
        {
            return this == TOP_DOWN;
        }
    }

    public enum TaskSpillingStrategy
    {
        ORDER_BY_CREATE_TIME, // When spilling is triggered, revoke tasks in order of oldest to newest
        ORDER_BY_REVOCABLE_BYTES, // When spilling is triggered, revoke tasks by most allocated revocable memory to least allocated revocable memory
        PER_TASK_MEMORY_THRESHOLD, // Spill any task after it reaches the per task memory threshold defined by experimental.spiller.max-revocable-task-memory
    }

    public enum SingleStreamSpillerChoice
    {
        LOCAL_FILE,
        TEMP_STORAGE
    }

    public enum PartialAggregationStrategy
    {
        ALWAYS, // Always do partial aggregation
        NEVER, // Never do partial aggregation
        AUTOMATIC // Let the optimizer decide for each aggregation
    }

    public enum AggregationIfToFilterRewriteStrategy
    {
        DISABLED,
        FILTER_WITH_IF, // Rewrites AGG(IF(condition, expr)) to AGG(IF(condition, expr)) FILTER (WHERE condition).
        UNWRAP_IF // Rewrites AGG(IF(condition, expr)) to AGG(expr) FILTER (WHERE condition).
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

    @Config("deprecated.legacy-date-timestamp-to-varchar-coercion")
    public FeaturesConfig setLegacyDateTimestampToVarcharCoercion(boolean legacyDateTimestampToVarcharCoercion)
    {
        this.legacyDateTimestampToVarcharCoercion = legacyDateTimestampToVarcharCoercion;
        return this;
    }

    public boolean isLegacyDateTimestampToVarcharCoercion()
    {
        return legacyDateTimestampToVarcharCoercion;
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

    @Config("reduce-agg-for-complex-types-enabled")
    public FeaturesConfig setReduceAggForComplexTypesEnabled(boolean reduceAggForComplexTypesEnabled)
    {
        this.reduceAggForComplexTypesEnabled = reduceAggForComplexTypesEnabled;
        return this;
    }

    public boolean isReduceAggForComplexTypesEnabled()
    {
        return reduceAggForComplexTypesEnabled;
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

    @NotNull
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

    public boolean isGroupedExecutionEnabled()
    {
        return groupedExecutionEnabled;
    }

    @Config("grouped-execution-enabled")
    @ConfigDescription("Use grouped execution when possible")
    public FeaturesConfig setGroupedExecutionEnabled(boolean groupedExecutionEnabled)
    {
        this.groupedExecutionEnabled = groupedExecutionEnabled;
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

    public int getMaxStageRetries()
    {
        return maxStageRetries;
    }

    @Config("max-stage-retries")
    @ConfigDescription("Maximum number of times that stages can be retried")
    public FeaturesConfig setMaxStageRetries(int maxStageRetries)
    {
        this.maxStageRetries = maxStageRetries;
        return this;
    }

    @Min(0)
    public int getConcurrentLifespansPerTask()
    {
        return concurrentLifespansPerTask;
    }

    @Config("concurrent-lifespans-per-task")
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

    public TaskSpillingStrategy getTaskSpillingStrategy()
    {
        return taskSpillingStrategy;
    }

    @Config("experimental.spiller.task-spilling-strategy")
    @ConfigDescription("The strategy used to pick which task to spill when spilling is enabled. See TaskSpillingStrategy.")
    public FeaturesConfig setTaskSpillingStrategy(TaskSpillingStrategy taskSpillingStrategy)
    {
        this.taskSpillingStrategy = taskSpillingStrategy;
        return this;
    }

    @Config("experimental.query-limit-spill-enabled")
    @ConfigDescription("Spill whenever the total memory used by the query (including revocable and non-revocable memory) exceeds maxTotalMemoryPerNode")
    public FeaturesConfig setQueryLimitSpillEnabled(boolean queryLimitSpillEnabled)
    {
        this.queryLimitSpillEnabled = queryLimitSpillEnabled;
        return this;
    }

    public boolean isQueryLimitSpillEnabled()
    {
        return queryLimitSpillEnabled;
    }

    public SingleStreamSpillerChoice getSingleStreamSpillerChoice()
    {
        return singleStreamSpillerChoice;
    }

    @Config(SINGLE_STREAM_SPILLER_CHOICE)
    @ConfigDescription("The SingleStreamSpiller to be used when spilling is enabled.")
    public FeaturesConfig setSingleStreamSpillerChoice(SingleStreamSpillerChoice singleStreamSpillerChoice)
    {
        this.singleStreamSpillerChoice = singleStreamSpillerChoice;
        return this;
    }

    @Config("experimental.spiller.spiller-temp-storage")
    @ConfigDescription("Temp storage used by spiller when single stream spiller is set to TEMP_STORAGE")
    public FeaturesConfig setSpillerTempStorage(String spillerTempStorage)
    {
        this.spillerTempStorage = spillerTempStorage;
        return this;
    }

    public String getSpillerTempStorage()
    {
        return spillerTempStorage;
    }

    public DataSize getMaxRevocableMemoryPerTask()
    {
        return maxRevocableMemoryPerTask;
    }

    @Config("experimental.spiller.max-revocable-task-memory")
    @ConfigDescription("Only used when task-spilling-strategy is PER_TASK_MEMORY_THRESHOLD")
    public FeaturesConfig setMaxRevocableMemoryPerTask(DataSize maxRevocableMemoryPerTask)
    {
        this.maxRevocableMemoryPerTask = maxRevocableMemoryPerTask;
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

    public AggregationPartitioningMergingStrategy getAggregationPartitioningMergingStrategy()
    {
        return aggregationPartitioningMergingStrategy;
    }

    @Config("optimizer.aggregation-partition-merging")
    public FeaturesConfig setAggregationPartitioningMergingStrategy(AggregationPartitioningMergingStrategy aggregationPartitioningMergingStrategy)
    {
        this.aggregationPartitioningMergingStrategy = requireNonNull(aggregationPartitioningMergingStrategy, "aggregationPartitioningMergingStrategy is null");
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

    public boolean isOptimizedScaleWriterProducerBuffer()
    {
        return optimizedScaleWriterProducerBuffer;
    }

    @Config("optimized-scale-writer-producer-buffer")
    public FeaturesConfig setOptimizedScaleWriterProducerBuffer(boolean optimizedScaleWriterProducerBuffer)
    {
        this.optimizedScaleWriterProducerBuffer = optimizedScaleWriterProducerBuffer;
        return this;
    }

    public boolean isOptimizeMetadataQueries()
    {
        return optimizeMetadataQueries;
    }

    @Config("optimizer.optimize-metadata-queries")
    @ConfigDescription("Enable optimization for metadata queries if the resulting partitions are not empty according to the partition stats")
    public FeaturesConfig setOptimizeMetadataQueries(boolean optimizeMetadataQueries)
    {
        this.optimizeMetadataQueries = optimizeMetadataQueries;
        return this;
    }

    public boolean isOptimizeMetadataQueriesIgnoreStats()
    {
        return optimizeMetadataQueriesIgnoreStats;
    }

    @Config("optimizer.optimize-metadata-queries-ignore-stats")
    @ConfigDescription("Enable optimization for metadata queries. Note if metadata entry has empty data, the result might be different (e.g. empty Hive partition)")
    public FeaturesConfig setOptimizeMetadataQueriesIgnoreStats(boolean optimizeMetadataQueriesIgnoreStats)
    {
        this.optimizeMetadataQueriesIgnoreStats = optimizeMetadataQueriesIgnoreStats;
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

    public PartialAggregationStrategy getPartialAggregationStrategy()
    {
        return partialAggregationStrategy;
    }

    @Config("optimizer.partial-aggregation-strategy")
    public FeaturesConfig setPartialAggregationStrategy(PartialAggregationStrategy partialAggregationStrategy)
    {
        this.partialAggregationStrategy = partialAggregationStrategy;
        return this;
    }

    public double getPartialAggregationByteReductionThreshold()
    {
        return partialAggregationByteReductionThreshold;
    }

    @Config("optimizer.partial-aggregation-byte-reduction-threshold")
    public FeaturesConfig setPartialAggregationByteReductionThreshold(double partialAggregationByteReductionThreshold)
    {
        this.partialAggregationByteReductionThreshold = partialAggregationByteReductionThreshold;
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

    public boolean isJoinSpillingEnabled()
    {
        return joinSpillingEnabled;
    }

    @Config(JOIN_SPILL_ENABLED)
    public FeaturesConfig setJoinSpillingEnabled(boolean joinSpillingEnabled)
    {
        this.joinSpillingEnabled = joinSpillingEnabled;
        return this;
    }

    @Config("experimental.aggregation-spill-enabled")
    @ConfigDescription("Spill aggregations if spill is enabled")
    public FeaturesConfig setAggregationSpillEnabled(boolean aggregationSpillEnabled)
    {
        this.aggregationSpillEnabled = aggregationSpillEnabled;
        return this;
    }

    public boolean isAggregationSpillEnabled()
    {
        return aggregationSpillEnabled;
    }

    @Config("experimental.distinct-aggregation-spill-enabled")
    @ConfigDescription("Spill distinct aggregations if aggregation spill is enabled")
    public FeaturesConfig setDistinctAggregationSpillEnabled(boolean distinctAggregationSpillEnabled)
    {
        this.distinctAggregationSpillEnabled = distinctAggregationSpillEnabled;
        return this;
    }

    public boolean isDistinctAggregationSpillEnabled()
    {
        return distinctAggregationSpillEnabled;
    }

    @Config("experimental.dedup-based-distinct-aggregation-spill-enabled")
    @ConfigDescription("Dedup input data for Distinct Aggregates before spilling")
    public FeaturesConfig setDedupBasedDistinctAggregationSpillEnabled(boolean dedupBasedDistinctAggregationSpillEnabled)
    {
        this.dedupBasedDistinctAggregationSpillEnabled = dedupBasedDistinctAggregationSpillEnabled;
        return this;
    }

    public boolean isDedupBasedDistinctAggregationSpillEnabled()
    {
        return dedupBasedDistinctAggregationSpillEnabled;
    }

    @Config("experimental.order-by-aggregation-spill-enabled")
    @ConfigDescription("Spill order-by aggregations if aggregation spill is enabled")
    public FeaturesConfig setOrderByAggregationSpillEnabled(boolean orderByAggregationSpillEnabled)
    {
        this.orderByAggregationSpillEnabled = orderByAggregationSpillEnabled;
        return this;
    }

    public boolean isOrderByAggregationSpillEnabled()
    {
        return orderByAggregationSpillEnabled;
    }

    @Config("experimental.window-spill-enabled")
    @ConfigDescription("Enable Window Operator Spilling if spill is enabled")
    public FeaturesConfig setWindowSpillEnabled(boolean windowSpillEnabled)
    {
        this.windowSpillEnabled = windowSpillEnabled;
        return this;
    }

    public boolean isWindowSpillEnabled()
    {
        return windowSpillEnabled;
    }

    @Config("experimental.order-by-spill-enabled")
    @ConfigDescription("Enable Order-by Operator Spilling if spill is enabled")
    public FeaturesConfig setOrderBySpillEnabled(boolean orderBySpillEnabled)
    {
        this.orderBySpillEnabled = orderBySpillEnabled;
        return this;
    }

    public boolean isOrderBySpillEnabled()
    {
        return orderBySpillEnabled;
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

    public boolean isRuntimeOptimizerEnabled()
    {
        return runtimeOptimizerEnabled;
    }

    @Config("experimental.runtime-optimizer-enabled")
    public FeaturesConfig setRuntimeOptimizerEnabled(boolean value)
    {
        this.runtimeOptimizerEnabled = value;
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

    public boolean isEnableStatsCollectionForTemporaryTable()
    {
        return enableStatsCollectionForTemporaryTable;
    }

    @Config("experimental.enable-stats-calculator")
    public FeaturesConfig setEnableStatsCalculator(boolean enableStatsCalculator)
    {
        this.enableStatsCalculator = enableStatsCalculator;
        return this;
    }

    @Config("experimental.enable-stats-collection-for-temporary-table")
    public FeaturesConfig setEnableStatsCollectionForTemporaryTable(boolean enableStatsCollectionForTemporaryTable)
    {
        this.enableStatsCollectionForTemporaryTable = enableStatsCollectionForTemporaryTable;
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

    @AssertTrue(message = SPILLER_SPILL_PATH + " must be configured when " + SPILL_ENABLED + " is set to true and " + SINGLE_STREAM_SPILLER_CHOICE + " is set to file")
    public boolean isSpillerSpillPathsConfiguredIfSpillEnabled()
    {
        return !isSpillEnabled() || !spillerSpillPaths.isEmpty() || singleStreamSpillerChoice != SingleStreamSpillerChoice.LOCAL_FILE;
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

    public boolean isEnableDynamicFiltering()
    {
        return enableDynamicFiltering;
    }

    @Config("experimental.enable-dynamic-filtering")
    public FeaturesConfig setEnableDynamicFiltering(boolean value)
    {
        this.enableDynamicFiltering = value;
        return this;
    }

    public int getDynamicFilteringMaxPerDriverRowCount()
    {
        return dynamicFilteringMaxPerDriverRowCount;
    }

    @Config("experimental.dynamic-filtering-max-per-driver-row-count")
    public FeaturesConfig setDynamicFilteringMaxPerDriverRowCount(int dynamicFilteringMaxPerDriverRowCount)
    {
        this.dynamicFilteringMaxPerDriverRowCount = dynamicFilteringMaxPerDriverRowCount;
        return this;
    }

    @MaxDataSize("1MB")
    public DataSize getDynamicFilteringMaxPerDriverSize()
    {
        return dynamicFilteringMaxPerDriverSize;
    }

    @Config("experimental.dynamic-filtering-max-per-driver-size")
    public FeaturesConfig setDynamicFilteringMaxPerDriverSize(DataSize dynamicFilteringMaxPerDriverSize)
    {
        this.dynamicFilteringMaxPerDriverSize = dynamicFilteringMaxPerDriverSize;
        return this;
    }

    public int getDynamicFilteringRangeRowLimitPerDriver()
    {
        return dynamicFilteringRangeRowLimitPerDriver;
    }

    @Config("experimental.dynamic-filtering-range-row-limit-per-driver")
    @ConfigDescription("Maximum number of build-side rows per driver up to which min and max values will be collected for dynamic filtering")
    public FeaturesConfig setDynamicFilteringRangeRowLimitPerDriver(int dynamicFilteringRangeRowLimitPerDriver)
    {
        this.dynamicFilteringRangeRowLimitPerDriver = dynamicFilteringRangeRowLimitPerDriver;
        return this;
    }

    public boolean isFragmentResultCachingEnabled()
    {
        return fragmentResultCachingEnabled;
    }

    @Config("experimental.fragment-result-caching-enabled")
    @ConfigDescription("Enable fragment result caching and read/write leaf fragment result pages from/to cache when applicable")
    public FeaturesConfig setFragmentResultCachingEnabled(boolean fragmentResultCachingEnabled)
    {
        this.fragmentResultCachingEnabled = fragmentResultCachingEnabled;
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

    public boolean isExchangeChecksumEnabled()
    {
        return exchangeChecksumEnabled;
    }

    @Config("exchange.compression-enabled")
    public FeaturesConfig setExchangeCompressionEnabled(boolean exchangeCompressionEnabled)
    {
        this.exchangeCompressionEnabled = exchangeCompressionEnabled;
        return this;
    }

    @Config("exchange.checksum-enabled")
    public FeaturesConfig setExchangeChecksumEnabled(boolean exchangeChecksumEnabled)
    {
        this.exchangeChecksumEnabled = exchangeChecksumEnabled;
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
    @ConfigDescription("The maximum number of materializing plan sections that can run concurrently")
    public FeaturesConfig setMaxConcurrentMaterializations(int maxConcurrentMaterializations)
    {
        this.maxConcurrentMaterializations = maxConcurrentMaterializations;
        return this;
    }

    @Min(1)
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

    @Config("experimental.pushdown-dereference-enabled")
    @ConfigDescription("Experimental: enable dereference pushdown")
    public FeaturesConfig setPushdownDereferenceEnabled(boolean pushdownDereferenceEnabled)
    {
        this.pushdownDereferenceEnabled = pushdownDereferenceEnabled;
        return this;
    }

    public boolean isPushdownDereferenceEnabled()
    {
        return pushdownDereferenceEnabled;
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

    @Config("index-loader-timeout")
    @ConfigDescription("Time limit for loading indexes for index joins")
    public FeaturesConfig setIndexLoaderTimeout(Duration indexLoaderTimeout)
    {
        this.indexLoaderTimeout = indexLoaderTimeout;
        return this;
    }

    public Duration getIndexLoaderTimeout()
    {
        return this.indexLoaderTimeout;
    }

    public boolean isOptimizedRepartitioningEnabled()
    {
        return optimizedRepartitioningEnabled;
    }

    @Config("experimental.optimized-repartitioning")
    @ConfigDescription("Experimental: Use optimized repartitioning")
    public FeaturesConfig setOptimizedRepartitioningEnabled(boolean optimizedRepartitioningEnabled)
    {
        this.optimizedRepartitioningEnabled = optimizedRepartitioningEnabled;
        return this;
    }

    public boolean isListBuiltInFunctionsOnly()
    {
        return listBuiltInFunctionsOnly;
    }

    @Config("list-built-in-functions-only")
    public FeaturesConfig setListBuiltInFunctionsOnly(boolean listBuiltInFunctionsOnly)
    {
        this.listBuiltInFunctionsOnly = listBuiltInFunctionsOnly;
        return this;
    }

    public PartitioningPrecisionStrategy getPartitioningPrecisionStrategy()
    {
        return partitioningPrecisionStrategy;
    }

    @Config("partitioning-precision-strategy")
    @ConfigDescription("Set strategy used to determine whether to repartition (AUTOMATIC, PREFER_EXACT)")
    public FeaturesConfig setPartitioningPrecisionStrategy(PartitioningPrecisionStrategy partitioningPrecisionStrategy)
    {
        this.partitioningPrecisionStrategy = partitioningPrecisionStrategy;
        return this;
    }

    public boolean isExperimentalFunctionsEnabled()
    {
        return experimentalFunctionsEnabled;
    }

    @Config("experimental-functions-enabled")
    public FeaturesConfig setExperimentalFunctionsEnabled(boolean experimentalFunctionsEnabled)
    {
        this.experimentalFunctionsEnabled = experimentalFunctionsEnabled;
        return this;
    }

    public boolean isUseLegacyScheduler()
    {
        return useLegacyScheduler;
    }

    @Config("use-legacy-scheduler")
    @ConfigDescription("Use the version of the scheduler before refactorings for section retries")
    public FeaturesConfig setUseLegacyScheduler(boolean useLegacyScheduler)
    {
        this.useLegacyScheduler = useLegacyScheduler;
        return this;
    }

    public boolean isOptimizeCommonSubExpressions()
    {
        return optimizeCommonSubExpressions;
    }

    @Config("optimize-common-sub-expressions")
    @ConfigDescription("Extract and compute common sub expression in projections")
    public FeaturesConfig setOptimizeCommonSubExpressions(boolean optimizeCommonSubExpressions)
    {
        this.optimizeCommonSubExpressions = optimizeCommonSubExpressions;
        return this;
    }

    public boolean isPreferDistributedUnion()
    {
        return preferDistributedUnion;
    }

    @Config("prefer-distributed-union")
    public FeaturesConfig setPreferDistributedUnion(boolean preferDistributedUnion)
    {
        this.preferDistributedUnion = preferDistributedUnion;
        return this;
    }

    public boolean isOptimizeNullsInJoin()
    {
        return optimizeNullsInJoin;
    }

    @Config("optimize-nulls-in-join")
    public FeaturesConfig setOptimizeNullsInJoin(boolean optimizeNullsInJoin)
    {
        this.optimizeNullsInJoin = optimizeNullsInJoin;
        return this;
    }

    public String getWarnOnNoTableLayoutFilter()
    {
        return warnOnNoTableLayoutFilter;
    }

    @Config("warn-on-no-table-layout-filter")
    public FeaturesConfig setWarnOnNoTableLayoutFilter(String warnOnNoTableLayoutFilter)
    {
        this.warnOnNoTableLayoutFilter = warnOnNoTableLayoutFilter;
        return this;
    }

    public boolean isInlineSqlFunctions()
    {
        return inlineSqlFunctions;
    }

    @Config("inline-sql-functions")
    public FeaturesConfig setInlineSqlFunctions(boolean inlineSqlFunctions)
    {
        this.inlineSqlFunctions = inlineSqlFunctions;
        return this;
    }

    public boolean isCheckAccessControlOnUtilizedColumnsOnly()
    {
        return checkAccessControlOnUtilizedColumnsOnly;
    }

    @Config("check-access-control-on-utilized-columns-only")
    public FeaturesConfig setCheckAccessControlOnUtilizedColumnsOnly(boolean checkAccessControlOnUtilizedColumnsOnly)
    {
        this.checkAccessControlOnUtilizedColumnsOnly = checkAccessControlOnUtilizedColumnsOnly;
        return this;
    }

    public boolean isSkipRedundantSort()
    {
        return skipRedundantSort;
    }

    @Config("optimizer.skip-redundant-sort")
    public FeaturesConfig setSkipRedundantSort(boolean value)
    {
        this.skipRedundantSort = value;
        return this;
    }

    public boolean isAllowWindowOrderByLiterals()
    {
        return isAllowWindowOrderByLiterals;
    }

    @Config("is-allow-window-order-by-literals")
    public FeaturesConfig setAllowWindowOrderByLiterals(boolean value)
    {
        this.isAllowWindowOrderByLiterals = value;
        return this;
    }

    public boolean isEnforceFixedDistributionForOutputOperator()
    {
        return enforceFixedDistributionForOutputOperator;
    }

    @Config("enforce-fixed-distribution-for-output-operator")
    public FeaturesConfig setEnforceFixedDistributionForOutputOperator(boolean enforceFixedDistributionForOutputOperator)
    {
        this.enforceFixedDistributionForOutputOperator = enforceFixedDistributionForOutputOperator;
        return this;
    }

    public boolean isEmptyJoinOptimization()
    {
        return optimizeJoinsWithEmptySources;
    }

    @Config("optimizer.optimize-joins-with-empty-sources")
    public FeaturesConfig setEmptyJoinOptimization(boolean value)
    {
        this.optimizeJoinsWithEmptySources = value;
        return this;
    }

    public boolean isLogFormattedQueryEnabled()
    {
        return logFormattedQueryEnabled;
    }

    @Config("log-formatted-query-enabled")
    @ConfigDescription("Log formatted prepared query instead of raw query when enabled")
    public FeaturesConfig setLogFormattedQueryEnabled(boolean logFormattedQueryEnabled)
    {
        this.logFormattedQueryEnabled = logFormattedQueryEnabled;
        return this;
    }

    public boolean isSpoolingOutputBufferEnabled()
    {
        return spoolingOutputBufferEnabled;
    }

    @Config("spooling-output-buffer-enabled")
    public FeaturesConfig setSpoolingOutputBufferEnabled(boolean value)
    {
        this.spoolingOutputBufferEnabled = value;
        return this;
    }

    public DataSize getSpoolingOutputBufferThreshold()
    {
        return spoolingOutputBufferThreshold;
    }

    @Config("spooling-output-buffer-threshold")
    public FeaturesConfig setSpoolingOutputBufferThreshold(DataSize spoolingOutputBufferThreshold)
    {
        this.spoolingOutputBufferThreshold = spoolingOutputBufferThreshold;
        return this;
    }

    public String getSpoolingOutputBufferTempStorage()
    {
        return spoolingOutputBufferTempStorage;
    }

    @Config("spooling-output-buffer-temp-storage")
    public FeaturesConfig setSpoolingOutputBufferTempStorage(String spoolingOutputBufferTempStorage)
    {
        this.spoolingOutputBufferTempStorage = spoolingOutputBufferTempStorage;
        return this;
    }

    public boolean isPrestoSparkAssignBucketToPartitionForPartitionedTableWriteEnabled()
    {
        return prestoSparkAssignBucketToPartitionForPartitionedTableWriteEnabled;
    }

    @Config("spark.assign-bucket-to-partition-for-partitioned-table-write-enabled")
    public FeaturesConfig setPrestoSparkAssignBucketToPartitionForPartitionedTableWriteEnabled(boolean prestoSparkAssignBucketToPartitionForPartitionedTableWriteEnabled)
    {
        this.prestoSparkAssignBucketToPartitionForPartitionedTableWriteEnabled = prestoSparkAssignBucketToPartitionForPartitionedTableWriteEnabled;
        return this;
    }

    public boolean isPartialResultsEnabled()
    {
        return partialResultsEnabled;
    }

    @Config("partial-results-enabled")
    @ConfigDescription("Enable returning partial results. Please note that queries might not read all the data when this is enabled.")
    public FeaturesConfig setPartialResultsEnabled(boolean partialResultsEnabled)
    {
        this.partialResultsEnabled = partialResultsEnabled;
        return this;
    }

    public double getPartialResultsCompletionRatioThreshold()
    {
        return partialResultsCompletionRatioThreshold;
    }

    @Config("partial-results-completion-ratio-threshold")
    @ConfigDescription("Minimum query completion ratio threshold for partial results")
    public FeaturesConfig setPartialResultsCompletionRatioThreshold(double partialResultsCompletionRatioThreshold)
    {
        this.partialResultsCompletionRatioThreshold = partialResultsCompletionRatioThreshold;
        return this;
    }

    public double getPartialResultsMaxExecutionTimeMultiplier()
    {
        return partialResultsMaxExecutionTimeMultiplier;
    }

    @Config("partial-results-max-execution-time-multiplier")
    @ConfigDescription("This value is multiplied by the time taken to reach the completion ratio threshold and is set as max task end time")
    public FeaturesConfig setPartialResultsMaxExecutionTimeMultiplier(double partialResultsMaxExecutionTimeMultiplier)
    {
        this.partialResultsMaxExecutionTimeMultiplier = partialResultsMaxExecutionTimeMultiplier;
        return this;
    }

    public boolean isOffsetClauseEnabled()
    {
        return offsetClauseEnabled;
    }

    @Config("offset-clause-enabled")
    @ConfigDescription("Enable support for OFFSET clause")
    public FeaturesConfig setOffsetClauseEnabled(boolean offsetClauseEnabled)
    {
        this.offsetClauseEnabled = offsetClauseEnabled;
        return this;
    }

    public boolean isMaterializedViewDataConsistencyEnabled()
    {
        return materializedViewDataConsistencyEnabled;
    }

    @Config("materialized-view-data-consistency-enabled")
    @ConfigDescription("When enabled and reading from materialized view, partition stitching is applied to achieve data consistency")
    public FeaturesConfig setMaterializedViewDataConsistencyEnabled(boolean materializedViewDataConsistencyEnabled)
    {
        this.materializedViewDataConsistencyEnabled = materializedViewDataConsistencyEnabled;
        return this;
    }

    public boolean isQueryOptimizationWithMaterializedViewEnabled()
    {
        return queryOptimizationWithMaterializedViewEnabled;
    }

    @Config("query-optimization-with-materialized-view-enabled")
    @ConfigDescription("Experimental: Enable query optimization using materialized view. It only supports simple query formats for now.")
    public FeaturesConfig setQueryOptimizationWithMaterializedViewEnabled(boolean value)
    {
        this.queryOptimizationWithMaterializedViewEnabled = value;
        return this;
    }

    public boolean isVerboseRuntimeStatsEnabled()
    {
        return verboseRuntimeStatsEnabled;
    }

    @Config("verbose-runtime-stats-enabled")
    @ConfigDescription("Enable logging all runtime stats.")
    public FeaturesConfig setVerboseRuntimeStatsEnabled(boolean value)
    {
        this.verboseRuntimeStatsEnabled = value;
        return this;
    }

    public AggregationIfToFilterRewriteStrategy getAggregationIfToFilterRewriteStrategy()
    {
        return aggregationIfToFilterRewriteStrategy;
    }

    @Config("optimizer.aggregation-if-to-filter-rewrite-strategy")
    @ConfigDescription("Set the strategy used to rewrite AGG IF to AGG FILTER")
    public FeaturesConfig setAggregationIfToFilterRewriteStrategy(AggregationIfToFilterRewriteStrategy strategy)
    {
        this.aggregationIfToFilterRewriteStrategy = strategy;
        return this;
    }
}
