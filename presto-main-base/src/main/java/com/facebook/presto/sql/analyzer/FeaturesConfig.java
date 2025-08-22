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
import com.facebook.airlift.configuration.LegacyConfig;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MaxDataSize;
import com.facebook.presto.CompressionCodec;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.sql.tree.CreateView;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.facebook.airlift.units.DataSize.Unit.KILOBYTE;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.AggregationPartitioningMergingStrategy.LEGACY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinNotNullInferenceStrategy.NONE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.TaskSpillingStrategy.ORDER_BY_CREATE_TIME;
import static com.facebook.presto.sql.expressions.ExpressionOptimizerManager.DEFAULT_EXPRESSION_OPTIMIZER_NAME;
import static com.facebook.presto.sql.tree.CreateView.Security.DEFINER;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
        "deprecated.legacy-join-using",
        "use-legacy-scheduler",
        "max-stage-retries",
        "deprecated.group-by-uses-equal",
        "experimental.table-writer-merge-operator-enabled"})
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
    private JoinDistributionType joinDistributionType = JoinDistributionType.AUTOMATIC;
    private DataSize joinMaxBroadcastTableSize = new DataSize(100, MEGABYTE);
    private boolean sizeBasedJoinDistributionTypeEnabled = true;
    private boolean colocatedJoinsEnabled = true;
    private boolean groupedExecutionEnabled = true;
    private boolean recoverableGroupedExecutionEnabled;
    private double maxFailedTaskPercentage = 0.3;
    private int concurrentLifespansPerTask;
    private boolean spatialJoinsEnabled = true;
    private boolean fastInequalityJoins = true;
    private TaskSpillingStrategy taskSpillingStrategy = ORDER_BY_CREATE_TIME;
    private boolean queryLimitSpillEnabled;
    private SingleStreamSpillerChoice singleStreamSpillerChoice = SingleStreamSpillerChoice.LOCAL_FILE;
    private String spillerTempStorage = "local";
    private DataSize maxRevocableMemoryPerTask = new DataSize(500, MEGABYTE);
    private JoinReorderingStrategy joinReorderingStrategy = JoinReorderingStrategy.AUTOMATIC;
    private PartialMergePushdownStrategy partialMergePushdownStrategy = PartialMergePushdownStrategy.NONE;
    private CteMaterializationStrategy cteMaterializationStrategy = CteMaterializationStrategy.NONE;
    private boolean cteFilterAndProjectionPushdownEnabled = true;
    private int cteHeuristicReplicationThreshold = 4;
    private int maxReorderedJoins = 9;
    private boolean useHistoryBasedPlanStatistics;
    private boolean trackHistoryBasedPlanStatistics;
    private boolean trackHistoryStatsFromFailedQuery = true;
    private boolean usePerfectlyConsistentHistories;
    private int historyCanonicalPlanNodeLimit = 1000;
    private Duration historyBasedOptimizerTimeout = new Duration(10, SECONDS);
    private String historyBasedOptimizerPlanCanonicalizationStrategies = "IGNORE_SAFE_CONSTANTS";
    private boolean logPlansUsedInHistoryBasedOptimizer;
    private boolean enforceTimeoutForHBOQueryRegistration;
    private boolean historyBasedOptimizerEstimateSizeUsingVariables;
    private boolean redistributeWrites;
    private boolean scaleWriters = true;
    private DataSize writerMinSize = new DataSize(32, MEGABYTE);
    private boolean optimizedScaleWriterProducerBuffer = true;
    private boolean optimizeMetadataQueries;
    private boolean optimizeMetadataQueriesIgnoreStats;
    private int optimizeMetadataQueriesCallThreshold = 100;
    private boolean optimizeHashGeneration = true;
    private boolean enableIntermediateAggregations;
    private boolean optimizeCaseExpressionPredicate;
    private boolean pushTableWriteThroughUnion = true;
    private CompressionCodec exchangeCompressionCodec = CompressionCodec.NONE;
    private boolean exchangeChecksumEnabled;
    private boolean optimizeMixedDistinctAggregations;
    private boolean forceSingleNodeOutput = true;
    private boolean pagesIndexEagerCompactionEnabled;
    private boolean distributedSort = true;
    private boolean optimizeJoinsWithEmptySources;
    private boolean logFormattedQueryEnabled;
    private boolean logInvokedFunctionNamesEnabled;

    private boolean dictionaryAggregation;
    private boolean spillEnabled;
    private boolean joinSpillingEnabled = true;
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
    private boolean enhancedCteSchedulingEnabled = true;
    // Give a default 10% selectivity coefficient factor to avoid hitting unknown stats in join stats estimates
    // which could result in syntactic join order. Set it to 0 to disable this feature
    private double defaultJoinSelectivityCoefficient;
    private double defaultWriterReplicationCoefficient = 3;
    private boolean pushAggregationThroughJoin = true;
    private double memoryRevokingTarget = 0.5;
    private double memoryRevokingThreshold = 0.9;
    private boolean useMarkDistinct = true;
    private boolean exploitConstraints = true;
    private boolean preferPartialAggregation = true;
    private PartialAggregationStrategy partialAggregationStrategy = PartialAggregationStrategy.ALWAYS;
    private double partialAggregationByteReductionThreshold = 0.5;
    private boolean adaptivePartialAggregationEnabled;
    private double adaptivePartialAggregationRowsReductionRatioThreshold = 0.8;
    private boolean optimizeTopNRowNumber = true;
    private boolean pushLimitThroughOuterJoin = true;
    private boolean optimizeConstantGroupingKeys = true;

    private Duration iterativeOptimizerTimeout = new Duration(3, MINUTES); // by default let optimizer wait a long time in case it retrieves some data from ConnectorMetadata
    private Duration queryAnalyzerTimeout = new Duration(3, MINUTES);
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
    private boolean pushdownSubfieldsFromLambdaEnabled;

    private Duration indexLoaderTimeout = new Duration(20, SECONDS);

    private boolean listBuiltInFunctionsOnly = true;
    private boolean experimentalFunctionsEnabled;
    private boolean optimizeCommonSubExpressions = true;
    private boolean preferDistributedUnion = true;
    private boolean optimizeNullsInJoin;
    private boolean optimizePayloadJoins;
    private boolean confidenceBasedBroadcastEnabled;
    private boolean retryQueryWithHistoryBasedOptimizationEnabled;
    private boolean treatLowConfidenceZeroEstimationAsUnknownEnabled;
    private boolean pushdownDereferenceEnabled;
    private boolean inlineSqlFunctions = true;
    private boolean checkAccessControlOnUtilizedColumnsOnly = true;
    private boolean checkAccessControlWithSubfields;
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
    private boolean materializedViewPartitionFilteringEnabled = true;
    private boolean queryOptimizationWithMaterializedViewEnabled;

    private AggregationIfToFilterRewriteStrategy aggregationIfToFilterRewriteStrategy = AggregationIfToFilterRewriteStrategy.DISABLED;
    private String analyzerType = "BUILTIN";
    private boolean verboseRuntimeStatsEnabled;
    private boolean verboseOptimizerInfoEnabled;

    private boolean streamingForPartialAggregationEnabled;
    private boolean preferMergeJoinForSortedInputs;
    private boolean preferSortMergeJoin;
    private boolean segmentedAggregationEnabled;

    private int maxStageCountForEagerScheduling = 25;
    private boolean quickDistinctLimitEnabled;

    private double hyperloglogStandardErrorWarningThreshold = 0.004;

    private boolean pushRemoteExchangeThroughGroupId;
    private boolean isOptimizeMultipleApproxPercentileOnSameFieldEnabled = true;
    private boolean nativeExecutionEnabled;
    private boolean disableTimeStampWithTimeZoneForNative;
    private boolean disableIPAddressForNative;
    private String nativeExecutionExecutablePath = "./presto_server";
    private String nativeExecutionProgramArguments = "";
    private boolean nativeExecutionProcessReuseEnabled = true;
    private boolean nativeEnforceJoinBuildInputPartition = true;
    private boolean randomizeOuterJoinNullKey;
    private RandomizeOuterJoinNullKeyStrategy randomizeOuterJoinNullKeyStrategy = RandomizeOuterJoinNullKeyStrategy.DISABLED;
    private ShardedJoinStrategy shardedJoinStrategy = ShardedJoinStrategy.DISABLED;
    private int joinShardCount = 100;
    private boolean isOptimizeConditionalAggregationEnabled;
    private boolean isRemoveRedundantDistinctAggregationEnabled = true;
    private boolean inPredicatesAsInnerJoinsEnabled;
    private double pushAggregationBelowJoinByteReductionThreshold = 1;
    private boolean prefilterForGroupbyLimit;
    private boolean isOptimizeJoinProbeWithEmptyBuildRuntime;
    private boolean useDefaultsForCorrelatedAggregationPushdownThroughOuterJoins = true;
    private boolean mergeDuplicateAggregationsEnabled = true;
    private boolean mergeAggregationsWithAndWithoutFilter;
    private boolean simplifyPlanWithEmptyInput = true;
    private PushDownFilterThroughCrossJoinStrategy pushDownFilterExpressionEvaluationThroughCrossJoin = PushDownFilterThroughCrossJoinStrategy.REWRITTEN_TO_INNER_JOIN;
    private boolean rewriteCrossJoinWithOrFilterToInnerJoin = true;
    private boolean rewriteCrossJoinWithArrayContainsFilterToInnerJoin = true;
    private LeftJoinArrayContainsToInnerJoinStrategy leftJoinWithArrayContainsToEquiJoinStrategy = LeftJoinArrayContainsToInnerJoinStrategy.DISABLED;
    private boolean rewriteCrossJoinWithArrayNotContainsFilterToAntiJoin = true;
    private JoinNotNullInferenceStrategy joinNotNullInferenceStrategy = NONE;
    private boolean leftJoinNullFilterToSemiJoin = true;
    private boolean broadcastJoinWithSmallBuildUnknownProbe;
    private boolean addPartialNodeForRowNumberWithLimit = true;
    private boolean inferInequalityPredicates;
    private boolean pullUpExpressionFromLambda;
    private boolean rewriteConstantArrayContainsToIn;
    private boolean rewriteExpressionWithConstantVariable = true;
    private boolean optimizeConditionalApproxDistinct = true;

    private boolean preProcessMetadataCalls;
    private boolean handleComplexEquiJoins;
    private boolean useHBOForScaledWriters;

    private boolean usePartialAggregationHistory;

    private boolean trackPartialAggregationHistory = true;

    private boolean removeRedundantCastToVarcharInJoin = true;
    private boolean skipHashGenerationForJoinWithTableScanInput;
    private boolean generateDomainFilters;
    private boolean printEstimatedStatsFromCache;
    private boolean removeCrossJoinWithSingleConstantRow = true;
    private CreateView.Security defaultViewSecurityMode = DEFINER;
    private boolean useHistograms;

    private boolean isInlineProjectionsOnValuesEnabled;
    private boolean includeValuesNodeInConnectorOptimizer = true;

    private boolean eagerPlanValidationEnabled;

    private boolean setExcludeInvalidWorkerSessionProperties;
    private int eagerPlanValidationThreadPoolSize = 20;
    private boolean innerJoinPushdownEnabled;
    private boolean inEqualityJoinPushdownEnabled;
    private boolean rewriteMinMaxByToTopNEnabled;
    private boolean broadcastSemiJoinForDelete = true;

    private boolean prestoSparkExecutionEnvironment;
    private boolean singleNodeExecutionEnabled;
    private boolean nativeExecutionScaleWritersThreadsEnabled;
    private boolean nativeExecutionTypeRewriteEnabled;
    private String expressionOptimizerName = DEFAULT_EXPRESSION_OPTIMIZER_NAME;
    private boolean addExchangeBelowPartialAggregationOverGroupId;
    private boolean addDistinctBelowSemiJoinBuild;
    private boolean pushdownSubfieldForMapFunctions = true;
    private long maxSerializableObjectSize = 1000;

    private boolean builtInSidecarFunctionsEnabled;

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

    public enum CteMaterializationStrategy
    {
        ALL, // Materialize all CTES
        NONE, // Materialize no CTES

        // HEURISTIC algorithm greedily prioritizes the earliest parent CTE that meets the heuristic criteria for materialization
        HEURISTIC, // Materialize CTES occuring  >= CTE_HEURISTIC_REPLICATION_THRESHOLD
        HEURISTIC_COMPLEX_QUERIES_ONLY // Materialize CTES occuring >= CTE_HEURISTIC_REPLICATION_THRESHOLD and having a join or an aggregate
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
        UNWRAP_IF_SAFE, // Rewrites AGG(IF(condition, expr)) to AGG(expr) FILTER (WHERE condition) if it is safe to do so.
        UNWRAP_IF // Rewrites AGG(IF(condition, expr)) to AGG(expr) FILTER (WHERE condition).
    }

    public enum RandomizeOuterJoinNullKeyStrategy
    {
        DISABLED,
        KEY_FROM_OUTER_JOIN, // Enabled only when join keys are from output of outer joins
        COST_BASED,
        ALWAYS
    }

    public enum ShardedJoinStrategy
    {
        DISABLED,
        COST_BASED,
        ALWAYS
    }

    public enum PushDownFilterThroughCrossJoinStrategy
    {
        DISABLED,
        REWRITTEN_TO_INNER_JOIN, // Enabled only when the change can enable rewriting cross join to inner join
        ALWAYS
    }

    /**
     * Strategy used in {@link com.facebook.presto.sql.planner.iterative.rule.AddNotNullFiltersToJoinNode.ExtractInferredNotNullVariablesVisitor}
     * to infer NOT NULL predicates
     */
    public enum JoinNotNullInferenceStrategy
    {
        /**
         * No NOT NULL predicates are inferred
         */
        NONE,

        /**
         * Use the equi-join condition and the join filter for inferring NOT NULL predicates.
         * For the join filter, try to map functions to a standard operator in {@link OperatorType}.
         * If this mapping is successful, use {@link OperatorType#isCalledOnNullInput()}
         * to check if this function can operate on NULL inputs.
         */
        INFER_FROM_STANDARD_OPERATORS,

        /**
         * Use the equi-join condition and the join filter for inferring NOT NULL predicates.
         * For the join filter, use the function's  {@link FunctionMetadata#isCalledOnNullInput()} value
         * to check if this function can operate on NULL inputs
         */
        USE_FUNCTION_METADATA
    }

    // TODO: Implement cost based strategy
    public enum LeftJoinArrayContainsToInnerJoinStrategy
    {
        DISABLED,
        ALWAYS_ENABLED
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

    public CteMaterializationStrategy getCteMaterializationStrategy()
    {
        return cteMaterializationStrategy;
    }

    @Config("cte-materialization-strategy")
    @ConfigDescription("Set strategy used to determine whether to materialize ctes (ALL, NONE, HEURISTIC, HEURISTIC_COMPLEX_QUERIES_ONLY)")
    public FeaturesConfig setCteMaterializationStrategy(CteMaterializationStrategy cteMaterializationStrategy)
    {
        this.cteMaterializationStrategy = cteMaterializationStrategy;
        return this;
    }

    public boolean getCteFilterAndProjectionPushdownEnabled()
    {
        return cteFilterAndProjectionPushdownEnabled;
    }

    @Config("cte-filter-and-projection-pushdown-enabled")
    @ConfigDescription("Enable pushing down filters and projections inside common table expressions")
    public FeaturesConfig setCteFilterAndProjectionPushdownEnabled(boolean cteFilterAndProjectionPushdownEnabled)
    {
        this.cteFilterAndProjectionPushdownEnabled = cteFilterAndProjectionPushdownEnabled;
        return this;
    }

    public int getCteHeuristicReplicationThreshold()
    {
        return cteHeuristicReplicationThreshold;
    }

    @Config("cte-heuristic-replication-threshold")
    @ConfigDescription("Used with CTE Materialization Strategy = Heuristic. CTES are only materialized if they are used greater than or equal to this number")
    public FeaturesConfig setCteHeuristicReplicationThreshold(int cteHeuristicReplicationThreshold)
    {
        this.cteHeuristicReplicationThreshold = cteHeuristicReplicationThreshold;
        return this;
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

    @Config("optimizer.size-based-join-distribution-type-enabled")
    public FeaturesConfig setSizeBasedJoinDistributionTypeEnabled(boolean considerTableSize)
    {
        this.sizeBasedJoinDistributionTypeEnabled = considerTableSize;
        return this;
    }

    public boolean isSizeBasedJoinDistributionTypeEnabled()
    {
        return sizeBasedJoinDistributionTypeEnabled;
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

    public boolean isUseHistoryBasedPlanStatistics()
    {
        return useHistoryBasedPlanStatistics;
    }

    @Config("optimizer.use-history-based-plan-statistics")
    public FeaturesConfig setUseHistoryBasedPlanStatistics(boolean useHistoryBasedPlanStatistics)
    {
        this.useHistoryBasedPlanStatistics = useHistoryBasedPlanStatistics;
        return this;
    }

    public boolean isTrackHistoryBasedPlanStatistics()
    {
        return trackHistoryBasedPlanStatistics;
    }

    @Config("optimizer.track-history-based-plan-statistics")
    public FeaturesConfig setTrackHistoryBasedPlanStatistics(boolean trackHistoryBasedPlanStatistics)
    {
        this.trackHistoryBasedPlanStatistics = trackHistoryBasedPlanStatistics;
        return this;
    }

    public boolean isTrackHistoryStatsFromFailedQuery()
    {
        return trackHistoryStatsFromFailedQuery;
    }

    @Config("optimizer.track-history-stats-from-failed-queries")
    public FeaturesConfig setTrackHistoryStatsFromFailedQuery(boolean trackHistoryStatsFromFailedQuery)
    {
        this.trackHistoryStatsFromFailedQuery = trackHistoryStatsFromFailedQuery;
        return this;
    }

    public boolean isUsePerfectlyConsistentHistories()
    {
        return usePerfectlyConsistentHistories;
    }

    @Config("optimizer.use-perfectly-consistent-histories")
    public FeaturesConfig setUsePerfectlyConsistentHistories(boolean usePerfectlyConsistentHistories)
    {
        this.usePerfectlyConsistentHistories = usePerfectlyConsistentHistories;
        return this;
    }

    @Min(0)
    public int getHistoryCanonicalPlanNodeLimit()
    {
        return historyCanonicalPlanNodeLimit;
    }

    @Config("optimizer.history-canonical-plan-node-limit")
    @ConfigDescription("Use history based optimizations only when number of nodes in canonical plan is within this limit. Size of canonical plan can become much larger than original plan leading to increased planning time, particularly in cases when limiting nodes like LimitNode, TopNNode etc. are present.")
    public FeaturesConfig setHistoryCanonicalPlanNodeLimit(int historyCanonicalPlanNodeLimit)
    {
        this.historyCanonicalPlanNodeLimit = historyCanonicalPlanNodeLimit;
        return this;
    }

    @NotNull
    public Duration getHistoryBasedOptimizerTimeout()
    {
        return historyBasedOptimizerTimeout;
    }

    @Config("optimizer.history-based-optimizer-timeout")
    public FeaturesConfig setHistoryBasedOptimizerTimeout(Duration historyBasedOptimizerTimeout)
    {
        this.historyBasedOptimizerTimeout = historyBasedOptimizerTimeout;
        return this;
    }

    @NotNull
    public String getHistoryBasedOptimizerPlanCanonicalizationStrategies()
    {
        return historyBasedOptimizerPlanCanonicalizationStrategies;
    }

    @Config("optimizer.history-based-optimizer-plan-canonicalization-strategies")
    public FeaturesConfig setHistoryBasedOptimizerPlanCanonicalizationStrategies(String historyBasedOptimizerPlanCanonicalizationStrategies)
    {
        this.historyBasedOptimizerPlanCanonicalizationStrategies = historyBasedOptimizerPlanCanonicalizationStrategies;
        return this;
    }

    public boolean isLogPlansUsedInHistoryBasedOptimizer()
    {
        return logPlansUsedInHistoryBasedOptimizer;
    }

    @Config("optimizer.log-plans-used-in-history-based-optimizer")
    public FeaturesConfig setLogPlansUsedInHistoryBasedOptimizer(boolean logPlansUsedInHistoryBasedOptimizer)
    {
        this.logPlansUsedInHistoryBasedOptimizer = logPlansUsedInHistoryBasedOptimizer;
        return this;
    }

    public boolean isEnforceTimeoutForHBOQueryRegistration()
    {
        return enforceTimeoutForHBOQueryRegistration;
    }

    @Config("optimizer.enforce-timeout-for-hbo-query-registration")
    public FeaturesConfig setEnforceTimeoutForHBOQueryRegistration(boolean enforceTimeoutForHBOQueryRegistration)
    {
        this.enforceTimeoutForHBOQueryRegistration = enforceTimeoutForHBOQueryRegistration;
        return this;
    }

    public boolean isHistoryBasedOptimizerEstimateSizeUsingVariables()
    {
        return historyBasedOptimizerEstimateSizeUsingVariables;
    }

    @Config("optimizer.history-based-optimizer-estimate-size-using-variables")
    public FeaturesConfig setHistoryBasedOptimizerEstimateSizeUsingVariables(boolean historyBasedOptimizerEstimateSizeUsingVariables)
    {
        this.historyBasedOptimizerEstimateSizeUsingVariables = historyBasedOptimizerEstimateSizeUsingVariables;
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

    public int getOptimizeMetadataQueriesCallThreshold()
    {
        return optimizeMetadataQueriesCallThreshold;
    }

    @Config("optimizer.optimize-metadata-queries-call-threshold")
    @ConfigDescription("The threshold number of service calls to metastore, used in optimization for metadata queries")
    public FeaturesConfig setOptimizeMetadataQueriesCallThreshold(int optimizeMetadataQueriesCallThreshold)
    {
        this.optimizeMetadataQueriesCallThreshold = optimizeMetadataQueriesCallThreshold;
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

    public boolean isExploitConstraints()
    {
        return exploitConstraints;
    }

    @Config("optimizer.exploit-constraints")
    public FeaturesConfig setExploitConstraints(boolean value)
    {
        this.exploitConstraints = value;
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

    public boolean isAdaptivePartialAggregationEnabled()
    {
        return adaptivePartialAggregationEnabled;
    }

    @Config("experimental.adaptive-partial-aggregation")
    public FeaturesConfig setAdaptivePartialAggregationEnabled(boolean adaptivePartialAggregationEnabled)
    {
        this.adaptivePartialAggregationEnabled = adaptivePartialAggregationEnabled;
        return this;
    }

    public double getAdaptivePartialAggregationRowsReductionRatioThreshold()
    {
        return adaptivePartialAggregationRowsReductionRatioThreshold;
    }

    @Config("experimental.adaptive-partial-aggregation-rows-reduction-ratio-threshold")
    public FeaturesConfig setAdaptivePartialAggregationRowsReductionRatioThreshold(double adaptivePartialAggregationRowsReductionRatioThreshold)
    {
        this.adaptivePartialAggregationRowsReductionRatioThreshold = adaptivePartialAggregationRowsReductionRatioThreshold;
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

    public boolean isOptimizeCaseExpressionPredicate()
    {
        return optimizeCaseExpressionPredicate;
    }

    @Config("optimizer.optimize-case-expression-predicate")
    public FeaturesConfig setOptimizeCaseExpressionPredicate(boolean optimizeCaseExpressionPredicate)
    {
        this.optimizeCaseExpressionPredicate = optimizeCaseExpressionPredicate;
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

    public boolean isConfidenceBasedBroadcastEnabled()
    {
        return confidenceBasedBroadcastEnabled;
    }

    @Config("optimizer.confidence-based-broadcast")
    public FeaturesConfig setConfidenceBasedBroadcastEnabled(boolean confidenceBasedBroadcastEnabled)
    {
        this.confidenceBasedBroadcastEnabled = confidenceBasedBroadcastEnabled;
        return this;
    }

    public boolean isRetryQueryWithHistoryBasedOptimizationEnabled()
    {
        return retryQueryWithHistoryBasedOptimizationEnabled;
    }

    @Config("optimizer.retry-query-with-history-based-optimization")
    public FeaturesConfig setRetryQueryWithHistoryBasedOptimizationEnabled(boolean retryQueryWithHistoryBasedOptimizationEnabled)
    {
        this.retryQueryWithHistoryBasedOptimizationEnabled = retryQueryWithHistoryBasedOptimizationEnabled;
        return this;
    }

    public boolean isTreatLowConfidenceZeroEstimationAsUnknownEnabled()
    {
        return treatLowConfidenceZeroEstimationAsUnknownEnabled;
    }

    @Config("optimizer.treat-low-confidence-zero-estimation-as-unknown")
    public FeaturesConfig setTreatLowConfidenceZeroEstimationAsUnknownEnabled(boolean treatLowConfidenceZeroEstimationAsUnknownEnabled)
    {
        this.treatLowConfidenceZeroEstimationAsUnknownEnabled = treatLowConfidenceZeroEstimationAsUnknownEnabled;
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

    public Duration getQueryAnalyzerTimeout()
    {
        return this.queryAnalyzerTimeout;
    }

    @Config("planner.query-analyzer-timeout")
    @ConfigDescription("Maximum running time for the query analyzer in case the processing takes too long or is stuck in an infinite loop.")
    public FeaturesConfig setQueryAnalyzerTimeout(Duration timeout)
    {
        this.queryAnalyzerTimeout = timeout;
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

    @Config("enhanced-cte-scheduling-enabled")
    public FeaturesConfig setEnhancedCTESchedulingEnabled(boolean enhancedCTEBlockingEnabled)
    {
        this.enhancedCteSchedulingEnabled = enhancedCTEBlockingEnabled;
        return this;
    }

    public boolean getEnhancedCTESchedulingEnabled()
    {
        return enhancedCteSchedulingEnabled;
    }

    @Config("optimizer.default-join-selectivity-coefficient")
    @ConfigDescription("Used when join selectivity estimation is unknown. Default 0 to disable the use of join selectivity, this will allow planner to fall back to FROM-clause join order when the join cardinality is unknown")
    public FeaturesConfig setDefaultJoinSelectivityCoefficient(double defaultJoinSelectivityCoefficient)
    {
        this.defaultJoinSelectivityCoefficient = defaultJoinSelectivityCoefficient;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getDefaultJoinSelectivityCoefficient()
    {
        return defaultJoinSelectivityCoefficient;
    }

    @Config("optimizer.default-writer-replication-coefficient")
    @ConfigDescription("Replication coefficient for costing write operations")
    public FeaturesConfig setDefaultWriterReplicationCoefficient(double defaultJoinSelectivityCoefficient)
    {
        this.defaultWriterReplicationCoefficient = defaultJoinSelectivityCoefficient;
        return this;
    }

    public double getDefaultWriterReplicationCoefficient()
    {
        return defaultWriterReplicationCoefficient;
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

    @Config("enable-dynamic-filtering")
    @LegacyConfig("experimental.enable-dynamic-filtering")
    public FeaturesConfig setEnableDynamicFiltering(boolean value)
    {
        this.enableDynamicFiltering = value;
        return this;
    }

    public int getDynamicFilteringMaxPerDriverRowCount()
    {
        return dynamicFilteringMaxPerDriverRowCount;
    }

    @Config("dynamic-filtering-max-per-driver-row-count")
    @LegacyConfig("experimental.dynamic-filtering-max-per-driver-row-count")
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

    @Config("dynamic-filtering-max-per-driver-size")
    @LegacyConfig("experimental.dynamic-filtering-max-per-driver-size")
    public FeaturesConfig setDynamicFilteringMaxPerDriverSize(DataSize dynamicFilteringMaxPerDriverSize)
    {
        this.dynamicFilteringMaxPerDriverSize = dynamicFilteringMaxPerDriverSize;
        return this;
    }

    public int getDynamicFilteringRangeRowLimitPerDriver()
    {
        return dynamicFilteringRangeRowLimitPerDriver;
    }

    @Config("dynamic-filtering-range-row-limit-per-driver")
    @LegacyConfig("experimental.dynamic-filtering-range-row-limit-per-driver")
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

    @Config("fragment-result-cache.enabled")
    @LegacyConfig("experimental.fragment-result-caching-enabled")
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

    public CompressionCodec getExchangeCompressionCodec()
    {
        return exchangeCompressionCodec;
    }

    public boolean isExchangeChecksumEnabled()
    {
        return exchangeChecksumEnabled;
    }

    @Config("exchange.compression-codec")
    public FeaturesConfig setExchangeCompressionCodec(CompressionCodec exchangeCompressionCodec)
    {
        this.exchangeCompressionCodec = exchangeCompressionCodec;
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

    @Config("optimizer.optimize-constant-grouping-keys")
    public FeaturesConfig setOptimizeConstantGroupingKeys(boolean optimizeConstantGroupingKeys)
    {
        this.optimizeConstantGroupingKeys = optimizeConstantGroupingKeys;
        return this;
    }

    public boolean isOptimizeConstantGroupingKeys()
    {
        return optimizeConstantGroupingKeys;
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

    @Config("pushdown-subfields-from-lambda-enabled")
    @ConfigDescription("Enable subfield pruning from lambda expressions")
    public FeaturesConfig setPushdownSubfieldsFromLambdaEnabled(boolean pushdownSubfieldsFromLambdaEnabled)
    {
        this.pushdownSubfieldsFromLambdaEnabled = pushdownSubfieldsFromLambdaEnabled;
        return this;
    }

    public boolean isPushdownSubfieldsFromLambdaEnabled()
    {
        return pushdownSubfieldsFromLambdaEnabled;
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

    @Config("optimize-payload-joins")
    public FeaturesConfig setOptimizePayloadJoins(boolean optimizePayloadJoins)
    {
        this.optimizePayloadJoins = optimizePayloadJoins;
        return this;
    }

    public boolean isOptimizePayloadJoins()
    {
        return optimizePayloadJoins;
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

    public boolean isCheckAccessControlWithSubfields()
    {
        return checkAccessControlWithSubfields;
    }

    @Config("check-access-control-with-subfields")
    public FeaturesConfig setCheckAccessControlWithSubfields(boolean checkAccessControlWithSubfields)
    {
        this.checkAccessControlWithSubfields = checkAccessControlWithSubfields;
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

    public boolean isLogInvokedFunctionNamesEnabled()
    {
        return logInvokedFunctionNamesEnabled;
    }

    @Config("log-invoked-function-names-enabled")
    @ConfigDescription("Log the names of the functions invoked by the query when enabled.")
    public FeaturesConfig setLogInvokedFunctionNamesEnabled(boolean logInvokedFunctionNamesEnabled)
    {
        this.logInvokedFunctionNamesEnabled = logInvokedFunctionNamesEnabled;
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

    public boolean isMaterializedViewPartitionFilteringEnabled()
    {
        return materializedViewPartitionFilteringEnabled;
    }

    @Config("materialized-view-data-consistency-enabled")
    @ConfigDescription("When enabled and reading from materialized view, partition stitching is applied to achieve data consistency")
    public FeaturesConfig setMaterializedViewDataConsistencyEnabled(boolean materializedViewDataConsistencyEnabled)
    {
        this.materializedViewDataConsistencyEnabled = materializedViewDataConsistencyEnabled;
        return this;
    }

    @Config("consider-query-filters-for-materialized-view-partitions")
    @ConfigDescription("When enabled and counting materialized view partitions, filters partition domains not in base query")
    public FeaturesConfig setMaterializedViewPartitionFilteringEnabled(boolean materializedViewPartitionFilteringEnabled)
    {
        this.materializedViewPartitionFilteringEnabled = materializedViewPartitionFilteringEnabled;
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

    public boolean isVerboseOptimizerInfoEnabled()
    {
        return verboseOptimizerInfoEnabled;
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

    @Config("optimizer.joins-not-null-inference-strategy")
    @ConfigDescription("Set the strategy used NOT NULL filter inference on Join Nodes")
    public FeaturesConfig setJoinsNotNullInferenceStrategy(JoinNotNullInferenceStrategy strategy)
    {
        this.joinNotNullInferenceStrategy = strategy;
        return this;
    }

    public JoinNotNullInferenceStrategy getJoinsNotNullInferenceStrategy()
    {
        return joinNotNullInferenceStrategy;
    }

    public String getAnalyzerType()
    {
        return analyzerType;
    }

    @Config("analyzer-type")
    @ConfigDescription("Set the analyzer type for parsing and analyzing.")
    public FeaturesConfig setAnalyzerType(String analyzerType)
    {
        this.analyzerType = analyzerType;
        return this;
    }

    @Config("pre-process-metadata-calls")
    @ConfigDescription("Pre process metadata calls before analyzer invocation")
    public FeaturesConfig setPreProcessMetadataCalls(boolean preProcessMetadataCalls)
    {
        this.preProcessMetadataCalls = preProcessMetadataCalls;
        return this;
    }

    public boolean isPreProcessMetadataCalls()
    {
        return preProcessMetadataCalls;
    }

    public boolean isStreamingForPartialAggregationEnabled()
    {
        return streamingForPartialAggregationEnabled;
    }

    @Config("streaming-for-partial-aggregation-enabled")
    public FeaturesConfig setStreamingForPartialAggregationEnabled(boolean streamingForPartialAggregationEnabled)
    {
        this.streamingForPartialAggregationEnabled = streamingForPartialAggregationEnabled;
        return this;
    }

    public int getMaxStageCountForEagerScheduling()
    {
        return maxStageCountForEagerScheduling;
    }

    @Min(1)
    @Config("execution-policy.max-stage-count-for-eager-scheduling")
    @ConfigDescription("When execution policy is set to adaptive, this number determines when to switch to phased execution.")
    public FeaturesConfig setMaxStageCountForEagerScheduling(int maxStageCountForEagerScheduling)
    {
        this.maxStageCountForEagerScheduling = maxStageCountForEagerScheduling;
        return this;
    }

    public double getHyperloglogStandardErrorWarningThreshold()
    {
        return hyperloglogStandardErrorWarningThreshold;
    }

    @Config("hyperloglog-standard-error-warning-threshold")
    @ConfigDescription("aggregation functions can produce low-precision results when the max standard error lower than this value.")
    public FeaturesConfig setHyperloglogStandardErrorWarningThreshold(double hyperloglogStandardErrorWarningThreshold)
    {
        this.hyperloglogStandardErrorWarningThreshold = hyperloglogStandardErrorWarningThreshold;
        return this;
    }

    public boolean isPreferMergeJoinForSortedInputs()
    {
        return preferMergeJoinForSortedInputs;
    }

    @Config("optimizer.prefer-merge-join-for-sorted-inputs")
    @ConfigDescription("Prefer merge join for sorted join inputs, e.g., tables pre-sorted, pre-partitioned by join columns." +
            "To make it work, the connector needs to guarantee and expose the data properties of the underlying table.")
    public FeaturesConfig setPreferMergeJoinForSortedInputs(boolean preferMergeJoinForSortedInputs)
    {
        this.preferMergeJoinForSortedInputs = preferMergeJoinForSortedInputs;
        return this;
    }

    public boolean isPreferSortMergeJoin()
    {
        return preferSortMergeJoin;
    }

    @Config("experimental.optimizer.prefer-sort-merge-join")
    @ConfigDescription("Prefer sort merge join for all joins. A SortNode is added if input is not already sorted.")
    public FeaturesConfig setPreferSortMergeJoin(boolean preferSortMergeJoin)
    {
        this.preferSortMergeJoin = preferSortMergeJoin;
        return this;
    }

    public boolean isSegmentedAggregationEnabled()
    {
        return segmentedAggregationEnabled;
    }

    @Config("optimizer.segmented-aggregation-enabled")
    public FeaturesConfig setSegmentedAggregationEnabled(boolean segmentedAggregationEnabled)
    {
        this.segmentedAggregationEnabled = segmentedAggregationEnabled;
        return this;
    }

    public boolean isQuickDistinctLimitEnabled()
    {
        return quickDistinctLimitEnabled;
    }

    @Config("optimizer.quick-distinct-limit-enabled")
    @ConfigDescription("Enable quick distinct limit queries that give results as soon as a new distinct value is found")
    public FeaturesConfig setQuickDistinctLimitEnabled(boolean quickDistinctLimitEnabled)
    {
        this.quickDistinctLimitEnabled = quickDistinctLimitEnabled;
        return this;
    }

    public boolean isPushRemoteExchangeThroughGroupId()
    {
        return pushRemoteExchangeThroughGroupId;
    }

    @Config("optimizer.push-remote-exchange-through-group-id")
    public FeaturesConfig setPushRemoteExchangeThroughGroupId(boolean value)
    {
        this.pushRemoteExchangeThroughGroupId = value;
        return this;
    }

    public boolean isOptimizeMultipleApproxPercentileOnSameFieldEnabled()
    {
        return isOptimizeMultipleApproxPercentileOnSameFieldEnabled;
    }

    @Config("optimizer.optimize-multiple-approx-percentile-on-same-field")
    @ConfigDescription("Enable combining individual approx_percentile calls on the same individual field to evaluation on an array")
    public FeaturesConfig setOptimizeMultipleApproxPercentileOnSameFieldEnabled(boolean isOptimizeMultipleApproxPercentileOnSameFieldEnabled)
    {
        this.isOptimizeMultipleApproxPercentileOnSameFieldEnabled = isOptimizeMultipleApproxPercentileOnSameFieldEnabled;
        return this;
    }

    @Config("native-execution-enabled")
    @ConfigDescription("Enable execution on native engine")
    public FeaturesConfig setNativeExecutionEnabled(boolean nativeExecutionEnabled)
    {
        this.nativeExecutionEnabled = nativeExecutionEnabled;
        return this;
    }

    public boolean isNativeExecutionEnabled()
    {
        return this.nativeExecutionEnabled;
    }

    @Config("disable-timestamp-with-timezone-for-native-execution")
    @ConfigDescription("Disable timestamp with timezone type on native engine")
    public FeaturesConfig setDisableTimeStampWithTimeZoneForNative(boolean disableTimeStampWithTimeZoneForNative)
    {
        this.disableTimeStampWithTimeZoneForNative = disableTimeStampWithTimeZoneForNative;
        return this;
    }

    public boolean isDisableTimeStampWithTimeZoneForNative()
    {
        return this.disableTimeStampWithTimeZoneForNative;
    }

    @Config("disable-ipaddress-for-native-execution")
    @ConfigDescription("Disable ipaddress type on native engine")
    public FeaturesConfig setDisableIPAddressForNative(boolean disableIPAddressForNative)
    {
        this.disableIPAddressForNative = disableIPAddressForNative;
        return this;
    }

    public boolean isDisableIPAddressForNative()
    {
        return this.disableIPAddressForNative;
    }

    @Config("native-execution-executable-path")
    @ConfigDescription("Native execution executable file path")
    public FeaturesConfig setNativeExecutionExecutablePath(String nativeExecutionExecutablePath)
    {
        this.nativeExecutionExecutablePath = nativeExecutionExecutablePath;
        return this;
    }

    public String getNativeExecutionExecutablePath()
    {
        return this.nativeExecutionExecutablePath;
    }

    @Config("native-execution-program-arguments")
    @ConfigDescription("Program arguments for native engine execution")
    public FeaturesConfig setNativeExecutionProgramArguments(String nativeExecutionProgramArguments)
    {
        this.nativeExecutionProgramArguments = nativeExecutionProgramArguments;
        return this;
    }

    public String getNativeExecutionProgramArguments()
    {
        return this.nativeExecutionProgramArguments;
    }

    @Config("native-execution-process-reuse-enabled")
    @ConfigDescription("Enable reuse the native process within the same JVM")
    public FeaturesConfig setNativeExecutionProcessReuseEnabled(boolean nativeExecutionProcessReuseEnabled)
    {
        this.nativeExecutionProcessReuseEnabled = nativeExecutionProcessReuseEnabled;
        return this;
    }

    public boolean isNativeExecutionProcessReuseEnabled()
    {
        return this.nativeExecutionProcessReuseEnabled;
    }

    @Config("native-enforce-join-build-input-partition")
    @ConfigDescription("Enforce that the join build input is partitioned on join key")
    public FeaturesConfig setNativeEnforceJoinBuildInputPartition(boolean nativeEnforceJoinBuildInputPartition)
    {
        this.nativeEnforceJoinBuildInputPartition = nativeEnforceJoinBuildInputPartition;
        return this;
    }

    public boolean isNativeEnforceJoinBuildInputPartition()
    {
        return this.nativeEnforceJoinBuildInputPartition;
    }

    public boolean isRandomizeOuterJoinNullKeyEnabled()
    {
        return randomizeOuterJoinNullKey;
    }

    @Config("optimizer.randomize-outer-join-null-key")
    @ConfigDescription("Randomize null join key for outer join")
    public FeaturesConfig setRandomizeOuterJoinNullKeyEnabled(boolean randomizeOuterJoinNullKey)
    {
        this.randomizeOuterJoinNullKey = randomizeOuterJoinNullKey;
        return this;
    }

    public RandomizeOuterJoinNullKeyStrategy getRandomizeOuterJoinNullKeyStrategy()
    {
        return randomizeOuterJoinNullKeyStrategy;
    }

    @Config("optimizer.randomize-outer-join-null-key-strategy")
    @ConfigDescription("When to apply randomization to null keys in outer join")
    public FeaturesConfig setRandomizeOuterJoinNullKeyStrategy(RandomizeOuterJoinNullKeyStrategy randomizeOuterJoinNullKeyStrategy)
    {
        this.randomizeOuterJoinNullKeyStrategy = randomizeOuterJoinNullKeyStrategy;
        return this;
    }

    public ShardedJoinStrategy getShardedJoinStrategy()
    {
        return shardedJoinStrategy;
    }

    @Config("optimizer.sharded-join-strategy")
    @ConfigDescription("When to apply sharding to mitigate skew in joins")
    public FeaturesConfig setShardedJoinStrategy(ShardedJoinStrategy shardedJoinStrategy)
    {
        this.shardedJoinStrategy = shardedJoinStrategy;
        return this;
    }

    public int getJoinShardCount()
    {
        return joinShardCount;
    }

    @Config("optimizer.join-shard-count")
    @ConfigDescription("Number of shards to use for sharded join optimization")
    public FeaturesConfig setJoinShardCount(int joinShardCount)
    {
        this.joinShardCount = joinShardCount;
        return this;
    }

    public boolean isOptimizeConditionalAggregationEnabled()
    {
        return isOptimizeConditionalAggregationEnabled;
    }

    @Config("optimizer.optimize-conditional-aggregation-enabled")
    @ConfigDescription("Enable rewriting IF(condition, AGG(x)) to AGG(x) with condition included in mask")
    public FeaturesConfig setOptimizeConditionalAggregationEnabled(boolean isOptimizeConditionalAggregationEnabled)
    {
        this.isOptimizeConditionalAggregationEnabled = isOptimizeConditionalAggregationEnabled;
        return this;
    }

    public boolean isRemoveRedundantDistinctAggregationEnabled()
    {
        return isRemoveRedundantDistinctAggregationEnabled;
    }

    @Config("optimizer.remove-redundant-distinct-aggregation-enabled")
    @ConfigDescription("Enable removing distinct aggregation node if input is already distinct")
    public FeaturesConfig setRemoveRedundantDistinctAggregationEnabled(boolean isRemoveRedundantDistinctAggregationEnabled)
    {
        this.isRemoveRedundantDistinctAggregationEnabled = isRemoveRedundantDistinctAggregationEnabled;
        return this;
    }

    public boolean isInPredicatesAsInnerJoinsEnabled()
    {
        return inPredicatesAsInnerJoinsEnabled;
    }

    @Config("optimizer.in-predicates-as-inner-joins-enabled")
    @ConfigDescription("Enable rewrite of In predicates to INNER joins")
    public FeaturesConfig setInPredicatesAsInnerJoinsEnabled(boolean inPredicatesAsInnerJoinsEnabled)
    {
        this.inPredicatesAsInnerJoinsEnabled = inPredicatesAsInnerJoinsEnabled;
        return this;
    }

    public double getPushAggregationBelowJoinByteReductionThreshold()
    {
        return pushAggregationBelowJoinByteReductionThreshold;
    }

    @Config("optimizer.push-aggregation-below-join-byte-reduction-threshold")
    @ConfigDescription("Byte reduction ratio threshold at which to disable pushdown of aggregation below inner join")
    public FeaturesConfig setPushAggregationBelowJoinByteReductionThreshold(double pushAggregationBelowJoinByteReductionThreshold)
    {
        this.pushAggregationBelowJoinByteReductionThreshold = pushAggregationBelowJoinByteReductionThreshold;
        return this;
    }

    public boolean isPrefilterForGroupbyLimit()
    {
        return prefilterForGroupbyLimit;
    }

    @Config("optimizer.prefilter-for-groupby-limit")
    @ConfigDescription("Enable optimizations for groupby limit queries")
    public FeaturesConfig setPrefilterForGroupbyLimit(boolean prefilterForGroupbyLimit)
    {
        this.prefilterForGroupbyLimit = prefilterForGroupbyLimit;
        return this;
    }

    public boolean isOptimizeJoinProbeForEmptyBuildRuntimeEnabled()
    {
        return isOptimizeJoinProbeWithEmptyBuildRuntime;
    }

    @Config("optimizer.optimize-probe-for-empty-build-runtime")
    @ConfigDescription("Optimize join probe at runtime if build side is empty")
    public FeaturesConfig setOptimizeJoinProbeForEmptyBuildRuntimeEnabled(boolean isOptimizeJoinProbeWithEmptyBuildRuntime)
    {
        this.isOptimizeJoinProbeWithEmptyBuildRuntime = isOptimizeJoinProbeWithEmptyBuildRuntime;
        return this;
    }

    public boolean isUseDefaultsForCorrelatedAggregationPushdownThroughOuterJoins()
    {
        return useDefaultsForCorrelatedAggregationPushdownThroughOuterJoins;
    }

    @Config("optimizer.use-defaults-for-correlated-aggregation-pushdown-through-outer-joins")
    @ConfigDescription("Enable using defaults for well-defined aggregation functions when pushing them through outer joins")
    public FeaturesConfig setUseDefaultsForCorrelatedAggregationPushdownThroughOuterJoins(boolean useDefaultsForCorrelatedAggregationPushdownThroughOuterJoins)
    {
        this.useDefaultsForCorrelatedAggregationPushdownThroughOuterJoins = useDefaultsForCorrelatedAggregationPushdownThroughOuterJoins;
        return this;
    }

    public boolean isMergeDuplicateAggregationsEnabled()
    {
        return mergeDuplicateAggregationsEnabled;
    }

    @Config("optimizer.merge-duplicate-aggregations")
    @ConfigDescription("Merge identical aggregation functions within the same aggregation node")
    public FeaturesConfig setMergeDuplicateAggregationsEnabled(boolean mergeDuplicateAggregationsEnabled)
    {
        this.mergeDuplicateAggregationsEnabled = mergeDuplicateAggregationsEnabled;
        return this;
    }

    public boolean isMergeAggregationsWithAndWithoutFilter()
    {
        return this.mergeAggregationsWithAndWithoutFilter;
    }

    @Config("optimizer.merge-aggregations-with-and-without-filter")
    @ConfigDescription("Enable optimization that merges the same agg with and without filter for efficiency")
    public FeaturesConfig setMergeAggregationsWithAndWithoutFilter(boolean mergeAggregationsWithAndWithoutFilter)
    {
        this.mergeAggregationsWithAndWithoutFilter = mergeAggregationsWithAndWithoutFilter;
        return this;
    }

    public boolean isSimplifyPlanWithEmptyInput()
    {
        return this.simplifyPlanWithEmptyInput;
    }

    @Config("optimizer.simplify-plan-with-empty-input")
    @ConfigDescription("Enable simplifying query plans with empty input")
    public FeaturesConfig setSimplifyPlanWithEmptyInput(boolean simplifyPlanWithEmptyInput)
    {
        this.simplifyPlanWithEmptyInput = simplifyPlanWithEmptyInput;
        return this;
    }

    public PushDownFilterThroughCrossJoinStrategy getPushDownFilterExpressionEvaluationThroughCrossJoin()
    {
        return this.pushDownFilterExpressionEvaluationThroughCrossJoin;
    }

    @Config("optimizer.push-down-filter-expression-evaluation-through-cross-join")
    @ConfigDescription("Push down expression evaluation in filter through cross join")
    public FeaturesConfig setPushDownFilterExpressionEvaluationThroughCrossJoin(PushDownFilterThroughCrossJoinStrategy strategy)
    {
        this.pushDownFilterExpressionEvaluationThroughCrossJoin = strategy;
        return this;
    }

    public boolean isRewriteCrossJoinWithOrFilterToInnerJoin()
    {
        return this.rewriteCrossJoinWithOrFilterToInnerJoin;
    }

    @Config("optimizer.rewrite-cross-join-with-or-filter-to-inner-join")
    @ConfigDescription("Enable optimization to rewrite cross join with or filter to inner join")
    public FeaturesConfig setRewriteCrossJoinWithOrFilterToInnerJoin(boolean rewriteCrossJoinWithOrFilterToInnerJoin)
    {
        this.rewriteCrossJoinWithOrFilterToInnerJoin = rewriteCrossJoinWithOrFilterToInnerJoin;
        return this;
    }

    public boolean isRewriteCrossJoinWithArrayContainsFilterToInnerJoin()
    {
        return this.rewriteCrossJoinWithArrayContainsFilterToInnerJoin;
    }

    @Config("optimizer.rewrite-cross-join-with-array-contains-filter-to-inner-join")
    @ConfigDescription("Enable optimization to rewrite cross join with array contains filter to inner join")
    public FeaturesConfig setRewriteCrossJoinWithArrayContainsFilterToInnerJoin(boolean rewriteCrossJoinWithArrayContainsFilterToInnerJoin)
    {
        this.rewriteCrossJoinWithArrayContainsFilterToInnerJoin = rewriteCrossJoinWithArrayContainsFilterToInnerJoin;
        return this;
    }

    public LeftJoinArrayContainsToInnerJoinStrategy getLeftJoinWithArrayContainsToEquiJoinStrategy()
    {
        return leftJoinWithArrayContainsToEquiJoinStrategy;
    }

    @Config("optimizer.left-join-with-array-contains-to-equi-join-strategy")
    @ConfigDescription("When to apply rewrite left join with array contains to equi join")
    public FeaturesConfig setLeftJoinWithArrayContainsToEquiJoinStrategy(LeftJoinArrayContainsToInnerJoinStrategy leftJoinWithArrayContainsToEquiJoinStrategy)
    {
        this.leftJoinWithArrayContainsToEquiJoinStrategy = leftJoinWithArrayContainsToEquiJoinStrategy;
        return this;
    }

    public boolean isRewriteCrossJoinWithArrayNotContainsFilterToAntiJoin()
    {
        return this.rewriteCrossJoinWithArrayNotContainsFilterToAntiJoin;
    }

    @Config("optimizer.rewrite-cross-join-with-array-not-contains-filter-to-anti-join")
    @ConfigDescription("Enable optimization to rewrite cross join with array not contains filter to anti join")
    public FeaturesConfig setRewriteCrossJoinWithArrayNotContainsFilterToAntiJoin(boolean rewriteCrossJoinWithArrayNotContainsFilterToAntiJoin)
    {
        this.rewriteCrossJoinWithArrayNotContainsFilterToAntiJoin = rewriteCrossJoinWithArrayNotContainsFilterToAntiJoin;
        return this;
    }

    public boolean isLeftJoinNullFilterToSemiJoin()
    {
        return this.leftJoinNullFilterToSemiJoin;
    }

    @Config("optimizer.rewrite-left-join-with-null-filter-to-semi-join")
    @ConfigDescription("Rewrite left join with is null check to semi join")
    public FeaturesConfig setLeftJoinNullFilterToSemiJoin(boolean leftJoinNullFilterToSemiJoin)
    {
        this.leftJoinNullFilterToSemiJoin = leftJoinNullFilterToSemiJoin;
        return this;
    }

    public boolean isBroadcastJoinWithSmallBuildUnknownProbe()
    {
        return this.broadcastJoinWithSmallBuildUnknownProbe;
    }

    @Config("experimental.optimizer.broadcast-join-with-small-build-unknown-probe")
    @ConfigDescription("Experimental: When probe side size is unknown but build size is within broadcast limit, choose broadcast join")
    public FeaturesConfig setBroadcastJoinWithSmallBuildUnknownProbe(boolean broadcastJoinWithSmallBuildUnknownProbe)
    {
        this.broadcastJoinWithSmallBuildUnknownProbe = broadcastJoinWithSmallBuildUnknownProbe;
        return this;
    }

    public boolean isAddPartialNodeForRowNumberWithLimitEnabled()
    {
        return this.addPartialNodeForRowNumberWithLimit;
    }

    @Config("optimizer.add-partial-node-for-row-number-with-limit")
    @ConfigDescription("Add partial row number node for row number node with limit")
    public FeaturesConfig setAddPartialNodeForRowNumberWithLimitEnabled(boolean addPartialNodeForRowNumberWithLimit)
    {
        this.addPartialNodeForRowNumberWithLimit = addPartialNodeForRowNumberWithLimit;
        return this;
    }

    public boolean isPullUpExpressionFromLambdaEnabled()
    {
        return this.pullUpExpressionFromLambda;
    }

    @Config("optimizer.pull-up-expression-from-lambda")
    @ConfigDescription("Pull up expression from lambda which does not refer to arguments of the lambda function")
    public FeaturesConfig setPullUpExpressionFromLambdaEnabled(boolean pullUpExpressionFromLambda)
    {
        this.pullUpExpressionFromLambda = pullUpExpressionFromLambda;
        return this;
    }

    public boolean getInferInequalityPredicates()
    {
        return inferInequalityPredicates;
    }

    @Config("optimizer.infer-inequality-predicates")
    @ConfigDescription("Enabled inference of inequality predicates for joins")
    public FeaturesConfig setInferInequalityPredicates(boolean inferInequalityPredicates)
    {
        this.inferInequalityPredicates = inferInequalityPredicates;
        return this;
    }

    public boolean isRewriteConstantArrayContainsToInEnabled()
    {
        return this.rewriteConstantArrayContainsToIn;
    }

    @Config("optimizer.rewrite-constant-array-contains-to-in")
    @ConfigDescription("Rewrite constant array contains function to IN expression")
    public FeaturesConfig setRewriteConstantArrayContainsToInEnabled(boolean rewriteConstantArrayContainsToIn)
    {
        this.rewriteConstantArrayContainsToIn = rewriteConstantArrayContainsToIn;
        return this;
    }

    public boolean isUseHBOForScaledWriters()
    {
        return this.useHBOForScaledWriters;
    }

    @Config("optimizer.use-hbo-for-scaled-writers")
    @ConfigDescription("Enable HBO for setting initial number of tasks for scaled writers")
    public FeaturesConfig setUseHBOForScaledWriters(boolean useHBOForScaledWriters)
    {
        this.useHBOForScaledWriters = useHBOForScaledWriters;
        return this;
    }

    public boolean isUsePartialAggregationHistory()
    {
        return this.usePartialAggregationHistory;
    }

    @Config("optimizer.use-partial-aggregation-history")
    @ConfigDescription("Use partial aggregation histories for splitting aggregations")
    public FeaturesConfig setUsePartialAggregationHistory(boolean usePartialAggregationHistory)
    {
        this.usePartialAggregationHistory = usePartialAggregationHistory;
        return this;
    }

    public boolean isTrackPartialAggregationHistory()
    {
        return this.trackPartialAggregationHistory;
    }

    @Config("optimizer.track-partial-aggregation-history")
    @ConfigDescription("Track partial aggregation histories")
    public FeaturesConfig setTrackPartialAggregationHistory(boolean trackPartialAggregationHistory)
    {
        this.trackPartialAggregationHistory = trackPartialAggregationHistory;
        return this;
    }

    public boolean isRemoveRedundantCastToVarcharInJoin()
    {
        return removeRedundantCastToVarcharInJoin;
    }

    @Config("optimizer.remove-redundant-cast-to-varchar-in-join")
    @ConfigDescription("If both left and right side of join clause are varchar cast from int/bigint, remove the cast")
    public FeaturesConfig setRemoveRedundantCastToVarcharInJoin(boolean removeRedundantCastToVarcharInJoin)
    {
        this.removeRedundantCastToVarcharInJoin = removeRedundantCastToVarcharInJoin;
        return this;
    }

    public boolean getHandleComplexEquiJoins()
    {
        return handleComplexEquiJoins;
    }

    @Config("optimizer.handle-complex-equi-joins")
    @ConfigDescription("Handle complex equi-join conditions to open up join space for join reordering")
    public FeaturesConfig setHandleComplexEquiJoins(boolean handleComplexEquiJoins)
    {
        this.handleComplexEquiJoins = handleComplexEquiJoins;
        return this;
    }

    public boolean isSkipHashGenerationForJoinWithTableScanInput()
    {
        return skipHashGenerationForJoinWithTableScanInput;
    }

    @Config("optimizer.skip-hash-generation-for-join-with-table-scan-input")
    @ConfigDescription("Skip hash generation for join, when input is table scan node")
    public FeaturesConfig setSkipHashGenerationForJoinWithTableScanInput(boolean skipHashGenerationForJoinWithTableScanInput)
    {
        this.skipHashGenerationForJoinWithTableScanInput = skipHashGenerationForJoinWithTableScanInput;
        return this;
    }

    public boolean getGenerateDomainFilters()
    {
        return generateDomainFilters;
    }

    @Config("optimizer.generate-domain-filters")
    @ConfigDescription("Infer predicates from column domains during predicate pushdown")
    public FeaturesConfig setGenerateDomainFilters(boolean generateDomainFilters)
    {
        this.generateDomainFilters = generateDomainFilters;
        return this;
    }

    public boolean isRewriteExpressionWithConstantVariable()
    {
        return this.rewriteExpressionWithConstantVariable;
    }

    @Config("optimizer.rewrite-expression-with-constant-variable")
    @ConfigDescription("Rewrite expression with constant variables")
    public FeaturesConfig setRewriteExpressionWithConstantVariable(boolean rewriteExpressionWithConstantVariable)
    {
        this.rewriteExpressionWithConstantVariable = rewriteExpressionWithConstantVariable;
        return this;
    }

    public boolean isOptimizeConditionalApproxDistinct()
    {
        return this.optimizeConditionalApproxDistinct;
    }

    @Config("optimizer.optimize-constant-approx-distinct")
    @ConfigDescription("Optimize out APPROX_DISTINCT over conditional constant expressions")
    public FeaturesConfig setOptimizeConditionalApproxDistinct(boolean optimizeConditionalApproxDistinct)
    {
        this.optimizeConditionalApproxDistinct = optimizeConditionalApproxDistinct;
        return this;
    }

    public CreateView.Security getDefaultViewSecurityMode()
    {
        return this.defaultViewSecurityMode;
    }

    @Config("default-view-security-mode")
    @ConfigDescription("Sets the default security mode for view creation. The options are definer/invoker.")
    public FeaturesConfig setDefaultViewSecurityMode(CreateView.Security securityMode)
    {
        this.defaultViewSecurityMode = securityMode;
        return this;
    }

    public boolean isPrintEstimatedStatsFromCache()
    {
        return this.printEstimatedStatsFromCache;
    }

    @Config("optimizer.print-estimated-stats-from-cache")
    @ConfigDescription("In the end of query optimization, print the estimation stats from cache populated during optimization instead of calculating from ground")
    public FeaturesConfig setPrintEstimatedStatsFromCache(boolean printEstimatedStatsFromCache)
    {
        this.printEstimatedStatsFromCache = printEstimatedStatsFromCache;
        return this;
    }

    public boolean isRemoveCrossJoinWithSingleConstantRow()
    {
        return this.removeCrossJoinWithSingleConstantRow;
    }

    @Config("optimizer.remove-cross-join-with-single-constant-row")
    @ConfigDescription("If one input of the cross join is a single row with constant value, remove this cross join and replace with a project node")
    public FeaturesConfig setRemoveCrossJoinWithSingleConstantRow(boolean removeCrossJoinWithSingleConstantRow)
    {
        this.removeCrossJoinWithSingleConstantRow = removeCrossJoinWithSingleConstantRow;
        return this;
    }

    public boolean isUseHistograms()
    {
        return useHistograms;
    }

    @Config("optimizer.use-histograms")
    @ConfigDescription("Use histogram statistics in cost-based calculations in the optimizer")
    public FeaturesConfig setUseHistograms(boolean useHistograms)
    {
        this.useHistograms = useHistograms;
        return this;
    }

    public boolean getInlineProjectionsOnValues()
    {
        return isInlineProjectionsOnValuesEnabled;
    }

    @Config("optimizer.inline-projections-on-values")
    @ConfigDescription("Inline deterministic projections on values input")
    public FeaturesConfig setInlineProjectionsOnValues(boolean isInlineProjectionsOnValuesEnabled)
    {
        this.isInlineProjectionsOnValuesEnabled = isInlineProjectionsOnValuesEnabled;
        return this;
    }

    public boolean isIncludeValuesNodeInConnectorOptimizer()
    {
        return includeValuesNodeInConnectorOptimizer;
    }

    @Config("optimizer.include-values-node-in-connector-optimizer")
    @ConfigDescription("Include values node in connector optimizer")
    public FeaturesConfig setIncludeValuesNodeInConnectorOptimizer(boolean includeValuesNodeInConnectorOptimizer)
    {
        this.includeValuesNodeInConnectorOptimizer = includeValuesNodeInConnectorOptimizer;
        return this;
    }

    @Config("eager-plan-validation-enabled")
    @ConfigDescription("Enable eager building and validation of logical plan before queueing")
    public FeaturesConfig setEagerPlanValidationEnabled(boolean eagerPlanValidationEnabled)
    {
        this.eagerPlanValidationEnabled = eagerPlanValidationEnabled;
        return this;
    }

    public boolean isEagerPlanValidationEnabled()
    {
        return this.eagerPlanValidationEnabled;
    }

    @Config("eager-plan-validation-thread-pool-size")
    @ConfigDescription("Size of thread pool to use when eager plan validation is enabled")
    public FeaturesConfig setEagerPlanValidationThreadPoolSize(int eagerPlanValidationThreadPoolSize)
    {
        this.eagerPlanValidationThreadPoolSize = eagerPlanValidationThreadPoolSize;
        return this;
    }

    public int getEagerPlanValidationThreadPoolSize()
    {
        return this.eagerPlanValidationThreadPoolSize;
    }

    @Config("optimizer.inner-join-pushdown-enabled")
    @ConfigDescription("Push down inner join predicates to database")
    public FeaturesConfig setInnerJoinPushdownEnabled(boolean innerJoinPushdownEnabled)
    {
        this.innerJoinPushdownEnabled = innerJoinPushdownEnabled;
        return this;
    }

    public boolean isInnerJoinPushdownEnabled()
    {
        return innerJoinPushdownEnabled;
    }

    @Config("optimizer.inequality-join-pushdown-enabled")
    @ConfigDescription("Push down inner join inequality predicates to database")
    public FeaturesConfig setInEqualityJoinPushdownEnabled(boolean inEqualityJoinPushdownEnabled)
    {
        this.inEqualityJoinPushdownEnabled = inEqualityJoinPushdownEnabled;
        return this;
    }

    public boolean isRewriteMinMaxByToTopNEnabled()
    {
        return rewriteMinMaxByToTopNEnabled;
    }

    @Config("optimizer.rewrite-minBy-maxBy-to-topN-enabled")
    @ConfigDescription("Rewrite min_by and max_by to topN")
    public FeaturesConfig setRewriteMinMaxByToTopNEnabled(boolean rewriteMinMaxByToTopNEnabled)
    {
        this.rewriteMinMaxByToTopNEnabled = rewriteMinMaxByToTopNEnabled;
        return this;
    }

    public boolean isBroadcastSemiJoinForDelete()
    {
        return broadcastSemiJoinForDelete;
    }

    @Config("optimizer.broadcast-semi-join-for-delete")
    @ConfigDescription("Enforce broadcast semi join in delete queries")
    public FeaturesConfig setBroadcastSemiJoinForDelete(boolean broadcastSemiJoinForDelete)
    {
        this.broadcastSemiJoinForDelete = broadcastSemiJoinForDelete;
        return this;
    }

    public boolean isInEqualityJoinPushdownEnabled()
    {
        return inEqualityJoinPushdownEnabled;
    }

    public boolean isPrestoSparkExecutionEnvironment()
    {
        return prestoSparkExecutionEnvironment;
    }

    @Config("presto-spark-execution-environment")
    public FeaturesConfig setPrestoSparkExecutionEnvironment(boolean prestoSparkExecutionEnvironment)
    {
        this.prestoSparkExecutionEnvironment = prestoSparkExecutionEnvironment;
        return this;
    }

    public boolean isSingleNodeExecutionEnabled()
    {
        return singleNodeExecutionEnabled;
    }

    @Config("single-node-execution-enabled")
    @ConfigDescription("Enable single node execution")
    public FeaturesConfig setSingleNodeExecutionEnabled(boolean singleNodeExecutionEnabled)
    {
        this.singleNodeExecutionEnabled = singleNodeExecutionEnabled;
        return this;
    }

    public boolean isNativeExecutionScaleWritersThreadsEnabled()
    {
        return nativeExecutionScaleWritersThreadsEnabled;
    }

    @Config("native-execution-scale-writer-threads-enabled")
    public FeaturesConfig setNativeExecutionScaleWritersThreadsEnabled(boolean nativeExecutionScaleWritersThreadsEnabled)
    {
        this.nativeExecutionScaleWritersThreadsEnabled = nativeExecutionScaleWritersThreadsEnabled;
        return this;
    }

    public boolean isNativeExecutionTypeRewriteEnabled()
    {
        return nativeExecutionTypeRewriteEnabled;
    }

    @Config("native-execution-type-rewrite-enabled")
    public FeaturesConfig setNativeExecutionTypeRewriteEnabled(boolean nativeExecutionTypeRewriteEnabled)
    {
        this.nativeExecutionTypeRewriteEnabled = nativeExecutionTypeRewriteEnabled;
        return this;
    }

    public String getExpressionOptimizerName()
    {
        return expressionOptimizerName;
    }

    @Config("expression-optimizer-name")
    @ConfigDescription("Set the expression optimizer name for parsing and analyzing.")
    public FeaturesConfig setExpressionOptimizerName(String expressionOptimizerName)
    {
        this.expressionOptimizerName = expressionOptimizerName;
        return this;
    }

    @Config("exclude-invalid-worker-session-properties")
    @ConfigDescription("Exclude worker session properties from invalid clusters")
    public FeaturesConfig setExcludeInvalidWorkerSessionProperties(boolean setExcludeInvalidWorkerSessionProperties)
    {
        this.setExcludeInvalidWorkerSessionProperties = setExcludeInvalidWorkerSessionProperties;
        return this;
    }

    public boolean isExcludeInvalidWorkerSessionProperties()
    {
        return this.setExcludeInvalidWorkerSessionProperties;
    }

    @Config("optimizer.add-exchange-below-partial-aggregation-over-group-id")
    @ConfigDescription("Enable adding an exchange below partial aggregation over a GroupId node to improve partial aggregation performance")
    public FeaturesConfig setAddExchangeBelowPartialAggregationOverGroupId(boolean addExchangeBelowPartialAggregationOverGroupId)
    {
        this.addExchangeBelowPartialAggregationOverGroupId = addExchangeBelowPartialAggregationOverGroupId;
        return this;
    }

    public boolean getAddExchangeBelowPartialAggregationOverGroupId()
    {
        return addExchangeBelowPartialAggregationOverGroupId;
    }

    @Config("optimizer.add-distinct-below-semi-join-build")
    @ConfigDescription("Add a distinct aggregation below build side of semi join")
    public FeaturesConfig setAddDistinctBelowSemiJoinBuild(boolean addDistinctBelowSemiJoinBuild)
    {
        this.addDistinctBelowSemiJoinBuild = addDistinctBelowSemiJoinBuild;
        return this;
    }

    public boolean isAddDistinctBelowSemiJoinBuild()
    {
        return addDistinctBelowSemiJoinBuild;
    }

    @Config("optimizer.pushdown-subfield-for-map-functions")
    @ConfigDescription("Enable subfield pruning for map functions, currently include map_subset and map_filter")
    public FeaturesConfig setPushdownSubfieldForMapFunctions(boolean pushdownSubfieldForMapFunctions)
    {
        this.pushdownSubfieldForMapFunctions = pushdownSubfieldForMapFunctions;
        return this;
    }

    public boolean isPushdownSubfieldForMapFunctions()
    {
        return pushdownSubfieldForMapFunctions;
    }

    @Config("max_serializable_object_size")
    @ConfigDescription("Configure the maximum byte size of a serializable object in expression interpreters")
    public FeaturesConfig setMaxSerializableObjectSize(long maxSerializableObjectSize)
    {
        this.maxSerializableObjectSize = maxSerializableObjectSize;
        return this;
    }

    public long getMaxSerializableObjectSize()
    {
        return maxSerializableObjectSize;
    }

    @Config("built-in-sidecar-functions-enabled")
    @ConfigDescription("Enable using CPP functions from sidecar over coordinator SQL implementations.")
    public FeaturesConfig setBuiltInSidecarFunctionsEnabled(boolean builtInSidecarFunctionsEnabled)
    {
        this.builtInSidecarFunctionsEnabled = builtInSidecarFunctionsEnabled;
        return this;
    }

    public boolean isBuiltInSidecarFunctionsEnabled()
    {
        return this.builtInSidecarFunctionsEnabled;
    }
}
