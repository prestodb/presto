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
package com.facebook.presto.spark;

import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.Distribution;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.FragmentStatsProvider;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsManager;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsTracker;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.DDLDefinitionTask;
import com.facebook.presto.execution.DataDefinitionTask;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateTimer;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.warnings.WarningCollectorFactory;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.QuerySessionSupplier;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.server.SessionPropertyDefaults;
import com.facebook.presto.server.security.SecurityConfig;
import com.facebook.presto.spark.accesscontrol.PrestoSparkAccessControlChecker;
import com.facebook.presto.spark.accesscontrol.PrestoSparkAuthenticatorProvider;
import com.facebook.presto.spark.accesscontrol.PrestoSparkCredentialsProvider;
import com.facebook.presto.spark.classloader_interface.ExecutionStrategy;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecution;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecutionFactory;
import com.facebook.presto.spark.classloader_interface.PrestoSparkConfInitializer;
import com.facebook.presto.spark.classloader_interface.PrestoSparkExecutionException;
import com.facebook.presto.spark.classloader_interface.PrestoSparkFatalException;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSession;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spark.execution.PrestoSparkAdaptiveQueryExecution;
import com.facebook.presto.spark.execution.PrestoSparkDataDefinitionExecution;
import com.facebook.presto.spark.execution.PrestoSparkExecutionExceptionFactory;
import com.facebook.presto.spark.execution.PrestoSparkStaticQueryExecution;
import com.facebook.presto.spark.execution.task.PrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.planner.PrestoSparkPlanFragmenter;
import com.facebook.presto.spark.planner.PrestoSparkQueryPlanner;
import com.facebook.presto.spark.planner.PrestoSparkQueryPlanner.PlanAndMore;
import com.facebook.presto.spark.planner.PrestoSparkRddFactory;
import com.facebook.presto.spark.planner.optimizers.AdaptivePlanOptimizers;
import com.facebook.presto.spark.util.PrestoSparkTransactionUtils;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.AnalyzerOptions;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.prestospark.PrestoSparkExecutionContext;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.sql.analyzer.BuiltInQueryPreparer;
import com.facebook.presto.sql.analyzer.BuiltInQueryPreparer.BuiltInPreparedQuery;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.utils.StatementUtils;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManager;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.storage.TempStorageManager;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.io.BaseEncoding;
import jakarta.inject.Inject;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.CollectionAccumulator;
import scala.Option;

import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.Session.SessionBuilder;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxExecutionTime;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxRunTime;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isAdaptiveQueryExecutionEnabled;
import static com.facebook.presto.spark.SparkErrorCode.MALFORMED_QUERY_FILE;
import static com.facebook.presto.spark.util.PrestoSparkExecutionUtils.getExecutionSettings;
import static com.facebook.presto.spark.util.PrestoSparkFailureUtils.toPrestoSparkFailure;
import static com.facebook.presto.spark.util.PrestoSparkUtils.createPagesSerde;
import static com.facebook.presto.spark.util.PrestoSparkUtils.getActionResultWithTimeout;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.util.AnalyzerUtil.createAnalyzerOptions;
import static com.facebook.presto.util.Failures.toFailure;
import static com.facebook.presto.util.QueryInfoUtils.toStatementStats;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PrestoSparkQueryExecutionFactory
        implements IPrestoSparkQueryExecutionFactory
{
    private static final Logger log = Logger.get(PrestoSparkQueryExecutionFactory.class);
    public static final String PRESTO_QUERY_ID_CONFIG = "presto_query_id";

    private final QueryIdGenerator queryIdGenerator;
    private final QuerySessionSupplier sessionSupplier;
    private final BuiltInQueryPreparer queryPreparer;
    private final PrestoSparkQueryPlanner queryPlanner;
    private final PrestoSparkAccessControlChecker accessControlChecker;
    private final PrestoSparkPlanFragmenter planFragmenter;
    private final PrestoSparkRddFactory rddFactory;
    private final PrestoSparkMetadataStorage metadataStorage;
    private final QueryMonitor queryMonitor;
    private final Codec<TaskInfo> taskInfoCodec;
    private final JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec;
    private final JsonCodec<PrestoSparkQueryStatusInfo> queryStatusInfoJsonCodec;
    private final JsonCodec<PrestoSparkQueryData> queryDataJsonCodec;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;
    private final BlockEncodingManager blockEncodingManager;
    private final PrestoSparkSettingsRequirements settingsRequirements;
    private final PrestoSparkExecutionExceptionFactory executionExceptionFactory;
    private final PrestoSparkTaskExecutorFactory prestoSparkTaskExecutorFactory;
    private final SessionPropertyDefaults sessionPropertyDefaults;
    private final WarningCollectorFactory warningCollectorFactory;
    private final PartitioningProviderManager partitioningProviderManager;

    private final Set<PrestoSparkCredentialsProvider> credentialsProviders;
    private final Set<PrestoSparkAuthenticatorProvider> authenticatorProviders;
    private final TempStorageManager tempStorageManager;
    private final String storageBasedBroadcastJoinStorage;
    private final NodeMemoryConfig nodeMemoryConfig;
    private final FeaturesConfig featuresConfig;
    private final QueryManagerConfig queryManagerConfig;
    private final SecurityConfig securityConfig;
    private final Set<PrestoSparkServiceWaitTimeMetrics> waitTimeMetrics;
    private final Map<Class<? extends Statement>, DataDefinitionTask<?>> ddlTasks;
    private final Optional<ErrorClassifier> errorClassifier;
    private final HistoryBasedPlanStatisticsTracker historyBasedPlanStatisticsTracker;
    private final AdaptivePlanOptimizers adaptivePlanOptimizers;
    private final FragmentStatsProvider fragmentStatsProvider;
    private final PlanCheckerProviderManager planCheckerProviderManager;

    @Inject
    public PrestoSparkQueryExecutionFactory(
            QueryIdGenerator queryIdGenerator,
            QuerySessionSupplier sessionSupplier,
            BuiltInQueryPreparer queryPreparer,
            PrestoSparkQueryPlanner queryPlanner,
            PrestoSparkAccessControlChecker accessControlChecker,
            PrestoSparkPlanFragmenter planFragmenter,
            PrestoSparkRddFactory rddFactory,
            PrestoSparkMetadataStorage metadataStorage,
            QueryMonitor queryMonitor,
            Codec<TaskInfo> taskInfoCodec,
            JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec,
            JsonCodec<PrestoSparkQueryStatusInfo> queryStatusInfoJsonCodec,
            JsonCodec<PrestoSparkQueryData> queryDataJsonCodec,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Metadata metadata,
            BlockEncodingManager blockEncodingManager,
            PrestoSparkSettingsRequirements settingsRequirements,
            PrestoSparkExecutionExceptionFactory executionExceptionFactory,
            PrestoSparkTaskExecutorFactory prestoSparkTaskExecutorFactory,
            SessionPropertyDefaults sessionPropertyDefaults,
            WarningCollectorFactory warningCollectorFactory,
            PartitioningProviderManager partitioningProviderManager,
            Set<PrestoSparkCredentialsProvider> credentialsProviders,
            Set<PrestoSparkAuthenticatorProvider> authenticatorProviders,
            TempStorageManager tempStorageManager,
            PrestoSparkConfig prestoSparkConfig,
            NodeMemoryConfig nodeMemoryConfig,
            FeaturesConfig featuresConfig,
            QueryManagerConfig queryManagerConfig,
            SecurityConfig securityConfig,
            Set<PrestoSparkServiceWaitTimeMetrics> waitTimeMetrics,
            Map<Class<? extends Statement>, DataDefinitionTask<?>> ddlTasks,
            Optional<ErrorClassifier> errorClassifier,
            HistoryBasedPlanStatisticsManager historyBasedPlanStatisticsManager,
            AdaptivePlanOptimizers adaptivePlanOptimizers,
            FragmentStatsProvider fragmentStatsProvider,
            PlanCheckerProviderManager planCheckerProviderManager)
    {
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
        this.queryPlanner = requireNonNull(queryPlanner, "queryPlanner is null");
        this.accessControlChecker = requireNonNull(accessControlChecker, "accessControlChecker is null");
        this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
        this.rddFactory = requireNonNull(rddFactory, "rddFactory is null");
        this.metadataStorage = requireNonNull(metadataStorage, "metadataStorage is null");
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        this.sparkTaskDescriptorJsonCodec = requireNonNull(sparkTaskDescriptorJsonCodec, "sparkTaskDescriptorJsonCodec is null");
        this.queryStatusInfoJsonCodec = requireNonNull(queryStatusInfoJsonCodec, "queryStatusInfoJsonCodec is null");
        this.queryDataJsonCodec = requireNonNull(queryDataJsonCodec, "queryDataJsonCodec is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.blockEncodingManager = requireNonNull(blockEncodingManager, "blockEncodingManager is null");
        this.settingsRequirements = requireNonNull(settingsRequirements, "settingsRequirements is null");
        this.executionExceptionFactory = requireNonNull(executionExceptionFactory, "executionExceptionFactory is null");
        this.prestoSparkTaskExecutorFactory = requireNonNull(prestoSparkTaskExecutorFactory, "prestoSparkTaskExecutorFactory is null");
        this.sessionPropertyDefaults = requireNonNull(sessionPropertyDefaults, "sessionPropertyDefaults is null");
        this.warningCollectorFactory = requireNonNull(warningCollectorFactory, "warningCollectorFactory is null");
        this.partitioningProviderManager = requireNonNull(partitioningProviderManager, "partitioningProviderManager is null");
        this.credentialsProviders = ImmutableSet.copyOf(requireNonNull(credentialsProviders, "credentialsProviders is null"));
        this.authenticatorProviders = ImmutableSet.copyOf(requireNonNull(authenticatorProviders, "authenticatorProviders is null"));
        this.tempStorageManager = requireNonNull(tempStorageManager, "tempStorageManager is null");
        this.storageBasedBroadcastJoinStorage = requireNonNull(prestoSparkConfig, "prestoSparkConfig is null").getStorageBasedBroadcastJoinStorage();
        this.nodeMemoryConfig = requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null");
        this.featuresConfig = requireNonNull(featuresConfig, "featuresConfig is null");
        this.queryManagerConfig = requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.securityConfig = requireNonNull(securityConfig, "securityConfig is null");
        this.waitTimeMetrics = ImmutableSet.copyOf(requireNonNull(waitTimeMetrics, "waitTimeMetrics is null"));
        this.ddlTasks = ImmutableMap.copyOf(requireNonNull(ddlTasks, "ddlTasks is null"));
        this.errorClassifier = requireNonNull(errorClassifier, "errorClassifier is null");
        this.historyBasedPlanStatisticsTracker = requireNonNull(historyBasedPlanStatisticsManager, "historyBasedPlanStatisticsManager is null").getHistoryBasedPlanStatisticsTracker();
        this.adaptivePlanOptimizers = requireNonNull(adaptivePlanOptimizers, "adaptivePlanOptimizers is null");
        this.fragmentStatsProvider = requireNonNull(fragmentStatsProvider, "fragmentStatsProvider is null");
        this.planCheckerProviderManager = requireNonNull(planCheckerProviderManager, "planCheckerProviderManager is null");
    }

    public static QueryInfo createQueryInfo(
            Session session,
            String query,
            QueryState queryState,
            Optional<PlanAndMore> planAndMore,
            Optional<String> sparkQueueName,
            Optional<ExecutionFailureInfo> failureInfo,
            QueryStateTimer queryStateTimer,
            Optional<StageInfo> rootStage,
            WarningCollector warningCollector)
    {
        checkArgument(failureInfo.isPresent() || queryState != FAILED, "unexpected query state: %s", queryState);

        int peakRunningTasks = 0;
        long peakUserMemoryReservationInBytes = 0;
        long peakTotalMemoryReservationInBytes = 0;
        long peakTaskUserMemoryInBytes = 0;
        long peakTaskTotalMemoryInBytes = 0;
        long peakNodeTotalMemoryInBytes = 0;

        List<StageInfo> allStages = getAllStages(rootStage);

        for (StageInfo stageInfo : allStages) {
            StageExecutionInfo stageExecutionInfo = stageInfo.getLatestAttemptExecutionInfo();
            for (TaskInfo taskInfo : stageExecutionInfo.getTasks()) {
                // there's no way to know how many tasks were running in parallel in Spark
                // for now let's assume that all the tasks were running in parallel
                peakRunningTasks++;
                long taskPeakUserMemoryInBytes = taskInfo.getStats().getPeakUserMemoryInBytes();
                long taskPeakTotalMemoryInBytes = taskInfo.getStats().getPeakTotalMemoryInBytes();
                peakUserMemoryReservationInBytes += taskPeakUserMemoryInBytes;
                peakTotalMemoryReservationInBytes += taskPeakTotalMemoryInBytes;
                peakTaskUserMemoryInBytes = max(peakTaskUserMemoryInBytes, taskPeakUserMemoryInBytes);
                peakTaskTotalMemoryInBytes = max(peakTaskTotalMemoryInBytes, taskPeakTotalMemoryInBytes);
                peakNodeTotalMemoryInBytes = max(taskInfo.getStats().getPeakNodeTotalMemoryInBytes(), peakNodeTotalMemoryInBytes);
            }
        }

        QueryStats queryStats = QueryStats.create(
                queryStateTimer,
                rootStage,
                allStages,
                peakRunningTasks,
                peakUserMemoryReservationInBytes,
                peakTotalMemoryReservationInBytes,
                peakTaskUserMemoryInBytes,
                peakTaskTotalMemoryInBytes,
                peakNodeTotalMemoryInBytes,
                session.getRuntimeStats());

        Optional<PrestoSparkExecutionContext> prestoSparkExecutionContext = Optional.empty();
        if (planAndMore.isPresent()) {
            prestoSparkExecutionContext = Optional.of(
                    PrestoSparkExecutionContext.create(
                            planAndMore.get().getPhysicalResourceSettings().getHashPartitionCount(),
                            planAndMore.get().getPhysicalResourceSettings().getMaxExecutorCount(),
                            planAndMore.get().getPhysicalResourceSettings().isHashPartitionCountAutoTuned(),
                            planAndMore.get().getPhysicalResourceSettings().isMaxExecutorCountAutoTuned()));
        }

        return new QueryInfo(
                session.getQueryId(),
                session.toSessionRepresentation(),
                queryState,
                new MemoryPoolId("spark-memory-pool"),
                queryStats.isScheduled(),
                URI.create("http://fake.invalid/query/" + session.getQueryId()),
                planAndMore.map(PlanAndMore::getFieldNames).orElse(ImmutableList.of()),
                query,
                Optional.empty(),
                Optional.empty(),
                queryStats,
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                planAndMore.flatMap(PlanAndMore::getUpdateType).orElse(null),
                rootStage,
                failureInfo.orElse(null),
                failureInfo.map(ExecutionFailureInfo::getErrorCode).orElse(null),
                warningCollector.getWarnings(),
                planAndMore.map(PlanAndMore::getInputs).orElse(ImmutableSet.of()),
                planAndMore.flatMap(PlanAndMore::getOutput),
                queryState.isDone(),
                sparkQueueName.map(ResourceGroupId::new),
                planAndMore.flatMap(PlanAndMore::getQueryType),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                planAndMore.map(PlanAndMore::getPlan).map(Plan::getStatsAndCosts).orElseGet(StatsAndCosts::empty),
                session.getOptimizerInformationCollector().getOptimizationInfo(),
                ImmutableList.of(),
                planAndMore.map(PlanAndMore::getInvokedScalarFunctions).orElseGet(ImmutableSet::of),
                planAndMore.map(PlanAndMore::getInvokedAggregateFunctions).orElseGet(ImmutableSet::of),
                planAndMore.map(PlanAndMore::getInvokedWindowFunctions).orElseGet(ImmutableSet::of),
                planAndMore.map(PlanAndMore::getPlanCanonicalInfo).orElseGet(ImmutableList::of),
                planAndMore.map(PlanAndMore::getPlan).map(Plan::getPlanIdNodeMap).orElseGet(ImmutableMap::of),
                prestoSparkExecutionContext);
    }

    public static StageInfo createStageInfo(QueryId queryId, SubPlan plan, List<TaskInfo> taskInfos)
    {
        ListMultimap<PlanFragmentId, TaskInfo> taskInfoMap = ArrayListMultimap.create();
        for (TaskInfo taskInfo : taskInfos) {
            PlanFragmentId fragmentId = new PlanFragmentId(taskInfo.getTaskId().getStageExecutionId().getStageId().getId());
            taskInfoMap.put(fragmentId, taskInfo);
        }
        return createStageInfo(queryId, plan, taskInfoMap);
    }

    public static StageInfo createStageInfo(QueryId queryId, SubPlan plan, ListMultimap<PlanFragmentId, TaskInfo> taskInfoMap)
    {
        PlanFragmentId planFragmentId = plan.getFragment().getId();
        StageId stageId = new StageId(queryId, planFragmentId.getId());
        List<TaskInfo> taskInfos = taskInfoMap.get(planFragmentId);
        long peakUserMemoryReservationInBytes = 0;
        long peakNodeTotalMemoryReservationInBytes = 0;
        for (TaskInfo taskInfo : taskInfos) {
            long taskPeakUserMemoryInBytes = taskInfo.getStats().getUserMemoryReservationInBytes();
            peakUserMemoryReservationInBytes += taskPeakUserMemoryInBytes;
            peakNodeTotalMemoryReservationInBytes = max(taskInfo.getStats().getPeakNodeTotalMemoryInBytes(), peakNodeTotalMemoryReservationInBytes);
        }
        StageExecutionInfo stageExecutionInfo = StageExecutionInfo.create(
                new StageExecutionId(stageId, 0),
                // TODO: figure out a way to know what exactly stage has caused a failure
                StageExecutionState.FINISHED,
                Optional.empty(),
                taskInfos,
                System.currentTimeMillis(),
                new Distribution().snapshot(),
                new RuntimeStats(),
                peakUserMemoryReservationInBytes,
                peakNodeTotalMemoryReservationInBytes,
                1,
                1);
        return new StageInfo(
                stageId,
                URI.create("http://fake.invalid/stage/" + stageId),
                Optional.of(plan.getFragment()),
                stageExecutionInfo,
                ImmutableList.of(),
                plan.getChildren().stream()
                        .map(child -> createStageInfo(queryId, child, taskInfoMap))
                        .collect(toImmutableList()),
                false);
    }

    public static PrestoSparkQueryStatusInfo createPrestoSparkQueryInfo(
            QueryInfo queryInfo,
            Optional<PlanAndMore> planAndMore,
            WarningCollector warningCollector,
            OptionalLong updateCount)
    {
        StatementStats stats = toStatementStats(queryInfo);

        // nullify stage stats to keep the object slim
        stats = new StatementStats(
                stats.getState(),
                stats.isWaitingForPrerequisites(),
                stats.isQueued(),
                stats.isScheduled(),
                stats.getNodes(),
                stats.getTotalSplits(),
                stats.getQueuedSplits(),
                stats.getRunningSplits(),
                stats.getCompletedSplits(),
                stats.getCpuTimeMillis(),
                stats.getWallTimeMillis(),
                stats.getWaitingForPrerequisitesTimeMillis(),
                stats.getQueuedTimeMillis(),
                stats.getElapsedTimeMillis(),
                stats.getProcessedRows(),
                stats.getProcessedBytes(),
                stats.getPeakMemoryBytes(),
                stats.getPeakTotalMemoryBytes(),
                stats.getPeakTaskTotalMemoryBytes(),
                stats.getSpilledBytes(),
                null,
                null);

        return new PrestoSparkQueryStatusInfo(
                queryInfo.getQueryId().getId(),
                planAndMore.map(PrestoSparkQueryExecutionFactory::getOutputColumns),
                stats,
                Optional.ofNullable(queryInfo.getFailureInfo()).map(PrestoSparkQueryExecutionFactory::toQueryError),
                warningCollector.getWarnings(),
                planAndMore.flatMap(PlanAndMore::getUpdateType),
                updateCount);
    }

    public static List<Column> getOutputColumns(PlanAndMore planAndMore)
    {
        ImmutableList.Builder<Column> result = ImmutableList.builder();
        List<String> columnNames = planAndMore.getFieldNames();
        List<Type> columnTypes = planAndMore.getPlan().getRoot().getOutputVariables().stream()
                .map(VariableReferenceExpression::getType)
                .collect(toImmutableList());
        checkArgument(
                columnNames.size() == columnTypes.size(),
                "Column names and types size mismatch: %s != %s",
                columnNames.size(),
                columnTypes.size());
        for (int i = 0; i < columnNames.size(); i++) {
            result.add(new Column(columnNames.get(i), columnTypes.get(i)));
        }
        return result.build();
    }

    public static <T> void waitForActionsCompletionWithTimeout(Collection<JavaFutureAction<T>> actions, long timeout, TimeUnit timeUnit, Set<PrestoSparkServiceWaitTimeMetrics> waitTimeMetrics)
            throws SparkException, TimeoutException
    {
        long deadline = System.currentTimeMillis() + timeUnit.toMillis(timeout);

        try {
            for (JavaFutureAction<?> action : actions) {
                long nextTimeoutInMillis = deadline - System.currentTimeMillis();
                if (nextTimeoutInMillis <= 0) {
                    throw new TimeoutException();
                }
                getActionResultWithTimeout(action, nextTimeoutInMillis, MILLISECONDS, waitTimeMetrics);
            }
        }
        finally {
            for (JavaFutureAction<?> action : actions) {
                if (!action.isDone()) {
                    action.cancel(true);
                }
            }
        }
    }

    private static QueryError toQueryError(ExecutionFailureInfo executionFailureInfo)
    {
        ErrorCode errorCode;
        if (executionFailureInfo.getErrorCode() != null) {
            errorCode = executionFailureInfo.getErrorCode();
        }
        else {
            errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
        }

        return new QueryError(
                firstNonNull(executionFailureInfo.getMessage(), "Internal error"),
                null,
                errorCode.getCode(),
                errorCode.getName(),
                errorCode.getType().toString(),
                errorCode.isRetriable(),
                executionFailureInfo.getErrorLocation(),
                executionFailureInfo.toFailureInfo());
    }

    @Override
    public IPrestoSparkQueryExecution create(
            SparkContext sparkContext,
            PrestoSparkSession prestoSparkSession,
            Optional<String> sqlText,
            Optional<String> sqlLocation,
            Optional<String> sqlFileHexHash,
            Optional<String> sqlFileSizeInBytes,
            Optional<String> sparkQueueName,
            PrestoSparkTaskExecutorFactoryProvider executorFactoryProvider,
            Optional<String> queryStatusInfoOutputLocation,
            Optional<String> queryDataOutputLocation,
            List<ExecutionStrategy> executionStrategies,
            Optional<CollectionAccumulator<Map<String, Long>>> bootstrapMetricsCollector)
    {
        PrestoSparkConfInitializer.checkInitialized(sparkContext);

        String sql;
        if (sqlText.isPresent()) {
            checkArgument(!sqlLocation.isPresent(), "sqlText and sqlLocation should not be set at the same time");
            sql = sqlText.get();
        }
        else {
            checkArgument(sqlLocation.isPresent(), "sqlText or sqlLocation must be present");
            byte[] sqlFileBytes = metadataStorage.read(sqlLocation.get());
            if (sqlFileSizeInBytes.isPresent()) {
                if (Integer.valueOf(sqlFileSizeInBytes.get()) != sqlFileBytes.length) {
                    throw new PrestoException(
                            MALFORMED_QUERY_FILE,
                            format("sql file size %s is different from expected sqlFileSizeInBytes %s", sqlFileBytes.length, sqlFileSizeInBytes.get()));
                }
            }
            if (sqlFileHexHash.isPresent()) {
                try {
                    MessageDigest md = MessageDigest.getInstance("SHA-512");
                    String actualHexHashCode = BaseEncoding.base16().lowerCase().encode(md.digest(sqlFileBytes));
                    if (!sqlFileHexHash.get().equals(actualHexHashCode)) {
                        throw new PrestoException(
                                MALFORMED_QUERY_FILE,
                                format("actual hash code %s is different from expected sqlFileHexHash %s", actualHexHashCode, sqlFileHexHash.get()));
                    }
                }
                catch (NoSuchAlgorithmException e) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "unsupported hash algorithm", e);
                }
            }
            sql = new String(sqlFileBytes, UTF_8);
        }

        log.info("Query: %s", sql);

        QueryStateTimer queryStateTimer = new QueryStateTimer(systemTicker());

        queryStateTimer.beginPlanning();

        QueryId queryId = queryIdGenerator.createNextQueryId();
        log.info("Starting execution for presto query: %s", queryId);
        System.out.printf("Query id: %s\n", queryId);

        sparkContext.conf().set(PRESTO_QUERY_ID_CONFIG, queryId.getId());

        SessionContext sessionContext = PrestoSparkSessionContext.createFromSessionInfo(
                prestoSparkSession,
                credentialsProviders,
                authenticatorProviders,
                sql);

        SessionBuilder sessionBuilder = sessionSupplier.createSessionBuilder(queryId, sessionContext, warningCollectorFactory);
        sessionPropertyDefaults.applyDefaultProperties(sessionBuilder, Optional.empty(), Optional.empty());

        if (!executionStrategies.isEmpty()) {
            log.info("Going to run with following strategies: %s", executionStrategies);
            PrestoSparkExecutionSettings prestoSparkExecutionSettings = getExecutionSettings(executionStrategies, sessionBuilder.build());

            // Update Spark setting in SparkConf, if present
            prestoSparkExecutionSettings.getSparkConfigProperties().forEach(sparkContext.conf()::set);

            // Update Presto settings in Session, if present
            transferSessionPropertiesToSession(sessionBuilder, prestoSparkExecutionSettings.getPrestoSessionProperties());

            Set<String> clientTags = new HashSet<>(sessionBuilder.getClientTags());
            executionStrategies.forEach(s -> clientTags.add(s.name()));
            sessionBuilder.setClientTags(clientTags);
        }

        WarningCollector warningCollector = sessionBuilder.getWarningCollector();
        Session session = sessionBuilder.build();

        PlanAndMore planAndMore = null;
        try {
            TransactionId transactionId = transactionManager.beginTransaction(true);
            session = session.beginTransactionId(transactionId, transactionManager, accessControl);

            queryMonitor.queryCreatedEvent(
                    new BasicQueryInfo(createQueryInfo(
                            session,
                            sql,
                            PLANNING,
                            Optional.empty(),
                            sparkQueueName,
                            Optional.empty(),
                            queryStateTimer,
                            Optional.empty(),
                            warningCollector)));

            // including queueing time
            Duration queryMaxRunTime = getQueryMaxRunTime(session);
            // excluding queueing time
            Duration queryMaxExecutionTime = getQueryMaxExecutionTime(session);
            // pick a smaller one as we are not tracking queueing for Presto on Spark
            Duration queryTimeout = queryMaxRunTime.compareTo(queryMaxExecutionTime) < 0 ? queryMaxRunTime : queryMaxExecutionTime;

            long queryCompletionDeadline = System.currentTimeMillis() + queryTimeout.toMillis();

            settingsRequirements.verify(sparkContext, session);

            queryStateTimer.beginAnalyzing();

            AnalyzerOptions analyzerOptions = createAnalyzerOptions(session, warningCollector);
            BuiltInPreparedQuery preparedQuery = queryPreparer.prepareQuery(analyzerOptions, sql, session.getPreparedStatements(), warningCollector);
            Optional<QueryType> queryType = StatementUtils.getQueryType(preparedQuery.getStatement().getClass());
            if (queryType.isPresent() && (queryType.get() == QueryType.DATA_DEFINITION || queryType.get() == QueryType.CONTROL)) {
                queryStateTimer.endAnalysis();
                DDLDefinitionTask<?> task = (DDLDefinitionTask<?>) ddlTasks.get(preparedQuery.getStatement().getClass());
                return new PrestoSparkDataDefinitionExecution(task, preparedQuery.getStatement(), transactionManager, accessControl, metadata, session, queryStateTimer, warningCollector, sql);
            }
            else if (preparedQuery.isExplainTypeValidate()) {
                return accessControlChecker.createExecution(session, preparedQuery, queryStateTimer, warningCollector, sql);
            }
            else {
                VariableAllocator variableAllocator = new VariableAllocator();
                PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
                planAndMore = queryPlanner.createQueryPlan(session, preparedQuery, warningCollector, variableAllocator, planNodeIdAllocator, sparkContext, sql);
                JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
                CollectionAccumulator<SerializedTaskInfo> taskInfoCollector = new CollectionAccumulator<>();
                taskInfoCollector.register(sparkContext, Option.empty(), true);
                CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector = new CollectionAccumulator<>();
                shuffleStatsCollector.register(sparkContext, Option.empty(), false);
                TempStorage tempStorage = tempStorageManager.getTempStorage(storageBasedBroadcastJoinStorage);
                queryStateTimer.endAnalysis();

                if (!isAdaptiveQueryExecutionEnabled(session)) {
                    return new PrestoSparkStaticQueryExecution(
                            javaSparkContext,
                            session,
                            queryMonitor,
                            taskInfoCollector,
                            shuffleStatsCollector,
                            prestoSparkTaskExecutorFactory,
                            executorFactoryProvider,
                            queryStateTimer,
                            warningCollector,
                            sql,
                            planAndMore,
                            sparkQueueName,
                            taskInfoCodec,
                            sparkTaskDescriptorJsonCodec,
                            queryStatusInfoJsonCodec,
                            queryDataJsonCodec,
                            rddFactory,
                            transactionManager,
                            createPagesSerde(blockEncodingManager),
                            executionExceptionFactory,
                            queryTimeout,
                            queryCompletionDeadline,
                            metadataStorage,
                            queryStatusInfoOutputLocation,
                            queryDataOutputLocation,
                            tempStorage,
                            nodeMemoryConfig,
                            featuresConfig,
                            queryManagerConfig,
                            waitTimeMetrics,
                            errorClassifier,
                            planFragmenter,
                            metadata,
                            partitioningProviderManager,
                            historyBasedPlanStatisticsTracker,
                            bootstrapMetricsCollector);
                }
                else {
                    return new PrestoSparkAdaptiveQueryExecution(
                            javaSparkContext,
                            session,
                            queryMonitor,
                            taskInfoCollector,
                            shuffleStatsCollector,
                            prestoSparkTaskExecutorFactory,
                            executorFactoryProvider,
                            queryStateTimer,
                            warningCollector,
                            sql,
                            planAndMore,
                            sparkQueueName,
                            taskInfoCodec,
                            sparkTaskDescriptorJsonCodec,
                            queryStatusInfoJsonCodec,
                            queryDataJsonCodec,
                            rddFactory,
                            transactionManager,
                            createPagesSerde(blockEncodingManager),
                            executionExceptionFactory,
                            queryTimeout,
                            queryCompletionDeadline,
                            metadataStorage,
                            queryStatusInfoOutputLocation,
                            queryDataOutputLocation,
                            tempStorage,
                            nodeMemoryConfig,
                            featuresConfig,
                            queryManagerConfig,
                            waitTimeMetrics,
                            errorClassifier,
                            planFragmenter,
                            metadata,
                            partitioningProviderManager,
                            historyBasedPlanStatisticsTracker,
                            adaptivePlanOptimizers,
                            variableAllocator,
                            planNodeIdAllocator,
                            fragmentStatsProvider,
                            bootstrapMetricsCollector,
                            planCheckerProviderManager);
                }
            }
        }
        catch (Throwable executionFailure) {
            queryStateTimer.beginFinishing();
            try {
                PrestoSparkTransactionUtils.rollback(session, transactionManager);
            }
            catch (RuntimeException rollbackFailure) {
                log.error(rollbackFailure, "Encountered error when performing rollback");
            }
            queryStateTimer.endQuery();

            Optional<ExecutionFailureInfo> failureInfo = Optional.empty();
            if (executionFailure instanceof PrestoSparkExecutionException) {
                failureInfo = executionExceptionFactory.extractExecutionFailureInfo((PrestoSparkExecutionException) executionFailure);
                verify(failureInfo.isPresent());
            }
            if (!failureInfo.isPresent()) {
                failureInfo = Optional.of(toFailure(executionFailure));
            }

            try {
                QueryInfo queryInfo = createQueryInfo(
                        session,
                        sql,
                        FAILED,
                        Optional.ofNullable(planAndMore),
                        sparkQueueName,
                        failureInfo,
                        queryStateTimer,
                        Optional.empty(),
                        warningCollector);
                queryMonitor.queryCompletedEvent(queryInfo);
                if (queryStatusInfoOutputLocation.isPresent()) {
                    PrestoSparkQueryStatusInfo prestoSparkQueryStatusInfo = createPrestoSparkQueryInfo(
                            queryInfo,
                            Optional.ofNullable(planAndMore),
                            warningCollector,
                            OptionalLong.empty());
                    metadataStorage.write(
                            queryStatusInfoOutputLocation.get(),
                            queryStatusInfoJsonCodec.toJsonBytes(prestoSparkQueryStatusInfo));
                }
            }
            catch (RuntimeException eventFailure) {
                log.error(eventFailure, "Error publishing query immediate failure event");
            }

            if (isFatalException(executionFailure)) {
                // Throw fatal error directly to allow spark fail over to other executors.
                throw executionFailure;
            }
            else {
                throw toPrestoSparkFailure(session, failureInfo.get());
            }
        }
    }

    private boolean isFatalException(Throwable t)
    {
        return t instanceof PrestoSparkFatalException;
    }

    @VisibleForTesting
    static Session.SessionBuilder transferSessionPropertiesToSession(Session.SessionBuilder session, Map<String, String> sessionProperties)
    {
        sessionProperties.forEach((key, value) -> {
            // Presto session properties may also contain catalog properties in format catalog.property_name=value
            String[] parts = key.split("\\.");
            if (parts.length == 1) {
                // system property
                session.setSystemProperty(parts[0], value);
            }
            else if (parts.length == 2) {
                // catalog property
                session.setCatalogSessionProperty(parts[0], parts[1], value);
            }
            else {
                throw new PrestoException(INVALID_SESSION_PROPERTY, "Unable to parse session property: " + key);
            }
        });

        return session;
    }
}
