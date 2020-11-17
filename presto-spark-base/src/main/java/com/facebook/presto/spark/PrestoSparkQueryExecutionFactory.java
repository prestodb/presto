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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.Distribution;
import com.facebook.presto.Session;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryPreparer;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateTimer;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget;
import com.facebook.presto.execution.scheduler.StreamingPlanSection;
import com.facebook.presto.execution.scheduler.StreamingSubPlan;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.execution.warnings.WarningCollectorFactory;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.QuerySessionSupplier;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.server.SessionPropertyDefaults;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecution;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecutionFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutor;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkConfInitializer;
import com.facebook.presto.spark.classloader_interface.PrestoSparkExecutionException;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSession;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats.Operation;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskInputs;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spark.execution.PrestoSparkExecutionExceptionFactory;
import com.facebook.presto.spark.execution.PrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.planner.PrestoSparkPlanFragmenter;
import com.facebook.presto.spark.planner.PrestoSparkQueryPlanner;
import com.facebook.presto.spark.planner.PrestoSparkQueryPlanner.PlanAndMore;
import com.facebook.presto.spark.planner.PrestoSparkRddFactory;
import com.facebook.presto.spark.util.PrestoSparkUtils;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import org.joda.time.DateTime;
import scala.Some;
import scala.Tuple2;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.ExceededMemoryLimitException.exceededLocalBroadcastMemoryLimit;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxBroadcastMemory;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxExecutionTime;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxRunTime;
import static com.facebook.presto.SystemSessionProperties.getWarningHandlingLevel;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.execution.scheduler.StreamingPlanSection.extractStreamingSections;
import static com.facebook.presto.execution.scheduler.TableWriteInfo.createTableWriteInfo;
import static com.facebook.presto.server.protocol.QueryResourceUtil.toStatementStats;
import static com.facebook.presto.spark.SparkErrorCode.EXCEEDED_SPARK_DRIVER_MAX_RESULT_SIZE;
import static com.facebook.presto.spark.SparkErrorCode.GENERIC_SPARK_ERROR;
import static com.facebook.presto.spark.SparkErrorCode.SPARK_EXECUTOR_LOST;
import static com.facebook.presto.spark.SparkErrorCode.SPARK_EXECUTOR_OOM;
import static com.facebook.presto.spark.classloader_interface.ScalaUtils.collectScalaIterator;
import static com.facebook.presto.spark.classloader_interface.ScalaUtils.emptyScalaIterator;
import static com.facebook.presto.spark.util.PrestoSparkUtils.createPagesSerde;
import static com.facebook.presto.spark.util.PrestoSparkUtils.toSerializedPage;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_PAGE_SINK_COMMIT;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textDistributedPlan;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagateIfPossible;
import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.nio.file.Files.notExists;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class PrestoSparkQueryExecutionFactory
        implements IPrestoSparkQueryExecutionFactory
{
    private static final Logger log = Logger.get(PrestoSparkQueryExecutionFactory.class);

    private final QueryIdGenerator queryIdGenerator;
    private final QuerySessionSupplier sessionSupplier;
    private final QueryPreparer queryPreparer;
    private final PrestoSparkQueryPlanner queryPlanner;
    private final PrestoSparkPlanFragmenter planFragmenter;
    private final PrestoSparkRddFactory rddFactory;
    private final QueryMonitor queryMonitor;
    private final JsonCodec<TaskInfo> taskInfoJsonCodec;
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

    private final Set<PrestoSparkCredentialsProvider> credentialsProviders;
    private final Set<PrestoSparkAuthenticatorProvider> authenticatorProviders;

    @Inject
    public PrestoSparkQueryExecutionFactory(
            QueryIdGenerator queryIdGenerator,
            QuerySessionSupplier sessionSupplier,
            QueryPreparer queryPreparer,
            PrestoSparkQueryPlanner queryPlanner,
            PrestoSparkPlanFragmenter planFragmenter,
            PrestoSparkRddFactory rddFactory,
            QueryMonitor queryMonitor,
            JsonCodec<TaskInfo> taskInfoJsonCodec,
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
            Set<PrestoSparkCredentialsProvider> credentialsProviders,
            Set<PrestoSparkAuthenticatorProvider> authenticatorProviders)
    {
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
        this.queryPlanner = requireNonNull(queryPlanner, "queryPlanner is null");
        this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
        this.rddFactory = requireNonNull(rddFactory, "rddFactory is null");
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.taskInfoJsonCodec = requireNonNull(taskInfoJsonCodec, "taskInfoJsonCodec is null");
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
        this.credentialsProviders = ImmutableSet.copyOf(requireNonNull(credentialsProviders, "credentialsProviders is null"));
        this.authenticatorProviders = ImmutableSet.copyOf(requireNonNull(authenticatorProviders, "authenticatorProviders is null"));
    }

    @Override
    public IPrestoSparkQueryExecution create(
            SparkContext sparkContext,
            PrestoSparkSession prestoSparkSession,
            String sql,
            Optional<String> sparkQueueName,
            PrestoSparkTaskExecutorFactoryProvider executorFactoryProvider,
            Optional<Path> queryStatusInfoOutputPath,
            Optional<Path> queryDataOutputPath)
    {
        PrestoSparkConfInitializer.checkInitialized(sparkContext);

        queryStatusInfoOutputPath.ifPresent(path -> checkArgument(notExists(path), "File already exist: %s", path));
        queryDataOutputPath.ifPresent(path -> checkArgument(notExists(path), "File already exist: %s", path));

        QueryStateTimer queryStateTimer = new QueryStateTimer(systemTicker());

        queryStateTimer.beginPlanning();

        QueryId queryId = queryIdGenerator.createNextQueryId();
        log.info("Starting execution for presto query: %s", queryId);
        SessionContext sessionContext = PrestoSparkSessionContext.createFromSessionInfo(
                prestoSparkSession,
                credentialsProviders,
                authenticatorProviders);

        Session session = sessionSupplier.createSession(queryId, sessionContext);

        session = sessionPropertyDefaults.newSessionWithDefaultProperties(session, Optional.empty(), Optional.empty());

        WarningCollector warningCollector = warningCollectorFactory.create(getWarningHandlingLevel(session));

        TransactionId transactionId = transactionManager.beginTransaction(true);
        session = session.beginTransactionId(transactionId, transactionManager, accessControl);

        // including queueing time
        Duration queryMaxRunTime = getQueryMaxRunTime(session);
        // excluding queueing time
        Duration queryMaxExecutionTime = getQueryMaxExecutionTime(session);
        // pick a smaller one as we are not tracking queueing for Presto on Spark
        Duration queryTimeout = queryMaxRunTime.compareTo(queryMaxExecutionTime) < 0 ? queryMaxRunTime : queryMaxExecutionTime;

        long queryCompletionDeadline = System.currentTimeMillis() + queryTimeout.toMillis();

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

        PlanAndMore planAndMore = null;
        try {
            settingsRequirements.verify(sparkContext, session);

            queryStateTimer.beginAnalyzing();

            PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, sql, warningCollector);
            planAndMore = queryPlanner.createQueryPlan(session, preparedQuery, warningCollector);
            SubPlan fragmentedPlan = planFragmenter.fragmentQueryPlan(session, planAndMore.getPlan(), warningCollector);
            log.info(textDistributedPlan(fragmentedPlan, metadata.getFunctionAndTypeManager(), session, true));
            TableWriteInfo tableWriteInfo = getTableWriteInfo(session, fragmentedPlan);

            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
            CollectionAccumulator<SerializedTaskInfo> taskInfoCollector = new CollectionAccumulator<>();
            taskInfoCollector.register(sparkContext, new Some<>("taskInfoCollector"), false);
            CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector = new CollectionAccumulator<>();
            shuffleStatsCollector.register(sparkContext, new Some<>("shuffleStatsCollector"), false);

            queryStateTimer.endAnalysis();

            return new PrestoSparkQueryExecution(
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
                    fragmentedPlan,
                    sparkQueueName,
                    taskInfoJsonCodec,
                    sparkTaskDescriptorJsonCodec,
                    queryStatusInfoJsonCodec,
                    queryDataJsonCodec,
                    rddFactory,
                    tableWriteInfo,
                    transactionManager,
                    createPagesSerde(blockEncodingManager),
                    executionExceptionFactory,
                    queryTimeout,
                    queryCompletionDeadline,
                    queryStatusInfoOutputPath,
                    queryDataOutputPath);
        }
        catch (Throwable executionFailure) {
            queryStateTimer.beginFinishing();
            try {
                rollback(session, transactionManager);
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
                if (queryStatusInfoOutputPath.isPresent()) {
                    PrestoSparkQueryStatusInfo prestoSparkQueryStatusInfo = createPrestoSparkQueryInfo(
                            queryInfo,
                            Optional.ofNullable(planAndMore),
                            warningCollector,
                            OptionalLong.empty());
                    writeJsonFile(
                            queryStatusInfoOutputPath.get(),
                            prestoSparkQueryStatusInfo,
                            queryStatusInfoJsonCodec);
                }
            }
            catch (RuntimeException eventFailure) {
                log.error(eventFailure, "Error publishing query immediate failure event");
            }

            throw failureInfo.get().toFailure();
        }
    }

    private TableWriteInfo getTableWriteInfo(Session session, SubPlan plan)
    {
        StreamingPlanSection streamingPlanSection = extractStreamingSections(plan);
        StreamingSubPlan streamingSubPlan = streamingPlanSection.getPlan();
        TableWriteInfo tableWriteInfo = createTableWriteInfo(streamingSubPlan, metadata, session);
        if (tableWriteInfo.getWriterTarget().isPresent()) {
            checkPageSinkCommitIsSupported(session, tableWriteInfo.getWriterTarget().get());
        }
        return tableWriteInfo;
    }

    private void checkPageSinkCommitIsSupported(Session session, ExecutionWriterTarget writerTarget)
    {
        ConnectorId connectorId;
        if (writerTarget instanceof ExecutionWriterTarget.DeleteHandle) {
            throw new PrestoException(NOT_SUPPORTED, "delete queries are not supported by presto on spark");
        }
        else if (writerTarget instanceof ExecutionWriterTarget.CreateHandle) {
            connectorId = ((ExecutionWriterTarget.CreateHandle) writerTarget).getHandle().getConnectorId();
        }
        else if (writerTarget instanceof ExecutionWriterTarget.InsertHandle) {
            connectorId = ((ExecutionWriterTarget.InsertHandle) writerTarget).getHandle().getConnectorId();
        }
        else {
            throw new IllegalArgumentException("unexpected writer target type: " + writerTarget.getClass());
        }
        verify(connectorId != null, "connectorId is null");
        Set<ConnectorCapabilities> connectorCapabilities = metadata.getConnectorCapabilities(session, connectorId);
        if (!connectorCapabilities.contains(SUPPORTS_PAGE_SINK_COMMIT)) {
            throw new PrestoException(NOT_SUPPORTED, "catalog does not support page sink commit: " + connectorId);
        }
    }

    private static void commit(Session session, TransactionManager transactionManager)
    {
        getFutureValue(transactionManager.asyncCommit(getTransactionInfo(session, transactionManager).getTransactionId()));
    }

    private static void rollback(Session session, TransactionManager transactionManager)
    {
        getFutureValue(transactionManager.asyncAbort(getTransactionInfo(session, transactionManager).getTransactionId()));
    }

    private static TransactionInfo getTransactionInfo(Session session, TransactionManager transactionManager)
    {
        Optional<TransactionInfo> transaction = session.getTransactionId()
                .flatMap(transactionManager::getOptionalTransactionInfo);
        checkState(transaction.isPresent(), "transaction is not present");
        checkState(transaction.get().isAutoCommitContext(), "transaction doesn't have auto commit context enabled");
        return transaction.get();
    }

    private static QueryInfo createQueryInfo(
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

        for (StageInfo stageInfo : getAllStages(rootStage)) {
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
                peakRunningTasks,
                succinctBytes(peakUserMemoryReservationInBytes),
                succinctBytes(peakTotalMemoryReservationInBytes),
                succinctBytes(peakTaskUserMemoryInBytes),
                succinctBytes(peakTaskTotalMemoryInBytes),
                succinctBytes(peakNodeTotalMemoryInBytes));

        return new QueryInfo(
                session.getQueryId(),
                session.toSessionRepresentation(),
                queryState,
                new MemoryPoolId("spark-memory-pool"),
                queryStats.isScheduled(),
                URI.create("http://fake.invalid/query/" + session.getQueryId()),
                planAndMore.map(PlanAndMore::getFieldNames).orElse(ImmutableList.of()),
                query,
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
                true,
                sparkQueueName.map(ResourceGroupId::new),
                planAndMore.flatMap(PlanAndMore::getQueryType),
                Optional.empty(),
                Optional.empty());
    }

    private static StageInfo createStageInfo(QueryId queryId, SubPlan plan, List<TaskInfo> taskInfos)
    {
        ListMultimap<PlanFragmentId, TaskInfo> taskInfoMap = ArrayListMultimap.create();
        for (TaskInfo taskInfo : taskInfos) {
            PlanFragmentId fragmentId = new PlanFragmentId(taskInfo.getTaskId().getStageExecutionId().getStageId().getId());
            taskInfoMap.put(fragmentId, taskInfo);
        }
        return createStageInfo(queryId, plan, taskInfoMap);
    }

    private static StageInfo createStageInfo(QueryId queryId, SubPlan plan, ListMultimap<PlanFragmentId, TaskInfo> taskInfoMap)
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
                DateTime.now(),
                new Distribution().snapshot(),
                succinctBytes(peakUserMemoryReservationInBytes),
                succinctBytes(peakNodeTotalMemoryReservationInBytes),
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

    private static PrestoSparkQueryStatusInfo createPrestoSparkQueryInfo(
            QueryInfo queryInfo,
            Optional<PlanAndMore> planAndMore,
            WarningCollector warningCollector,
            OptionalLong updateCount)
    {
        StatementStats stats = toStatementStats(queryInfo);

        // nullify stage stats to keep the object slim
        stats = new StatementStats(
                stats.getState(),
                stats.isQueued(),
                stats.isScheduled(),
                stats.getNodes(),
                stats.getTotalSplits(),
                stats.getQueuedSplits(),
                stats.getRunningSplits(),
                stats.getCompletedSplits(),
                stats.getCpuTimeMillis(),
                stats.getWallTimeMillis(),
                stats.getQueuedTimeMillis(),
                stats.getElapsedTimeMillis(),
                stats.getProcessedRows(),
                stats.getProcessedBytes(),
                stats.getPeakMemoryBytes(),
                stats.getPeakTotalMemoryBytes(),
                stats.getPeakTaskTotalMemoryBytes(),
                stats.getSpilledBytes(),
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

    private static List<Column> getOutputColumns(PlanAndMore planAndMore)
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
                executionFailureInfo.getErrorLocation(),
                executionFailureInfo.toFailureInfo());
    }

    private static <T> void writeJsonFile(Path outputPath, T object, JsonCodec<T> codec)
    {
        try {
            Files.write(outputPath, codec.toJsonBytes(object));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class PrestoSparkQueryExecution
            implements IPrestoSparkQueryExecution
    {
        private final JavaSparkContext sparkContext;
        private final Session session;
        private final QueryMonitor queryMonitor;
        private final CollectionAccumulator<SerializedTaskInfo> taskInfoCollector;
        private final CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector;
        // used to create tasks on the Driver
        private final PrestoSparkTaskExecutorFactory taskExecutorFactory;
        // used to create tasks on executor, serializable
        private final PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider;
        private final QueryStateTimer queryStateTimer;
        private final WarningCollector warningCollector;
        private final String query;
        private final PlanAndMore planAndMore;
        private final SubPlan fragmentedPlan;
        private final Optional<String> sparkQueueName;

        private final JsonCodec<TaskInfo> taskInfoJsonCodec;
        private final JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec;
        private final JsonCodec<PrestoSparkQueryStatusInfo> queryStatusInfoJsonCodec;
        private final JsonCodec<PrestoSparkQueryData> queryDataJsonCodec;
        private final PrestoSparkRddFactory rddFactory;
        private final TableWriteInfo tableWriteInfo;
        private final TransactionManager transactionManager;
        private final PagesSerde pagesSerde;
        private final PrestoSparkExecutionExceptionFactory executionExceptionFactory;
        private final Duration queryTimeout;
        private final Optional<Path> queryStatusInfoOutputPath;
        private final Optional<Path> queryDataOutputPath;

        private final long queryCompletionDeadline;

        private PrestoSparkQueryExecution(
                JavaSparkContext sparkContext,
                Session session,
                QueryMonitor queryMonitor,
                CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
                CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector,
                PrestoSparkTaskExecutorFactory taskExecutorFactory,
                PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider,
                QueryStateTimer queryStateTimer,
                WarningCollector warningCollector,
                String query,
                PlanAndMore planAndMore,
                SubPlan fragmentedPlan,
                Optional<String> sparkQueueName,
                JsonCodec<TaskInfo> taskInfoJsonCodec,
                JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec,
                JsonCodec<PrestoSparkQueryStatusInfo> queryStatusInfoJsonCodec,
                JsonCodec<PrestoSparkQueryData> queryDataJsonCodec,
                PrestoSparkRddFactory rddFactory,
                TableWriteInfo tableWriteInfo,
                TransactionManager transactionManager,
                PagesSerde pagesSerde,
                PrestoSparkExecutionExceptionFactory executionExceptionFactory,
                Duration queryTimeout,
                long queryCompletionDeadline,
                Optional<Path> queryStatusInfoOutputPath,
                Optional<Path> queryDataOutputPath)
        {
            this.sparkContext = requireNonNull(sparkContext, "sparkContext is null");
            this.session = requireNonNull(session, "session is null");
            this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
            this.taskInfoCollector = requireNonNull(taskInfoCollector, "taskInfoCollector is null");
            this.shuffleStatsCollector = requireNonNull(shuffleStatsCollector, "shuffleStatsCollector is null");
            this.taskExecutorFactory = requireNonNull(taskExecutorFactory, "taskExecutorFactory is null");
            this.taskExecutorFactoryProvider = requireNonNull(taskExecutorFactoryProvider, "taskExecutorFactoryProvider is null");
            this.queryStateTimer = requireNonNull(queryStateTimer, "queryStateTimer is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
            this.query = requireNonNull(query, "query is null");
            this.planAndMore = requireNonNull(planAndMore, "planAndMore is null");
            this.fragmentedPlan = requireNonNull(fragmentedPlan, "fragmentedPlan is null");
            this.sparkQueueName = requireNonNull(sparkQueueName, "sparkQueueName is null");

            this.taskInfoJsonCodec = requireNonNull(taskInfoJsonCodec, "taskInfoJsonCodec is null");
            this.sparkTaskDescriptorJsonCodec = requireNonNull(sparkTaskDescriptorJsonCodec, "sparkTaskDescriptorJsonCodec is null");
            this.queryStatusInfoJsonCodec = requireNonNull(queryStatusInfoJsonCodec, "queryStatusInfoJsonCodec is null");
            this.queryDataJsonCodec = requireNonNull(queryDataJsonCodec, "queryDataJsonCodec is null");
            this.rddFactory = requireNonNull(rddFactory, "rddFactory is null");
            this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
            this.executionExceptionFactory = requireNonNull(executionExceptionFactory, "executionExceptionFactory is null");
            this.queryTimeout = requireNonNull(queryTimeout, "queryTimeout is null");
            this.queryCompletionDeadline = queryCompletionDeadline;
            this.queryStatusInfoOutputPath = requireNonNull(queryStatusInfoOutputPath, "queryStatusInfoOutputPath is null");
            this.queryDataOutputPath = requireNonNull(queryDataOutputPath, "queryDataOutputPath is null");
        }

        @Override
        public List<List<Object>> execute()
        {
            queryStateTimer.beginRunning();

            List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>> rddResults;
            try {
                rddResults = doExecute(fragmentedPlan);
                queryStateTimer.beginFinishing();
                commit(session, transactionManager);
                queryStateTimer.endQuery();
            }
            catch (Throwable executionException) {
                queryStateTimer.beginFinishing();
                try {
                    rollback(session, transactionManager);
                }
                catch (RuntimeException rollbackFailure) {
                    log.error(rollbackFailure, "Encountered error when performing rollback");
                }

                Optional<ExecutionFailureInfo> failureInfo = Optional.empty();
                if (executionException instanceof SparkException) {
                    SparkException sparkException = (SparkException) executionException;
                    failureInfo = executionExceptionFactory.extractExecutionFailureInfo(sparkException);

                    if (!failureInfo.isPresent()) {
                        // not a SparkException with Presto failure info encoded
                        PrestoException wrappedPrestoException;
                        if (sparkException.getMessage().contains("most recent failure: JVM_OOM")) {
                            wrappedPrestoException = new PrestoException(SPARK_EXECUTOR_OOM, executionException);
                        }
                        else if (sparkException.getMessage().matches(".*Total size of serialized results .* is bigger than allowed maxResultSize.*")) {
                            wrappedPrestoException = new PrestoException(EXCEEDED_SPARK_DRIVER_MAX_RESULT_SIZE, executionException);
                        }
                        else if (sparkException.getMessage().contains("Executor heartbeat timed out") ||
                                sparkException.getMessage().contains("Unable to talk to the executor")) {
                            wrappedPrestoException = new PrestoException(SPARK_EXECUTOR_LOST, executionException);
                        }
                        else {
                            wrappedPrestoException = new PrestoException(GENERIC_SPARK_ERROR, executionException);
                        }

                        failureInfo = Optional.of(toFailure(wrappedPrestoException));
                    }
                }
                else if (executionException instanceof PrestoSparkExecutionException) {
                    failureInfo = executionExceptionFactory.extractExecutionFailureInfo((PrestoSparkExecutionException) executionException);
                }
                else if (executionException instanceof TimeoutException) {
                    failureInfo = Optional.of(toFailure(new PrestoException(EXCEEDED_TIME_LIMIT, "Query exceeded maximum time limit of " + queryTimeout, executionException)));
                }

                if (!failureInfo.isPresent()) {
                    failureInfo = Optional.of(toFailure(executionException));
                }

                queryStateTimer.endQuery();

                try {
                    queryCompletedEvent(failureInfo, OptionalLong.empty());
                }
                catch (RuntimeException eventFailure) {
                    log.error(eventFailure, "Error publishing query completed event");
                }

                throw failureInfo.get().toFailure();
            }

            processShuffleStats();

            ConnectorSession connectorSession = session.toConnectorSession();
            List<Type> types = fragmentedPlan.getFragment().getTypes();
            ImmutableList.Builder<List<Object>> result = ImmutableList.builder();
            for (Tuple2<MutablePartitionId, PrestoSparkSerializedPage> tuple : rddResults) {
                Page page = pagesSerde.deserialize(toSerializedPage(tuple._2));
                checkArgument(page.getChannelCount() == types.size(), "expected %s channels, got %s", types.size(), page.getChannelCount());
                for (int position = 0; position < page.getPositionCount(); position++) {
                    List<Object> columns = new ArrayList<>();
                    for (int channel = 0; channel < page.getChannelCount(); channel++) {
                        columns.add(types.get(channel).getObjectValue(connectorSession.getSqlFunctionProperties(), page.getBlock(channel), position));
                    }
                    result.add(unmodifiableList(columns));
                }
            }
            List<List<Object>> results = result.build();

            // Based on com.facebook.presto.server.protocol.Query#getNextResult
            OptionalLong updateCount = OptionalLong.empty();
            if (planAndMore.getUpdateType().isPresent() &&
                    types.size() == 1 &&
                    types.get(0).equals(BIGINT) &&
                    results.size() == 1 &&
                    results.get(0).size() == 1) {
                updateCount = OptionalLong.of(((Number) results.get(0).get(0)).longValue());
            }

            // successfully finished
            try {
                queryCompletedEvent(Optional.empty(), updateCount);
            }
            catch (RuntimeException eventFailure) {
                log.error(eventFailure, "Error publishing query completed event");
            }

            queryDataOutputPath.ifPresent(path -> writeJsonFile(
                    path,
                    new PrestoSparkQueryData(
                            getOutputColumns(planAndMore),
                            results),
                    queryDataJsonCodec));

            return results;
        }

        public List<Type> getOutputTypes()
        {
            return fragmentedPlan.getFragment().getTypes();
        }

        public Optional<String> getUpdateType()
        {
            return planAndMore.getUpdateType();
        }

        private List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>> doExecute(SubPlan root)
                throws SparkException, TimeoutException
        {
            PlanFragment rootFragment = root.getFragment();

            if (rootFragment.getPartitioning().equals(COORDINATOR_DISTRIBUTION)) {
                PrestoSparkTaskDescriptor taskDescriptor = new PrestoSparkTaskDescriptor(
                        session.toSessionRepresentation(),
                        session.getIdentity().getExtraCredentials(),
                        rootFragment,
                        tableWriteInfo);
                SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor = new SerializedPrestoSparkTaskDescriptor(sparkTaskDescriptorJsonCodec.toJsonBytes(taskDescriptor));

                Map<PlanFragmentId, RddAndMore<PrestoSparkSerializedPage>> inputRdds = new HashMap<>();
                for (SubPlan child : root.getChildren()) {
                    inputRdds.put(child.getFragment().getId(), createRdd(child, PrestoSparkSerializedPage.class));
                }

                Map<String, JavaFutureAction<List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>>>> inputFutures = inputRdds.entrySet().stream()
                        .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> entry.getValue().getRdd().collectAsync()));

                waitForActionsCompletionWithTimeout(inputFutures.values(), computeNextTimeout(), MILLISECONDS);

                Map<String, List<PrestoSparkSerializedPage>> inputs = inputFutures.entrySet().stream()
                        .collect(toImmutableMap(
                                Map.Entry::getKey,
                                entry -> getUnchecked(entry.getValue()).stream().map(Tuple2::_2).collect(toImmutableList())));

                IPrestoSparkTaskExecutor<PrestoSparkSerializedPage> prestoSparkTaskExecutor = taskExecutorFactory.create(
                        0,
                        0,
                        serializedTaskDescriptor,
                        emptyScalaIterator(),
                        new PrestoSparkTaskInputs(ImmutableMap.of(), ImmutableMap.of(), inputs),
                        taskInfoCollector,
                        shuffleStatsCollector,
                        PrestoSparkSerializedPage.class);
                return collectScalaIterator(prestoSparkTaskExecutor);
            }

            RddAndMore<PrestoSparkSerializedPage> rootRdd = createRdd(root, PrestoSparkSerializedPage.class);
            return rootRdd.collectAndDestroyDependenciesWithTimeout(computeNextTimeout(), MILLISECONDS);
        }

        private <T extends PrestoSparkTaskOutput> RddAndMore<T> createRdd(SubPlan subPlan, Class<T> outputType)
                throws SparkException, TimeoutException
        {
            ImmutableMap.Builder<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> rddInputs = ImmutableMap.builder();
            ImmutableMap.Builder<PlanFragmentId, Broadcast<List<PrestoSparkSerializedPage>>> broadcastInputs = ImmutableMap.builder();
            ImmutableList.Builder<Broadcast<?>> broadcastDependencies = ImmutableList.builder();

            for (SubPlan child : subPlan.getChildren()) {
                PlanFragment childFragment = child.getFragment();
                if (childFragment.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION)) {
                    RddAndMore<PrestoSparkSerializedPage> childRdd = createRdd(child, PrestoSparkSerializedPage.class);

                    // TODO: The driver might still OOM on a very large broadcast, think of how to prevent that from happening
                    List<PrestoSparkSerializedPage> broadcastPages = childRdd.collectAndDestroyDependenciesWithTimeout(computeNextTimeout(), MILLISECONDS).stream()
                            .map(Tuple2::_2)
                            .collect(toList());

                    int compressedBroadcastSizeInBytes = broadcastPages.stream()
                            .mapToInt(page -> page.getBytes().length)
                            .sum();
                    int uncompressedBroadcastSizeInBytes = broadcastPages.stream()
                            .mapToInt(PrestoSparkSerializedPage::getUncompressedSizeInBytes)
                            .sum();
                    DataSize maxBroadcastSize = getQueryMaxBroadcastMemory(session);
                    long maxBroadcastSizeInBytes = maxBroadcastSize.toBytes();

                    if (compressedBroadcastSizeInBytes > maxBroadcastSizeInBytes) {
                        throw exceededLocalBroadcastMemoryLimit(maxBroadcastSize, format("Compressed broadcast size: %s", succinctBytes(compressedBroadcastSizeInBytes)));
                    }

                    if (uncompressedBroadcastSizeInBytes > maxBroadcastSizeInBytes) {
                        throw exceededLocalBroadcastMemoryLimit(maxBroadcastSize, format("Uncompressed broadcast size: %s", succinctBytes(compressedBroadcastSizeInBytes)));
                    }

                    Broadcast<List<PrestoSparkSerializedPage>> broadcast = sparkContext.broadcast(broadcastPages);
                    broadcastInputs.put(childFragment.getId(), broadcast);
                    broadcastDependencies.add(broadcast);
                }
                else {
                    RddAndMore<PrestoSparkMutableRow> childRdd = createRdd(child, PrestoSparkMutableRow.class);
                    rddInputs.put(childFragment.getId(), childRdd.getRdd());
                    broadcastDependencies.addAll(childRdd.getBroadcastDependencies());
                }
            }
            JavaPairRDD<MutablePartitionId, T> rdd = rddFactory.createSparkRdd(
                    sparkContext,
                    session,
                    subPlan.getFragment(),
                    rddInputs.build(),
                    broadcastInputs.build(),
                    taskExecutorFactoryProvider,
                    taskInfoCollector,
                    shuffleStatsCollector,
                    tableWriteInfo,
                    outputType);
            return new RddAndMore<>(rdd, broadcastDependencies.build());
        }

        private void queryCompletedEvent(Optional<ExecutionFailureInfo> failureInfo, OptionalLong updateCount)
        {
            List<SerializedTaskInfo> serializedTaskInfos = taskInfoCollector.value();
            List<TaskInfo> taskInfos = serializedTaskInfos.stream()
                    .map(SerializedTaskInfo::getBytes)
                    .map(PrestoSparkUtils::decompress)
                    .map(taskInfoJsonCodec::fromJson)
                    .collect(toImmutableList());
            StageInfo stageInfo = createStageInfo(session.getQueryId(), fragmentedPlan, taskInfos);
            QueryState queryState = failureInfo.isPresent() ? FAILED : FINISHED;

            QueryInfo queryInfo = createQueryInfo(
                    session,
                    query,
                    queryState,
                    Optional.of(planAndMore),
                    sparkQueueName,
                    failureInfo,
                    queryStateTimer,
                    Optional.of(stageInfo),
                    warningCollector);

            queryMonitor.queryCompletedEvent(queryInfo);
            if (queryStatusInfoOutputPath.isPresent()) {
                PrestoSparkQueryStatusInfo prestoSparkQueryStatusInfo = createPrestoSparkQueryInfo(
                        queryInfo,
                        Optional.of(planAndMore),
                        warningCollector,
                        updateCount);
                writeJsonFile(
                        queryStatusInfoOutputPath.get(),
                        prestoSparkQueryStatusInfo,
                        queryStatusInfoJsonCodec);
            }
        }

        private void processShuffleStats()
        {
            List<PrestoSparkShuffleStats> statsList = new ArrayList<>(shuffleStatsCollector.value());
            Map<ShuffleStatsKey, List<PrestoSparkShuffleStats>> statsMap = new TreeMap<>();
            for (PrestoSparkShuffleStats stats : statsList) {
                ShuffleStatsKey key = new ShuffleStatsKey(stats.getFragmentId(), stats.getOperation());
                statsMap.computeIfAbsent(key, (ignored) -> new ArrayList<>()).add(stats);
            }
            log.info("Shuffle statistics summary:");
            for (Map.Entry<ShuffleStatsKey, List<PrestoSparkShuffleStats>> fragment : statsMap.entrySet()) {
                logShuffleStatsSummary(fragment.getKey(), fragment.getValue());
            }
        }

        private void logShuffleStatsSummary(ShuffleStatsKey key, List<PrestoSparkShuffleStats> statsList)
        {
            long totalProcessedRows = 0;
            long totalProcessedRowBatches = 0;
            long totalProcessedBytes = 0;
            long totalElapsedWallTimeMills = 0;
            for (PrestoSparkShuffleStats stats : statsList) {
                totalProcessedRows += stats.getProcessedRows();
                totalProcessedRowBatches += stats.getProcessedRowBatches();
                totalProcessedBytes += stats.getProcessedBytes();
                totalElapsedWallTimeMills += stats.getElapsedWallTimeMills();
            }
            long totalElapsedWallTimeSeconds = totalElapsedWallTimeMills / 1000;
            long rowsPerSecond = totalProcessedRows;
            long rowBatchesPerSecond = totalProcessedRowBatches;
            long bytesPerSecond = totalProcessedBytes;
            if (totalElapsedWallTimeSeconds > 0) {
                rowsPerSecond = totalProcessedRows / totalElapsedWallTimeSeconds;
                rowBatchesPerSecond = totalProcessedRowBatches / totalElapsedWallTimeSeconds;
                bytesPerSecond = totalProcessedBytes / totalElapsedWallTimeSeconds;
            }
            long averageRowSize = 0;
            if (totalProcessedRows > 0) {
                averageRowSize = totalProcessedBytes / totalProcessedRows;
            }
            long averageRowBatchSize = 0;
            if (totalProcessedRowBatches > 0) {
                averageRowBatchSize = totalProcessedBytes / totalProcessedRowBatches;
            }
            log.info(
                    "Fragment: %s, Operation: %s, Rows: %s, Row Batches: %s, Size: %s, Avg Row Size: %s, Avg Row Batch Size: %s, Time: %s, %s rows/s, %s batches/s, %s/s",
                    key.getFragmentId(),
                    key.getOperation(),
                    totalProcessedRows,
                    totalProcessedRowBatches,
                    DataSize.succinctBytes(totalProcessedBytes),
                    DataSize.succinctBytes(averageRowSize),
                    DataSize.succinctBytes(averageRowBatchSize),
                    Duration.succinctDuration(totalElapsedWallTimeMills, MILLISECONDS),
                    rowsPerSecond,
                    rowBatchesPerSecond,
                    DataSize.succinctBytes(bytesPerSecond));
        }

        private long computeNextTimeout()
                throws TimeoutException
        {
            long timeout = queryCompletionDeadline - System.currentTimeMillis();
            if (timeout <= 0) {
                throw new TimeoutException();
            }
            return timeout;
        }
    }

    private static <T> void waitForActionsCompletionWithTimeout(Collection<JavaFutureAction<T>> actions, long timeout, TimeUnit timeUnit)
            throws SparkException, TimeoutException
    {
        long deadline = System.currentTimeMillis() + timeUnit.toMillis(timeout);

        try {
            for (JavaFutureAction<?> action : actions) {
                long nextTimeoutInMillis = deadline - System.currentTimeMillis();
                if (nextTimeoutInMillis <= 0) {
                    throw new TimeoutException();
                }
                getActionResultWithTimeout(action, nextTimeoutInMillis, MILLISECONDS);
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

    private static <T> T getActionResultWithTimeout(JavaFutureAction<T> action, long timeout, TimeUnit timeUnit)
            throws SparkException, TimeoutException
    {
        long deadline = System.currentTimeMillis() + timeUnit.toMillis(timeout);
        try {
            while (true) {
                long nextTimeoutInMillis = deadline - System.currentTimeMillis();
                if (nextTimeoutInMillis <= 0) {
                    throw new TimeoutException();
                }
                try {
                    return action.get(nextTimeoutInMillis, MILLISECONDS);
                }
                catch (TimeoutException e) {
                    // guard against spurious wakeup
                    if (deadline - System.currentTimeMillis() <= 0) {
                        throw e;
                    }
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            propagateIfPossible(e.getCause(), SparkException.class);
            propagateIfPossible(e.getCause(), RuntimeException.class);

            // this should never happen
            throw new UncheckedExecutionException(e.getCause());
        }
        finally {
            if (!action.isDone()) {
                action.cancel(true);
            }
        }
    }

    private static class RddAndMore<T extends PrestoSparkTaskOutput>
    {
        private final JavaPairRDD<MutablePartitionId, T> rdd;
        private final List<Broadcast<?>> broadcastDependencies;

        private boolean collected;

        private RddAndMore(JavaPairRDD<MutablePartitionId, T> rdd, List<Broadcast<?>> broadcastDependencies)
        {
            this.rdd = requireNonNull(rdd, "rdd is null");
            this.broadcastDependencies = ImmutableList.copyOf(requireNonNull(broadcastDependencies, "broadcastDependencies is null"));
        }

        public List<Tuple2<MutablePartitionId, T>> collectAndDestroyDependenciesWithTimeout(long timeout, TimeUnit timeUnit)
                throws SparkException, TimeoutException
        {
            checkState(!collected, "already collected");
            collected = true;
            List<Tuple2<MutablePartitionId, T>> result = getActionResultWithTimeout(rdd.collectAsync(), timeout, timeUnit);
            broadcastDependencies.forEach(Broadcast::destroy);
            return result;
        }

        public JavaPairRDD<MutablePartitionId, T> getRdd()
        {
            return rdd;
        }

        public List<Broadcast<?>> getBroadcastDependencies()
        {
            return broadcastDependencies;
        }
    }

    private static class ShuffleStatsKey
            implements Comparable<ShuffleStatsKey>
    {
        private final int fragmentId;
        private final Operation operation;

        private ShuffleStatsKey(int fragmentId, Operation operation)
        {
            this.fragmentId = fragmentId;
            this.operation = requireNonNull(operation, "operation is null");
        }

        public int getFragmentId()
        {
            return fragmentId;
        }

        public Operation getOperation()
        {
            return operation;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ShuffleStatsKey that = (ShuffleStatsKey) o;
            return fragmentId == that.fragmentId &&
                    operation == that.operation;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(fragmentId, operation);
        }

        @Override
        public int compareTo(ShuffleStatsKey that)
        {
            return ComparisonChain.start()
                    .compare(this.fragmentId, that.fragmentId)
                    .compare(this.operation, that.operation)
                    .result();
        }
    }
}
