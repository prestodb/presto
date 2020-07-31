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
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.common.Page;
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
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.QuerySessionSupplier;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecution;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecutionFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutor;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkConfInitializer;
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
import com.facebook.presto.spark.execution.PrestoSparkExecutionException;
import com.facebook.presto.spark.execution.PrestoSparkExecutionExceptionFactory;
import com.facebook.presto.spark.execution.PrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.planner.PrestoSparkPlanFragmenter;
import com.facebook.presto.spark.planner.PrestoSparkQueryPlanner;
import com.facebook.presto.spark.planner.PrestoSparkQueryPlanner.PlanAndMore;
import com.facebook.presto.spark.planner.PrestoSparkRddFactory;
import com.facebook.presto.spark.util.PrestoSparkUtils;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.page.PagesSerde;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.execution.scheduler.StreamingPlanSection.extractStreamingSections;
import static com.facebook.presto.execution.scheduler.TableWriteInfo.createTableWriteInfo;
import static com.facebook.presto.spark.classloader_interface.ScalaUtils.collectScalaIterator;
import static com.facebook.presto.spark.classloader_interface.ScalaUtils.emptyScalaIterator;
import static com.facebook.presto.spark.util.PrestoSparkUtils.toSerializedPage;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_PAGE_SINK_COMMIT;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textDistributedPlan;
import static com.facebook.presto.util.Failures.toFailure;
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
    private final JsonCodec<QueryInfo> queryInfoJsonCodec;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;
    private final BlockEncodingManager blockEncodingManager;
    private final PrestoSparkSettingsRequirements settingsRequirements;
    private final PrestoSparkExecutionExceptionFactory executionExceptionFactory;
    private final PrestoSparkTaskExecutorFactory prestoSparkTaskExecutorFactory;

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
            JsonCodec<QueryInfo> queryInfoJsonCodec,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Metadata metadata,
            BlockEncodingManager blockEncodingManager,
            PrestoSparkSettingsRequirements settingsRequirements,
            PrestoSparkExecutionExceptionFactory executionExceptionFactory,
            PrestoSparkTaskExecutorFactory prestoSparkTaskExecutorFactory,
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
        this.queryInfoJsonCodec = requireNonNull(queryInfoJsonCodec, "queryInfoJsonCodec is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.blockEncodingManager = requireNonNull(blockEncodingManager, "blockEncodingManager is null");
        this.settingsRequirements = requireNonNull(settingsRequirements, "settingsRequirements is null");
        this.executionExceptionFactory = requireNonNull(executionExceptionFactory, "executionExceptionFactory is null");
        this.prestoSparkTaskExecutorFactory = requireNonNull(prestoSparkTaskExecutorFactory, "prestoSparkTaskExecutorFactory is null");
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
            Optional<Path> queryInfoOutputPath)
    {
        PrestoSparkConfInitializer.checkInitialized(sparkContext);

        queryInfoOutputPath.ifPresent(path -> checkArgument(notExists(path), "File already exist: %s", path));

        QueryStateTimer queryStateTimer = new QueryStateTimer(systemTicker());

        queryStateTimer.beginPlanning();

        QueryId queryId = queryIdGenerator.createNextQueryId();
        log.info("Starting execution for presto query: %s", queryId);
        SessionContext sessionContext = PrestoSparkSessionContext.createFromSessionInfo(
                prestoSparkSession,
                credentialsProviders,
                authenticatorProviders);

        // TODO: implement warning collection
        WarningCollector warningCollector = WarningCollector.NOOP;

        Session session = sessionSupplier.createSession(queryId, sessionContext);
        settingsRequirements.verify(sparkContext, session);

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

        PlanAndMore planAndMore = null;
        try {
            queryStateTimer.beginAnalyzing();

            PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, sql, warningCollector);
            planAndMore = queryPlanner.createQueryPlan(session, preparedQuery, warningCollector);
            SubPlan fragmentedPlan = planFragmenter.fragmentQueryPlan(session, planAndMore.getPlan(), warningCollector);
            log.info(textDistributedPlan(fragmentedPlan, metadata.getFunctionManager(), session, true));
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
                    queryInfoJsonCodec,
                    rddFactory,
                    tableWriteInfo,
                    transactionManager,
                    new PagesSerde(blockEncodingManager, Optional.empty(), Optional.empty(), Optional.empty()),
                    executionExceptionFactory,
                    queryInfoOutputPath);
        }
        catch (RuntimeException executionFailure) {
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
                queryInfoOutputPath.ifPresent(path -> writeQueryInfo(path, queryInfo, queryInfoJsonCodec));
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

        for (StageInfo stageInfo : getAllStages(rootStage)) {
            StageExecutionInfo stageExecutionInfo = stageInfo.getLatestAttemptExecutionInfo();
            for (TaskInfo taskInfo : stageExecutionInfo.getTasks()) {
                // there's no way to know how many tasks were running in parallel in Spark
                // for now let's assume that all the tasks were running in parallel
                peakRunningTasks++;
                long taskPeakUserMemoryInBytes = taskInfo.getStats().getUserMemoryReservationInBytes();
                long taskPeakTotalMemoryInBytes = taskInfo.getStats().getPeakTotalMemoryInBytes();
                peakUserMemoryReservationInBytes += taskPeakUserMemoryInBytes;
                peakTotalMemoryReservationInBytes += taskPeakTotalMemoryInBytes;
                peakTaskUserMemoryInBytes = max(peakTaskUserMemoryInBytes, taskPeakUserMemoryInBytes);
                peakTaskTotalMemoryInBytes = max(peakTaskTotalMemoryInBytes, taskPeakTotalMemoryInBytes);
            }
        }

        QueryStats queryStats = QueryStats.create(
                queryStateTimer,
                rootStage,
                peakRunningTasks,
                succinctBytes(peakUserMemoryReservationInBytes),
                succinctBytes(peakTotalMemoryReservationInBytes),
                succinctBytes(peakTaskUserMemoryInBytes),
                succinctBytes(peakTaskTotalMemoryInBytes));

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
        for (TaskInfo taskInfo : taskInfos) {
            long taskPeakUserMemoryInBytes = taskInfo.getStats().getUserMemoryReservationInBytes();
            peakUserMemoryReservationInBytes += taskPeakUserMemoryInBytes;
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

    private static void writeQueryInfo(Path queryInfoOutputPath, QueryInfo queryInfo, JsonCodec<QueryInfo> queryInfoJsonCodec)
    {
        try {
            Files.write(queryInfoOutputPath, queryInfoJsonCodec.toJsonBytes(queryInfo));
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
        private final JsonCodec<QueryInfo> queryInfoJsonCodec;
        private final PrestoSparkRddFactory rddFactory;
        private final TableWriteInfo tableWriteInfo;
        private final TransactionManager transactionManager;
        private final PagesSerde pagesSerde;
        private final PrestoSparkExecutionExceptionFactory executionExceptionFactory;
        private final Optional<Path> queryInfoOutputPath;

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
                JsonCodec<QueryInfo> queryInfoJsonCodec,
                PrestoSparkRddFactory rddFactory,
                TableWriteInfo tableWriteInfo,
                TransactionManager transactionManager,
                PagesSerde pagesSerde,
                PrestoSparkExecutionExceptionFactory executionExceptionFactory,
                Optional<Path> queryInfoOutputPath)
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
            this.queryInfoJsonCodec = requireNonNull(queryInfoJsonCodec, "queryInfoJsonCodec is null");
            this.rddFactory = requireNonNull(rddFactory, "rddFactory is null");
            this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
            this.executionExceptionFactory = requireNonNull(executionExceptionFactory, "executionExceptionFactory is null");
            this.queryInfoOutputPath = requireNonNull(queryInfoOutputPath, "queryInfoOutputPath is null");
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
            catch (Exception executionFailure) {
                queryStateTimer.beginFinishing();
                try {
                    rollback(session, transactionManager);
                }
                catch (RuntimeException rollbackFailure) {
                    log.error(rollbackFailure, "Encountered error when performing rollback");
                }

                Optional<ExecutionFailureInfo> failureInfo = Optional.empty();
                if (executionFailure instanceof SparkException) {
                    failureInfo = executionExceptionFactory.extractExecutionFailureInfo((SparkException) executionFailure);
                }
                if (!failureInfo.isPresent() && executionFailure instanceof PrestoSparkExecutionException) {
                    failureInfo = executionExceptionFactory.extractExecutionFailureInfo((PrestoSparkExecutionException) executionFailure);
                }
                if (!failureInfo.isPresent()) {
                    failureInfo = Optional.of(toFailure(executionFailure));
                }

                queryStateTimer.endQuery();

                try {
                    queryCompletedEvent(failureInfo);
                }
                catch (RuntimeException eventFailure) {
                    log.error(eventFailure, "Error publishing query completed event");
                }

                throw failureInfo.get().toFailure();
            }

            processShuffleStats();

            // successfully finished
            try {
                queryCompletedEvent(Optional.empty());
            }
            catch (RuntimeException eventFailure) {
                log.error(eventFailure, "Error publishing query completed event");
            }

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
            return result.build();
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
                throws SparkException
        {
            PlanFragment rootFragment = root.getFragment();

            if (rootFragment.getPartitioning().equals(COORDINATOR_DISTRIBUTION)) {
                PrestoSparkTaskDescriptor taskDescriptor = new PrestoSparkTaskDescriptor(
                        session.toSessionRepresentation(),
                        session.getIdentity().getExtraCredentials(),
                        rootFragment,
                        tableWriteInfo);
                SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor = new SerializedPrestoSparkTaskDescriptor(sparkTaskDescriptorJsonCodec.toJsonBytes(taskDescriptor));

                Map<PlanFragmentId, RddAndMore<PrestoSparkSerializedPage>> inputRdds = root.getChildren().stream()
                        .collect(toImmutableMap(child -> child.getFragment().getId(), child -> createRdd(child, PrestoSparkSerializedPage.class)));

                Map<String, Future<List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>>>> inputFutures = inputRdds.entrySet().stream()
                        .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> entry.getValue().getRdd().collectAsync()));

                waitFor(inputFutures.values());

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
            return rootRdd.collectAndDestroyDependencies();
        }

        private <T extends PrestoSparkTaskOutput> RddAndMore<T> createRdd(SubPlan subPlan, Class<T> outputType)
        {
            ImmutableMap.Builder<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> rddInputs = ImmutableMap.builder();
            ImmutableMap.Builder<PlanFragmentId, Broadcast<List<PrestoSparkSerializedPage>>> broadcastInputs = ImmutableMap.builder();
            ImmutableList.Builder<Broadcast<?>> broadcastDependencies = ImmutableList.builder();

            for (SubPlan child : subPlan.getChildren()) {
                PlanFragment childFragment = child.getFragment();
                if (childFragment.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION)) {
                    RddAndMore<PrestoSparkSerializedPage> childRdd = createRdd(child, PrestoSparkSerializedPage.class);
                    List<PrestoSparkSerializedPage> broadcastPages = childRdd.collectAndDestroyDependencies().stream()
                            .map(Tuple2::_2)
                            .collect(toList());
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

        private void queryCompletedEvent(Optional<ExecutionFailureInfo> failureInfo)
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
            queryInfoOutputPath.ifPresent(path -> writeQueryInfo(path, queryInfo, queryInfoJsonCodec));
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
            long totalProcessedBytes = 0;
            long totalElapsedWallTimeMills = 0;
            for (PrestoSparkShuffleStats stats : statsList) {
                totalProcessedRows += stats.getProcessedRows();
                totalProcessedBytes += stats.getProcessedBytes();
                totalElapsedWallTimeMills += stats.getElapsedWallTimeMills();
            }
            long totalElapsedWallTimeSeconds = totalElapsedWallTimeMills / 1000;
            long rowsPerSecond = totalProcessedRows;
            long bytesPerSecond = totalProcessedBytes;
            if (totalElapsedWallTimeSeconds > 0) {
                rowsPerSecond = totalProcessedRows / totalElapsedWallTimeSeconds;
                bytesPerSecond = totalProcessedBytes / totalElapsedWallTimeSeconds;
            }
            long averageRowSize = 0;
            if (totalProcessedRows > 0) {
                averageRowSize = totalProcessedBytes / totalProcessedRows;
            }
            log.info(
                    "Fragment: %s, Operation: %s, Rows: %s, Size: %s, Avg Row Size: %s, Time: %s, %srows/s, %s/s",
                    key.getFragmentId(),
                    key.getOperation(),
                    totalProcessedRows,
                    DataSize.succinctBytes(totalProcessedBytes),
                    DataSize.succinctBytes(averageRowSize),
                    Duration.succinctDuration(totalElapsedWallTimeMills, MILLISECONDS),
                    rowsPerSecond,
                    DataSize.succinctBytes(bytesPerSecond));
        }

        private static <T> void waitFor(Collection<Future<T>> futures)
                throws SparkException
        {
            try {
                for (Future<?> future : futures) {
                    future.get();
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
                for (Future<?> future : futures) {
                    if (!future.isDone()) {
                        future.cancel(true);
                    }
                }
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

        public List<Tuple2<MutablePartitionId, T>> collectAndDestroyDependencies()
        {
            checkState(!collected, "already collected");
            collected = true;
            List<Tuple2<MutablePartitionId, T>> result = rdd.collect();
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
