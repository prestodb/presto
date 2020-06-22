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
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget;
import com.facebook.presto.execution.scheduler.StreamingPlanSection;
import com.facebook.presto.execution.scheduler.StreamingSubPlan;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.security.AccessControl;
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
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskInputs;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedTaskStats;
import com.facebook.presto.spark.execution.PrestoSparkExecutionException;
import com.facebook.presto.spark.execution.PrestoSparkExecutionExceptionFactory;
import com.facebook.presto.spark.execution.PrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.planner.PrestoSparkPlanFragmenter;
import com.facebook.presto.spark.planner.PrestoSparkQueryPlanner;
import com.facebook.presto.spark.planner.PrestoSparkQueryPlanner.PlanAndUpdateType;
import com.facebook.presto.spark.planner.PrestoSparkRddFactory;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import scala.Some;
import scala.Tuple2;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
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
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
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
    private final JsonCodec<TaskStats> taskStatsJsonCodec;
    private final JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec;
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
            JsonCodec<TaskStats> taskStatsJsonCodec,
            JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec,
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
        this.taskStatsJsonCodec = requireNonNull(taskStatsJsonCodec, "taskStatsJsonCodec is null");
        this.sparkTaskDescriptorJsonCodec = requireNonNull(sparkTaskDescriptorJsonCodec, "sparkTaskDescriptorJsonCodec is null");
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
            PrestoSparkTaskExecutorFactoryProvider executorFactoryProvider)
    {
        PrestoSparkConfInitializer.checkInitialized(sparkContext);

        QueryId queryId = queryIdGenerator.createNextQueryId();
        SessionContext sessionContext = PrestoSparkSessionContext.createFromSessionInfo(
                prestoSparkSession,
                credentialsProviders,
                authenticatorProviders);

        // TODO: implement warning collection
        WarningCollector warningCollector = WarningCollector.NOOP;

        // TODO: implement query monitor
        // queryMonitor.queryCreatedEvent();

        Session session = sessionSupplier.createSession(queryId, sessionContext);
        settingsRequirements.verify(sparkContext, session);

        TransactionId transactionId = transactionManager.beginTransaction(true);
        session = session.beginTransactionId(transactionId, transactionManager, accessControl);

        try {
            PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, sql, warningCollector);
            PlanAndUpdateType planAndUpdateType = queryPlanner.createQueryPlan(session, preparedQuery, warningCollector);
            SubPlan fragmentedPlan = planFragmenter.fragmentQueryPlan(session, planAndUpdateType.getPlan(), warningCollector);
            log.info(textDistributedPlan(fragmentedPlan, metadata.getFunctionManager(), session, true));
            TableWriteInfo tableWriteInfo = getTableWriteInfo(session, fragmentedPlan);

            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector = new CollectionAccumulator<>();
            taskStatsCollector.register(sparkContext, new Some<>("taskStatsCollector"), false);

            return new PrestoSparkQueryExecution(
                    javaSparkContext,
                    session,
                    queryMonitor,
                    taskStatsCollector,
                    prestoSparkTaskExecutorFactory,
                    executorFactoryProvider,
                    fragmentedPlan,
                    planAndUpdateType.getUpdateType(),
                    taskStatsJsonCodec,
                    sparkTaskDescriptorJsonCodec,
                    rddFactory,
                    tableWriteInfo,
                    transactionManager,
                    new PagesSerde(blockEncodingManager, Optional.empty(), Optional.empty(), Optional.empty()), executionExceptionFactory);
        }
        catch (RuntimeException executionFailure) {
            try {
                rollback(session, transactionManager);
            }
            catch (RuntimeException rollbackFailure) {
                log.error(rollbackFailure, "Encountered error when performing rollback");
            }
            try {
                // TODO: implement query monitor
                // queryMonitor.queryImmediateFailureEvent();
            }
            catch (RuntimeException eventFailure) {
                log.error(eventFailure, "Error publishing query immediate failure event");
            }

            if (executionFailure instanceof PrestoSparkExecutionException) {
                Optional<ExecutionFailureInfo> executionFailureInfo = executionExceptionFactory.extractExecutionFailureInfo((PrestoSparkExecutionException) executionFailure);
                verify(executionFailureInfo.isPresent());
                throw executionFailureInfo.get().toFailure();
            }
            throw executionFailure;
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

    public static class PrestoSparkQueryExecution
            implements IPrestoSparkQueryExecution
    {
        private final JavaSparkContext sparkContext;
        private final Session session;
        private final QueryMonitor queryMonitor;
        private final CollectionAccumulator<SerializedTaskStats> taskStatsCollector;
        // used to create tasks on the Driver
        private final PrestoSparkTaskExecutorFactory taskExecutorFactory;
        // used to create tasks on executor, serializable
        private final PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider;
        private final SubPlan plan;
        private final Optional<String> updateType;
        private final JsonCodec<TaskStats> taskStatsJsonCodec;
        private final JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec;
        private final PrestoSparkRddFactory rddFactory;
        private final TableWriteInfo tableWriteInfo;
        private final TransactionManager transactionManager;
        private final PagesSerde pagesSerde;
        private final PrestoSparkExecutionExceptionFactory executionExceptionFactory;

        private PrestoSparkQueryExecution(
                JavaSparkContext sparkContext,
                Session session,
                QueryMonitor queryMonitor,
                CollectionAccumulator<SerializedTaskStats> taskStatsCollector,
                PrestoSparkTaskExecutorFactory taskExecutorFactory,
                PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider,
                SubPlan plan,
                Optional<String> updateType,
                JsonCodec<TaskStats> taskStatsJsonCodec,
                JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec,
                PrestoSparkRddFactory rddFactory,
                TableWriteInfo tableWriteInfo,
                TransactionManager transactionManager,
                PagesSerde pagesSerde,
                PrestoSparkExecutionExceptionFactory executionExceptionFactory)
        {
            this.sparkContext = requireNonNull(sparkContext, "sparkContext is null");
            this.session = requireNonNull(session, "session is null");
            this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
            this.taskStatsCollector = requireNonNull(taskStatsCollector, "taskStatsCollector is null");
            this.taskExecutorFactory = requireNonNull(taskExecutorFactory, "taskExecutorFactory is null");
            this.taskExecutorFactoryProvider = requireNonNull(taskExecutorFactoryProvider, "taskExecutorFactoryProvider is null");
            this.plan = requireNonNull(plan, "plan is null");
            this.updateType = updateType;
            this.taskStatsJsonCodec = requireNonNull(taskStatsJsonCodec, "taskStatsJsonCodec is null");
            this.sparkTaskDescriptorJsonCodec = requireNonNull(sparkTaskDescriptorJsonCodec, "sparkTaskDescriptorJsonCodec is null");
            this.rddFactory = requireNonNull(rddFactory, "rddFactory is null");
            this.tableWriteInfo = requireNonNull(tableWriteInfo, "tableWriteInfo is null");
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
            this.executionExceptionFactory = requireNonNull(executionExceptionFactory, "executionExceptionFactory is null");
        }

        @Override
        public List<List<Object>> execute()
        {
            List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>> rddResults;
            try {
                rddResults = doExecute(plan);
                commit(session, transactionManager);
            }
            catch (Exception executionFailure) {
                try {
                    rollback(session, transactionManager);
                }
                catch (RuntimeException rollbackFailure) {
                    log.error(rollbackFailure, "Encountered error when performing rollback");
                }

                try {
                    queryCompletedEvent(Optional.of(executionFailure));
                }
                catch (RuntimeException eventFailure) {
                    log.error(eventFailure, "Error publishing query completed event");
                }

                if (executionFailure instanceof SparkException) {
                    Optional<ExecutionFailureInfo> executionFailureInfo = executionExceptionFactory.extractExecutionFailureInfo((SparkException) executionFailure);
                    if (executionFailureInfo.isPresent()) {
                        throw executionFailureInfo.get().toFailure();
                    }
                    throw toFailure(executionFailure).toFailure();
                }

                if (executionFailure instanceof PrestoSparkExecutionException) {
                    Optional<ExecutionFailureInfo> executionFailureInfo = executionExceptionFactory.extractExecutionFailureInfo((PrestoSparkExecutionException) executionFailure);
                    verify(executionFailureInfo.isPresent());
                    throw executionFailureInfo.get().toFailure();
                }

                throw toFailure(executionFailure).toFailure();
            }

            // successfully finished
            queryCompletedEvent(Optional.empty());

            ConnectorSession connectorSession = session.toConnectorSession();
            List<Type> types = plan.getFragment().getTypes();
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
            return plan.getFragment().getTypes();
        }

        public Optional<String> getUpdateType()
        {
            return updateType;
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
                        taskStatsCollector,
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
                    taskStatsCollector,
                    tableWriteInfo,
                    outputType);
            return new RddAndMore<>(rdd, broadcastDependencies.build());
        }

        private void queryCompletedEvent(Optional<Throwable> failure)
        {
            // TODO: implement query monitor and collect query info
            // QueryInfo queryInfo = createQueryInfo(failure);
            // queryMonitor.queryCompletedEvent(queryInfo);
        }

        private QueryInfo createQueryInfo(Optional<Throwable> failure)
        {
            List<SerializedTaskStats> serializedTaskStats = taskStatsCollector.value();
            List<TaskStats> taskStats = serializedTaskStats.stream()
                    .map(SerializedTaskStats::getBytes)
                    .map(taskStatsJsonCodec::fromJson)
                    .collect(toImmutableList());
            // TODO: create query info
            return null;
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
}
