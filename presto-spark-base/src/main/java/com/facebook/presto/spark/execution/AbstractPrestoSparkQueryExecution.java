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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsTracker;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateTimer;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget;
import com.facebook.presto.execution.scheduler.StreamingPlanSection;
import com.facebook.presto.execution.scheduler.StreamingSubPlan;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spark.ErrorClassifier;
import com.facebook.presto.spark.PrestoSparkBroadcastDependency;
import com.facebook.presto.spark.PrestoSparkMemoryBasedBroadcastDependency;
import com.facebook.presto.spark.PrestoSparkMetadataStorage;
import com.facebook.presto.spark.PrestoSparkNativeStorageBasedDependency;
import com.facebook.presto.spark.PrestoSparkQueryData;
import com.facebook.presto.spark.PrestoSparkQueryExecutionFactory;
import com.facebook.presto.spark.PrestoSparkQueryStatusInfo;
import com.facebook.presto.spark.PrestoSparkServiceWaitTimeMetrics;
import com.facebook.presto.spark.PrestoSparkStorageBasedBroadcastDependency;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.RddAndMore;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecution;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutor;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkExecutionException;
import com.facebook.presto.spark.classloader_interface.PrestoSparkJavaExecutionTaskInputs;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkPartitioner;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleSerializer;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkStorageHandle;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spark.execution.task.PrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.planner.PrestoSparkPlanFragmenter;
import com.facebook.presto.spark.planner.PrestoSparkQueryPlanner.PlanAndMore;
import com.facebook.presto.spark.planner.PrestoSparkRddFactory;
import com.facebook.presto.spark.util.PrestoSparkTransactionUtils;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorCapabilities;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.storage.StorageCapabilities;
import com.facebook.presto.spi.storage.TempDataOperationContext;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.spark.MapOutputStatistics;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SimpleFutureAction;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.ShuffledRDD;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.airlift.units.DataSize.Unit.BYTE;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxBroadcastMemory;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxTotalMemoryPerNode;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.scheduler.StreamingPlanSection.extractStreamingSections;
import static com.facebook.presto.execution.scheduler.TableWriteInfo.createTableWriteInfo;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getSparkBroadcastJoinMaxMemoryOverride;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isStorageBasedBroadcastJoinEnabled;
import static com.facebook.presto.spark.PrestoSparkSettingsRequirements.SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS_CONFIG;
import static com.facebook.presto.spark.SparkErrorCode.EXCEEDED_SPARK_DRIVER_MAX_RESULT_SIZE;
import static com.facebook.presto.spark.SparkErrorCode.GENERIC_SPARK_ERROR;
import static com.facebook.presto.spark.SparkErrorCode.SPARK_EXECUTOR_LOST;
import static com.facebook.presto.spark.SparkErrorCode.SPARK_EXECUTOR_OOM;
import static com.facebook.presto.spark.SparkErrorCode.UNSUPPORTED_STORAGE_TYPE;
import static com.facebook.presto.spark.classloader_interface.ScalaUtils.collectScalaIterator;
import static com.facebook.presto.spark.classloader_interface.ScalaUtils.emptyScalaIterator;
import static com.facebook.presto.spark.planner.PrestoSparkRddFactory.getRDDName;
import static com.facebook.presto.spark.util.PrestoSparkFailureUtils.toPrestoSparkFailure;
import static com.facebook.presto.spark.util.PrestoSparkUtils.classTag;
import static com.facebook.presto.spark.util.PrestoSparkUtils.computeNextTimeout;
import static com.facebook.presto.spark.util.PrestoSparkUtils.deserializeZstdCompressed;
import static com.facebook.presto.spark.util.PrestoSparkUtils.toSerializedPage;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_PAGE_SINK_COMMIT;
import static com.facebook.presto.spi.storage.StorageCapabilities.REMOTELY_ACCESSIBLE;
import static com.facebook.presto.sql.planner.PlanFragmenterUtils.isRootFragment;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static java.lang.Math.min;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.spark.util.Utils.isLocalMaster;

public abstract class AbstractPrestoSparkQueryExecution
        implements IPrestoSparkQueryExecution
{
    private static final Logger log = Logger.get(AbstractPrestoSparkQueryExecution.class);

    protected final Session session;
    protected final QueryMonitor queryMonitor;
    protected final CollectionAccumulator<SerializedTaskInfo> taskInfoCollector;
    protected final CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector;
    // used to create tasks on the Driver
    protected final PrestoSparkTaskExecutorFactory taskExecutorFactory;
    // used to create tasks on executor, serializable
    protected final PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider;
    protected final QueryStateTimer queryStateTimer;
    protected final WarningCollector warningCollector;
    protected final String query;
    protected final PlanAndMore planAndMore;
    protected final Optional<String> sparkQueueName;
    protected final Codec<TaskInfo> taskInfoCodec;
    protected final JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec;
    protected final JsonCodec<PrestoSparkQueryStatusInfo> queryStatusInfoJsonCodec;
    protected final JsonCodec<PrestoSparkQueryData> queryDataJsonCodec;
    protected final PrestoSparkRddFactory rddFactory;
    protected final TransactionManager transactionManager;
    protected final PagesSerde pagesSerde;
    protected final PrestoSparkExecutionExceptionFactory executionExceptionFactory;
    protected final Duration queryTimeout;
    protected final Metadata metadata;
    protected final PrestoSparkMetadataStorage metadataStorage;
    protected final Optional<String> queryStatusInfoOutputLocation;
    protected final Optional<String> queryDataOutputLocation;
    protected final long queryCompletionDeadline;
    protected final TempStorage tempStorage;
    protected final NodeMemoryConfig nodeMemoryConfig;
    protected final FeaturesConfig featuresConfig;
    protected final QueryManagerConfig queryManagerConfig;
    protected final Set<PrestoSparkServiceWaitTimeMetrics> waitTimeMetrics;
    protected final Optional<ErrorClassifier> errorClassifier;
    protected final JavaSparkContext sparkContext;
    protected final PrestoSparkPlanFragmenter planFragmenter;
    protected final PartitioningProviderManager partitioningProviderManager;
    protected final HistoryBasedPlanStatisticsTracker historyBasedPlanStatisticsTracker;
    private AtomicReference<SubPlan> finalFragmentedPlan = new AtomicReference<>();
    @GuardedBy("this")
    private final Map<PlanFragmentId, RddAndMore> fragmentIdToRdd = new HashMap<>();
    private final Optional<CollectionAccumulator<Map<String, Long>>> bootstrapMetricsCollector;

    public AbstractPrestoSparkQueryExecution(
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
            Optional<String> sparkQueueName,
            Codec<TaskInfo> taskInfoCodec,
            JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec,
            JsonCodec<PrestoSparkQueryStatusInfo> queryStatusInfoJsonCodec,
            JsonCodec<PrestoSparkQueryData> queryDataJsonCodec,
            PrestoSparkRddFactory rddFactory,
            TransactionManager transactionManager,
            PagesSerde pagesSerde,
            PrestoSparkExecutionExceptionFactory executionExceptionFactory,
            Duration queryTimeout,
            long queryCompletionDeadline,
            PrestoSparkMetadataStorage metadataStorage,
            Optional<String> queryStatusInfoOutputLocation,
            Optional<String> queryDataOutputLocation,
            TempStorage tempStorage,
            NodeMemoryConfig nodeMemoryConfig,
            FeaturesConfig featuresConfig,
            QueryManagerConfig queryManagerConfig,
            Set<PrestoSparkServiceWaitTimeMetrics> waitTimeMetrics,
            Optional<ErrorClassifier> errorClassifier,
            PrestoSparkPlanFragmenter planFragmenter,
            Metadata metadata,
            PartitioningProviderManager partitioningProviderManager,
            HistoryBasedPlanStatisticsTracker historyBasedPlanStatisticsTracker,
            Optional<CollectionAccumulator<Map<String, Long>>> bootstrapMetricsCollector)
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
        this.sparkQueueName = requireNonNull(sparkQueueName, "sparkQueueName is null");

        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        this.sparkTaskDescriptorJsonCodec = requireNonNull(sparkTaskDescriptorJsonCodec, "sparkTaskDescriptorJsonCodec is null");
        this.queryStatusInfoJsonCodec = requireNonNull(queryStatusInfoJsonCodec, "queryStatusInfoJsonCodec is null");
        this.queryDataJsonCodec = requireNonNull(queryDataJsonCodec, "queryDataJsonCodec is null");
        this.rddFactory = requireNonNull(rddFactory, "rddFactory is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.executionExceptionFactory = requireNonNull(executionExceptionFactory, "executionExceptionFactory is null");
        this.queryTimeout = requireNonNull(queryTimeout, "queryTimeout is null");
        this.queryCompletionDeadline = queryCompletionDeadline;
        this.metadataStorage = requireNonNull(metadataStorage, "metadataStorage is null");
        this.queryStatusInfoOutputLocation = requireNonNull(queryStatusInfoOutputLocation, "queryStatusInfoOutputLocation is null");
        this.queryDataOutputLocation = requireNonNull(queryDataOutputLocation, "queryDataOutputLocation is null");
        this.tempStorage = requireNonNull(tempStorage, "tempStorage is null");
        this.nodeMemoryConfig = requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null");
        this.featuresConfig = requireNonNull(featuresConfig, "featuresConfig is null");
        this.queryManagerConfig = requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.waitTimeMetrics = requireNonNull(waitTimeMetrics, "waitTimeMetrics is null");
        this.errorClassifier = requireNonNull(errorClassifier, "errorClassifier is null");
        this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.partitioningProviderManager = requireNonNull(partitioningProviderManager, "partitioningProviderManager is null");
        this.historyBasedPlanStatisticsTracker = requireNonNull(historyBasedPlanStatisticsTracker, "historyBasedPlanStatisticsTracker is null");
        this.bootstrapMetricsCollector = requireNonNull(bootstrapMetricsCollector, "bootstrapTimeCollector is null");
    }

    protected static JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow> partitionBy(
            int planFragmentId,
            JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow> rdd,
            PartitioningScheme partitioningScheme)
    {
        Partitioner partitioner = createPartitioner(partitioningScheme);
        JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow> javaPairRdd = rdd.partitionBy(partitioner);
        ShuffledRDD<MutablePartitionId, PrestoSparkMutableRow, PrestoSparkMutableRow> shuffledRdd = (ShuffledRDD<MutablePartitionId, PrestoSparkMutableRow, PrestoSparkMutableRow>) javaPairRdd.rdd();
        shuffledRdd.setSerializer(new PrestoSparkShuffleSerializer());
        shuffledRdd.setName(getRDDName(planFragmentId));
        return JavaPairRDD.fromRDD(
                shuffledRdd,
                classTag(MutablePartitionId.class),
                classTag(PrestoSparkMutableRow.class));
    }

    protected static Partitioner createPartitioner(PartitioningScheme partitioningScheme)
    {
        PartitioningHandle partitioning = partitioningScheme.getPartitioning().getHandle();
        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            return new PrestoSparkPartitioner(1);
        }
        if (partitioning.equals(FIXED_HASH_DISTRIBUTION)
                || partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)
                || partitioning.getConnectorId().isPresent()) {
            int[] bucketToPartition = partitioningScheme.getBucketToPartition().orElseThrow(
                    () -> new IllegalArgumentException("bucketToPartition is expected to be assigned at this point"));
            checkArgument(bucketToPartition.length > 0, "bucketToPartition is expected to be non empty");
            int numberOfPartitions = IntStream.of(bucketToPartition)
                    .max()
                    .getAsInt() + 1;
            return new PrestoSparkPartitioner(numberOfPartitions);
        }
        throw new IllegalArgumentException("Unexpected partitioning: " + partitioning);
    }

    @Override
    public List<List<Object>> execute()
    {
        List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>> rddResults;
        try {
            tuneMaxExecutorsCount();
            rddResults = doExecute();
            queryStateTimer.beginFinishing();
            PrestoSparkTransactionUtils.commit(session, transactionManager);
            queryStateTimer.endQuery();
        }
        catch (Throwable executionException) {
            queryStateTimer.beginFinishing();
            try {
                PrestoSparkTransactionUtils.rollback(session, transactionManager);
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
                    else if (errorClassifier.isPresent()) {
                        wrappedPrestoException = errorClassifier.get().classify(executionException);
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

            throw toPrestoSparkFailure(session, failureInfo.get());
        }

        processShuffleStats();

        ConnectorSession connectorSession = session.toConnectorSession();
        List<Type> types = getOutputTypes();
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
                results.get(0).size() == 1 &&
                results.get(0).get(0) != null) {
            updateCount = OptionalLong.of(((Number) results.get(0).get(0)).longValue());
        }

        // successfully finished
        try {
            queryCompletedEvent(Optional.empty(), updateCount);
        }
        catch (RuntimeException eventFailure) {
            log.error(eventFailure, "Error publishing query completed event");
        }

        if (queryDataOutputLocation.isPresent()) {
            metadataStorage.write(
                    queryDataOutputLocation.get(),
                    queryDataJsonCodec.toJsonBytes(new PrestoSparkQueryData(PrestoSparkQueryExecutionFactory.getOutputColumns(planAndMore), results)));
        }

        return results;
    }

    public List<Type> getOutputTypes()
    {
        Optional<SubPlan> subPlanOptional = getFinalFragmentedPlan();
        verify(subPlanOptional.isPresent(), "finalFragmentedPlan is null");
        return subPlanOptional.get().getFragment().getTypes();
    }

    public Optional<String> getUpdateType()
    {
        return planAndMore.getUpdateType();
    }

    protected abstract List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>> doExecute()
            throws SparkException, TimeoutException;

    protected List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>> collectPages(TableWriteInfo tableWriteInfo, PlanFragment rootFragment, Map<PlanFragmentId, RddAndMore<PrestoSparkSerializedPage>> inputRdds)
            throws SparkException, TimeoutException
    {
        PrestoSparkTaskDescriptor taskDescriptor = new PrestoSparkTaskDescriptor(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                rootFragment,
                tableWriteInfo);
        SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor = new SerializedPrestoSparkTaskDescriptor(sparkTaskDescriptorJsonCodec.toJsonBytes(taskDescriptor));

        Map<String, JavaFutureAction<List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>>>> inputFutures = inputRdds.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> entry.getValue().getRdd().collectAsync()));

        PrestoSparkQueryExecutionFactory.waitForActionsCompletionWithTimeout(inputFutures.values(), computeNextTimeout(queryCompletionDeadline), MILLISECONDS, waitTimeMetrics);

        // release memory retained by the RDDs (splits and dependencies)
        inputRdds = null;

        ImmutableMap.Builder<String, List<PrestoSparkSerializedPage>> inputs = ImmutableMap.builder();
        long totalNumberOfPagesReceived = 0;
        long totalCompressedSizeInBytes = 0;
        long totalUncompressedSizeInBytes = 0;
        for (Map.Entry<String, JavaFutureAction<List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>>>> inputFuture : inputFutures.entrySet()) {
            // Use a mutable list to allow memory release on per page basis
            List<PrestoSparkSerializedPage> pages = new ArrayList<>();
            List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>> tuples = getUnchecked(inputFuture.getValue());
            long currentFragmentOutputCompressedSizeInBytes = 0;
            long currentFragmentOutputUncompressedSizeInBytes = 0;
            for (Tuple2<MutablePartitionId, PrestoSparkSerializedPage> tuple : tuples) {
                PrestoSparkSerializedPage page = tuple._2;
                currentFragmentOutputCompressedSizeInBytes += page.getSize();
                currentFragmentOutputUncompressedSizeInBytes += page.getUncompressedSizeInBytes();
                log.info("Received %s rows from partition %s in fragment %s", page.getPositionCount(), tuple._1.getPartition(), inputFuture.getKey());
                pages.add(page);
            }
            log.info(
                    "Received %s pages from fragment %s. Compressed size: %s. Uncompressed size: %s.",
                    pages.size(),
                    inputFuture.getKey(),
                    DataSize.succinctBytes(currentFragmentOutputCompressedSizeInBytes),
                    DataSize.succinctBytes(currentFragmentOutputUncompressedSizeInBytes));
            totalNumberOfPagesReceived += pages.size();
            totalCompressedSizeInBytes += currentFragmentOutputCompressedSizeInBytes;
            totalUncompressedSizeInBytes += currentFragmentOutputUncompressedSizeInBytes;
            inputs.put(inputFuture.getKey(), pages);
        }

        log.info(
                "Received %s pages in total. Compressed size: %s. Uncompressed size: %s.",
                totalNumberOfPagesReceived,
                DataSize.succinctBytes(totalCompressedSizeInBytes),
                DataSize.succinctBytes(totalUncompressedSizeInBytes));

        IPrestoSparkTaskExecutor<PrestoSparkSerializedPage> prestoSparkTaskExecutor = taskExecutorFactory.create(
                0,
                0,
                serializedTaskDescriptor,
                emptyScalaIterator(),
                new PrestoSparkJavaExecutionTaskInputs(ImmutableMap.of(), ImmutableMap.of(), inputs.build()),
                taskInfoCollector,
                shuffleStatsCollector,
                PrestoSparkSerializedPage.class);
        return collectScalaIterator(prestoSparkTaskExecutor);
    }

    @VisibleForTesting
    public <T extends PrestoSparkTaskOutput> RddAndMore<T> createRdd(SubPlan subPlan, Class<T> outputType, TableWriteInfo tableWriteInfo)
            throws SparkException, TimeoutException
    {
        ImmutableMap.Builder<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> rddInputs = ImmutableMap.builder();
        ImmutableMap.Builder<PlanFragmentId, Broadcast<?>> broadcastInputs = ImmutableMap.builder();
        ImmutableList.Builder<PrestoSparkBroadcastDependency<?>> broadcastDependencies = ImmutableList.builder();

        for (SubPlan child : subPlan.getChildren()) {
            PlanFragment childFragment = child.getFragment();
            if (childFragment.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION)) {
                RddAndMore<?> childRdd;
                PrestoSparkBroadcastDependency<?> broadcastDependency;
                if (isStorageBasedBroadcastJoinEnabled(session)) {
                    childRdd = createRdd(child, PrestoSparkStorageHandle.class, tableWriteInfo);
                }
                else {
                    childRdd = createRdd(child, PrestoSparkSerializedPage.class, tableWriteInfo);
                }
                broadcastDependency = createBroadcastDependency(childRdd);
                broadcastInputs.put(childFragment.getId(), broadcastDependency.executeBroadcast(sparkContext));
                broadcastDependencies.add(broadcastDependency);
            }
            else {
                RddAndMore<PrestoSparkMutableRow> childRdd = createRdd(child, PrestoSparkMutableRow.class, tableWriteInfo);
                rddInputs.put(childFragment.getId(), partitionBy(childFragment.getId().getId(), childRdd.getRdd(), child.getFragment().getPartitioningScheme()));
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

    protected void validateStorageCapabilities(TempStorage tempStorage)
    {
        boolean isLocalMode = isLocalMaster(sparkContext.getConf());
        List<StorageCapabilities> storageCapabilities = tempStorage.getStorageCapabilities();
        if (!isLocalMode && !storageCapabilities.contains(REMOTELY_ACCESSIBLE)) {
            throw new PrestoException(UNSUPPORTED_STORAGE_TYPE, "Configured TempStorage does not support remote access required for distributing broadcast tables.");
        }
    }

    /**
     * Updates the taskInfoMap to ensure it stores the most relevant {@link TaskInfo} for each
     * logical task, identified by task ID (excluding attempt number).
     * <p>
     * This method ensures that, for each logical task, the map retains the latest successful
     * attempt if available, or otherwise the most recent attempt based on attempt number. Warnings
     * are logged in cases of unexpected duplicate or multiple successful attempts.
     *
     * @param taskInfoMap the map from logical task ID (taskId excluding attempt number) to
     * {@link TaskInfo}
     * @param taskInfo the {@link TaskInfo} to consider for updating the map
     */
    private void updateTaskInfoMap(HashMap<String, TaskInfo> taskInfoMap, TaskInfo taskInfo)
    {
        TaskId newTaskId = taskInfo.getTaskId();
        String taskIdWithoutAttemptId = new StringBuilder()
                .append(newTaskId.getStageExecutionId().toString())
                .append(".")
                .append(newTaskId.getId())
                .toString();
        if (!taskInfoMap.containsKey(taskIdWithoutAttemptId)) {
            taskInfoMap.put(taskIdWithoutAttemptId, taskInfo);
            return;
        }

        TaskInfo storedTaskInfo = taskInfoMap.get(taskIdWithoutAttemptId);
        TaskId storedTaskId = storedTaskInfo.getTaskId();
        TaskState storedTaskState = storedTaskInfo.getTaskStatus().getState();
        TaskState newTaskState = taskInfo.getTaskStatus().getState();
        if (storedTaskState == TaskState.FINISHED) {
            if (newTaskState == TaskState.FINISHED) {
                log.warn("Multiple attempts of the same task have succeeded %s vs %s",
                        storedTaskId.toString(), newTaskId.toString());
            }
            // Successful one has been stored. Nothing needs to be done.
            return;
        }

        int storedAttemptNumber = storedTaskId.getAttemptNumber();
        int newAttemptNumber = newTaskId.getAttemptNumber();
        if (newTaskState == TaskState.FINISHED || storedAttemptNumber < newAttemptNumber) {
            taskInfoMap.put(taskIdWithoutAttemptId, taskInfo);
        }
        if (storedAttemptNumber == newAttemptNumber) {
            log.warn("Received multiple identical TaskId %s vs %s",
                    storedTaskId.toString(), newTaskId.toString());
        }
    }

    protected void queryCompletedEvent(Optional<ExecutionFailureInfo> failureInfo, OptionalLong updateCount)
    {
        List<SerializedTaskInfo> serializedTaskInfos = taskInfoCollector.value();
        HashMap<String, TaskInfo> taskInfoMap = new HashMap<>();
        long totalSerializedTaskInfoSizeInBytes = 0;
        for (SerializedTaskInfo serializedTaskInfo : serializedTaskInfos) {
            byte[] bytes = serializedTaskInfo.getBytesAndClear();
            totalSerializedTaskInfoSizeInBytes += bytes.length;
            TaskInfo taskInfo = deserializeZstdCompressed(taskInfoCodec, bytes);
            updateTaskInfoMap(taskInfoMap, taskInfo);
        }
        taskInfoCollector.reset();

        log.info("Total serialized task info count %s size: %s. Total deduped task info count %s",
                serializedTaskInfos.size(),
                DataSize.succinctBytes(totalSerializedTaskInfoSizeInBytes),
                taskInfoMap.size());

        Optional<StageInfo> stageInfoOptional = getFinalFragmentedPlan().map(finalFragmentedPlan ->
                PrestoSparkQueryExecutionFactory.createStageInfo(
                        session.getQueryId(),
                        finalFragmentedPlan,
                        taskInfoMap.values().stream().collect(Collectors.toList())));
        QueryState queryState = failureInfo.isPresent() ? FAILED : FINISHED;

        QueryInfo queryInfo = PrestoSparkQueryExecutionFactory.createQueryInfo(
                session,
                query,
                queryState,
                Optional.of(planAndMore),
                sparkQueueName,
                failureInfo,
                queryStateTimer,
                stageInfoOptional,
                warningCollector);

        queryMonitor.queryCompletedEvent(queryInfo);
        historyBasedPlanStatisticsTracker.updateStatistics(queryInfo);
        if (queryStatusInfoOutputLocation.isPresent()) {
            PrestoSparkQueryStatusInfo prestoSparkQueryStatusInfo = PrestoSparkQueryExecutionFactory.createPrestoSparkQueryInfo(
                    queryInfo,
                    Optional.of(planAndMore),
                    warningCollector,
                    updateCount);
            metadataStorage.write(
                    queryStatusInfoOutputLocation.get(),
                    queryStatusInfoJsonCodec.toJsonBytes(prestoSparkQueryStatusInfo));
        }
        processBootstrapStats();
    }

    protected final void setFinalFragmentedPlan(SubPlan subPlan)
    {
        verify(subPlan != null, "subPlan is null");
        boolean updated = finalFragmentedPlan.compareAndSet(null, subPlan);
        verify(updated, "finalFragmentedPlan is already non-null");
    }

    public final Optional<SubPlan> getFinalFragmentedPlan()
    {
        return Optional.ofNullable(finalFragmentedPlan.get());
    }

    protected void processShuffleStats()
    {
        List<PrestoSparkShuffleStats> statsList = shuffleStatsCollector.value();
        Map<ShuffleStatsKey, List<PrestoSparkShuffleStats>> statsMap = new TreeMap<>();
        for (PrestoSparkShuffleStats stats : statsList) {
            ShuffleStatsKey key = new ShuffleStatsKey(stats.getFragmentId(), stats.getOperation());
            statsMap.computeIfAbsent(key, (ignored) -> new ArrayList<>()).add(stats);
        }
        log.info("Shuffle statistics summary:");
        for (Map.Entry<ShuffleStatsKey, List<PrestoSparkShuffleStats>> fragment : statsMap.entrySet()) {
            logShuffleStatsSummary(fragment.getKey(), fragment.getValue());
        }
        shuffleStatsCollector.reset();
    }

    protected void logShuffleStatsSummary(ShuffleStatsKey key, List<PrestoSparkShuffleStats> statsList)
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

    protected Optional<int[]> getBucketToPartition(Session session, PartitioningHandle partitioningHandle, int hashPartitionCount)
    {
        if (partitioningHandle.equals(FIXED_HASH_DISTRIBUTION)) {
            return Optional.of(IntStream.range(0, hashPartitionCount).toArray());
        }
        //  FIXED_ARBITRARY_DISTRIBUTION is used for UNION ALL
        //  UNION ALL inputs could be source inputs or shuffle inputs
        if (partitioningHandle.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            // given modular hash function, partition count could be arbitrary size
            // simply reuse hash_partition_count for convenience
            // it can also be set by a separate session property if needed
            return Optional.of(IntStream.range(0, hashPartitionCount).toArray());
        }
        if (partitioningHandle.getConnectorId().isPresent()) {
            int connectorPartitionCount = getPartitionCount(session, partitioningHandle);
            return Optional.of(IntStream.range(0, connectorPartitionCount).toArray());
        }
        return Optional.empty();
    }

    protected int getPartitionCount(Session session, PartitioningHandle partitioning)
    {
        ConnectorNodePartitioningProvider partitioningProvider = getPartitioningProvider(partitioning);
        return partitioningProvider.getBucketCount(
                partitioning.getTransactionHandle().orElse(null),
                session.toConnectorSession(),
                partitioning.getConnectorHandle());
    }

    protected ConnectorNodePartitioningProvider getPartitioningProvider(PartitioningHandle partitioning)
    {
        ConnectorId connectorId = partitioning.getConnectorId()
                .orElseThrow(() -> new IllegalArgumentException("Unexpected partitioning: " + partitioning));
        return partitioningProviderManager.getPartitioningProvider(connectorId);
    }

    protected SubPlan configureOutputPartitioning(Session session, SubPlan subPlan, int hashPartitionCount)
    {
        PlanFragment fragment = subPlan.getFragment();
        if (!fragment.getPartitioningScheme().getBucketToPartition().isPresent()) {
            PartitioningHandle partitioningHandle = fragment.getPartitioningScheme().getPartitioning().getHandle();
            Optional<int[]> bucketToPartition = getBucketToPartition(session, partitioningHandle, hashPartitionCount);
            if (bucketToPartition.isPresent()) {
                fragment = fragment.withBucketToPartition(bucketToPartition);
            }
        }
        return new SubPlan(
                fragment,
                subPlan.getChildren().stream()
                        .map(child -> configureOutputPartitioning(session, child, hashPartitionCount))
                        .collect(toImmutableList()));
    }

    @VisibleForTesting
    public TableWriteInfo getTableWriteInfo(Session session, SubPlan plan)
    {
        StreamingPlanSection streamingPlanSection = extractStreamingSections(plan);
        StreamingSubPlan streamingSubPlan = streamingPlanSection.getPlan();
        TableWriteInfo tableWriteInfo = createTableWriteInfo(streamingSubPlan, metadata, session);
        if (tableWriteInfo.getWriterTarget().isPresent()) {
            checkPageSinkCommitIsSupported(session, tableWriteInfo.getWriterTarget().get());
        }
        return tableWriteInfo;
    }

    @VisibleForTesting
    public TableWriteInfo getTableWriteInfo(Session session, PlanNode planNode)
    {
        TableWriteInfo tableWriteInfo = createTableWriteInfo(planNode, metadata, session);
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
        else if (writerTarget instanceof ExecutionWriterTarget.RefreshMaterializedViewHandle) {
            connectorId = ((ExecutionWriterTarget.RefreshMaterializedViewHandle) writerTarget).getHandle().getConnectorId();
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

    // Returns RDD for specified fragmented SubPlan
    // This method ensures that RDD is created only once for a sub-plan, where identity is determined by fragment id
    // For broadcast RDDs, it returns RDD to be broadcasted.
    protected synchronized <T extends PrestoSparkTaskOutput> RddAndMore<T> createRddForSubPlan(SubPlan subPlan,
            TableWriteInfo tableWriteInfo,
            Optional<Class<?>> outputTypeOptional)
            throws SparkException, TimeoutException
    {
        if (fragmentIdToRdd.containsKey(subPlan.getFragment().getId())) {
            return fragmentIdToRdd.get(subPlan.getFragment().getId());
        }

        ImmutableMap.Builder<PlanFragmentId, JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>> rddInputs = ImmutableMap.builder();
        ImmutableMap.Builder<PlanFragmentId, Broadcast<?>> broadcastInputs = ImmutableMap.builder();
        ImmutableList.Builder<PrestoSparkBroadcastDependency<?>> broadcastDependencies = ImmutableList.builder();
        for (SubPlan child : subPlan.getChildren()) {
            RddAndMore<?> childRdd = createRddForSubPlan(child, tableWriteInfo, Optional.empty());
            if (childRdd.isBroadcastDistribution()) {
                PrestoSparkBroadcastDependency<?> broadcastDependency = createBroadcastDependency(childRdd);
                broadcastInputs.put(child.getFragment().getId(), broadcastDependency.executeBroadcast(sparkContext));
                broadcastDependencies.add(broadcastDependency);
            }
            else {
                rddInputs.put(child.getFragment().getId(), (JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>) childRdd.getRdd());
                broadcastDependencies.addAll(childRdd.getBroadcastDependencies());
            }
        }

        Class outputType = outputTypeOptional.orElseGet(() -> getOutputType(subPlan));
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

        // For intermediate, non-broadcast stages - we use partitioned RDD
        // These stages produce PrestoSparkMutableRow
        if (outputType == PrestoSparkMutableRow.class) {
            rdd = (JavaPairRDD<MutablePartitionId, T>) partitionBy(subPlan.getFragment().getId().getId(), (JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow>) rdd, subPlan.getFragment().getPartitioningScheme());
        }

        RddAndMore rddAndMore = new RddAndMore<T>(rdd, broadcastDependencies.build(), Optional.ofNullable(subPlan.getFragment().getPartitioningScheme().getPartitioning().getHandle()));
        fragmentIdToRdd.put(subPlan.getFragment().getId(), rddAndMore);
        return rddAndMore;
    }

    protected Optional<RddAndMore> getRdd(PlanFragmentId planFragmentId)
    {
        return Optional.ofNullable(fragmentIdToRdd.get(planFragmentId));
    }

    // Returns output type of RDD for a subPlan
    private Class getOutputType(SubPlan subPlan)
    {
        // Root node has SerializedPage as output
        if (isRootFragment(subPlan.getFragment())) {
            return PrestoSparkSerializedPage.class;
        }
        // Broadcast node can have SerializedPage vs Storage handle depending on how broadcast is done
        if (isBroadcastDistribution(subPlan)) {
            return getOutputTypeForBroadcastNode();
        }
        // Everything else is Mutable row
        return PrestoSparkMutableRow.class;
    }

    private Class getOutputTypeForBroadcastNode()
    {
        if (isStorageBasedBroadcastJoinEnabled(session)) {
            return PrestoSparkStorageHandle.class; // Handle to file
        }
        else {
            return PrestoSparkSerializedPage.class; // In Memory broadcast
        }
    }

    private boolean isBroadcastDistribution(SubPlan subPlan)
    {
        return subPlan.getFragment().getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION);
    }

    private PrestoSparkBroadcastDependency<?> createBroadcastDependency(RddAndMore<?> childRdd)
    {
        PrestoSparkBroadcastDependency<?> broadcastDependency;
        DataSize maxBroadcastMemory = getSparkBroadcastJoinMaxMemoryOverride(session);
        if (maxBroadcastMemory == null) {
            maxBroadcastMemory = new DataSize(min(nodeMemoryConfig.getMaxQueryBroadcastMemory().toBytes(), getQueryMaxBroadcastMemory(session).toBytes()), BYTE);
        }

        if (featuresConfig.isNativeExecutionEnabled()) {
            return new PrestoSparkNativeStorageBasedDependency(
                    (RddAndMore<PrestoSparkSerializedPage>) childRdd,
                    maxBroadcastMemory,
                    queryCompletionDeadline,
                    waitTimeMetrics,
                    pagesSerde);
        }

        if (isStorageBasedBroadcastJoinEnabled(session)) {
            validateStorageCapabilities(tempStorage);
            TempDataOperationContext tempDataOperationContext = new TempDataOperationContext(
                    session.getSource(),
                    session.getQueryId().getId(),
                    session.getClientInfo(),
                    Optional.of(session.getClientTags()),
                    session.getIdentity());

            broadcastDependency = new PrestoSparkStorageBasedBroadcastDependency(
                    (RddAndMore<PrestoSparkStorageHandle>) childRdd,
                    maxBroadcastMemory,
                    getQueryMaxTotalMemoryPerNode(session),
                    queryCompletionDeadline,
                    tempStorage,
                    tempDataOperationContext,
                    waitTimeMetrics);
        }
        else {
            broadcastDependency = new PrestoSparkMemoryBasedBroadcastDependency(
                    (RddAndMore<PrestoSparkSerializedPage>) childRdd,
                    maxBroadcastMemory,
                    queryCompletionDeadline,
                    waitTimeMetrics);
        }
        return broadcastDependency;
    }

    @VisibleForTesting
    public FragmentExecutionResult executeFragment(SubPlan plan,
            TableWriteInfo tableWriteInfo,
            Optional<Class<?>> outputType)
            throws SparkException, TimeoutException
    {
        RddAndMore rddAndMore = createRddForSubPlan(plan, tableWriteInfo, outputType);
        List<ShuffleDependency> shuffleDependencies = rddAndMore.getShuffleDependencies();
        SimpleFutureAction<MapOutputStatistics> mapOutputStatisticsFutureAction = null;

        // For PoS, we don't expect more than 1 shuffle dependency.
        verify(shuffleDependencies.size() <= 1, "More than 1 shuffle dependency found");
        if (!shuffleDependencies.isEmpty()
                // We can only execute map stage on RDD with more than 0 partition(Non-Empty tables)
                && shuffleDependencies.get(0).rdd().partitions().length > 0) {
            ShuffleDependency shuffleDependency = shuffleDependencies.get(0);
            mapOutputStatisticsFutureAction = sparkContext.sc().submitMapStage(shuffleDependency);
        }
        return new FragmentExecutionResult(rddAndMore, Optional.ofNullable(mapOutputStatisticsFutureAction));
    }

    private static class ShuffleStatsKey
            implements Comparable<ShuffleStatsKey>
    {
        private final int fragmentId;
        private final PrestoSparkShuffleStats.Operation operation;

        public ShuffleStatsKey(int fragmentId, PrestoSparkShuffleStats.Operation operation)
        {
            this.fragmentId = fragmentId;
            this.operation = requireNonNull(operation, "operation is null");
        }

        public int getFragmentId()
        {
            return fragmentId;
        }

        public PrestoSparkShuffleStats.Operation getOperation()
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

    private void processBootstrapStats()
    {
        if (!this.bootstrapMetricsCollector.isPresent()) {
            return;
        }
        List<Map<String, Long>> bootstrapStats = this.bootstrapMetricsCollector.get().value();
        int loggedBootstrapCount = bootstrapStats.size();
        if (loggedBootstrapCount > 0) {
            Set<String> statsKeySet = bootstrapStats.get(0).keySet();
            StringBuilder metricsLog = new StringBuilder();
            metricsLog.append("Average executor bootstrap durations in milliseconds: \n");
            for (String statsKey : statsKeySet) {
                double avgDuration = 0.0;
                for (int i = 0; i < loggedBootstrapCount; i++) {
                    avgDuration = (avgDuration * i + bootstrapStats.get(i).get(statsKey)) / (i + 1);
                }
                metricsLog.append(String.format("%s: %.2f \n", statsKey, avgDuration));
            }
            log.info(metricsLog.toString());
        }
        else {
            log.info("No entry found in bootstrapMetricsCollector");
        }
    }

    private void tuneMaxExecutorsCount()
    {
        // Executor allocation is currently only supported at root level of the plan
        // In future this could be extended to fragment level configuration
        if (planAndMore.getPhysicalResourceSettings().isMaxExecutorCountAutoTuned()) {
            sparkContext.sc().conf().set(SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS_CONFIG,
                    Integer.toString(planAndMore.getPhysicalResourceSettings().getMaxExecutorCount()));
        }
    }
}
