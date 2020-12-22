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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.TestingGcMonitor;
import com.facebook.presto.Session;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.event.SplitMonitor;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.execution.buffer.OutputBufferMemoryManager;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.FragmentResultCacheManager;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spark.PrestoSparkAuthenticatorProvider;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutor;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskInputs;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskSource;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spark.execution.PrestoSparkPageOutputOperator.PrestoSparkPageOutputFactory;
import com.facebook.presto.spark.execution.PrestoSparkRowBatch.RowTupleSupplier;
import com.facebook.presto.spark.execution.PrestoSparkRowOutputOperator.PreDeterminedPartitionFunction;
import com.facebook.presto.spark.execution.PrestoSparkRowOutputOperator.PrestoSparkRowOutputFactory;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.security.TokenAuthenticator;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.OutputPartitioning;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import org.joda.time.DateTime;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.execution.TaskState.FAILED;
import static com.facebook.presto.execution.TaskStatus.STARTING_VERSION;
import static com.facebook.presto.execution.buffer.BufferState.FINISHED;
import static com.facebook.presto.metadata.MetadataUpdates.DEFAULT_METADATA_UPDATES;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getShuffleOutputTargetAverageRowSize;
import static com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats.Operation.WRITE;
import static com.facebook.presto.spark.util.PrestoSparkUtils.compress;
import static com.facebook.presto.spark.util.PrestoSparkUtils.decompress;
import static com.facebook.presto.spark.util.PrestoSparkUtils.toPrestoSparkSerializedPage;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagateIfPossible;
import static com.google.common.collect.Iterables.getFirst;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class PrestoSparkTaskExecutorFactory
        implements IPrestoSparkTaskExecutorFactory
{
    private static final Logger log = Logger.get(PrestoSparkTaskExecutorFactory.class);

    private final SessionPropertyManager sessionPropertyManager;
    private final BlockEncodingManager blockEncodingManager;
    private final FunctionAndTypeManager functionAndTypeManager;

    private final JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec;
    private final JsonCodec<TaskSource> taskSourceJsonCodec;
    private final JsonCodec<TaskInfo> taskInfoJsonCodec;

    private final Executor notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final ScheduledExecutorService memoryUpdateExecutor;

    private final LocalExecutionPlanner localExecutionPlanner;
    private final PrestoSparkExecutionExceptionFactory executionExceptionFactory;
    private final TaskExecutor taskExecutor;
    private final SplitMonitor splitMonitor;

    private final Set<PrestoSparkAuthenticatorProvider> authenticatorProviders;

    private final DataSize maxUserMemory;
    private final DataSize maxTotalMemory;
    private final DataSize maxBroadcastMemory;
    private final DataSize maxRevocableMemory;
    private final DataSize maxSpillMemory;
    private final DataSize sinkMaxBufferSize;

    private final boolean perOperatorCpuTimerEnabled;
    private final boolean cpuTimerEnabled;

    private final boolean perOperatorAllocationTrackingEnabled;
    private final boolean allocationTrackingEnabled;

    @Inject
    public PrestoSparkTaskExecutorFactory(
            SessionPropertyManager sessionPropertyManager,
            BlockEncodingManager blockEncodingManager,
            FunctionAndTypeManager functionAndTypeManager,
            JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec,
            JsonCodec<TaskSource> taskSourceJsonCodec,
            JsonCodec<TaskInfo> taskInfoJsonCodec,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            ScheduledExecutorService memoryUpdateExecutor,
            LocalExecutionPlanner localExecutionPlanner,
            PrestoSparkExecutionExceptionFactory executionExceptionFactory,
            TaskExecutor taskExecutor,
            SplitMonitor splitMonitor,
            Set<PrestoSparkAuthenticatorProvider> authenticatorProviders,
            FragmentResultCacheManager fragmentResultCacheManager,
            ObjectMapper objectMapper,
            TaskManagerConfig taskManagerConfig,
            NodeMemoryConfig nodeMemoryConfig,
            NodeSpillConfig nodeSpillConfig)
    {
        this(
                sessionPropertyManager,
                blockEncodingManager,
                functionAndTypeManager,
                taskDescriptorJsonCodec,
                taskSourceJsonCodec,
                taskInfoJsonCodec,
                notificationExecutor,
                yieldExecutor,
                memoryUpdateExecutor,
                localExecutionPlanner,
                executionExceptionFactory,
                taskExecutor,
                splitMonitor,
                authenticatorProviders,
                requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null").getMaxQueryMemoryPerNode(),
                requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null").getMaxQueryTotalMemoryPerNode(),
                requireNonNull(nodeMemoryConfig, "nodeSpillConfig is null").getMaxQueryBroadcastMemory(),
                requireNonNull(nodeSpillConfig, "nodeSpillConfig is null").getMaxRevocableMemoryPerNode(),
                requireNonNull(nodeSpillConfig, "nodeSpillConfig is null").getMaxSpillPerNode(),
                requireNonNull(taskManagerConfig, "taskManagerConfig is null").getSinkMaxBufferSize(),
                requireNonNull(taskManagerConfig, "taskManagerConfig is null").isPerOperatorCpuTimerEnabled(),
                requireNonNull(taskManagerConfig, "taskManagerConfig is null").isTaskCpuTimerEnabled(),
                requireNonNull(taskManagerConfig, "taskManagerConfig is null").isPerOperatorAllocationTrackingEnabled(),
                requireNonNull(taskManagerConfig, "taskManagerConfig is null").isTaskAllocationTrackingEnabled());
    }

    public PrestoSparkTaskExecutorFactory(
            SessionPropertyManager sessionPropertyManager,
            BlockEncodingManager blockEncodingManager,
            FunctionAndTypeManager functionAndTypeManager,
            JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec,
            JsonCodec<TaskSource> taskSourceJsonCodec,
            JsonCodec<TaskInfo> taskInfoJsonCodec,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            ScheduledExecutorService memoryUpdateExecutor,
            LocalExecutionPlanner localExecutionPlanner,
            PrestoSparkExecutionExceptionFactory executionExceptionFactory,
            TaskExecutor taskExecutor,
            SplitMonitor splitMonitor,
            Set<PrestoSparkAuthenticatorProvider> authenticatorProviders,
            DataSize maxUserMemory,
            DataSize maxTotalMemory,
            DataSize maxBroadcastMemory,
            DataSize maxRevocableMemory,
            DataSize maxSpillMemory,
            DataSize sinkMaxBufferSize,
            boolean perOperatorCpuTimerEnabled,
            boolean cpuTimerEnabled,
            boolean perOperatorAllocationTrackingEnabled,
            boolean allocationTrackingEnabled)
    {
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.blockEncodingManager = requireNonNull(blockEncodingManager, "blockEncodingManager is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.taskDescriptorJsonCodec = requireNonNull(taskDescriptorJsonCodec, "sparkTaskDescriptorJsonCodec is null");
        this.taskSourceJsonCodec = requireNonNull(taskSourceJsonCodec, "taskSourceJsonCodec is null");
        this.taskInfoJsonCodec = requireNonNull(taskInfoJsonCodec, "taskInfoJsonCodec is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.yieldExecutor = requireNonNull(yieldExecutor, "yieldExecutor is null");
        this.memoryUpdateExecutor = requireNonNull(memoryUpdateExecutor, "memoryUpdateExecutor is null");
        this.localExecutionPlanner = requireNonNull(localExecutionPlanner, "localExecutionPlanner is null");
        this.executionExceptionFactory = requireNonNull(executionExceptionFactory, "executionExceptionFactory is null");
        this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
        this.splitMonitor = requireNonNull(splitMonitor, "splitMonitor is null");
        this.authenticatorProviders = ImmutableSet.copyOf(requireNonNull(authenticatorProviders, "authenticatorProviders is null"));
        // Ordering is needed to make sure serialized plans are consistent for the same map
        this.maxUserMemory = requireNonNull(maxUserMemory, "maxUserMemory is null");
        this.maxTotalMemory = requireNonNull(maxTotalMemory, "maxTotalMemory is null");
        this.maxBroadcastMemory = requireNonNull(maxBroadcastMemory, "maxBroadcastMemory is null");
        this.maxRevocableMemory = requireNonNull(maxRevocableMemory, "maxRevocableMemory is null");
        this.maxSpillMemory = requireNonNull(maxSpillMemory, "maxSpillMemory is null");
        this.sinkMaxBufferSize = requireNonNull(sinkMaxBufferSize, "sinkMaxBufferSize is null");
        this.perOperatorCpuTimerEnabled = perOperatorCpuTimerEnabled;
        this.cpuTimerEnabled = cpuTimerEnabled;
        this.perOperatorAllocationTrackingEnabled = perOperatorAllocationTrackingEnabled;
        this.allocationTrackingEnabled = allocationTrackingEnabled;
    }

    @Override
    public <T extends PrestoSparkTaskOutput> IPrestoSparkTaskExecutor<T> create(
            int partitionId,
            int attemptNumber,
            SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor,
            Iterator<SerializedPrestoSparkTaskSource> serializedTaskSources,
            PrestoSparkTaskInputs inputs,
            CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
            CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector,
            Class<T> outputType)
    {
        try {
            return doCreate(
                    partitionId,
                    attemptNumber,
                    serializedTaskDescriptor,
                    serializedTaskSources,
                    inputs,
                    taskInfoCollector,
                    shuffleStatsCollector,
                    outputType);
        }
        catch (RuntimeException e) {
            throw executionExceptionFactory.toPrestoSparkExecutionException(e);
        }
    }

    public <T extends PrestoSparkTaskOutput> IPrestoSparkTaskExecutor<T> doCreate(
            int partitionId,
            int attemptNumber,
            SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor,
            Iterator<SerializedPrestoSparkTaskSource> serializedTaskSources,
            PrestoSparkTaskInputs inputs,
            CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
            CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector,
            Class<T> outputType)
    {
        PrestoSparkTaskDescriptor taskDescriptor = taskDescriptorJsonCodec.fromJson(serializedTaskDescriptor.getBytes());
        ImmutableMap.Builder<String, TokenAuthenticator> extraAuthenticators = ImmutableMap.builder();
        authenticatorProviders.forEach(provider -> extraAuthenticators.putAll(provider.getTokenAuthenticators()));

        Session session = taskDescriptor.getSession().toSession(
                sessionPropertyManager,
                taskDescriptor.getExtraCredentials(),
                extraAuthenticators.build());
        PlanFragment fragment = taskDescriptor.getFragment();
        StageId stageId = new StageId(session.getQueryId(), fragment.getId().getId());
        // TODO: include attemptId in taskId
        TaskId taskId = new TaskId(new StageExecutionId(stageId, 0), partitionId);

        List<TaskSource> taskSources = getTaskSources(serializedTaskSources);

        log.info("Task [%s] received %d splits.",
                taskId,
                taskSources.stream()
                        .mapToInt(taskSource -> taskSource.getSplits().size())
                        .sum());

        OptionalLong totalSplitSize = computeAllSplitsSize(taskSources);
        if (totalSplitSize.isPresent()) {
            log.info("Total split size: %s bytes.", totalSplitSize.getAsLong());
        }

        // TODO: Remove this once we can display the plan on Spark UI.

        log.info(PlanPrinter.textPlanFragment(fragment, functionAndTypeManager, session, true));

        MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("spark-executor-memory-pool"), maxTotalMemory);
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(maxSpillMemory);

        QueryContext queryContext = new QueryContext(
                session.getQueryId(),
                maxUserMemory,
                maxTotalMemory,
                maxBroadcastMemory,
                maxRevocableMemory,
                memoryPool,
                new TestingGcMonitor(),
                notificationExecutor,
                yieldExecutor,
                maxSpillMemory,
                spillSpaceTracker);

        TaskStateMachine taskStateMachine = new TaskStateMachine(taskId, notificationExecutor);
        TaskContext taskContext = queryContext.addTaskContext(
                taskStateMachine,
                session,
                perOperatorCpuTimerEnabled,
                cpuTimerEnabled,
                perOperatorAllocationTrackingEnabled,
                allocationTrackingEnabled,
                false);

        ImmutableMap.Builder<PlanNodeId, List<PrestoSparkShuffleInput>> shuffleInputs = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, List<java.util.Iterator<PrestoSparkSerializedPage>>> pageInputs = ImmutableMap.builder();
        for (RemoteSourceNode remoteSource : fragment.getRemoteSourceNodes()) {
            List<PrestoSparkShuffleInput> remoteSourceRowInputs = new ArrayList<>();
            List<java.util.Iterator<PrestoSparkSerializedPage>> remoteSourcePageInputs = new ArrayList<>();
            for (PlanFragmentId sourceFragmentId : remoteSource.getSourceFragmentIds()) {
                Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> shuffleInput = inputs.getShuffleInputs().get(sourceFragmentId.toString());
                Broadcast<List<PrestoSparkSerializedPage>> broadcastInput = inputs.getBroadcastInputs().get(sourceFragmentId.toString());
                List<PrestoSparkSerializedPage> inMemoryInput = inputs.getInMemoryInputs().get(sourceFragmentId.toString());

                if (shuffleInput != null) {
                    checkArgument(broadcastInput == null, "single remote source is not expected to accept different kind of inputs");
                    checkArgument(inMemoryInput == null, "single remote source is not expected to accept different kind of inputs");
                    remoteSourceRowInputs.add(new PrestoSparkShuffleInput(sourceFragmentId.getId(), shuffleInput));
                    continue;
                }

                if (broadcastInput != null) {
                    checkArgument(inMemoryInput == null, "single remote source is not expected to accept different kind of inputs");
                    // TODO: Enable NullifyingIterator once migrated to one task per JVM model
                    // NullifyingIterator removes element from the list upon return
                    // This allows GC to gradually reclaim memory
                    // remoteSourcePageInputs.add(getNullifyingIterator(broadcastInput.value()));
                    remoteSourcePageInputs.add(broadcastInput.value().iterator());
                    continue;
                }

                if (inMemoryInput != null) {
                    remoteSourcePageInputs.add(inMemoryInput.iterator());
                    continue;
                }

                throw new IllegalArgumentException("Input not found for sourceFragmentId: " + sourceFragmentId);
            }
            if (!remoteSourceRowInputs.isEmpty()) {
                shuffleInputs.put(remoteSource.getId(), remoteSourceRowInputs);
            }
            if (!remoteSourcePageInputs.isEmpty()) {
                pageInputs.put(remoteSource.getId(), remoteSourcePageInputs);
            }
        }

        OutputBufferMemoryManager memoryManager = new OutputBufferMemoryManager(
                sinkMaxBufferSize.toBytes(),
                () -> queryContext.getTaskContextByTaskId(taskId).localSystemMemoryContext(),
                notificationExecutor);

        Optional<OutputPartitioning> preDeterminedPartition = Optional.empty();
        if (fragment.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            int partitionCount = getHashPartitionCount(session);
            preDeterminedPartition = Optional.of(new OutputPartitioning(
                    new PreDeterminedPartitionFunction(partitionId % partitionCount, partitionCount),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    false,
                    OptionalInt.empty()));
        }
        Output<T> output = configureOutput(
                outputType,
                blockEncodingManager,
                memoryManager,
                getShuffleOutputTargetAverageRowSize(session),
                preDeterminedPartition);
        PrestoSparkOutputBuffer<?> outputBuffer = output.getOutputBuffer();

        LocalExecutionPlan localExecutionPlan = localExecutionPlanner.plan(
                taskContext,
                fragment.getRoot(),
                fragment.getPartitioningScheme(),
                fragment.getStageExecutionDescriptor(),
                fragment.getTableScanSchedulingOrder(),
                output.getOutputFactory(),
                new PrestoSparkRemoteSourceFactory(
                        blockEncodingManager,
                        shuffleInputs.build(),
                        pageInputs.build(),
                        partitionId,
                        shuffleStatsCollector),
                taskDescriptor.getTableWriteInfo(),
                true);

        taskStateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                outputBuffer.setNoMoreRows();
            }
        });

        PrestoSparkTaskExecution taskExecution = new PrestoSparkTaskExecution(
                taskStateMachine,
                taskContext,
                localExecutionPlan,
                taskExecutor,
                splitMonitor,
                notificationExecutor,
                memoryUpdateExecutor);

        taskExecution.start(taskSources);

        return new PrestoSparkTaskExecutor<>(
                taskContext,
                taskStateMachine,
                output.getOutputSupplier(),
                taskInfoJsonCodec,
                taskInfoCollector,
                shuffleStatsCollector,
                executionExceptionFactory,
                output.getOutputBufferType(),
                outputBuffer);
    }

    private static OptionalLong computeAllSplitsSize(List<TaskSource> taskSources)
    {
        long sum = 0;
        for (TaskSource taskSource : taskSources) {
            for (ScheduledSplit scheduledSplit : taskSource.getSplits()) {
                ConnectorSplit connectorSplit = scheduledSplit.getSplit().getConnectorSplit();
                if (!connectorSplit.getSplitSizeInBytes().isPresent()) {
                    return OptionalLong.empty();
                }

                sum += connectorSplit.getSplitSizeInBytes().getAsLong();
            }
        }

        return OptionalLong.of(sum);
    }

    private List<TaskSource> getTaskSources(Iterator<SerializedPrestoSparkTaskSource> serializedTaskSources)
    {
        ImmutableList.Builder<TaskSource> result = ImmutableList.builder();
        while (serializedTaskSources.hasNext()) {
            SerializedPrestoSparkTaskSource serializedTaskSource = serializedTaskSources.next();
            result.add(taskSourceJsonCodec.fromJson(decompress(serializedTaskSource.getBytes())));
        }
        return result.build();
    }

    @SuppressWarnings("unchecked")
    private static <T extends PrestoSparkTaskOutput> Output<T> configureOutput(
            Class<T> outputType,
            BlockEncodingManager blockEncodingManager,
            OutputBufferMemoryManager memoryManager,
            DataSize targetAverageRowSize,
            Optional<OutputPartitioning> preDeterminedPartition)
    {
        if (outputType.equals(PrestoSparkMutableRow.class)) {
            PrestoSparkOutputBuffer<PrestoSparkRowBatch> outputBuffer = new PrestoSparkOutputBuffer<>(memoryManager);
            OutputFactory outputFactory = new PrestoSparkRowOutputFactory(outputBuffer, targetAverageRowSize, preDeterminedPartition);
            OutputSupplier<T> outputSupplier = (OutputSupplier<T>) new RowOutputSupplier(outputBuffer);
            return new Output<>(OutputBufferType.SPARK_ROW_OUTPUT_BUFFER, outputBuffer, outputFactory, outputSupplier);
        }
        else if (outputType.equals(PrestoSparkSerializedPage.class)) {
            PrestoSparkOutputBuffer<PrestoSparkBufferedSerializedPage> outputBuffer = new PrestoSparkOutputBuffer<>(memoryManager);
            OutputFactory outputFactory = new PrestoSparkPageOutputFactory(outputBuffer, blockEncodingManager);
            OutputSupplier<T> outputSupplier = (OutputSupplier<T>) new PageOutputSupplier(outputBuffer);
            return new Output<>(OutputBufferType.SPARK_PAGE_OUTPUT_BUFFER, outputBuffer, outputFactory, outputSupplier);
        }
        else {
            throw new IllegalArgumentException("Unexpected output type: " + outputType.getName());
        }
    }

    private static class PrestoSparkTaskExecutor<T extends PrestoSparkTaskOutput>
            extends AbstractIterator<Tuple2<MutablePartitionId, T>>
            implements IPrestoSparkTaskExecutor<T>
    {
        private final TaskContext taskContext;
        private final TaskStateMachine taskStateMachine;
        private final OutputSupplier<T> outputSupplier;
        private final JsonCodec<TaskInfo> taskInfoJsonCodec;
        private final CollectionAccumulator<SerializedTaskInfo> taskInfoCollector;
        private final CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector;
        private final PrestoSparkExecutionExceptionFactory executionExceptionFactory;
        private final OutputBufferType outputBufferType;
        private final PrestoSparkOutputBuffer<?> outputBuffer;

        private final UUID taskInstanceId = randomUUID();

        private Tuple2<MutablePartitionId, T> next;

        private Long start;
        private long processedRows;
        private long processedRowBatches;
        private long processedBytes;

        private PrestoSparkTaskExecutor(
                TaskContext taskContext,
                TaskStateMachine taskStateMachine,
                OutputSupplier<T> outputSupplier,
                JsonCodec<TaskInfo> taskInfoJsonCodec,
                CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
                CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector,
                PrestoSparkExecutionExceptionFactory executionExceptionFactory,
                OutputBufferType outputBufferType,
                PrestoSparkOutputBuffer<?> outputBuffer)
        {
            this.taskContext = requireNonNull(taskContext, "taskContext is null");
            this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
            this.outputSupplier = requireNonNull(outputSupplier, "outputSupplier is null");
            this.taskInfoJsonCodec = requireNonNull(taskInfoJsonCodec, "taskInfoJsonCodec is null");
            this.taskInfoCollector = requireNonNull(taskInfoCollector, "taskInfoCollector is null");
            this.shuffleStatsCollector = requireNonNull(shuffleStatsCollector, "shuffleStatsCollector is null");
            this.executionExceptionFactory = requireNonNull(executionExceptionFactory, "executionExceptionFactory is null");
            this.outputBufferType = requireNonNull(outputBufferType, "outputBufferType is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        }

        @Override
        public boolean hasNext()
        {
            if (next == null) {
                next = computeNext();
            }
            return next != null;
        }

        @Override
        public Tuple2<MutablePartitionId, T> next()
        {
            if (next == null) {
                next = computeNext();
            }
            if (next == null) {
                throw new NoSuchElementException();
            }
            Tuple2<MutablePartitionId, T> result = next;
            next = null;
            return result;
        }

        protected Tuple2<MutablePartitionId, T> computeNext()
        {
            try {
                return doComputeNext();
            }
            catch (RuntimeException e) {
                throw executionExceptionFactory.toPrestoSparkExecutionException(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                taskStateMachine.abort();
                throw new RuntimeException(e);
            }
        }

        private Tuple2<MutablePartitionId, T> doComputeNext()
                throws InterruptedException
        {
            if (start == null) {
                start = System.currentTimeMillis();
            }
            Tuple2<MutablePartitionId, T> output = outputSupplier.getNext();

            if (output != null) {
                processedRows += output._2.getPositionCount();
                processedRowBatches++;
                processedBytes += output._2.getSize();
                return output;
            }

            // task finished
            TaskState taskState = taskStateMachine.getState();
            checkState(taskState.isDone(), "task is expected to be done");

            long end = System.currentTimeMillis();
            PrestoSparkShuffleStats shuffleStats = new PrestoSparkShuffleStats(
                    taskContext.getTaskId().getStageExecutionId().getStageId().getId(),
                    taskContext.getTaskId().getId(),
                    WRITE,
                    processedRows,
                    processedRowBatches,
                    processedBytes,
                    end - start - outputSupplier.getTimeSpentWaitingForOutputInMillis());
            shuffleStatsCollector.add(shuffleStats);

            TaskInfo taskInfo = createTaskInfo(taskContext, taskStateMachine, taskInstanceId, outputBufferType, outputBuffer);
            SerializedTaskInfo serializedTaskInfo = new SerializedTaskInfo(
                    taskInfo.getTaskId().getStageExecutionId().getStageId().getId(),
                    taskInfo.getTaskId().getId(),
                    compress(taskInfoJsonCodec.toJsonBytes(taskInfo)));
            taskInfoCollector.add(serializedTaskInfo);

            LinkedBlockingQueue<Throwable> failures = taskStateMachine.getFailureCauses();
            if (failures.isEmpty()) {
                return null;
            }

            Throwable failure = getFirst(failures, null);
            propagateIfPossible(failure, Error.class);
            propagateIfPossible(failure, RuntimeException.class);
            propagateIfPossible(failure, InterruptedException.class);
            throw new RuntimeException(failure);
        }

        private static TaskInfo createTaskInfo(
                TaskContext taskContext,
                TaskStateMachine taskStateMachine,
                UUID taskInstanceId,
                OutputBufferType outputBufferType,
                PrestoSparkOutputBuffer<?> outputBuffer)
        {
            TaskId taskId = taskContext.getTaskId();
            TaskState taskState = taskContext.getState();
            TaskStats taskStats = taskContext.getTaskStats();

            List<ExecutionFailureInfo> failures = ImmutableList.of();
            if (taskState == FAILED) {
                failures = toFailures(taskStateMachine.getFailureCauses());
            }

            TaskStatus taskStatus = new TaskStatus(
                    taskInstanceId.getLeastSignificantBits(),
                    taskInstanceId.getMostSignificantBits(),
                    STARTING_VERSION,
                    taskState,
                    URI.create("http://fake.invalid/task/" + taskId),
                    taskContext.getCompletedDriverGroups(),
                    failures,
                    taskStats.getQueuedPartitionedDrivers(),
                    taskStats.getRunningPartitionedDrivers(),
                    0,
                    false,
                    taskStats.getPhysicalWrittenDataSizeInBytes(),
                    taskStats.getUserMemoryReservationInBytes(),
                    taskStats.getSystemMemoryReservationInBytes(),
                    taskStats.getPeakNodeTotalMemoryInBytes(),
                    taskStats.getFullGcCount(),
                    taskStats.getFullGcTimeInMillis());

            OutputBufferInfo outputBufferInfo = new OutputBufferInfo(
                    outputBufferType.name(),
                    FINISHED,
                    false,
                    false,
                    0,
                    0,
                    outputBuffer.getTotalRowsProcessed(),
                    outputBuffer.getTotalPagesProcessed(),
                    ImmutableList.of());

            return new TaskInfo(
                    taskId,
                    taskStatus,
                    DateTime.now(),
                    outputBufferInfo,
                    ImmutableSet.of(),
                    taskStats,
                    false,
                    DEFAULT_METADATA_UPDATES);
        }
    }

    private static class Output<T extends PrestoSparkTaskOutput>
    {
        private final OutputBufferType outputBufferType;
        private final PrestoSparkOutputBuffer<?> outputBuffer;
        private final OutputFactory outputFactory;
        private final OutputSupplier<T> outputSupplier;

        private Output(
                OutputBufferType outputBufferType,
                PrestoSparkOutputBuffer<?> outputBuffer,
                OutputFactory outputFactory,
                OutputSupplier<T> outputSupplier)
        {
            this.outputBufferType = requireNonNull(outputBufferType, "outputBufferType is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.outputFactory = requireNonNull(outputFactory, "outputFactory is null");
            this.outputSupplier = requireNonNull(outputSupplier, "outputSupplier is null");
        }

        public OutputBufferType getOutputBufferType()
        {
            return outputBufferType;
        }

        public PrestoSparkOutputBuffer<?> getOutputBuffer()
        {
            return outputBuffer;
        }

        public OutputFactory getOutputFactory()
        {
            return outputFactory;
        }

        public OutputSupplier<T> getOutputSupplier()
        {
            return outputSupplier;
        }
    }

    private interface OutputSupplier<T extends PrestoSparkTaskOutput>
    {
        Tuple2<MutablePartitionId, T> getNext()
                throws InterruptedException;

        long getTimeSpentWaitingForOutputInMillis();
    }

    private static class RowOutputSupplier
            implements OutputSupplier<PrestoSparkMutableRow>
    {
        private final PrestoSparkOutputBuffer<PrestoSparkRowBatch> outputBuffer;
        private RowTupleSupplier currentRowTupleSupplier;

        private long timeSpentWaitingForOutputInMillis;

        private RowOutputSupplier(PrestoSparkOutputBuffer<PrestoSparkRowBatch> outputBuffer)
        {
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        }

        @Override
        public Tuple2<MutablePartitionId, PrestoSparkMutableRow> getNext()
                throws InterruptedException
        {
            Tuple2<MutablePartitionId, PrestoSparkMutableRow> next = null;
            while (next == null) {
                if (currentRowTupleSupplier == null) {
                    long start = System.currentTimeMillis();
                    PrestoSparkRowBatch rowBatch = outputBuffer.get();
                    long end = System.currentTimeMillis();
                    timeSpentWaitingForOutputInMillis += (end - start);
                    if (rowBatch == null) {
                        return null;
                    }
                    currentRowTupleSupplier = rowBatch.createRowTupleSupplier();
                }
                next = currentRowTupleSupplier.getNext();
                if (next == null) {
                    currentRowTupleSupplier = null;
                }
            }
            return next;
        }

        @Override
        public long getTimeSpentWaitingForOutputInMillis()
        {
            return timeSpentWaitingForOutputInMillis;
        }
    }

    private static class PageOutputSupplier
            implements OutputSupplier<PrestoSparkSerializedPage>
    {
        private static final MutablePartitionId DEFAULT_PARTITION = new MutablePartitionId();

        private final PrestoSparkOutputBuffer<PrestoSparkBufferedSerializedPage> outputBuffer;

        private long timeSpentWaitingForOutputInMillis;

        private PageOutputSupplier(PrestoSparkOutputBuffer<PrestoSparkBufferedSerializedPage> outputBuffer)
        {
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        }

        @Override
        public Tuple2<MutablePartitionId, PrestoSparkSerializedPage> getNext()
                throws InterruptedException
        {
            long start = System.currentTimeMillis();
            PrestoSparkBufferedSerializedPage page = outputBuffer.get();
            long end = System.currentTimeMillis();
            timeSpentWaitingForOutputInMillis += (end - start);
            if (page == null) {
                return null;
            }
            return new Tuple2<>(DEFAULT_PARTITION, toPrestoSparkSerializedPage(page.getSerializedPage()));
        }

        @Override
        public long getTimeSpentWaitingForOutputInMillis()
        {
            return timeSpentWaitingForOutputInMillis;
        }
    }

    private enum OutputBufferType
    {
        SPARK_ROW_OUTPUT_BUFFER,
        SPARK_PAGE_OUTPUT_BUFFER,
    }
}
