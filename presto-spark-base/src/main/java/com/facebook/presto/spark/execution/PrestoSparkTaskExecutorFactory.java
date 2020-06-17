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
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.event.SplitMonitor;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.execution.buffer.OutputBufferMemoryManager;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.metadata.SessionPropertyManager;
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
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskInputs;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskSource;
import com.facebook.presto.spark.classloader_interface.SerializedTaskStats;
import com.facebook.presto.spark.execution.PrestoSparkPageOutputOperator.PrestoSparkPageOutputFactory;
import com.facebook.presto.spark.execution.PrestoSparkRowBatch.RowTupleSupplier;
import com.facebook.presto.spark.execution.PrestoSparkRowOutputOperator.PrestoSparkRowOutputFactory;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.security.TokenAuthenticator;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.spark.util.PrestoSparkUtils.toPrestoSparkSerializedPage;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagateIfPossible;
import static com.google.common.collect.Iterables.getFirst;
import static java.util.Objects.requireNonNull;

public class PrestoSparkTaskExecutorFactory
        implements IPrestoSparkTaskExecutorFactory
{
    private static final Logger log = Logger.get(PrestoSparkTaskExecutorFactory.class);

    private final SessionPropertyManager sessionPropertyManager;
    private final BlockEncodingManager blockEncodingManager;

    private final JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec;
    private final JsonCodec<TaskSource> taskSourceJsonCodec;
    private final JsonCodec<TaskStats> taskStatsJsonCodec;

    private final Executor notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;

    private final LocalExecutionPlanner localExecutionPlanner;
    private final PrestoSparkExecutionExceptionFactory executionExceptionFactory;
    private final TaskExecutor taskExecutor;
    private final SplitMonitor splitMonitor;

    private final Set<PrestoSparkAuthenticatorProvider> authenticatorProviders;

    private final DataSize maxUserMemory;
    private final DataSize maxTotalMemory;
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
            JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec,
            JsonCodec<TaskSource> taskSourceJsonCodec,
            JsonCodec<TaskStats> taskStatsJsonCodec,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            LocalExecutionPlanner localExecutionPlanner,
            PrestoSparkExecutionExceptionFactory executionExceptionFactory,
            TaskExecutor taskExecutor,
            SplitMonitor splitMonitor,
            Set<PrestoSparkAuthenticatorProvider> authenticatorProviders,
            TaskManagerConfig taskManagerConfig,
            NodeMemoryConfig nodeMemoryConfig,
            NodeSpillConfig nodeSpillConfig)
    {
        this(
                sessionPropertyManager,
                blockEncodingManager,
                taskDescriptorJsonCodec,
                taskSourceJsonCodec,
                taskStatsJsonCodec,
                notificationExecutor,
                yieldExecutor,
                localExecutionPlanner,
                executionExceptionFactory,
                taskExecutor,
                splitMonitor,
                authenticatorProviders,
                requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null").getMaxQueryMemoryPerNode(),
                requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null").getMaxQueryTotalMemoryPerNode(),
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
            JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec,
            JsonCodec<TaskSource> taskSourceJsonCodec,
            JsonCodec<TaskStats> taskStatsJsonCodec,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            LocalExecutionPlanner localExecutionPlanner,
            PrestoSparkExecutionExceptionFactory executionExceptionFactory,
            TaskExecutor taskExecutor,
            SplitMonitor splitMonitor,
            Set<PrestoSparkAuthenticatorProvider> authenticatorProviders,
            DataSize maxUserMemory,
            DataSize maxTotalMemory,
            DataSize maxSpillMemory,
            DataSize sinkMaxBufferSize,
            boolean perOperatorCpuTimerEnabled,
            boolean cpuTimerEnabled,
            boolean perOperatorAllocationTrackingEnabled,
            boolean allocationTrackingEnabled)
    {
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.blockEncodingManager = requireNonNull(blockEncodingManager, "blockEncodingManager is null");
        this.taskDescriptorJsonCodec = requireNonNull(taskDescriptorJsonCodec, "sparkTaskDescriptorJsonCodec is null");
        this.taskSourceJsonCodec = requireNonNull(taskSourceJsonCodec, "taskSourceJsonCodec is null");
        this.taskStatsJsonCodec = requireNonNull(taskStatsJsonCodec, "taskStatsJsonCodec is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.yieldExecutor = requireNonNull(yieldExecutor, "yieldExecutor is null");
        this.localExecutionPlanner = requireNonNull(localExecutionPlanner, "localExecutionPlanner is null");
        this.executionExceptionFactory = requireNonNull(executionExceptionFactory, "executionExceptionFactory is null");
        this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
        this.splitMonitor = requireNonNull(splitMonitor, "splitMonitor is null");
        this.authenticatorProviders = ImmutableSet.copyOf(requireNonNull(authenticatorProviders, "authenticatorProviders is null"));
        this.maxUserMemory = requireNonNull(maxUserMemory, "maxUserMemory is null");
        this.maxTotalMemory = requireNonNull(maxTotalMemory, "maxTotalMemory is null");
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
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector,
            Class<T> outputType)
    {
        try {
            return doCreate(partitionId, attemptNumber, serializedTaskDescriptor, serializedTaskSources, inputs, taskStatsCollector, outputType);
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
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector,
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

        MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("spark-executor-memory-pool"), maxTotalMemory);
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(maxSpillMemory);

        QueryContext queryContext = new QueryContext(
                session.getQueryId(),
                maxUserMemory,
                maxTotalMemory,
                maxUserMemory,
                memoryPool,
                new TestingGcMonitor(),
                notificationExecutor,
                yieldExecutor,
                maxSpillMemory,
                spillSpaceTracker);

        TaskContext taskContext = queryContext.addTaskContext(
                new TaskStateMachine(taskId, notificationExecutor),
                session,
                perOperatorCpuTimerEnabled,
                cpuTimerEnabled,
                perOperatorAllocationTrackingEnabled,
                allocationTrackingEnabled,
                false);

        ImmutableMap.Builder<PlanNodeId, List<scala.collection.Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>>> rowInputs = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, List<java.util.Iterator<PrestoSparkSerializedPage>>> pageInputs = ImmutableMap.builder();
        for (RemoteSourceNode remoteSource : fragment.getRemoteSourceNodes()) {
            List<scala.collection.Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>> remoteSourceRowInputs = new ArrayList<>();
            List<java.util.Iterator<PrestoSparkSerializedPage>> remoteSourcePageInputs = new ArrayList<>();
            for (PlanFragmentId sourceFragmentId : remoteSource.getSourceFragmentIds()) {
                Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> shuffleInput = inputs.getShuffleInputs().get(sourceFragmentId.toString());
                Broadcast<List<PrestoSparkSerializedPage>> broadcastInput = inputs.getBroadcastInputs().get(sourceFragmentId.toString());
                List<PrestoSparkSerializedPage> inMemoryInput = inputs.getInMemoryInputs().get(sourceFragmentId.toString());

                if (shuffleInput != null) {
                    checkArgument(broadcastInput == null, "single remote source is not expected to accept different kind of inputs");
                    checkArgument(inMemoryInput == null, "single remote source is not expected to accept different kind of inputs");
                    remoteSourceRowInputs.add(shuffleInput);
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
                rowInputs.put(remoteSource.getId(), remoteSourceRowInputs);
            }
            if (!remoteSourcePageInputs.isEmpty()) {
                pageInputs.put(remoteSource.getId(), remoteSourcePageInputs);
            }
        }

        OutputBufferMemoryManager memoryManager = new OutputBufferMemoryManager(
                sinkMaxBufferSize.toBytes(),
                () -> queryContext.getTaskContextByTaskId(taskId).localSystemMemoryContext(),
                notificationExecutor);
        PagesSerde pagesSerde = new PagesSerde(blockEncodingManager, Optional.empty(), Optional.empty(), Optional.empty());
        Output<T> output = configureOutput(outputType, pagesSerde, memoryManager);
        PrestoSparkOutputBuffer<?> outputBuffer = output.getOutputBuffer();

        LocalExecutionPlan localExecutionPlan = localExecutionPlanner.plan(
                taskContext,
                fragment.getRoot(),
                fragment.getPartitioningScheme(),
                fragment.getStageExecutionDescriptor(),
                fragment.getTableScanSchedulingOrder(),
                output.getOutputFactory(),
                new PrestoSparkRemoteSourceFactory(
                        pagesSerde,
                        rowInputs.build(),
                        pageInputs.build()),
                taskDescriptor.getTableWriteInfo(),
                true);

        TaskStateMachine taskStateMachine = new TaskStateMachine(taskId, notificationExecutor);
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
                notificationExecutor);

        taskExecution.start(taskSources);

        return new PrestoSparkTaskExecutor<>(
                taskContext,
                taskStateMachine,
                output.getOutputSupplier(),
                taskStatsJsonCodec,
                taskStatsCollector,
                executionExceptionFactory);
    }

    private List<TaskSource> getTaskSources(Iterator<SerializedPrestoSparkTaskSource> serializedTaskSources)
    {
        ImmutableList.Builder<TaskSource> result = ImmutableList.builder();
        while (serializedTaskSources.hasNext()) {
            SerializedPrestoSparkTaskSource serializedTaskSource = serializedTaskSources.next();
            result.add(taskSourceJsonCodec.fromJson(serializedTaskSource.getBytes()));
        }
        return result.build();
    }

    @SuppressWarnings("unchecked")
    private static <T extends PrestoSparkTaskOutput> Output<T> configureOutput(
            Class<T> outputType,
            PagesSerde pagesSerde,
            OutputBufferMemoryManager memoryManager)
    {
        if (outputType.equals(PrestoSparkMutableRow.class)) {
            PrestoSparkOutputBuffer<PrestoSparkRowBatch> outputBuffer = new PrestoSparkOutputBuffer<>(memoryManager);
            OutputFactory outputFactory = new PrestoSparkRowOutputFactory(outputBuffer);
            OutputSupplier<T> outputSupplier = (OutputSupplier<T>) new RowOutputSupplier(outputBuffer);
            return new Output<>(outputBuffer, outputFactory, outputSupplier);
        }
        else if (outputType.equals(PrestoSparkSerializedPage.class)) {
            PrestoSparkOutputBuffer<PrestoSparkBufferedSerializedPage> outputBuffer = new PrestoSparkOutputBuffer<>(memoryManager);
            OutputFactory outputFactory = new PrestoSparkPageOutputFactory(outputBuffer, pagesSerde);
            OutputSupplier<T> outputSupplier = (OutputSupplier<T>) new PageOutputSupplier(outputBuffer);
            return new Output<>(outputBuffer, outputFactory, outputSupplier);
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
        private final JsonCodec<TaskStats> taskStatsJsonCodec;
        private final CollectionAccumulator<SerializedTaskStats> taskStatsCollector;
        private final PrestoSparkExecutionExceptionFactory executionExceptionFactory;

        private Tuple2<MutablePartitionId, T> next;

        private PrestoSparkTaskExecutor(
                TaskContext taskContext,
                TaskStateMachine taskStateMachine,
                OutputSupplier<T> outputSupplier,
                JsonCodec<TaskStats> taskStatsJsonCodec,
                CollectionAccumulator<SerializedTaskStats> taskStatsCollector,
                PrestoSparkExecutionExceptionFactory executionExceptionFactory)
        {
            this.taskContext = requireNonNull(taskContext, "taskContext is null");
            this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
            this.outputSupplier = requireNonNull(outputSupplier, "outputSupplier is null");
            this.taskStatsJsonCodec = requireNonNull(taskStatsJsonCodec, "taskStatsJsonCodec is null");
            this.taskStatsCollector = requireNonNull(taskStatsCollector, "taskStatsCollector is null");
            this.executionExceptionFactory = requireNonNull(executionExceptionFactory, "executionExceptionFactory is null");
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
            Tuple2<MutablePartitionId, T> output = outputSupplier.getNext();
            if (output != null) {
                return output;
            }

            //  TODO: Implement task stats collection
            //  TaskStats taskStats = taskContext.getTaskStats();
            //  byte[] taskStatsSerialized = taskStatsJsonCodec.toJsonBytes(taskStats);
            //  taskStatsCollector.add(new SerializedTaskStats(taskStatsSerialized));

            // task finished
            TaskState taskState = taskStateMachine.getState();
            checkState(taskState.isDone(), "task is expected to be done");
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
    }

    private static class Output<T extends PrestoSparkTaskOutput>
    {
        private final PrestoSparkOutputBuffer<?> outputBuffer;
        private final OutputFactory outputFactory;
        private final OutputSupplier<T> outputSupplier;

        private Output(
                PrestoSparkOutputBuffer<?> outputBuffer,
                OutputFactory outputFactory,
                OutputSupplier<T> outputSupplier)
        {
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.outputFactory = requireNonNull(outputFactory, "outputFactory is null");
            this.outputSupplier = requireNonNull(outputSupplier, "outputSupplier is null");
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
    }

    private static class RowOutputSupplier
            implements OutputSupplier<PrestoSparkMutableRow>
    {
        private final PrestoSparkOutputBuffer<PrestoSparkRowBatch> outputBuffer;
        private RowTupleSupplier currentRowTupleSupplier;

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
                    PrestoSparkRowBatch rowBatch = outputBuffer.get();
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
    }

    private static class PageOutputSupplier
            implements OutputSupplier<PrestoSparkSerializedPage>
    {
        private static final MutablePartitionId DEFAULT_PARTITION = new MutablePartitionId();

        private final PrestoSparkOutputBuffer<PrestoSparkBufferedSerializedPage> outputBuffer;

        private PageOutputSupplier(PrestoSparkOutputBuffer<PrestoSparkBufferedSerializedPage> outputBuffer)
        {
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        }

        @Override
        public Tuple2<MutablePartitionId, PrestoSparkSerializedPage> getNext()
                throws InterruptedException
        {
            PrestoSparkBufferedSerializedPage page = outputBuffer.get();
            if (page == null) {
                return null;
            }
            return new Tuple2<>(DEFAULT_PARTITION, toPrestoSparkSerializedPage(page.getSerializedPage()));
        }
    }
}
