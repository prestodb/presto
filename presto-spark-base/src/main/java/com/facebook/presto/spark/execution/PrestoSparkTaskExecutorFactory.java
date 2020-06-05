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
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.execution.buffer.OutputBufferMemoryManager;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spark.PrestoSparkAuthenticatorProvider;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutor;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.classloader_interface.PrestoSparkRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskInputs;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedTaskStats;
import com.facebook.presto.spark.execution.PrestoSparkOutputOperator.PrestoSparkOutputFactory;
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
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import io.airlift.units.DataSize;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

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
    public IPrestoSparkTaskExecutor create(
            int partitionId,
            int attemptNumber,
            SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor,
            PrestoSparkTaskInputs inputs,
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector)
    {
        try {
            return doCreate(partitionId, attemptNumber, serializedTaskDescriptor, inputs, taskStatsCollector);
        }
        catch (RuntimeException e) {
            throw executionExceptionFactory.toPrestoSparkExecutionException(e);
        }
    }

    public IPrestoSparkTaskExecutor doCreate(
            int partitionId,
            int attemptNumber,
            SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor,
            PrestoSparkTaskInputs inputs,
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector)
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

        log.info("Task [%s] received %d splits.",
                taskId,
                taskDescriptor.getSources().stream()
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

        OutputBufferMemoryManager memoryManager = new OutputBufferMemoryManager(
                sinkMaxBufferSize.toBytes(),
                () -> queryContext.getTaskContextByTaskId(taskId).localSystemMemoryContext(),
                notificationExecutor);
        PrestoSparkRowBuffer rowBuffer = new PrestoSparkRowBuffer(memoryManager);

        ImmutableMap.Builder<PlanNodeId, Iterator<PrestoSparkRow>> shuffleInputs = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, Iterator<PrestoSparkSerializedPage>> broadcastInputs = ImmutableMap.builder();
        for (RemoteSourceNode remoteSource : fragment.getRemoteSourceNodes()) {
            List<Iterator<PrestoSparkRow>> shuffleRemoteSourceInputs = new ArrayList<>();
            List<Iterator<PrestoSparkSerializedPage>> broadcastRemoteSourceInputs = new ArrayList<>();
            for (PlanFragmentId sourceFragmentId : remoteSource.getSourceFragmentIds()) {
                Iterator<Tuple2<Integer, PrestoSparkRow>> shuffleInput = inputs.getShuffleInputs().get(sourceFragmentId.toString());
                Broadcast<List<PrestoSparkSerializedPage>> broadcastInput = inputs.getBroadcastInputs().get(sourceFragmentId.toString());
                checkArgument(shuffleInput != null || broadcastInput != null, "Input not found for sourceFragmentId: %s", sourceFragmentId);
                checkArgument(shuffleInput == null || broadcastInput == null, "Single remote source cannot accept both, broadcast and shuffle inputs");
                if (shuffleInput != null) {
                    shuffleRemoteSourceInputs.add(Iterators.transform(shuffleInput, tuple -> tuple._2));
                }
                if (broadcastInput != null) {
                    // TODO: Enable NullifyingIterator once migrated to one task per JVM model
                    // NullifyingIterator removes element from the list upon return
                    // This allows GC to gradually reclaim memory
                    // broadcastRemoteSourceInputs.add(getNullifyingIterator(broadcastInput.value()));
                    broadcastRemoteSourceInputs.add(broadcastInput.value().iterator());
                }
            }
            if (!shuffleRemoteSourceInputs.isEmpty()) {
                shuffleInputs.put(remoteSource.getId(), Iterators.concat(shuffleRemoteSourceInputs.iterator()));
            }
            if (!broadcastRemoteSourceInputs.isEmpty()) {
                broadcastInputs.put(remoteSource.getId(), Iterators.concat(broadcastRemoteSourceInputs.iterator()));
            }
        }

        LocalExecutionPlan localExecutionPlan = localExecutionPlanner.plan(
                taskContext,
                fragment.getRoot(),
                fragment.getPartitioningScheme(),
                fragment.getStageExecutionDescriptor(),
                fragment.getTableScanSchedulingOrder(),
                new PrestoSparkOutputFactory(rowBuffer),
                new PrestoSparkRemoteSourceFactory(
                        new PagesSerde(blockEncodingManager, Optional.empty(), Optional.empty(), Optional.empty()),
                        shuffleInputs.build(),
                        broadcastInputs.build()),
                taskDescriptor.getTableWriteInfo(),
                true);

        TaskStateMachine taskStateMachine = new TaskStateMachine(taskId, notificationExecutor);
        taskStateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                rowBuffer.setNoMoreRows();
            }
        });

        PrestoSparkTaskExecution taskExecution = new PrestoSparkTaskExecution(
                taskStateMachine,
                taskContext,
                localExecutionPlan,
                taskExecutor,
                splitMonitor,
                notificationExecutor);

        taskExecution.start(taskDescriptor.getSources());

        return new PrestoSparkTaskExecutor(
                taskContext,
                taskStateMachine,
                rowBuffer,
                taskStatsJsonCodec,
                taskStatsCollector,
                executionExceptionFactory);
    }

    private static class PrestoSparkTaskExecutor
            extends AbstractIterator<Tuple2<Integer, PrestoSparkRow>>
            implements IPrestoSparkTaskExecutor
    {
        private final TaskContext taskContext;
        private final TaskStateMachine taskStateMachine;
        private final PrestoSparkRowBuffer rowBuffer;
        private final JsonCodec<TaskStats> taskStatsJsonCodec;
        private final CollectionAccumulator<SerializedTaskStats> taskStatsCollector;
        private final PrestoSparkExecutionExceptionFactory executionExceptionFactory;

        private List<PrestoSparkRow> currentRowBatch;
        private int indexInBatch;

        private PrestoSparkTaskExecutor(
                TaskContext taskContext,
                TaskStateMachine taskStateMachine,
                PrestoSparkRowBuffer rowBuffer,
                JsonCodec<TaskStats> taskStatsJsonCodec,
                CollectionAccumulator<SerializedTaskStats> taskStatsCollector,
                PrestoSparkExecutionExceptionFactory executionExceptionFactory)
        {
            this.taskContext = requireNonNull(taskContext, "taskContext is null");
            this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
            this.rowBuffer = requireNonNull(rowBuffer, "rowBuffer is null");
            this.taskStatsJsonCodec = requireNonNull(taskStatsJsonCodec, "taskStatsJsonCodec is null");
            this.taskStatsCollector = requireNonNull(taskStatsCollector, "taskStatsCollector is null");
            this.executionExceptionFactory = requireNonNull(executionExceptionFactory, "executionExceptionFactory is null");
        }

        @Override
        protected Tuple2<Integer, PrestoSparkRow> computeNext()
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

        private Tuple2<Integer, PrestoSparkRow> doComputeNext()
                throws InterruptedException
        {
            PrestoSparkRow row = getNextRow();
            if (row != null) {
                return new Tuple2<>(row.getPartition(), row);
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
                return endOfData();
            }

            Throwable failure = getFirst(failures, null);
            propagateIfPossible(failure, Error.class);
            propagateIfPossible(failure, RuntimeException.class);
            propagateIfPossible(failure, InterruptedException.class);
            throw new RuntimeException(failure);
        }

        private PrestoSparkRow getNextRow()
                throws InterruptedException
        {
            if (currentRowBatch == null || indexInBatch >= currentRowBatch.size()) {
                currentRowBatch = rowBuffer.get();
                indexInBatch = 0;
                if (currentRowBatch == null || currentRowBatch.isEmpty()) {
                    return null;
                }
            }
            PrestoSparkRow row = currentRowBatch.get(indexInBatch);
            indexInBatch++;
            return row;
        }
    }
}
