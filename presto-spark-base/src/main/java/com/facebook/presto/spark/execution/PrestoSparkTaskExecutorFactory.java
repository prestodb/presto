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
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.execution.buffer.PagesSerdeUtil;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutor;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskInputs;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkPage;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedTaskStats;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class PrestoSparkTaskExecutorFactory
        implements IPrestoSparkTaskExecutorFactory
{
    private static final Logger log = Logger.get(PrestoSparkTaskExecutorFactory.class);

    private final SessionPropertyManager sessionPropertyManager;

    private final JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec;
    private final JsonCodec<TaskStats> taskStatsJsonCodec;

    private final Executor notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;

    private final LocalExecutionPlanner localExecutionPlanner;
    private final BlockEncodingSerde blockEncodingSerde;

    private final DataSize maxUserMemory;
    private final DataSize maxTotalMemory;
    private final DataSize maxSpillMemory;

    private final boolean perOperatorCpuTimerEnabled;
    private final boolean cpuTimerEnabled;

    @Inject
    public PrestoSparkTaskExecutorFactory(
            SessionPropertyManager sessionPropertyManager,
            JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec,
            JsonCodec<TaskStats> taskStatsJsonCodec,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            LocalExecutionPlanner localExecutionPlanner,
            BlockEncodingSerde blockEncodingSerde,
            TaskManagerConfig taskManagerConfig,
            NodeMemoryConfig nodeMemoryConfig,
            NodeSpillConfig nodeSpillConfig)
    {
        this(
                sessionPropertyManager,
                taskDescriptorJsonCodec,
                taskStatsJsonCodec,
                notificationExecutor,
                yieldExecutor,
                localExecutionPlanner,
                blockEncodingSerde,
                requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null").getMaxQueryMemoryPerNode(),
                requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null").getMaxQueryTotalMemoryPerNode(),
                requireNonNull(nodeSpillConfig, "nodeSpillConfig is null").getMaxSpillPerNode(),
                requireNonNull(taskManagerConfig, "taskManagerConfig is null").isPerOperatorCpuTimerEnabled(),
                requireNonNull(taskManagerConfig, "taskManagerConfig is null").isTaskCpuTimerEnabled());
    }

    public PrestoSparkTaskExecutorFactory(
            SessionPropertyManager sessionPropertyManager,
            JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec,
            JsonCodec<TaskStats> taskStatsJsonCodec,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            LocalExecutionPlanner localExecutionPlanner,
            BlockEncodingSerde blockEncodingSerde,
            DataSize maxUserMemory,
            DataSize maxTotalMemory,
            DataSize maxSpillMemory,
            boolean perOperatorCpuTimerEnabled,
            boolean cpuTimerEnabled)
    {
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.taskDescriptorJsonCodec = requireNonNull(taskDescriptorJsonCodec, "sparkTaskDescriptorJsonCodec is null");
        this.taskStatsJsonCodec = requireNonNull(taskStatsJsonCodec, "taskStatsJsonCodec is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.yieldExecutor = requireNonNull(yieldExecutor, "yieldExecutor is null");
        this.localExecutionPlanner = requireNonNull(localExecutionPlanner, "localExecutionPlanner is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.maxUserMemory = requireNonNull(maxUserMemory, "maxUserMemory is null");
        this.maxTotalMemory = requireNonNull(maxTotalMemory, "maxTotalMemory is null");
        this.maxSpillMemory = requireNonNull(maxSpillMemory, "maxSpillMemory is null");
        this.perOperatorCpuTimerEnabled = perOperatorCpuTimerEnabled;
        this.cpuTimerEnabled = cpuTimerEnabled;
    }

    @Override
    public IPrestoSparkTaskExecutor create(
            int partitionId,
            int attemptNumber,
            SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor,
            PrestoSparkTaskInputs inputs,
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector)
    {
        PrestoSparkTaskDescriptor taskDescriptor = taskDescriptorJsonCodec.fromJson(serializedTaskDescriptor.getBytes());

        Session session = taskDescriptor.getSession().toSession(sessionPropertyManager, taskDescriptor.getExtraCredentials());
        PlanFragment fragment = taskDescriptor.getFragment();
        StageId stageId = new StageId(session.getQueryId(), fragment.getId().getId());
        // TODO: include attemptId in taskId
        TaskId taskId = new TaskId(new StageExecutionId(stageId, 0), partitionId);

        log.info("Task [%s] received %d splits.",
                taskId,
                taskDescriptor.getTaskSourcesBySchedulingOrder().stream()
                        .mapToInt(taskSource -> taskSource.getSplits().size())
                        .sum());

        MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("spark-executor-memory-pool"), maxTotalMemory);
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(maxSpillMemory);

        QueryContext queryContext = new QueryContext(
                session.getQueryId(),
                maxUserMemory,
                maxTotalMemory,
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
                false);

        PrestoSparkOutputBuffer outputBuffer = new PrestoSparkOutputBuffer();
        ImmutableMap<PlanNodeId, Iterator<SerializedPage>> sparkInputs = inputs.getInputsMap().entrySet().stream()
                .collect(toImmutableMap(
                        entry -> new PlanNodeId(entry.getKey()),
                        entry -> Iterators.transform(entry.getValue(), tuple -> readSerializedPages(tuple._2))));

        LocalExecutionPlan localExecutionPlan = localExecutionPlanner.plan(
                taskContext,
                fragment.getRoot(),
                fragment.getPartitioningScheme(),
                fragment.getStageExecutionDescriptor(),
                fragment.getTableScanSchedulingOrder(),
                outputBuffer,
                new PrestoSparkRemoteSourceFactory(sparkInputs, blockEncodingSerde),
                taskDescriptor.getTableWriteInfo());

        Map<PlanNodeId, DriverFactory> partitionedDriverFactories = new HashMap<>();
        List<DriverFactory> unpartitionedDriverFactories = new ArrayList<>();
        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            Optional<PlanNodeId> sourceId = driverFactory.getSourceId();
            if (sourceId.isPresent() && fragment.getTableScanSchedulingOrder().contains(sourceId.get())) {
                checkState(!partitionedDriverFactories.containsKey(driverFactory.getSourceId().get()));
                partitionedDriverFactories.put(driverFactory.getSourceId().get(), driverFactory);
            }
            else {
                unpartitionedDriverFactories.add(driverFactory);
            }
        }

        List<Driver> unpartitionedDrivers = createUnpartitionedDrivers(unpartitionedDriverFactories, taskContext);
        Iterator<Driver> partitionedDriverIterator = createPartitionedDriverIterator(partitionedDriverFactories, taskContext, taskDescriptor.getTaskSourcesBySchedulingOrder());
        return new PrestoSparkTaskExecutor(
                taskContext,
                unpartitionedDrivers,
                partitionedDriverIterator,
                outputBuffer,
                taskStatsJsonCodec,
                taskStatsCollector);
    }

    public List<Driver> createUnpartitionedDrivers(List<DriverFactory> unpartitionedDriverFactories, TaskContext taskContext)
    {
        List<Driver> drivers = new ArrayList<>();
        for (DriverFactory driverFactory : unpartitionedDriverFactories) {
            PipelineContext pipelineContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), false);

            for (int i = 0; i < driverFactory.getDriverInstances().orElse(1); i++) {
                DriverContext driverContext = pipelineContext.addDriverContext();
                Driver driver = driverFactory.createDriver(driverContext);
                drivers.add(driver);
            }

            driverFactory.noMoreDrivers();
        }

        return ImmutableList.copyOf(drivers);
    }

    public Iterator<Driver> createPartitionedDriverIterator(Map<PlanNodeId, DriverFactory> partitionedDriverFactories, TaskContext taskContext, List<TaskSource> taskSourcesBySchedulingOrder)
    {
        List<PartitionedDriverGenerator> partitionedDriverGenerators = new ArrayList<>();
        for (TaskSource source : taskSourcesBySchedulingOrder) {
            DriverFactory driverFactory = partitionedDriverFactories.get(source.getPlanNodeId());
            checkState(driverFactory != null);

            PipelineContext pipelineContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), true);
            partitionedDriverGenerators.add(new PartitionedDriverGenerator(driverFactory, pipelineContext, source.getSplits()));
        }

        return Iterators.concat(partitionedDriverGenerators.iterator());
    }

    private static class PrestoSparkTaskExecutor
            extends AbstractIterator<Tuple2<Integer, SerializedPrestoSparkPage>>
            implements IPrestoSparkTaskExecutor
    {
        private static final int MAX_PARTITIONED_DRIVER_COUNT = 100;

        private final TaskContext taskContext;

        private final List<Driver> unpartitionedDrivers;

        private final Set<Driver> currentPartitionedDrivers;
        private final Iterator<Driver> remainingPartitionedDrivers;

        private final PrestoSparkOutputBuffer outputBuffer;
        private final JsonCodec<TaskStats> taskStatsJsonCodec;
        private final CollectionAccumulator<SerializedTaskStats> taskStatsCollector;

        private PrestoSparkTaskExecutor(
                TaskContext taskContext,
                List<Driver> unpartitionedDrivers,
                Iterator<Driver> partitionedDriverIterator,
                PrestoSparkOutputBuffer outputBuffer,
                JsonCodec<TaskStats> taskStatsJsonCodec,
                CollectionAccumulator<SerializedTaskStats> taskStatsCollector)
        {
            this.taskContext = requireNonNull(taskContext, "taskContext is null");
            this.unpartitionedDrivers = ImmutableList.copyOf(requireNonNull(unpartitionedDrivers, "unpartitionedDrivers is null"));
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.taskStatsJsonCodec = requireNonNull(taskStatsJsonCodec, "taskStatsJsonCodec is null");
            this.taskStatsCollector = requireNonNull(taskStatsCollector, "taskStatsCollector is null");

            this.currentPartitionedDrivers = new HashSet<>();
            this.remainingPartitionedDrivers = requireNonNull(partitionedDriverIterator, "partitionedDriverIterator is null");
            createPartitionedDriversIfNecessary();
        }

        @Override
        protected Tuple2<Integer, SerializedPrestoSparkPage> computeNext()
        {
            boolean done = false;
            while (!done && !outputBuffer.hasPagesBuffered()) {
                boolean processed = false;
                for (Driver driver : Iterables.concat(unpartitionedDrivers, currentPartitionedDrivers)) {
                    if (!driver.isFinished()) {
                        // TODO: avoid busy looping, wait on blocked future
                        driver.process();
                        processed = true;
                    }
                }
                done = !processed;
            }

            // clean up finished partitioned drivers
            Set<Driver> finishedPartitionedDrivers = new HashSet<>();
            for (Driver driver : currentPartitionedDrivers) {
                if (driver.isFinished()) {
                    finishedPartitionedDrivers.add(driver);
                }
            }
            currentPartitionedDrivers.removeAll(finishedPartitionedDrivers);
            createPartitionedDriversIfNecessary();

            if (done && !outputBuffer.hasPagesBuffered()) {
                TaskStats taskStats = taskContext.getTaskStats();
                byte[] taskStatsSerialized = taskStatsJsonCodec.toJsonBytes(taskStats);
                taskStatsCollector.add(new SerializedTaskStats(taskStatsSerialized));
                return endOfData();
            }

            PageWithPartitionId pageWithPartitionId = outputBuffer.getNext();
            SerializedPage serializedPage = pageWithPartitionId.getPage();

            log.debug("[%s] Producing page for partition: %s\n", taskContext.getTaskId(), pageWithPartitionId.getPartitionId());

            return new Tuple2<>(pageWithPartitionId.getPartitionId(), writeSerializedPage(serializedPage));
        }

        private void createPartitionedDriversIfNecessary()
        {
            while (currentPartitionedDrivers.size() < MAX_PARTITIONED_DRIVER_COUNT && remainingPartitionedDrivers.hasNext()) {
                currentPartitionedDrivers.add(remainingPartitionedDrivers.next());
            }
        }
    }

    private static class PartitionedDriverGenerator
            extends AbstractIterator<Driver>
    {
        private final DriverFactory driverFactory;
        private final PipelineContext pipelineContext;

        private final Iterator<ScheduledSplit> splitIterator;

        private PartitionedDriverGenerator(DriverFactory driverFactory, PipelineContext pipelineContext, Set<ScheduledSplit> splits)
        {
            this.driverFactory = requireNonNull(driverFactory, "driverFactory is null");
            this.pipelineContext = requireNonNull(pipelineContext, "pipelineContext is null");
            this.splitIterator = ImmutableSet.copyOf(requireNonNull(splits, "splitIterator is null")).iterator();
        }

        @Override
        protected Driver computeNext()
        {
            if (!splitIterator.hasNext()) {
                return endOfData();
            }
            Driver driver = driverFactory.createDriver(pipelineContext.addDriverContext());
            ScheduledSplit split = splitIterator.next();
            driver.updateSource(new TaskSource(split.getPlanNodeId(), ImmutableSet.of(split), true));

            if (!splitIterator.hasNext()) {
                driverFactory.noMoreDrivers();
            }

            return driver;
        }
    }

    private static SerializedPrestoSparkPage writeSerializedPage(SerializedPage page)
    {
        SliceOutput sliceOutput = new DynamicSliceOutput(page.getUncompressedSizeInBytes());
        PagesSerdeUtil.writeSerializedPage(sliceOutput, page);
        try {
            sliceOutput.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new SerializedPrestoSparkPage(sliceOutput.getUnderlyingSlice().getBytes());
    }

    private static SerializedPage readSerializedPages(SerializedPrestoSparkPage page)
    {
        return PagesSerdeUtil.readSerializedPages(Slices.wrappedBuffer(page.getBytes()).getInput()).next();
    }
}
