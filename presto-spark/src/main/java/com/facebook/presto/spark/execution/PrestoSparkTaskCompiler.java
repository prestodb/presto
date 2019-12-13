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
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spark.SparkTaskDescriptor;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.execution.buffer.PagesSerdeUtil.readSerializedPages;
import static com.facebook.presto.execution.buffer.PagesSerdeUtil.writeSerializedPage;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class PrestoSparkTaskCompiler
{
    private static final Logger log = Logger.get(PrestoSparkTaskCompiler.class);

    private final SessionPropertyManager sessionPropertyManager;

    private final JsonCodec<SparkTaskDescriptor> sparkTaskDescriptorJsonCodec;
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
    public PrestoSparkTaskCompiler(
            SessionPropertyManager sessionPropertyManager,
            JsonCodec<SparkTaskDescriptor> sparkTaskDescriptorJsonCodec,
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
                sparkTaskDescriptorJsonCodec,
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

    public PrestoSparkTaskCompiler(
            SessionPropertyManager sessionPropertyManager,
            JsonCodec<SparkTaskDescriptor> sparkTaskDescriptorJsonCodec,
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
        this.sparkTaskDescriptorJsonCodec = requireNonNull(sparkTaskDescriptorJsonCodec, "sparkTaskDescriptorJsonCodec is null");
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

    public Iterator<Tuple2<Integer, byte[]>> compile(
            int sparkTaskId,
            byte[] serializedTaskDescriptor,
            Map<String, Iterator<Tuple2<Integer, byte[]>>> inputs,
            CollectionAccumulator<byte[]> taskStatsCollector)
    {
        SparkTaskDescriptor taskDescriptor = sparkTaskDescriptorJsonCodec.fromJson(serializedTaskDescriptor);

        Session session = taskDescriptor.getSession().toSession(sessionPropertyManager, taskDescriptor.getExtraCredentials());
        PlanFragment fragment = taskDescriptor.getFragment();
        StageId stageId = new StageId(session.getQueryId(), fragment.getId().getId());
        TaskId taskId = new TaskId(new StageExecutionId(stageId, 0), sparkTaskId);

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
                OptionalInt.empty(),
                false);

        SparkOutputBuffer outputBuffer = new SparkOutputBuffer();
        ImmutableMap<PlanNodeId, Iterator<SerializedPage>> sparkInputs = inputs.entrySet().stream()
                .collect(toImmutableMap(
                        entry -> new PlanNodeId(entry.getKey()),
                        entry -> Iterators.transform(entry.getValue(), tuple -> deserializeSerializedPage(tuple._2))));

        LocalExecutionPlan localExecutionPlan = localExecutionPlanner.plan(
                taskContext,
                fragment.getRoot(),
                fragment.getPartitioningScheme(),
                fragment.getStageExecutionDescriptor(),
                fragment.getTableScanSchedulingOrder(),
                outputBuffer,
                new SparkRemoteSourceFactory(sparkInputs, blockEncodingSerde),
                taskDescriptor.getTableWriteInfo());

        List<Driver> drivers = createDrivers(
                localExecutionPlan,
                taskContext,
                fragment.getTableScanSchedulingOrder(),
                taskDescriptor.getSources());

        return new SparkDriverProcessor(taskContext, drivers, outputBuffer, taskStatsJsonCodec, taskStatsCollector);
    }

    public List<Driver> createDrivers(
            LocalExecutionPlan localExecutionPlan,
            TaskContext taskContext,
            List<PlanNodeId> tableScanSchedulingOrder,
            List<TaskSource> sources)
    {
        // create drivers
        List<Driver> drivers = new ArrayList<>();
        Map<PlanNodeId, DriverFactory> driverFactoriesBySource = new HashMap<>();
        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            for (int i = 0; i < driverFactory.getDriverInstances().orElse(1); i++) {
                if (driverFactory.getSourceId().isPresent()) {
                    boolean partitioned = tableScanSchedulingOrder.contains(driverFactory.getSourceId().get());
                    if (partitioned) {
                        checkState(driverFactoriesBySource.put(driverFactory.getSourceId().get(), driverFactory) == null);
                    }
                    else {
                        DriverContext driverContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), false).addDriverContext();
                        Driver driver = driverFactory.createDriver(driverContext);
                        drivers.add(driver);
                    }
                }
                else {
                    DriverContext driverContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), false).addDriverContext();
                    Driver driver = driverFactory.createDriver(driverContext);
                    drivers.add(driver);
                }
            }
        }

        // TODO: avoid pre-creating drivers for all task sources
        for (TaskSource source : sources) {
            DriverFactory driverFactory = driverFactoriesBySource.get(source.getPlanNodeId());
            checkState(driverFactory != null);
            boolean partitioned = tableScanSchedulingOrder.contains(driverFactory.getSourceId().get());
            for (ScheduledSplit split : source.getSplits()) {
                DriverContext driverContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), partitioned).addDriverContext();
                Driver driver = driverFactory.createDriver(driverContext);
                driver.updateSource(new TaskSource(split.getPlanNodeId(), ImmutableSet.of(split), true));
                drivers.add(driver);
            }
        }

        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            driverFactory.noMoreDrivers();
        }

        return ImmutableList.copyOf(drivers);
    }

    private static class SparkDriverProcessor
            extends AbstractIterator<Tuple2<Integer, byte[]>>
    {
        private final TaskContext taskContext;
        private final List<Driver> drivers;
        private final SparkOutputBuffer outputBuffer;
        private final JsonCodec<TaskStats> taskStatsJsonCodec;
        private final CollectionAccumulator<byte[]> taskStatsCollector;

        private SparkDriverProcessor(
                TaskContext taskContext,
                List<Driver> drivers,
                SparkOutputBuffer outputBuffer,
                JsonCodec<TaskStats> taskStatsJsonCodec,
                CollectionAccumulator<byte[]> taskStatsCollector)
        {
            this.taskContext = requireNonNull(taskContext, "taskContext is null");
            this.drivers = ImmutableList.copyOf(requireNonNull(drivers, "drivers is null"));
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.taskStatsJsonCodec = requireNonNull(taskStatsJsonCodec, "taskStatsJsonCodec is null");
            this.taskStatsCollector = requireNonNull(taskStatsCollector, "taskStatsCollector is null");
        }

        @Override
        protected Tuple2<Integer, byte[]> computeNext()
        {
            boolean done = false;
            while (!done && !outputBuffer.hasPagesBuffered()) {
                boolean processed = false;
                for (Driver driver : drivers) {
                    if (!driver.isFinished()) {
                        // TODO: avoid busy looping, wait on blocked future
                        driver.process();
                        processed = true;
                    }
                }
                done = !processed;
            }

            if (done && !outputBuffer.hasPagesBuffered()) {
                TaskStats taskStats = taskContext.getTaskStats();
                byte[] taskStatsSerialized = taskStatsJsonCodec.toJsonBytes(taskStats);
                taskStatsCollector.add(taskStatsSerialized);
                return endOfData();
            }

            PageWithPartition pageWithPartition = outputBuffer.getNext();
            SerializedPage serializedPage = pageWithPartition.getPage();

            log.debug("[%s] Producing page for partition: %s\n", taskContext.getTaskId(), pageWithPartition.getPartition());

            return new Tuple2<>(pageWithPartition.getPartition(), serializePage(serializedPage));
        }
    }

    private static byte[] serializePage(SerializedPage page)
    {
        SliceOutput sliceOutput = new DynamicSliceOutput(page.getUncompressedSizeInBytes());
        writeSerializedPage(sliceOutput, page);
        try {
            sliceOutput.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return sliceOutput.getUnderlyingSlice().getBytes();
    }

    private static SerializedPage deserializeSerializedPage(byte[] data)
    {
        return readSerializedPages(Slices.wrappedBuffer(data).getInput()).next();
    }
}
