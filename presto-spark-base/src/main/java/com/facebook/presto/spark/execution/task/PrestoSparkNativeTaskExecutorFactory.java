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
package com.facebook.presto.spark.execution.task;

import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.common.RuntimeUnit;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.Location;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.metadata.RemoteTransactionHandle;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spark.PrestoSparkConfig;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.accesscontrol.PrestoSparkAuthenticatorProvider;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutor;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkNativeTaskInputs;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleReadDescriptor;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskInputs;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskSource;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spark.execution.BroadcastFileInfo;
import com.facebook.presto.spark.execution.PrestoSparkBroadcastTableCacheManager;
import com.facebook.presto.spark.execution.PrestoSparkExecutionExceptionFactory;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionProcess;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionProcessFactory;
import com.facebook.presto.spark.execution.shuffle.PrestoSparkShuffleInfoTranslator;
import com.facebook.presto.spark.execution.shuffle.PrestoSparkShuffleWriteInfo;
import com.facebook.presto.spark.util.PrestoSparkStatsCollectionUtils;
import com.facebook.presto.spark.util.PrestoSparkUtils;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoTransportException;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.security.TokenAuthenticator;
import com.facebook.presto.spi.storage.TempStorageHandle;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.storage.TempStorageManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.sun.management.OperatingSystemMXBean;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

import javax.inject.Inject;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.airlift.units.DataSize.succinctBytes;
import static com.facebook.presto.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getNativeTerminateWithCoreTimeout;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isNativeTerminateWithCoreWhenUnresponsiveEnabled;
import static com.facebook.presto.spark.util.PrestoSparkUtils.deserializeZstdCompressed;
import static com.facebook.presto.spark.util.PrestoSparkUtils.serializeZstdCompressed;
import static com.facebook.presto.spark.util.PrestoSparkUtils.toPrestoSparkSerializedPage;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static com.facebook.presto.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * PrestoSparkNativeTaskExecutorFactory is responsible for launching the external native process and managing the communication
 * between Java process and native process (by using the {@Link NativeExecutionTask}).
 * It will send necessary metadata (e.g, plan fragment, session properties etc.) as a part of
 * BatchTaskUpdateRequest. It will poll the remote CPP task for status and results (pages/data if applicable)
 * and send these back to the Spark's RDD api
 * <p>
 * PrestoSparkNativeTaskExecutorFactory is singleton instantiated once per executor.
 * <p>
 * For every task it receives, it does the following
 * 1. Create the Native execution Process (NativeTaskExecutionFactory) ensure that is it created only once.
 * 2. Serialize and pass the planFragment, source-metadata (taskSources), sink-metadata (tableWriteInfo or shuffleWriteInfo)
 * and submit a nativeExecutionTask.
 * 3. Return Iterator to sparkRDD layer. RDD execution will call the .next() methods, which will
 * 3.a Call {@link NativeExecutionTask}'s pollResult() to retrieve {@link SerializedPage} back from external process.
 * 3.b If no more output is available, then check if task has finished successfully or with exception
 * If task finished with exception - fail the spark task (throw exception)
 * IF task finished successfully - collect statistics through taskInfo object and add to accumulator
 */
public class PrestoSparkNativeTaskExecutorFactory
        implements IPrestoSparkTaskExecutorFactory
{
    private static final Logger log = Logger.get(PrestoSparkNativeTaskExecutorFactory.class);

    // For Presto-on-Spark, we do not have remoteSourceTasks as the shuffle data is
    // in persistent shuffle.
    // Current protocol for Split mandates having a remoteSourceTaskId as the
    // part of the split info. So for shuffleRead split we set it to a dummy
    // value that is ignored by the shuffle-reader
    private static final TaskId DUMMY_TASK_ID = TaskId.valueOf("remotesourcetaskid.0.0.0.0");

    private final SessionPropertyManager sessionPropertyManager;
    private final JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec;
    private final JsonCodec<BroadcastFileInfo> broadcastFileInfoJsonCodec;
    private final Codec<TaskSource> taskSourceCodec;
    private final Codec<TaskInfo> taskInfoCodec;
    private final PrestoSparkExecutionExceptionFactory executionExceptionFactory;
    private final Set<PrestoSparkAuthenticatorProvider> authenticatorProviders;
    private final NativeExecutionProcessFactory nativeExecutionProcessFactory;
    private final NativeExecutionTaskFactory nativeExecutionTaskFactory;
    private final PrestoSparkShuffleInfoTranslator shuffleInfoTranslator;
    private final PagesSerde pagesSerde;
    private final TempStorageManager tempStorageManager;
    private final String nativeTempStorage;
    private NativeExecutionProcess nativeExecutionProcess;

    private static class CpuTracker
    {
        private OperatingSystemMXBean operatingSystemMXBean;
        private OptionalLong startCpuTime;

        public CpuTracker()
        {
            if (ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean) {
                // we want the com.sun.management sub-interface of java.lang.management.OperatingSystemMXBean
                operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
                startCpuTime = OptionalLong.of(operatingSystemMXBean.getProcessCpuTime());
            }
            else {
                startCpuTime = OptionalLong.empty();
            }
        }

        OptionalLong get()
        {
            if (operatingSystemMXBean != null) {
                long endCpuTime = operatingSystemMXBean.getProcessCpuTime();
                return OptionalLong.of(endCpuTime - startCpuTime.getAsLong());
            }
            else {
                return OptionalLong.empty();
            }
        }
    }

    @Inject
    public PrestoSparkNativeTaskExecutorFactory(
            SessionPropertyManager sessionPropertyManager,
            BlockEncodingManager blockEncodingManager,
            JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec,
            JsonCodec<BroadcastFileInfo> broadcastFileInfoJsonCodec,
            Codec<TaskSource> taskSourceCodec,
            Codec<TaskInfo> taskInfoCodec,
            PrestoSparkExecutionExceptionFactory executionExceptionFactory,
            Set<PrestoSparkAuthenticatorProvider> authenticatorProviders,
            PrestoSparkBroadcastTableCacheManager prestoSparkBroadcastTableCacheManager,
            NativeExecutionProcessFactory nativeExecutionProcessFactory,
            NativeExecutionTaskFactory nativeExecutionTaskFactory,
            PrestoSparkShuffleInfoTranslator shuffleInfoTranslator,
            TempStorageManager tempStorageManager,
            PrestoSparkConfig prestoSparkConfig,
            FeaturesConfig featureConfig)
    {
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.taskDescriptorJsonCodec = requireNonNull(taskDescriptorJsonCodec, "sparkTaskDescriptorJsonCodec is null");
        this.taskSourceCodec = requireNonNull(taskSourceCodec, "taskSourceCodec is null");
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        this.broadcastFileInfoJsonCodec = requireNonNull(broadcastFileInfoJsonCodec, "broadcastFileInfoJsonCodec is null");
        this.executionExceptionFactory = requireNonNull(executionExceptionFactory, "executionExceptionFactory is null");
        this.authenticatorProviders = ImmutableSet.copyOf(requireNonNull(authenticatorProviders, "authenticatorProviders is null"));
        this.nativeExecutionProcessFactory = requireNonNull(nativeExecutionProcessFactory, "processFactory is null");
        this.nativeExecutionTaskFactory = requireNonNull(nativeExecutionTaskFactory, "taskFactory is null");
        this.shuffleInfoTranslator = requireNonNull(shuffleInfoTranslator, "shuffleInfoFactory is null");
        this.pagesSerde = PrestoSparkUtils.createPagesSerde(requireNonNull(blockEncodingManager, "blockEncodingManager is null"));
        this.tempStorageManager = requireNonNull(tempStorageManager, "tempStorageManager is null");
        this.nativeTempStorage = requireNonNull(featureConfig, "featureConfig is null").getSpillerTempStorage();
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
        CpuTracker cpuTracker = new CpuTracker();

        PrestoSparkTaskDescriptor taskDescriptor = taskDescriptorJsonCodec.fromJson(
                serializedTaskDescriptor.getBytes());
        Optional<TempStorageHandle> nativeTempStorageHandle = Optional.of(
                tempStorageManager.getTempStorage(
                                this.nativeTempStorage)
                        .deserialize(taskDescriptor.getSerializedNativeTempStorageHandle()));
        ImmutableMap.Builder<String, TokenAuthenticator> extraAuthenticators = ImmutableMap.builder();
        authenticatorProviders.forEach(provider -> extraAuthenticators.putAll(provider.getTokenAuthenticators()));

        Session session = taskDescriptor.getSession().toSession(
                sessionPropertyManager,
                taskDescriptor.getExtraCredentials(),
                extraAuthenticators.build());
        PlanFragment fragment = taskDescriptor.getFragment();
        StageId stageId = new StageId(session.getQueryId(), fragment.getId().getId());
        TaskId taskId = new TaskId(new StageExecutionId(stageId, 0), partitionId, attemptNumber);

        // TODO: Remove this once we can display the plan on Spark UI.
        // Currently, `textPlanFragment` throws an exception if json-based UDFs are used in the query, which can only
        // happen in native execution mode. To resolve this error, `JsonFileBasedFunctionNamespaceManager` must be
        // loaded on the executors as well (which is actually not required for native execution). To do so, we need a
        // mechanism to ship the JSON file containing the UDF metadata to workers, which does not exist as of today.
        // TODO: Address this issue; more details in https://github.com/prestodb/presto/issues/19600
        log.info("Logging plan fragment is not supported for presto-on-spark native execution, yet");

        if (fragment.getPartitioning().isCoordinatorOnly()) {
            throw new UnsupportedOperationException("Coordinator only fragment execution is not supported by native task executor");
        }

        checkArgument(
                inputs instanceof PrestoSparkNativeTaskInputs,
                format("PrestoSparkNativeTaskInputs is required for native execution, but %s is provided", inputs.getClass().getName()));

        // 1. Start the native process if it hasn't already been started or dead
        createAndStartNativeExecutionProcess(session, nativeTempStorageHandle);

        // 2. compute the task info to send to cpp process
        PrestoSparkNativeTaskInputs nativeInputs = (PrestoSparkNativeTaskInputs) inputs;

        // 2.a Populate Read info
        List<TaskSource> taskSources = getTaskSources(serializedTaskSources, fragment, session, nativeInputs);

        // 2.b Populate Shuffle Write info
        Optional<PrestoSparkShuffleWriteInfo> shuffleWriteInfo = nativeInputs.getShuffleWriteDescriptor()
                .map(descriptor -> shuffleInfoTranslator.createShuffleWriteInfo(session, descriptor));
        Optional<String> serializedShuffleWriteInfo = shuffleWriteInfo.map(shuffleInfoTranslator::createSerializedWriteInfo);

        boolean terminateWithCoreWhenUnresponsive = isNativeTerminateWithCoreWhenUnresponsiveEnabled(session);
        Duration terminateWithCoreTimeout = getNativeTerminateWithCoreTimeout(session);
        try {
            // 3. Submit the task to cpp process for execution
            log.info(format("Submitting native execution task. taskId %s", taskId.toString()));
            NativeExecutionTask task = nativeExecutionTaskFactory.createNativeExecutionTask(
                    session,
                    nativeExecutionProcess.getLocation(),
                    taskId,
                    fragment,
                    ImmutableList.copyOf(taskSources),
                    taskDescriptor.getTableWriteInfo(),
                    Optional.of(taskDescriptor.getSerializedNativeTempStorageHandle()),
                    serializedShuffleWriteInfo,
                    fragment.getPartitioningScheme().getPartitioning()
                            .getHandle().equals(FIXED_BROADCAST_DISTRIBUTION));

            log.info("Creating task and will wait for remote task completion");
            TaskInfo taskInfo = task.start();

            // task creation might have failed
            processTaskInfoForErrorsOrCompletion(taskInfo);

            // 4. return output to spark RDD layer
            return new PrestoSparkNativeTaskOutputIterator<>(
                    partitionId,
                    task,
                    outputType,
                    taskInfoCollector,
                    taskInfoCodec,
                    executionExceptionFactory,
                    cpuTracker,
                    nativeExecutionProcess,
                    terminateWithCoreWhenUnresponsive,
                    terminateWithCoreTimeout);
        }
        catch (RuntimeException e) {
            throw processFailure(e, nativeExecutionProcess, terminateWithCoreWhenUnresponsive, terminateWithCoreTimeout);
        }
    }

    @Override
    public void close()
    {
        if (nativeExecutionProcess != null) {
            nativeExecutionProcess.close();
        }
    }

    private static void completeTask(boolean success, CollectionAccumulator<SerializedTaskInfo> taskInfoCollector, NativeExecutionTask task, Codec<TaskInfo> taskInfoCodec, CpuTracker cpuTracker)
    {
        // stop the task
        task.stop(success);

        OptionalLong processCpuTime = cpuTracker.get();

        // collect statistics (if available)
        Optional<TaskInfo> taskInfoOptional = tryGetTaskInfo(task);
        if (!taskInfoOptional.isPresent()) {
            log.error("Missing taskInfo. Statistics might be inaccurate");
            return;
        }

        // Record process-wide CPU time spent while executing this task. Since we run one task at a time,
        // process-wide CPU time matches task's CPU time.
        processCpuTime.ifPresent(cpuTime -> taskInfoOptional.get().getStats().getRuntimeStats()
                .addMetricValue("javaProcessCpuTime", RuntimeUnit.NANO, cpuTime));

        SerializedTaskInfo serializedTaskInfo = new SerializedTaskInfo(serializeZstdCompressed(taskInfoCodec, taskInfoOptional.get()));
        taskInfoCollector.add(serializedTaskInfo);

        // Update Spark Accumulators for spark internal metrics
        PrestoSparkStatsCollectionUtils.collectMetrics(taskInfoOptional.get());
    }

    private static Optional<TaskInfo> tryGetTaskInfo(NativeExecutionTask task)
    {
        try {
            return task.getTaskInfo();
        }
        catch (RuntimeException e) {
            log.debug(e, "TaskInfo is not available");
            return Optional.empty();
        }
    }

    private static void processTaskInfoForErrorsOrCompletion(TaskInfo taskInfo)
    {
        if (!taskInfo.getTaskStatus().getState().isDone()) {
            log.info("processTaskInfoForErrors: task is not done yet.. %s", taskInfo);
            return;
        }

        if (!taskInfo.getTaskStatus().getState().equals(TaskState.FINISHED)) {
            // task failed with errors
            RuntimeException failure = taskInfo.getTaskStatus().getFailures().stream()
                    .findFirst()
                    .map(ExecutionFailureInfo::toException)
                    .orElseGet(() -> new PrestoException(GENERIC_INTERNAL_ERROR, "Native task failed for an unknown reason"));
            throw failure;
        }

        log.info("processTaskInfoForErrors: task completed successfully = %s", taskInfo);
    }

    private void createAndStartNativeExecutionProcess(Session session,
            Optional<TempStorageHandle> nativeTempStorageHandle)
    {
        requireNonNull(nativeExecutionProcessFactory, "Trying to instantiate native process but factory is null");

        try {
            // create the CPP sidecar process if it doesn't exist.
            // We create this when the first task is scheduled
            nativeExecutionProcess = nativeExecutionProcessFactory.getNativeExecutionProcess(
                    session, nativeTempStorageHandle);
            nativeExecutionProcess.start();
        }
        catch (ExecutionException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<TaskSource> getTaskSources(
            Iterator<SerializedPrestoSparkTaskSource> serializedTaskSources,
            PlanFragment fragment,
            Session session,
            PrestoSparkNativeTaskInputs nativeTaskInputs)
    {
        List<TaskSource> taskSources = new ArrayList<>();

        // Populate TableScan sources
        long totalSerializedSizeInBytes = 0;
        while (serializedTaskSources.hasNext()) {
            SerializedPrestoSparkTaskSource serializedTaskSource = serializedTaskSources.next();
            taskSources.add(deserializeZstdCompressed(taskSourceCodec, serializedTaskSource.getBytes()));
            totalSerializedSizeInBytes += serializedTaskSource.getBytes().length;
        }

        // When joining bucketed table with a non-bucketed table with a filter on "$bucket",
        // some tasks may not have splits for the bucketed table. In this case we still need
        // to send no-more-splits message to Velox.
        Set<PlanNodeId> planNodeIdsWithSources = taskSources.stream().map(TaskSource::getPlanNodeId).collect(Collectors.toSet());
        Set<PlanNodeId> tableScanIds = Sets.newHashSet(scheduleOrder(fragment.getRoot()));
        tableScanIds.stream()
                .filter(id -> !planNodeIdsWithSources.contains(id))
                .forEach(id -> taskSources.add(new TaskSource(id, ImmutableSet.of(), true)));

        log.info("Total serialized size of all table scan task sources: %s", succinctBytes(totalSerializedSizeInBytes));

        // Populate remote sources - ShuffleRead & Broadcast.
        ImmutableList.Builder<TaskSource> shuffleTaskSources = ImmutableList.builder();
        ImmutableList.Builder<TaskSource> broadcastTaskSources = ImmutableList.builder();
        AtomicLong nextSplitId = new AtomicLong();
        taskSources.stream()
                .flatMap(source -> source.getSplits().stream())
                .mapToLong(ScheduledSplit::getSequenceId)
                .max()
                .ifPresent(id -> nextSplitId.set(id + 1));

        for (RemoteSourceNode remoteSource : fragment.getRemoteSourceNodes()) {
            for (PlanFragmentId sourceFragmentId : remoteSource.getSourceFragmentIds()) {
                PrestoSparkShuffleReadDescriptor shuffleReadDescriptor =
                        nativeTaskInputs.getShuffleReadDescriptors().get(sourceFragmentId.toString());
                if (shuffleReadDescriptor != null) {
                    ScheduledSplit split = new ScheduledSplit(nextSplitId.getAndIncrement(), remoteSource.getId(), new Split(REMOTE_CONNECTOR_ID, new RemoteTransactionHandle(), new RemoteSplit(
                            new Location(format("batch://%s?shuffleInfo=%s", DUMMY_TASK_ID,
                                    shuffleInfoTranslator.createSerializedReadInfo(
                                            shuffleInfoTranslator.createShuffleReadInfo(session, shuffleReadDescriptor)))),
                            DUMMY_TASK_ID)));
                    Set<ScheduledSplit> shuffleSplits = shuffleInfoTranslator.postProcessSplits(ImmutableSet.of(split), session);
                    TaskSource source = new TaskSource(remoteSource.getId(), shuffleSplits, ImmutableSet.of(Lifespan.taskWide()), true);
                    shuffleTaskSources.add(source);
                }

                Broadcast<?> broadcast = nativeTaskInputs.getBroadcastInputs().get(sourceFragmentId.toString());
                if (broadcast != null) {
                    Set<ScheduledSplit> splits =
                            ((List<?>) broadcast.value()).stream()
                                    .map(PrestoSparkSerializedPage.class::cast)
                                    .map(prestoSparkSerializedPage -> PrestoSparkUtils.toSerializedPage(prestoSparkSerializedPage))
                                    .map(serializedPage -> pagesSerde.deserialize(serializedPage))
                                    // Extract filePath.
                                    .flatMap(page -> IntStream.range(0, page.getPositionCount())
                                            .mapToObj(position -> VarcharType.VARCHAR.getObjectValue(null, page.getBlock(0), position)))
                                    .map(String.class::cast)
                                    .map(filePath -> new BroadcastFileInfo(filePath))
                                    .map(broadcastFileInfo -> new ScheduledSplit(
                                            nextSplitId.getAndIncrement(),
                                            remoteSource.getId(),
                                            new Split(
                                                    REMOTE_CONNECTOR_ID,
                                                    new RemoteTransactionHandle(),
                                                    new RemoteSplit(
                                                            new Location(
                                                                    format("batch://%s?broadcastInfo=%s", DUMMY_TASK_ID, broadcastFileInfoJsonCodec.toJson(broadcastFileInfo))),
                                                            DUMMY_TASK_ID))))
                                    .collect(toImmutableSet());

                    TaskSource source = new TaskSource(remoteSource.getId(), splits, ImmutableSet.of(Lifespan.taskWide()), true);
                    broadcastTaskSources.add(source);
                }
            }
        }

        taskSources.addAll(shuffleTaskSources.build());
        taskSources.addAll(broadcastTaskSources.build());
        return taskSources;
    }

    private Optional<TableWriterNode> findTableWriteNode(PlanNode node)
    {
        return searchFrom(node)
                .where(TableWriterNode.class::isInstance)
                .findFirst();
    }

    private static class PrestoSparkNativeTaskOutputIterator<T extends PrestoSparkTaskOutput>
            extends AbstractIterator<Tuple2<MutablePartitionId, T>>
            implements IPrestoSparkTaskExecutor<T>
    {
        private final int partitionId;
        private final NativeExecutionTask nativeExecutionTask;
        private Optional<SerializedPage> next = Optional.empty();
        private final CollectionAccumulator<SerializedTaskInfo> taskInfoCollectionAccumulator;
        private final Codec<TaskInfo> taskInfoCodec;
        private final Class<T> outputType;
        private final PrestoSparkExecutionExceptionFactory executionExceptionFactory;
        private final CpuTracker cpuTracker;
        private final NativeExecutionProcess nativeExecutionProcess;
        private final boolean terminateWithCoreWhenUnresponsive;
        private final Duration terminateWithCoreTimeout;

        public PrestoSparkNativeTaskOutputIterator(
                int partitionId,
                NativeExecutionTask nativeExecutionTask,
                Class<T> outputType,
                CollectionAccumulator<SerializedTaskInfo> taskInfoCollectionAccumulator,
                Codec<TaskInfo> taskInfoCodec,
                PrestoSparkExecutionExceptionFactory executionExceptionFactory,
                CpuTracker cpuTracker,
                NativeExecutionProcess nativeExecutionProcess,
                boolean terminateWithCoreWhenUnresponsive,
                Duration terminateWithCoreTimeout)
        {
            this.partitionId = partitionId;
            this.nativeExecutionTask = nativeExecutionTask;
            this.taskInfoCollectionAccumulator = taskInfoCollectionAccumulator;
            this.taskInfoCodec = taskInfoCodec;
            this.outputType = outputType;
            this.executionExceptionFactory = executionExceptionFactory;
            this.cpuTracker = cpuTracker;
            this.nativeExecutionProcess = requireNonNull(nativeExecutionProcess, "nativeExecutionProcess is null");
            this.terminateWithCoreWhenUnresponsive = terminateWithCoreWhenUnresponsive;
            this.terminateWithCoreTimeout = requireNonNull(terminateWithCoreTimeout, "terminateWithCoreTimeout is null");
        }

        /**
         * This function is called by Spark's RDD layer to check if there are output pages
         * There are 2 scenarios
         * 1. ShuffleMap Task - Always returns false. But the internal function calls do all the work needed
         * 2. Result Task     - True until pages are available. False once all pages have been extracted
         *
         * @return if output is available
         */
        @Override
        public boolean hasNext()
        {
            next = computeNext();
            return next.isPresent();
        }

        /**
         * This function returns the next available page fetched from CPP process
         * <p>
         * Has 3 main responsibilities
         * 1)  wait-for-pages-or-completion
         * <p>
         * The thread running this method will wait until either of the 3 conditions happen
         * *      1. We get a page
         * *      2. Task has finished successfully
         * *      3. Task has finished with error
         * <p>
         * For ShuffleMap Task, as of now, the CPP process returns no pages.
         * So the thread will be in WAITING state till the CPP task is done and returns an Optional.empty()
         * once the task has terminated
         * <p>
         * For a Result Task, this function will return pages retrieved from CPP side once we got them.
         * Once all the pages have been read and the task has been terminates
         * <p>
         * 2) Exception handling
         * The function also checks if the task has finished
         * with exceptions and throws the appropriate exception back to spark's RDD processing
         * layer
         * <p>
         * 3) Statistics collection
         * For both, when the task finished successfully or with exception, it tries to collect
         * statistics if TaskInfo object is available
         *
         * @return Optional<SerializedPage> outputPage
         */
        private Optional<SerializedPage> computeNext()
        {
            try {
                Object taskFinishedOrHasResult = nativeExecutionTask.getTaskFinishedOrHasResult();
                // Blocking wait if task is still running or hasn't produced any output page
                synchronized (taskFinishedOrHasResult) {
                    while (!nativeExecutionTask.isTaskDone() && !nativeExecutionTask.hasResult()) {
                        taskFinishedOrHasResult.wait();
                    }
                }

                // For ShuffleMap Task, this will always return Optional.empty()
                Optional<SerializedPage> pageOptional = nativeExecutionTask.pollResult();

                if (pageOptional.isPresent()) {
                    return pageOptional;
                }

                // Double check if current task's already done (since thread could be awoken by either having output or task is done above)
                synchronized (taskFinishedOrHasResult) {
                    while (!nativeExecutionTask.isTaskDone()) {
                        taskFinishedOrHasResult.wait();
                    }
                }

                Optional<TaskInfo> taskInfo = nativeExecutionTask.getTaskInfo();

                processTaskInfoForErrorsOrCompletion(taskInfo.get());
            }
            catch (RuntimeException ex) {
                // For a failed task, if taskInfo is present we still want to log the metrics
                completeTask(false, taskInfoCollectionAccumulator, nativeExecutionTask, taskInfoCodec, cpuTracker);
                throw executionExceptionFactory.toPrestoSparkExecutionException(processFailure(
                        ex,
                        nativeExecutionProcess,
                        terminateWithCoreWhenUnresponsive,
                        terminateWithCoreTimeout));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            // Reaching here marks the end of task processing
            completeTask(true, taskInfoCollectionAccumulator, nativeExecutionTask, taskInfoCodec, cpuTracker);
            return Optional.empty();
        }

        @Override
        public Tuple2<MutablePartitionId, T> next()
        {
            // Result Tasks only have outputType of PrestoSparkSerializedPage.
            checkArgument(outputType == PrestoSparkSerializedPage.class,
                    format("PrestoSparkNativeTaskExecutorFactory only outputType=PrestoSparkSerializedPage " +
                            "But tried to extract outputType=%s", outputType));

            // Set partition ID to help match the results to the task on the driver for debugging.
            MutablePartitionId mutablePartitionId = new MutablePartitionId();
            mutablePartitionId.setPartition(partitionId);
            return new Tuple2<>(mutablePartitionId, (T) toPrestoSparkSerializedPage(next.get()));
        }
    }

    private static RuntimeException processFailure(
            RuntimeException failure,
            NativeExecutionProcess process,
            boolean terminateWithCoreWhenUnresponsive,
            Duration terminateWithCoreTimeout)
    {
        if (isCommunicationLoss(failure)) {
            PrestoTransportException transportException = (PrestoTransportException) failure;
            String message;
            // lost communication with the native execution process
            if (process.isAlive()) {
                // process is unresponsive
                if (terminateWithCoreWhenUnresponsive) {
                    process.terminateWithCore(terminateWithCoreTimeout);
                }
                message = "Native execution process is alive but unresponsive";
            }
            else {
                message = "Native execution process is dead";
                String crashReport = process.getCrashReport();
                if (!crashReport.isEmpty()) {
                    message += ":\n" + crashReport;
                }
            }

            return new PrestoTransportException(
                    transportException::getErrorCode,
                    transportException.getRemoteHost(),
                    message,
                    failure);
        }
        return failure;
    }

    private static boolean isCommunicationLoss(RuntimeException failure)
    {
        if (!(failure instanceof PrestoTransportException)) {
            return false;
        }
        PrestoTransportException transportException = (PrestoTransportException) failure;
        return TOO_MANY_REQUESTS_FAILED.toErrorCode().equals(transportException.getErrorCode());
    }
}
