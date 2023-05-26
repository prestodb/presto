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
import com.facebook.presto.Session;
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
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.RemoteTransactionHandle;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spark.PrestoSparkAuthenticatorProvider;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutor;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkNativeTaskInputs;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleReadDescriptor;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskInputs;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskOutput;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskSource;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spark.execution.shuffle.PrestoSparkShuffleInfoTranslator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.security.TokenAuthenticator;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.SystemSessionProperties.isNativeExecutionEnabled;
import static com.facebook.presto.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static com.facebook.presto.spark.util.PrestoSparkUtils.deserializeZstdCompressed;
import static com.facebook.presto.spark.util.PrestoSparkUtils.serializeZstdCompressed;
import static com.facebook.presto.spark.util.PrestoSparkUtils.toPrestoSparkSerializedPage;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoSparkNativeTaskExecutorFactory
        implements IPrestoSparkTaskExecutorFactory
{
    private static final Logger log = Logger.get(PrestoSparkNativeTaskExecutorFactory.class);

    private final SessionPropertyManager sessionPropertyManager;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec;
    private final Codec<TaskSource> taskSourceCodec;
    private final Codec<TaskInfo> taskInfoCodec;
    private final PrestoSparkExecutionExceptionFactory executionExceptionFactory;
    private final Set<PrestoSparkAuthenticatorProvider> authenticatorProviders;
    private final PrestoSparkBroadcastTableCacheManager prestoSparkBroadcastTableCacheManager;
    private final NativeExecutionProcessFactory nativeExecutionProcessFactory;
    private final NativeExecutionTaskFactory nativeExecutionTaskFactory;
    private final PrestoSparkShuffleInfoTranslator shuffleInfoTranslator;
    private NativeExecutionProcess nativeExecutionProcess;

    @Inject
    public PrestoSparkNativeTaskExecutorFactory(
            SessionPropertyManager sessionPropertyManager,
            FunctionAndTypeManager functionAndTypeManager,
            JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec,
            Codec<TaskSource> taskSourceCodec,
            Codec<TaskInfo> taskInfoCodec,
            PrestoSparkExecutionExceptionFactory executionExceptionFactory,
            Set<PrestoSparkAuthenticatorProvider> authenticatorProviders,
            PrestoSparkBroadcastTableCacheManager prestoSparkBroadcastTableCacheManager,
            NativeExecutionProcessFactory nativeExecutionProcessFactory,
            NativeExecutionTaskFactory nativeExecutionTaskFactory,
            PrestoSparkShuffleInfoTranslator shuffleInfoTranslator)
    {
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.taskDescriptorJsonCodec = requireNonNull(taskDescriptorJsonCodec, "sparkTaskDescriptorJsonCodec is null");
        this.taskSourceCodec = requireNonNull(taskSourceCodec, "taskSourceCodec is null");
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        this.executionExceptionFactory = requireNonNull(executionExceptionFactory, "executionExceptionFactory is null");
        this.authenticatorProviders = ImmutableSet.copyOf(requireNonNull(authenticatorProviders, "authenticatorProviders is null"));
        // Ordering is needed to make sure serialized plans are consistent for the same map
        this.prestoSparkBroadcastTableCacheManager = requireNonNull(prestoSparkBroadcastTableCacheManager, "prestoSparkBroadcastTableCacheManager is null");
        this.nativeExecutionProcessFactory = requireNonNull(nativeExecutionProcessFactory, "processFactory is null");
        this.nativeExecutionTaskFactory = requireNonNull(nativeExecutionTaskFactory, "taskFactory is null");
        this.shuffleInfoTranslator = requireNonNull(shuffleInfoTranslator, "shuffleInfoFactory is null");
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
        TaskId taskId = new TaskId(new StageExecutionId(stageId, 0), partitionId, attemptNumber);

        // Clear the cache if the cache does not have broadcast table for current stageId.
        // We will only cache 1 HT at any time. If the stageId changes, we will drop the old cached HT
        prestoSparkBroadcastTableCacheManager.removeCachedTablesForStagesOtherThan(stageId);

        // TODO: Remove this once we can display the plan on Spark UI.
        // Currently, `textPlanFragment` throws an exception if json-based UDFs are used in the query, which can only
        // happen in native execution mode. To resolve this error, `JsonFileBasedFunctionNamespaceManager` must be
        // loaded on the executors as well (which is actually not required for native execution). To do so, we need a
        // mechanism to ship the JSON file containing the UDF metadata to workers, which does not exist as of today.
        // TODO: Address this issue; more details in https://github.com/prestodb/presto/issues/19600
        if (isNativeExecutionEnabled(session)) {
            log.info("Logging plan fragment is not supported for presto-on-spark native execution, yet");
        }
        else {
            log.info(PlanPrinter.textPlanFragment(fragment, functionAndTypeManager, session, true));
        }

        if (isNativeExecutionEnabled(session) && !fragment.getPartitioning().isCoordinatorOnly()) {
            log.info("Using native execution");

            // 1. Start the native process if it hasn't already been started or dead
            createAndStartNativeExecutionProcess(session);

            // 2. compute the task info to send to cpp process
            checkArgument(
                    inputs instanceof PrestoSparkNativeTaskInputs,
                    format("PrestoSparkNativeTaskInputs is required for native execution, but %s is provided", inputs.getClass().getName()));
            PrestoSparkNativeTaskInputs nativeInputs = (PrestoSparkNativeTaskInputs) inputs;

            // 3.b Populate Read info
            List<TaskSource> taskSources = getTaskSources(serializedTaskSources, fragment, session, nativeInputs);

            // 3.a Populate Write info
            Optional<PrestoSparkShuffleWriteInfo> shuffleWriteInfo = nativeInputs.getShuffleWriteDescriptor().isPresent()
                    && !findTableWriteNode(fragment.getRoot()).isPresent()
                    && !(fragment.getRoot() instanceof OutputNode) ?
                    Optional.of(shuffleInfoTranslator.createShuffleWriteInfo(session, nativeInputs.getShuffleWriteDescriptor().get())) : Optional.empty();
            Optional<String> serializedShuffleWriteInfo = shuffleWriteInfo.map(shuffleInfoTranslator::createSerializedWriteInfo);

            // 4. Submit the task to cpp process for execution
            log.info("Submitting native execution task ");
            NativeExecutionTask task = nativeExecutionTaskFactory.createNativeExecutionTask(
                    session,
                    uriBuilderFrom(URI.create("http://127.0.0.1")).port(nativeExecutionProcess.getPort()).build(),
                    taskId,
                    fragment,
                    ImmutableList.copyOf(taskSources),
                    taskDescriptor.getTableWriteInfo(),
                    serializedShuffleWriteInfo);
            CompletableFuture<TaskInfo> taskInfoCompletableFuture = task.newStart();
            log.info("Created task successfully.. Will wait for remote task completion");
            try {
                taskInfoCompletableFuture.get();
            }
            catch (Exception ex) {
                log.error("Error executing native task", ex);
            }

            // 5. return output to spark RDD layer
            return new PrestoSparkNativeTaskExecutor<>(task, taskInfoCollector, shuffleStatsCollector, taskInfoCodec);
        }

        return null;
    }

    private void createAndStartNativeExecutionProcess(Session session)
    {
        requireNonNull(nativeExecutionProcessFactory, "Trying to instantiate native process but factory is null");

        try {
            // create the CPP sidecar process if it doesn't exist.
            // We create this when the first task is scheduled
            nativeExecutionProcess = nativeExecutionProcessFactory.getNativeExecutionProcess(
                    session,
                    uriBuilderFrom(URI.create("http://127.0.0.1")).port(7777).build());
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
        // Populate TableScan sources
        long totalSerializedSizeInBytes = 0;
        List<TaskSource> taskSources = new ArrayList<>();
        while (serializedTaskSources.hasNext()) {
            SerializedPrestoSparkTaskSource serializedTaskSource = serializedTaskSources.next();
            totalSerializedSizeInBytes += serializedTaskSource.getBytes().length;
            taskSources.add(deserializeZstdCompressed(taskSourceCodec, serializedTaskSource.getBytes()));
        }
        log.info("Total serialized size of all table scan task sources: %s", succinctBytes(totalSerializedSizeInBytes));

        // Populate ShuffleRead sources
        ImmutableList.Builder<TaskSource> shuffleTaskSources = ImmutableList.builder();
        AtomicLong nextSplitId = new AtomicLong();
        TaskId dummyTaskId = TaskId.valueOf("dummy.0.0.0.0");
        taskSources.stream()
                .flatMap(source -> source.getSplits().stream())
                .mapToLong(ScheduledSplit::getSequenceId)
                .max()
                .ifPresent(id -> nextSplitId.set(id + 1));

        log.info("Populating ShuffleRead sources");
        for (RemoteSourceNode remoteSource : fragment.getRemoteSourceNodes()) {
            for (PlanFragmentId sourceFragmentId : remoteSource.getSourceFragmentIds()) {
                PrestoSparkShuffleReadDescriptor shuffleReadDescriptor =
                        nativeTaskInputs.getShuffleReadDescriptors().get(sourceFragmentId.toString());
                if (shuffleReadDescriptor != null) {
                    ScheduledSplit split = new ScheduledSplit(nextSplitId.getAndIncrement(), remoteSource.getId(), new Split(REMOTE_CONNECTOR_ID, new RemoteTransactionHandle(), new RemoteSplit(
                            new Location(format("batch://%s?shuffleInfo=%s", dummyTaskId,
                                    shuffleInfoTranslator.createSerializedReadInfo(
                                            shuffleInfoTranslator.createShuffleReadInfo(session, shuffleReadDescriptor)))),
                            dummyTaskId)));
                    TaskSource source = new TaskSource(remoteSource.getId(), ImmutableSet.of(split), ImmutableSet.of(Lifespan.taskWide()), true);
                    shuffleTaskSources.add(source);
                    log.info("Added shuffle taskSource=%s", source);
                }
            }
        }

        taskSources.addAll(shuffleTaskSources.build());

        return taskSources;
    }

    private Optional<TableWriterNode> findTableWriteNode(PlanNode node)
    {
        return searchFrom(node)
                .where(TableWriterNode.class::isInstance)
                .findFirst();
    }

    private static class PrestoSparkNativeTaskExecutor<T extends PrestoSparkTaskOutput>
            extends AbstractIterator<Tuple2<MutablePartitionId, T>>
            implements IPrestoSparkTaskExecutor<T>
    {
        private final NativeExecutionTask nativeExecutionTask;
        private final CollectionAccumulator<SerializedTaskInfo> taskInfoCollector;
        private final CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector;
        private final Codec<TaskInfo> taskInfoCodec;

        private Optional<SerializedPage> next = Optional.empty();

        public PrestoSparkNativeTaskExecutor(
                NativeExecutionTask nativeExecutionTask,
                CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
                CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector,
                Codec<TaskInfo> taskInfoCodec)
        {
            this.nativeExecutionTask = nativeExecutionTask;
            this.taskInfoCollector = taskInfoCollector;
            this.shuffleStatsCollector = shuffleStatsCollector;
            this.taskInfoCodec = taskInfoCodec;
        }

        @Override
        public boolean hasNext()
        {
            next = computeNext();
            return next.isPresent();
        }

        @Override
        public Tuple2<MutablePartitionId, T> next()
        {
            return new Tuple2<>(new MutablePartitionId(), (T) toPrestoSparkSerializedPage(next.get()));
        }

        private Optional<SerializedPage> computeNext()
        {
            try {
                Optional<SerializedPage> page = nativeExecutionTask.pollResult();
                if (page.isPresent()) {
                    return page;
                }

                Optional<TaskInfo> taskInfo = nativeExecutionTask.getTaskInfo();
                if (!taskInfo.isPresent() || !taskInfo.get().getTaskStatus().getState().isDone()) {
                    throw new IllegalStateException();
                }

                // Update TaskInfo
                SerializedTaskInfo serializedTaskInfo = new SerializedTaskInfo(serializeZstdCompressed(taskInfoCodec, taskInfo.get()));
                taskInfoCollector.add(serializedTaskInfo);

                // Task might have finished but failed
                // Process any errors here and throw exception
                // so that it is propagated to Spark
                TaskStatus status = taskInfo.get().getTaskStatus();
                if (status.getState() != TaskState.FINISHED) {
                    throw status.getFailures().stream()
                            .findFirst()
                            .map(ExecutionFailureInfo::toException)
                            .orElse(new PrestoException(GENERIC_INTERNAL_ERROR, "Native task failed for an unknown reason"));
                }

                // This marks the end of processing.
                // After reaching here, the hasNext would
                // return false
                return Optional.empty();
            }
            catch (InterruptedException e) {
                log.error(e);
                throw new RuntimeException(e);
            }
        }
    }
}
