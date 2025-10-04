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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.concurrent.ThreadPoolExecutorMBean;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.stats.DecayCounter;
import com.facebook.airlift.stats.ExponentialDecay;
import com.facebook.airlift.units.Duration;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.Session;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SafeEventLoopGroup;
import com.facebook.presto.execution.SchedulerStatsTracker;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.AbstractEventExecutorGroup;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.server.remotetask.HttpRemoteTaskWithEventLoop.createHttpRemoteTaskWithEventLoop;
import static com.facebook.presto.server.thrift.ThriftCodecWrapper.wrapThriftCodec;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class HttpRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final HttpClient httpClient;
    private final LocationFactory locationFactory;
    private final Codec<TaskStatus> taskStatusCodec;
    private final Codec<TaskInfo> taskInfoCodec;
    //Json codec required for TaskUpdateRequest endpoint which uses JSON and returns a TaskInfo
    private final Codec<TaskInfo> taskInfoJsonCodec;
    private final Codec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final Codec<TaskInfo> taskInfoResponseCodec;
    private final Codec<PlanFragment> planFragmentCodec;
    private final Duration maxErrorDuration;
    private final Duration taskStatusRefreshMaxWait;
    private final Duration taskInfoRefreshMaxWait;
    private final HandleResolver handleResolver;

    private final Duration taskInfoUpdateInterval;
    private final ExecutorService coreExecutor;
    private final Executor executor;
    private final ThreadPoolExecutorMBean executorMBean;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final ScheduledExecutorService errorScheduledExecutor;
    private final RemoteTaskStats stats;
    private final boolean binaryTransportEnabled;
    private final boolean thriftTransportEnabled;
    private final boolean taskInfoThriftTransportEnabled;
    private final boolean taskUpdateRequestThriftSerdeEnabled;
    private final boolean taskInfoResponseThriftSerdeEnabled;
    private final Protocol thriftProtocol;
    private final int maxTaskUpdateSizeInBytes;
    private final MetadataManager metadataManager;
    private final QueryManager queryManager;
    private final DecayCounter taskUpdateRequestSize;
    private final boolean taskUpdateSizeTrackingEnabled;
    private final Optional<SafeEventLoopGroup> eventLoopGroup;

    @Inject
    public HttpRemoteTaskFactory(
            QueryManagerConfig config,
            TaskManagerConfig taskConfig,
            @ForScheduler HttpClient httpClient,
            LocationFactory locationFactory,
            JsonCodec<TaskStatus> taskStatusJsonCodec,
            SmileCodec<TaskStatus> taskStatusSmileCodec,
            ThriftCodec<TaskStatus> taskStatusThriftCodec,
            JsonCodec<TaskInfo> taskInfoJsonCodec,
            SmileCodec<TaskInfo> taskInfoSmileCodec,
            ThriftCodec<TaskInfo> taskInfoThriftCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestJsonCodec,
            SmileCodec<TaskUpdateRequest> taskUpdateRequestSmileCodec,
            ThriftCodec<TaskUpdateRequest> taskUpdateRequestThriftCodec,
            JsonCodec<PlanFragment> planFragmentJsonCodec,
            SmileCodec<PlanFragment> planFragmentSmileCodec,
            RemoteTaskStats stats,
            InternalCommunicationConfig communicationConfig,
            MetadataManager metadataManager,
            QueryManager queryManager,
            HandleResolver handleResolver)
    {
        this.httpClient = httpClient;
        this.locationFactory = locationFactory;
        this.maxErrorDuration = config.getRemoteTaskMaxErrorDuration();
        this.taskStatusRefreshMaxWait = taskConfig.getStatusRefreshMaxWait();
        this.taskInfoUpdateInterval = taskConfig.getInfoUpdateInterval();
        this.taskInfoRefreshMaxWait = taskConfig.getInfoRefreshMaxWait();
        this.handleResolver = handleResolver;

        this.coreExecutor = newCachedThreadPool(daemonThreadsNamed("remote-task-callback-%s"));
        this.executor = new BoundedExecutor(coreExecutor, config.getRemoteTaskMaxCallbackThreads());
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) coreExecutor);
        this.stats = requireNonNull(stats, "stats is null");
        requireNonNull(communicationConfig, "communicationConfig is null");
        binaryTransportEnabled = communicationConfig.isBinaryTransportEnabled();
        thriftTransportEnabled = communicationConfig.isThriftTransportEnabled();
        taskInfoThriftTransportEnabled = communicationConfig.isTaskInfoThriftTransportEnabled();
        taskUpdateRequestThriftSerdeEnabled = communicationConfig.isTaskUpdateRequestThriftSerdeEnabled();
        taskInfoResponseThriftSerdeEnabled = communicationConfig.isTaskInfoResponseThriftSerdeEnabled();

        thriftProtocol = communicationConfig.getThriftProtocol();
        this.maxTaskUpdateSizeInBytes = toIntExact(requireNonNull(communicationConfig, "communicationConfig is null").getMaxTaskUpdateSize().toBytes());

        if (thriftTransportEnabled) {
            this.taskStatusCodec = wrapThriftCodec(taskStatusThriftCodec);
        }
        else if (binaryTransportEnabled) {
            this.taskStatusCodec = taskStatusSmileCodec;
        }
        else {
            this.taskStatusCodec = taskStatusJsonCodec;
        }

        if (taskInfoThriftTransportEnabled) {
            this.taskInfoCodec = wrapThriftCodec(taskInfoThriftCodec);
        }
        else if (binaryTransportEnabled) {
            this.taskInfoCodec = taskInfoSmileCodec;
        }
        else {
            this.taskInfoCodec = taskInfoJsonCodec;
        }

        if (taskUpdateRequestThriftSerdeEnabled) {
            this.taskUpdateRequestCodec = wrapThriftCodec(taskUpdateRequestThriftCodec);
        }
        else if (binaryTransportEnabled) {
            this.taskUpdateRequestCodec = taskUpdateRequestSmileCodec;
        }
        else {
            this.taskUpdateRequestCodec = taskUpdateRequestJsonCodec;
        }

        if (taskInfoResponseThriftSerdeEnabled) {
            this.taskInfoResponseCodec = wrapThriftCodec(taskInfoThriftCodec);
        }
        else if (binaryTransportEnabled) {
            this.taskInfoResponseCodec = taskInfoSmileCodec;
        }
        else {
            this.taskInfoResponseCodec = taskInfoJsonCodec;
        }

        this.taskInfoJsonCodec = taskInfoJsonCodec;
        this.planFragmentCodec = planFragmentJsonCodec;

        this.metadataManager = metadataManager;
        this.queryManager = queryManager;

        this.updateScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("task-info-update-scheduler-%s"));
        this.errorScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("remote-task-error-delay-%s"));
        this.taskUpdateRequestSize = new DecayCounter(ExponentialDecay.oneMinute());
        this.taskUpdateSizeTrackingEnabled = taskConfig.isTaskUpdateSizeTrackingEnabled();

        this.eventLoopGroup = Optional.of(new SafeEventLoopGroup(config.getRemoteTaskMaxCallbackThreads(),
                new ThreadFactoryBuilder().setNameFormat("task-event-loop-%s").setDaemon(true).build(), taskConfig.getSlowMethodThresholdOnEventLoop())
        {
            @Override
            protected EventLoop newChild(Executor executor, Object... args)
            {
                return new SafeEventLoop(this, executor);
            }
        });
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @Managed
    public double getTaskUpdateRequestSize()
    {
        return taskUpdateRequestSize.getCount();
    }

    @PreDestroy
    public void stop()
    {
        coreExecutor.shutdownNow();
        updateScheduledExecutor.shutdownNow();
        errorScheduledExecutor.shutdownNow();

        eventLoopGroup.map(AbstractEventExecutorGroup::shutdownGracefully);
    }

    @Override
    public RemoteTask createRemoteTask(
            Session session,
            TaskId taskId,
            InternalNode node,
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            NodeTaskMap.NodeStatsTracker nodeStatsTracker,
            boolean summarizeTaskInfo,
            TableWriteInfo tableWriteInfo,
            SchedulerStatsTracker schedulerStatsTracker)
    {
        return createHttpRemoteTaskWithEventLoop(
                session,
                taskId,
                node.getNodeIdentifier(),
                locationFactory.createLegacyTaskLocation(node, taskId),
                locationFactory.createTaskLocation(node, taskId),
                fragment,
                initialSplits,
                outputBuffers,
                httpClient,
                maxErrorDuration,
                taskStatusRefreshMaxWait,
                taskInfoRefreshMaxWait,
                taskInfoUpdateInterval,
                summarizeTaskInfo,
                taskStatusCodec,
                taskInfoCodec,
                taskInfoJsonCodec,
                taskUpdateRequestCodec,
                taskInfoResponseCodec,
                planFragmentCodec,
                nodeStatsTracker,
                stats,
                binaryTransportEnabled,
                thriftTransportEnabled,
                taskInfoThriftTransportEnabled,
                taskUpdateRequestThriftSerdeEnabled,
                taskInfoResponseThriftSerdeEnabled,
                thriftProtocol,
                tableWriteInfo,
                maxTaskUpdateSizeInBytes,
                metadataManager,
                queryManager,
                taskUpdateRequestSize,
                taskUpdateSizeTrackingEnabled,
                handleResolver,
                schedulerStatsTracker,
                (SafeEventLoopGroup.SafeEventLoop) eventLoopGroup.get().next());
    }
}
