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

import com.facebook.presto.Session;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.NodeTaskMap.PartitionedSplitCountTracker;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.smile.Codec;
import com.facebook.presto.server.smile.SmileCodec;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.Multimap;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.OptionalInt;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.facebook.presto.server.smile.JsonCodecWrapper.wrapJsonCodec;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class HttpRemoteTaskFactory
        implements RemoteTaskFactory
{
    private static final Logger LOG = Logger.get(HttpRemoteTaskFactory.class);

    private final HttpClient httpClient;
    private final LocationFactory locationFactory;
    private final Codec<TaskStatus> taskStatusCodec;
    private final Codec<TaskInfo> taskInfoCodec;
    private final Codec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final Duration maxErrorDuration;
    private final Duration taskStatusRefreshMaxWait;
    private final Duration taskInfoRefreshMaxWait;
    private final Duration taskInfoUpdateInterval;
    private final ExecutorService coreExecutor;
    private final Executor executor;
    private final ThreadPoolExecutorMBean executorMBean;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final ScheduledExecutorService errorScheduledExecutor;
    private final RemoteTaskStats stats;
    private final boolean isBinaryTransportEnabled;

    @Inject
    public HttpRemoteTaskFactory(QueryManagerConfig config,
            TaskManagerConfig taskConfig,
            @ForScheduler HttpClient httpClient,
            LocationFactory locationFactory,
            JsonCodec<TaskStatus> taskStatusJsonCodec,
            SmileCodec<TaskStatus> taskStatusSmileCodec,
            JsonCodec<TaskInfo> taskInfoJsonCodec,
            SmileCodec<TaskInfo> taskInfoSmileCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestJsonCodec,
            SmileCodec<TaskUpdateRequest> taskUpdateRequestSmileCodec,
            RemoteTaskStats stats,
            InternalCommunicationConfig communicationConfig)
    {
        this.httpClient = httpClient;
        this.locationFactory = locationFactory;
        this.maxErrorDuration = config.getRemoteTaskMaxErrorDuration();
        this.taskStatusRefreshMaxWait = taskConfig.getStatusRefreshMaxWait();
        this.taskInfoUpdateInterval = taskConfig.getInfoUpdateInterval();
        this.taskInfoRefreshMaxWait = taskConfig.getInfoRefreshMaxWait();
        this.coreExecutor = newCachedThreadPool(daemonThreadsNamed("remote-task-callback-%s"));
        this.executor = new BoundedExecutor(coreExecutor, config.getRemoteTaskMaxCallbackThreads());
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) coreExecutor);
        this.stats = requireNonNull(stats, "stats is null");
        isBinaryTransportEnabled = requireNonNull(communicationConfig, "communicationConfig is null").isBinaryTransportEnabled();

        if (isBinaryTransportEnabled) {
            this.taskStatusCodec = taskStatusSmileCodec;
            this.taskInfoCodec = taskInfoSmileCodec;
            this.taskUpdateRequestCodec = taskUpdateRequestSmileCodec;
        }
        else {
            this.taskStatusCodec = wrapJsonCodec(taskStatusJsonCodec);
            this.taskInfoCodec = wrapJsonCodec(taskInfoJsonCodec);
            this.taskUpdateRequestCodec = wrapJsonCodec(taskUpdateRequestJsonCodec);
        }

        this.updateScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("task-info-update-scheduler-%s"));
        this.errorScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("remote-task-error-delay-%s"));
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @PreDestroy
    public void stop()
    {
        coreExecutor.shutdownNow();
        updateScheduledExecutor.shutdownNow();
        errorScheduledExecutor.shutdownNow();
    }

    @Override
    public RemoteTask createRemoteTask(Session session,
            TaskId taskId,
            InternalNode node,
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OptionalInt totalPartitions,
            OutputBuffers outputBuffers,
            PartitionedSplitCountTracker partitionedSplitCountTracker,
            boolean summarizeTaskInfo)
    {
        return new HttpRemoteTask(session,
                taskId,
                node.getNodeIdentifier(),
                locationFactory.createTaskLocation(node, taskId),
                fragment,
                initialSplits,
                totalPartitions,
                outputBuffers,
                httpClient,
                executor,
                updateScheduledExecutor,
                errorScheduledExecutor,
                maxErrorDuration,
                taskStatusRefreshMaxWait,
                taskInfoRefreshMaxWait,
                taskInfoUpdateInterval,
                summarizeTaskInfo,
                taskStatusCodec,
                taskInfoCodec,
                taskUpdateRequestCodec,
                partitionedSplitCountTracker,
                stats,
                isBinaryTransportEnabled);
    }
}
