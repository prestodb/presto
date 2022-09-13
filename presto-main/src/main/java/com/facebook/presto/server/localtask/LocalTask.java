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
package com.facebook.presto.server.localtask;

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.PageBufferInfo;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.server.SimpleHttpResponseCallback;
import com.facebook.presto.server.SimpleHttpResponseHandler;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.remotetask.RemoteTaskStats;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.presto.execution.TaskInfo.createInitialTask;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LocalTask
{
    private static final Logger log = Logger.get(LocalTask.class);
    private final Session session;
    private final TaskId taskId;
    private final String nodeId;
    private final PlanFragment planFragment;
    @GuardedBy("this")
    private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>();
    private final HttpClient httpClient;
    private final TableWriteInfo tableWriteInfo;
    private final RemoteTaskStats stats;
    private final List<TaskSource> sources;
    private final AtomicLong nextSplitId = new AtomicLong();
    private TaskInfo taskInfo;
    private final Executor executor;
    private final ScheduledExecutorService updateScheduledExecutor;
    @GuardedBy("this")
    private ScheduledFuture<?> scheduledFuture;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final JsonCodec<PlanFragment> planFragmentCodec;
    private final JsonCodec<TaskInfo> taskInfoCodec;

    public LocalTask(
            Session session,
            TaskId taskId,
            String nodeId,
            URI location,
            PlanFragment planFragment,
            List<TaskSource> sources,
            OutputBuffers outputBuffers,
            HttpClient httpClient,
            TableWriteInfo tableWriteInfo,
            RemoteTaskStats stats,
            Executor executor,
            ScheduledExecutorService updateScheduledExecutor,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
            JsonCodec<PlanFragment> planFragmentCodec,
            JsonCodec<TaskInfo> taskInfoCodec)
    {
        requireNonNull(session, "session is null");
        requireNonNull(taskId, "taskId is null");
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(location, "location is null");
        requireNonNull(planFragment, "planFragment is null");
        requireNonNull(outputBuffers, "outputBuffers is null");
        requireNonNull(httpClient, "httpClient is null");

        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            this.taskId = taskId;
            this.session = session;
            this.nodeId = nodeId;
            this.planFragment = planFragment;
            this.outputBuffers.set(outputBuffers);
            this.httpClient = httpClient;
            this.tableWriteInfo = tableWriteInfo;
            this.stats = stats;
            this.sources = sources;
            this.executor = executor;
            this.updateScheduledExecutor = updateScheduledExecutor;
            List<BufferInfo> bufferStates = outputBuffers.getBuffers()
                    .keySet().stream()
                    .map(outputId -> new BufferInfo(outputId, false, 0, 0, PageBufferInfo.empty()))
                    .collect(toImmutableList());
            this.taskInfo = createInitialTask(taskId, location, bufferStates, new TaskStats(DateTime.now(), null), nodeId);
            this.taskUpdateRequestCodec = taskUpdateRequestCodec;
            this.planFragmentCodec = planFragmentCodec;
            this.taskInfoCodec = taskInfoCodec;
        }
    }

    private List<TaskSource> getSources(Multimap<PlanNodeId, Split> splits)
    {
        Set<PlanNodeId> sourcePlanNodeIds = new HashSet<>();
        sourcePlanNodeIds.addAll(planFragment.getTableScanSchedulingOrder());
        sourcePlanNodeIds.addAll(planFragment.getRemoteSourceNodes().stream()
                .map(PlanNode::getId)
                .collect(toImmutableSet()));

        List<TaskSource> sources = new ArrayList<>();
        for (PlanNodeId planNodeId : sourcePlanNodeIds) {
            Set<ScheduledSplit> scheduledSplits = new HashSet<>();
            for (Split split : splits.get(planNodeId)) {
                scheduledSplits.add(new ScheduledSplit(nextSplitId.getAndIncrement(), planNodeId, split));
            }
            // TODO figure out meaning of Lifespan
            Set<Lifespan> noMoreSplitsForLifespan = ImmutableSet.copyOf(Arrays.asList(new Lifespan(false, 0)));
            sources.add(new TaskSource(planNodeId, scheduledSplits, noMoreSplitsForLifespan, false));
        }
        return sources;
    }

    public TaskInfo getTaskInfo()
    {
        return taskInfo;
    }

    public void start()
    {
        sendUpdate();
        scheduleGetTaskInfo();
    }

    private synchronized void sendUpdate()
    {
        Optional<byte[]> fragment = Optional.of(planFragment.toBytes(planFragmentCodec));
        Optional<TableWriteInfo> writeInfo = Optional.of(tableWriteInfo);
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                fragment,
                sources,
                outputBuffers.get(),
                writeInfo);
        byte[] taskUpdateRequestJson = taskUpdateRequestCodec.toBytes(updateRequest);

        HttpUriBuilder uriBuilder = uriBuilderFrom(taskInfo.getTaskStatus().getSelf());
        Request request = setContentTypeHeaders(false, preparePost())
                .setUri(uriBuilder.build())
                .setBodyGenerator(createStaticBodyGenerator(taskUpdateRequestJson))
                .build();

        ResponseHandler responseHandler = createAdaptingJsonResponseHandler(taskInfoCodec);
        ListenableFuture<BaseResponse<TaskInfo>> future = httpClient.executeAsync(request, responseHandler);
        // TODO add new error code instread of REMOTE_TASK_ERROR
        FutureCallback callback = new SimpleHttpResponseHandler<>(new UpdateResponseHandler(), request.getUri(), stats.getHttpResponseStats(), REMOTE_TASK_ERROR);

        Futures.addCallback(
                future,
                callback,
                executor);
    }

    private synchronized void scheduleGetTaskInfo()
    {
        scheduledFuture = updateScheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                HttpUriBuilder uriBuilder = uriBuilderFrom(taskInfo.getTaskStatus().getSelf());
                Request request = setContentTypeHeaders(false, prepareGet())
                        .setUri(uriBuilder.build())
                        .build();

                ResponseHandler responseHandler = createAdaptingJsonResponseHandler(taskInfoCodec);
                ListenableFuture<BaseResponse<TaskInfo>> future = httpClient.executeAsync(request, responseHandler);
                FutureCallback callback = new SimpleHttpResponseHandler<>(new TaskInfoHandler(), request.getUri(), stats.getHttpResponseStats(), REMOTE_TASK_ERROR);

                Futures.addCallback(
                        future,
                        callback,
                        executor);
            }
            catch (Throwable t) {
                throw t;
            }
        }, 0, 100, MILLISECONDS);
    }

    private synchronized void stop()
    {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
    }

    private class UpdateResponseHandler
            implements SimpleHttpResponseCallback<TaskInfo>
    {
        @Override
        public void success(TaskInfo value)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                log.error("success " + value.getTaskId());
                taskInfo = value;
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                log.error("failed " + cause);
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                log.error("fatal " + cause);
            }
        }
    }

    private class TaskInfoHandler
            implements SimpleHttpResponseCallback<TaskInfo>
    {
        @Override
        public void success(TaskInfo value)
        {
            try (SetThreadName ignored = new SetThreadName("TaskInfoHandler-%s", taskId)) {
                log.error("TaskInfoHandler success " + value.getTaskId());
                taskInfo = value;
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("TaskInfoHandler-%s", taskId)) {
                log.error("TaskInfoHandler failed " + cause);
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("TaskInfoHandler-%s", taskId)) {
                log.error("TaskInfoHandler fatal " + cause);
            }
        }
    }
}