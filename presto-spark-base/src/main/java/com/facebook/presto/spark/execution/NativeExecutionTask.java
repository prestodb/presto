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

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.PageBufferInfo;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.server.SimpleHttpResponseCallback;
import com.facebook.presto.server.SimpleHttpResponseHandler;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.remotetask.RemoteTaskStats;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.presto.execution.TaskInfo.createInitialTask;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_TASK_ERROR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * NativeExecutionTask provide the abstraction of executing tasks via c++ worker.
 * The plan is to use it for native execution of Presto on Spark.
 * The plan and splits is provided at creation of NativeExecutionTask, it doesn't
 * support adding more splits during execution.
 */
public class NativeExecutionTask
{
    private static final Logger log = Logger.get(NativeExecutionTask.class);
    private static final int GET_TASK_INFO_INTERVALS = 100;
    private static final String LOCAL_URI = "http://127.0.0.1/v1/task/";

    private final Session session;
    private final TaskId taskId;
    private final PlanFragment planFragment;
    private final OutputBuffers outputBuffers;
    private final HttpClient httpClient;
    private final TableWriteInfo tableWriteInfo;
    private final RemoteTaskStats stats;
    private final List<TaskSource> sources;
    private final Executor executor;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final JsonCodec<PlanFragment> planFragmentCodec;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final AtomicReference<TaskInfo> taskInfo = new AtomicReference<>();

    @GuardedBy("this")
    private ScheduledFuture<?> scheduledFuture;

    public NativeExecutionTask(
            Session session,
            TaskId taskId,
            PlanFragment planFragment,
            List<TaskSource> sources,
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
        requireNonNull(planFragment, "planFragment is null");
        requireNonNull(sources, "sources is null");
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        requireNonNull(stats, "stats is null");
        requireNonNull(executor, "executor is null");
        requireNonNull(updateScheduledExecutor, "updateScheduledExecutor is null");
        requireNonNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");
        requireNonNull(planFragmentCodec, "planFragmentCodec is null");
        requireNonNull(taskInfoCodec, "taskInfoCodec is null");

        try (SetThreadName ignored = new SetThreadName("NativeExecutionTask-%s", taskId)) {
            this.taskId = taskId;
            this.session = session;
            this.planFragment = planFragment;
            this.httpClient = httpClient;
            this.tableWriteInfo = tableWriteInfo;
            this.stats = stats;
            this.sources = sources;
            this.executor = executor;
            this.updateScheduledExecutor = updateScheduledExecutor;
            this.outputBuffers = createInitialEmptyOutputBuffers(PARTITIONED);
            List<BufferInfo> bufferStates = outputBuffers.getBuffers()
                    .keySet().stream()
                    .map(outputId -> new BufferInfo(outputId, false, 0, 0, PageBufferInfo.empty()))
                    .collect(toImmutableList());
            this.taskInfo.set(createInitialTask(taskId, URI.create(LOCAL_URI + taskId), bufferStates, new TaskStats(DateTime.now(), null), ""));
            this.taskUpdateRequestCodec = taskUpdateRequestCodec;
            this.planFragmentCodec = planFragmentCodec;
            this.taskInfoCodec = taskInfoCodec;
        }
    }

    public TaskInfo getTaskInfo()
    {
        return taskInfo.get();
    }

    public void start()
    {
        sendUpdateRequest();
        scheduleGetTaskInfo();
    }

    public void stop()
    {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    private void sendUpdateRequest()
    {
        Optional<byte[]> fragment = Optional.of(planFragment.toBytes(planFragmentCodec));
        Optional<TableWriteInfo> writeInfo = Optional.of(tableWriteInfo);
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                fragment,
                sources,
                outputBuffers,
                writeInfo);
        byte[] taskUpdateRequestJson = taskUpdateRequestCodec.toBytes(updateRequest);

        HttpUriBuilder uriBuilder = uriBuilderFrom(taskInfo.get().getTaskStatus().getSelf());
        Request request = setContentTypeHeaders(false, preparePost())
                .setUri(uriBuilder.build())
                .setBodyGenerator(createStaticBodyGenerator(taskUpdateRequestJson))
                .build();

        ResponseHandler responseHandler = createAdaptingJsonResponseHandler(taskInfoCodec);
        ListenableFuture<BaseResponse<TaskInfo>> future = httpClient.executeAsync(request, responseHandler);
        FutureCallback callback = new SimpleHttpResponseHandler<>(new UpdateResponseHandler(), request.getUri(), stats.getHttpResponseStats(), NATIVE_EXECUTION_TASK_ERROR);

        Futures.addCallback(
                future,
                callback,
                executor);
    }

    private void scheduleGetTaskInfo()
    {
        scheduledFuture = updateScheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                HttpUriBuilder uriBuilder = uriBuilderFrom(taskInfo.get().getTaskStatus().getSelf());
                Request request = setContentTypeHeaders(false, prepareGet())
                        .setUri(uriBuilder.build())
                        .build();

                ResponseHandler responseHandler = createAdaptingJsonResponseHandler(taskInfoCodec);
                ListenableFuture<BaseResponse<TaskInfo>> future = httpClient.executeAsync(request, responseHandler);
                FutureCallback callback = new SimpleHttpResponseHandler<>(new TaskInfoHandler(), request.getUri(), stats.getHttpResponseStats(), NATIVE_EXECUTION_TASK_ERROR);

                Futures.addCallback(
                        future,
                        callback,
                        executor);
            }
            catch (Throwable t) {
                throw t;
            }
        }, 0, GET_TASK_INFO_INTERVALS, MILLISECONDS);
    }

    private class UpdateResponseHandler
            implements SimpleHttpResponseCallback<TaskInfo>
    {
        @Override
        public void success(TaskInfo value)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                log.debug("success " + value.getTaskId());
                taskInfo.set(value);
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
                log.debug("TaskInfoHandler success " + value.getTaskId());
                taskInfo.set(value);
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
