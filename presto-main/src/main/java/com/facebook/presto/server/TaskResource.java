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
package com.facebook.presto.server;

import com.facebook.presto.Session;
import com.facebook.presto.execution.BufferResult;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.stats.TimeStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.CompletionCallback;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Manages tasks on this worker node
 */
@Path("/v1/task")
public class TaskResource
{
    private static final Duration DEFAULT_MAX_WAIT_TIME = new Duration(2, SECONDS);

    private final TaskManager taskManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final TimeStat readFromOutputBufferTime = new TimeStat();
    private final TimeStat resultsRequestTime = new TimeStat();

    @Inject
    public TaskResource(TaskManager taskManager,
            SessionPropertyManager sessionPropertyManager,
            @ForAsyncHttp BoundedExecutor responseExecutor,
            @ForAsyncHttp ScheduledExecutorService timeoutExecutor)
    {
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<TaskInfo> getAllTaskInfo(@Context UriInfo uriInfo)
    {
        List<TaskInfo> allTaskInfo = taskManager.getAllTaskInfo();
        if (shouldSummarize(uriInfo)) {
            allTaskInfo = ImmutableList.copyOf(transform(allTaskInfo, TaskInfo::summarize));
        }
        return allTaskInfo;
    }

    @POST
    @Path("{taskId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createOrUpdateTask(@PathParam("taskId") TaskId taskId, TaskUpdateRequest taskUpdateRequest, @Context UriInfo uriInfo)
    {
        requireNonNull(taskUpdateRequest, "taskUpdateRequest is null");

        Session session = taskUpdateRequest.getSession().toSession(sessionPropertyManager);
        TaskInfo taskInfo = taskManager.updateTask(session,
                taskId,
                taskUpdateRequest.getFragment(),
                taskUpdateRequest.getSources(),
                taskUpdateRequest.getOutputIds());

        if (shouldSummarize(uriInfo)) {
            taskInfo = taskInfo.summarize();
        }

        return Response.ok().entity(taskInfo).build();
    }

    @GET
    @Path("{taskId}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getTaskInfo(@PathParam("taskId") final TaskId taskId,
            @HeaderParam(PRESTO_CURRENT_STATE) TaskState currentState,
            @HeaderParam(PRESTO_MAX_WAIT) Duration maxWait,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");

        if (currentState == null || maxWait == null) {
            TaskInfo taskInfo = taskManager.getTaskInfo(taskId);
            if (shouldSummarize(uriInfo)) {
                taskInfo = taskInfo.summarize();
            }
            asyncResponse.resume(taskInfo);
            return;
        }

        Duration waitTime = randomizeWaitTime(maxWait);
        CompletableFuture<TaskInfo> futureTaskInfo = addTimeout(
                taskManager.getTaskInfo(taskId, currentState),
                () -> taskManager.getTaskInfo(taskId),
                waitTime,
                timeoutExecutor);

        if (shouldSummarize(uriInfo)) {
            futureTaskInfo = futureTaskInfo.thenApply(TaskInfo::summarize);
        }

        // For hard timeout, add an additional 5 seconds to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + 5000, MILLISECONDS);
        bindAsyncResponse(asyncResponse, futureTaskInfo, responseExecutor)
                .withTimeout(timeout);
    }

    @GET
    @Path("{taskId}/status")
    @Produces(MediaType.APPLICATION_JSON)
    public void getTaskStatus(@PathParam("taskId") TaskId taskId,
            @HeaderParam(PRESTO_CURRENT_STATE) TaskState currentState,
            @HeaderParam(PRESTO_MAX_WAIT) Duration maxWait,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");

        if (currentState == null || maxWait == null) {
            TaskStatus taskStatus = taskManager.getTaskInfo(taskId).getTaskStatus();
            asyncResponse.resume(taskStatus);
            return;
        }

        Duration waitTime = randomizeWaitTime(maxWait);
        CompletableFuture<TaskStatus> futureTaskStatus = addTimeout(
                taskManager.getTaskStatus(taskId, currentState),
                () -> taskManager.getTaskStatus(taskId),
                waitTime,
                timeoutExecutor);

        // For hard timeout, add an additional 5 seconds to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + 5000, MILLISECONDS);
        bindAsyncResponse(asyncResponse, futureTaskStatus, responseExecutor)
                .withTimeout(timeout);
    }

    @DELETE
    @Path("{taskId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteTask(@PathParam("taskId") TaskId taskId,
            @QueryParam("abort") @DefaultValue("true") boolean abort,
            @Context UriInfo uriInfo)
    {
        requireNonNull(taskId, "taskId is null");

        TaskInfo taskInfo;
        if (abort) {
            taskInfo = taskManager.abortTask(taskId);
        }
        else {
            taskInfo = taskManager.cancelTask(taskId);
        }

        if (shouldSummarize(uriInfo)) {
            taskInfo = taskInfo.summarize();
        }
        return Response.ok(taskInfo).build();
    }

    @GET
    @Path("{taskId}/results/{outputId}/{token}")
    @Produces(PRESTO_PAGES)
    public void getResults(@PathParam("taskId") TaskId taskId,
            @PathParam("outputId") TaskId outputId,
            @PathParam("token") final long token,
            @HeaderParam(PRESTO_MAX_SIZE) DataSize maxSize,
            @Suspended AsyncResponse asyncResponse)
            throws InterruptedException
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(outputId, "outputId is null");

        long start = System.nanoTime();
        CompletableFuture<BufferResult> bufferResultFuture = taskManager.getTaskResults(taskId, outputId, token, maxSize);
        Duration waitTime = randomizeWaitTime(DEFAULT_MAX_WAIT_TIME);
        bufferResultFuture = addTimeout(
                bufferResultFuture,
                () -> BufferResult.emptyResults(taskManager.getTaskInstanceId(taskId), token, false),
                waitTime,
                timeoutExecutor);

        CompletableFuture<Response> responseFuture = bufferResultFuture.thenApply(result -> {
            List<Page> pages = result.getPages();

            GenericEntity<?> entity = null;
            Status status;
            if (pages.isEmpty()) {
                status = Status.NO_CONTENT;
            }
            else {
                entity = new GenericEntity<>(pages, new TypeToken<List<Page>>() {}.getType());
                status = Status.OK;
            }

            return Response.status(status)
                    .entity(entity)
                    .header(PRESTO_TASK_INSTANCE_ID, result.getTaskInstanceId())
                    .header(PRESTO_PAGE_TOKEN, result.getToken())
                    .header(PRESTO_PAGE_NEXT_TOKEN, result.getNextToken())
                    .header(PRESTO_BUFFER_COMPLETE, result.isBufferComplete())
                    .build();
        });

        // For hard timeout, add an additional 5 seconds to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + 5000, MILLISECONDS);
        bindAsyncResponse(asyncResponse, responseFuture, responseExecutor)
                .withTimeout(timeout,
                        Response.status(Status.NO_CONTENT)
                                .header(PRESTO_TASK_INSTANCE_ID, taskManager.getTaskInstanceId(taskId))
                                .header(PRESTO_PAGE_TOKEN, token)
                                .header(PRESTO_PAGE_NEXT_TOKEN, token)
                                .header(PRESTO_BUFFER_COMPLETE, false)
                                .build());

        responseFuture.whenComplete((response, exception) -> readFromOutputBufferTime.add(Duration.nanosSince(start)));
        asyncResponse.register((CompletionCallback) throwable -> resultsRequestTime.add(Duration.nanosSince(start)));
    }

    @DELETE
    @Path("{taskId}/results/{outputId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response abortResults(@PathParam("taskId") TaskId taskId, @PathParam("outputId") TaskId outputId, @Context UriInfo uriInfo)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(outputId, "outputId is null");

        TaskInfo taskInfo = taskManager.abortTaskResults(taskId, outputId);
        if (shouldSummarize(uriInfo)) {
            taskInfo = taskInfo.summarize();
        }
        return Response.ok(taskInfo).build();
    }

    @Managed
    @Nested
    public TimeStat getReadFromOutputBufferTime()
    {
        return readFromOutputBufferTime;
    }

    @Managed
    @Nested
    public TimeStat getResultsRequestTime()
    {
        return resultsRequestTime;
    }

    private static boolean shouldSummarize(UriInfo uriInfo)
    {
        return uriInfo.getQueryParameters().containsKey("summarize");
    }

    private static Duration randomizeWaitTime(Duration waitTime)
    {
        // Randomize in [T/2, T], so wait is not near zero and the client-supplied max wait time is respected
        long halfWaitMillis = waitTime.toMillis() / 2;
        return new Duration(halfWaitMillis + ThreadLocalRandom.current().nextLong(halfWaitMillis), MILLISECONDS);
    }
}
