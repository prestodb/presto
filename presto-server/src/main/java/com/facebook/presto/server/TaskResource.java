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

import com.facebook.presto.PrestoMediaTypes;
import com.facebook.presto.execution.BufferResult;
import com.facebook.presto.execution.NoSuchBufferException;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.operator.Page;
import com.google.common.reflect.TypeToken;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import java.util.List;
import java.util.NoSuchElementException;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Manages tasks on this worker node
 */
@Path("/v1/task")
public class TaskResource
{
    private static final DataSize DEFAULT_MAX_SIZE = new DataSize(10, Unit.MEGABYTE);
    private static final Duration DEFAULT_MAX_WAIT_TIME = new Duration(1, SECONDS);

    private final TaskManager taskManager;

    @Inject
    public TaskResource(TaskManager taskManager)
    {
        this.taskManager = checkNotNull(taskManager, "taskManager is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<TaskInfo> getAllTaskInfo(@Context() UriInfo uriInfo)
    {
        return taskManager.getAllTaskInfo(isFullTaskInfoRequested(uriInfo));
    }

    @POST
    @Path("{taskId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createOrUpdateTask(@PathParam("taskId") TaskId taskId, TaskUpdateRequest taskUpdateRequest, @Context UriInfo uriInfo)
    {
        checkNotNull(taskUpdateRequest, "taskUpdateRequest is null");

        TaskInfo taskInfo = taskManager.updateTask(taskUpdateRequest.getSession(),
                taskId,
                taskUpdateRequest.getFragment(),
                taskUpdateRequest.getSources(),
                taskUpdateRequest.getOutputIds());

        return Response.ok().entity(taskInfo).build();
    }

    @GET
    @Path("{taskId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTaskInfo(@PathParam("taskId") TaskId taskId,
            @HeaderParam(PRESTO_CURRENT_STATE) TaskState currentState,
            @HeaderParam(PRESTO_MAX_WAIT) Duration maxWait,
            @Context() UriInfo uriInfo)
            throws InterruptedException
    {
        checkNotNull(taskId, "taskId is null");

        if (maxWait != null) {
            taskManager.waitForStateChange(taskId, currentState, maxWait);
        }

        try {
            TaskInfo taskInfo = taskManager.getTaskInfo(taskId, isFullTaskInfoRequested(uriInfo));
            return Response.ok(taskInfo).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @DELETE
    @Path("{taskId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelTask(@PathParam("taskId") TaskId taskId)
    {
        checkNotNull(taskId, "taskId is null");

        try {
            TaskInfo taskInfo = taskManager.cancelTask(taskId);
            if (taskInfo != null) {
                return Response.ok(taskInfo).build();
            }
        }
        catch (NoSuchElementException ignored) {
        }
        return Response.status(Status.NOT_FOUND).build();
    }

    @GET
    @Path("{taskId}/results/{outputId}/{token}")
    @Produces(PrestoMediaTypes.PRESTO_PAGES)
    public Response getResults(@PathParam("taskId") TaskId taskId,
            @PathParam("outputId") String outputId,
            @PathParam("token") long token)
            throws InterruptedException
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(outputId, "outputId is null");

        long remainingNanos = DEFAULT_MAX_WAIT_TIME.roundTo(NANOSECONDS);
        long start = System.nanoTime();
        long end = start + remainingNanos;
        int maxSleepMillis = 1;

        while (remainingNanos > 0) {
            // todo we need a much better way to determine if a task is unknown (e.g. not scheduled yet), done, or there is current no more data
            try {
                BufferResult result = taskManager.getTaskResults(taskId, outputId, token, DEFAULT_MAX_SIZE, new Duration(remainingNanos, NANOSECONDS));
                List<Page> pages = result.getPages();

                if (!pages.isEmpty()) {
                    GenericEntity<?> entity = new GenericEntity<>(pages, new TypeToken<List<Page>>() {}.getType());
                    return Response.ok(entity)
                            .header(PRESTO_PAGE_TOKEN, result.getToken())
                            .header(PRESTO_PAGE_NEXT_TOKEN, result.getNextToken())
                            .build();
                }
                else if (result.isBufferClosed()) {
                    return Response.status(Status.GONE)
                            .header(PRESTO_PAGE_TOKEN, result.getToken())
                            .header(PRESTO_PAGE_NEXT_TOKEN, result.getNextToken())
                            .build();
                }
                else {
                    return Response.status(Status.NO_CONTENT)
                            .header(PRESTO_PAGE_TOKEN, result.getToken())
                            .header(PRESTO_PAGE_NEXT_TOKEN, result.getNextToken())
                            .build();
                }
            }
            catch (NoSuchElementException | NoSuchBufferException ignored) {
            }

            // task hasn't been scheduled yet.
            // sleep for a bit, before retrying
            NANOSECONDS.sleep(Math.min(remainingNanos, MILLISECONDS.toNanos(maxSleepMillis)));
            remainingNanos = end - System.nanoTime();
            maxSleepMillis *= 2;
        }

        // task doesn't exist yet and wait time has expired
        return Response.status(Status.NO_CONTENT)
                .header(PRESTO_PAGE_TOKEN, token)
                .header(PRESTO_PAGE_NEXT_TOKEN, token)
                .build();
    }

    @DELETE
    @Path("{taskId}/results/{outputId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response abortResults(@PathParam("taskId") TaskId taskId, @PathParam("outputId") String outputId)
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(outputId, "outputId is null");

        try {
            TaskInfo taskInfo = taskManager.abortTaskResults(taskId, outputId);
            return Response.ok(taskInfo).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.NOT_FOUND).build();
        }
    }

    private boolean isFullTaskInfoRequested(UriInfo uriInfo)
    {
        return uriInfo.getQueryParameters().containsKey("full");
    }
}
