/*
 * Copyright 2004-present Facebook. All Rights Reserved.
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
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.RateLimiter;
import io.airlift.log.Logger;
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
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_SEQUENCE_ID;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Manages tasks on this worker node
 */
@Path("/v1/task")
public class TaskResource
{
    private static final int DEFAULT_MAX_PAGE_COUNT = 10;
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
        try {
            checkNotNull(taskUpdateRequest, "taskUpdateRequest is null");

            TaskInfo taskInfo = taskManager.updateTask(taskUpdateRequest.getSession(),
                    taskId,
                    taskUpdateRequest.getFragment(),
                    taskUpdateRequest.getSources(),
                    taskUpdateRequest.getOutputIds());

            return Response.ok().entity(taskInfo).build();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
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

    private static final Logger log = Logger.get(TaskResource.class);
    private final RateLimiter limiter = RateLimiter.create(0.2);

    @GET
    @Path("{taskId}/results/{outputId}/{pageSequenceId}")
    @Produces(PrestoMediaTypes.PRESTO_PAGES)
    public Response getResults(@PathParam("taskId") TaskId taskId,
            @PathParam("outputId") String outputId,
            @PathParam("pageSequenceId") long pageSequenceId)
            throws InterruptedException
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(outputId, "outputId is null");

        // todo we need a much better way to determine if a task is unknown (e.g. not scheduled yet), done, or there is current no more data
        try {
            BufferResult result = taskManager.getTaskResults(taskId, outputId, pageSequenceId, DEFAULT_MAX_PAGE_COUNT, DEFAULT_MAX_WAIT_TIME);
            if (!result.isEmpty()) {
                GenericEntity<?> entity = new GenericEntity<>(result.getElements(), new TypeToken<List<Page>>() {}.getType());
                return Response.ok(entity).header(PRESTO_PAGE_SEQUENCE_ID, result.getStartingSequenceId()).build();
            }
            else if (result.isBufferClosed() || isDone(taskId)) {
                return Response.status(Status.GONE).header(PRESTO_PAGE_SEQUENCE_ID, result.getStartingSequenceId()).build();
            }
            else {
                return Response.status(Status.NO_CONTENT).header(PRESTO_PAGE_SEQUENCE_ID, result.getStartingSequenceId()).build();
            }
        }
        catch (NoSuchElementException | NoSuchBufferException ignored) {
            // task has not been scheduled or buffer has not been created yet
            // this is treated the same as no-data
            if (limiter.tryAcquire()) {
                log.debug(ignored, "Error getting results");
            }
        }

        // this is a safe race condition, because isDone will only be true if the task is failed or if all results have been consumed
        if (isDone(taskId)) {
            return Response.status(Status.GONE).header(PRESTO_PAGE_SEQUENCE_ID, pageSequenceId).build();
        }
        else {
            return Response.status(Status.NO_CONTENT).header(PRESTO_PAGE_SEQUENCE_ID, pageSequenceId).build();
        }
    }

    private boolean isDone(TaskId taskId)
    {
        try {
            return taskManager.getTaskInfo(taskId, false).getState().isDone();
        }
        catch (NoSuchElementException e) {
            return false;
        }
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
