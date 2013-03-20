/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.PrestoMediaTypes;
import com.facebook.presto.execution.NoSuchBufferException;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.operator.Page;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
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
    public List<TaskInfo> getAllTaskInfo()
    {
        return taskManager.getAllTaskInfo();
    }

    @POST
    @Path("{taskId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateTask(@PathParam("taskId") String taskId, TaskUpdateRequest taskUpdateRequest, @Context UriInfo uriInfo)
    {
        try {
            checkNotNull(taskUpdateRequest, "taskUpdateRequest is null");

            TaskInfo taskInfo = taskManager.updateTask(taskUpdateRequest.getSession(),
                    taskUpdateRequest.getQueryId(),
                    taskUpdateRequest.getStageId(),
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
    public Response getTaskInfo(@PathParam("taskId") String taskId)
    {
        checkNotNull(taskId, "taskId is null");

        try {
            TaskInfo taskInfo = taskManager.getTaskInfo(taskId);
            return Response.ok(taskInfo).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @DELETE
    @Path("{taskId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelTask(@PathParam("taskId") String taskId)
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
    @Path("{taskId}/results/{outputId}")
    @Produces(PrestoMediaTypes.PRESTO_PAGES)
    public Response getResults(@PathParam("taskId") String taskId, @PathParam("outputId") String outputId)
            throws InterruptedException
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(outputId, "outputId is null");

        // todo we need a much better way to determine if a task is unknown (e.g. not scheduled yet), done, or there is current no more data
        try {
            List<Page> pages = taskManager.getTaskResults(taskId, outputId, DEFAULT_MAX_PAGE_COUNT, DEFAULT_MAX_WAIT_TIME);
            if (pages.isEmpty()) {
                // this is a safe race condition, because is done will only be true if the task is failed or if all results have been consumed
                // todo also return "GONE" if the specific buffer is finished
                if (isDone(taskId)) {
                    return Response.status(Status.GONE).build();
                }
                else {
                    return Response.status(Status.NO_CONTENT).build();
                }
            }
            GenericEntity<?> entity = new GenericEntity<>(pages, new TypeToken<List<Page>>() {}.getType());
            return Response.ok(entity).build();
        }
        catch (NoSuchBufferException e) {
            return Response.status(Status.NO_CONTENT).build();
        }
        catch (NoSuchElementException e) {
            if (isDone(taskId)) {
                return Response.status(Status.GONE).build();
            }
            else {
                return Response.status(Status.NO_CONTENT).build();
            }
        }
    }

    private boolean isDone(String taskId)
    {
        try {
            return taskManager.getTaskInfo(taskId).getState().isDone();
        }
        catch (NoSuchElementException e) {
            return false;
        }
    }

    @DELETE
    @Path("{taskId}/results/{outputId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response abortResults(@PathParam("taskId") String taskId, @PathParam("outputId") String outputId)
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
}
