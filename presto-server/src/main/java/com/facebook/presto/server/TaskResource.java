/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.PrestoMediaTypes;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.operator.Page;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
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

    @PUT
    @Path("{taskId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response create(@PathParam("taskId") String taskId, QueryFragmentRequest queryFragmentRequest, @Context UriInfo uriInfo)
    {
        try {
            checkNotNull(queryFragmentRequest, "queryFragmentRequest is null");

            TaskInfo taskInfo = taskManager.createTask(queryFragmentRequest.getSession(),
                    queryFragmentRequest.getQueryId(),
                    queryFragmentRequest.getStageId(),
                    taskId,
                    queryFragmentRequest.getFragment(),
                    queryFragmentRequest.getExchangeSources(),
                    queryFragmentRequest.getOutputIds());

            URI pagesUri = uriBuilderFrom(uriInfo.getRequestUri()).appendPath(taskInfo.getTaskId()).build();
            return Response.created(pagesUri).entity(taskInfo).build();
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
    public void cancelTask(@PathParam("taskId") String taskId)
    {
        checkNotNull(taskId, "taskId is null");

        taskManager.cancelTask(taskId);
    }

    @GET
    @Path("{taskId}/results/{outputId}")
    @Produces(PrestoMediaTypes.PRESTO_PAGES)
    public Response getResults(@PathParam("taskId") String taskId, @PathParam("outputId") String outputId)
            throws InterruptedException
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(outputId, "outputId is null");

        try {
            List<Page> pages = taskManager.getTaskResults(taskId, outputId, DEFAULT_MAX_PAGE_COUNT, DEFAULT_MAX_WAIT_TIME);
            if (pages.isEmpty()) {
                // this is a safe race condition, because is done will only be true if the task is failed or if all results have been consumed
                if (taskManager.getTaskInfo(taskId).getState().isDone()) {
                    return Response.status(Status.GONE).build();
                }
                else {
                    return Response.status(Status.NO_CONTENT).build();
                }
            }
            GenericEntity<?> entity = new GenericEntity<>(pages, new TypeToken<List<Page>>() {}.getType());
            return Response.ok(entity).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @DELETE
    @Path("{taskId}/results/{outputId}")
    @Produces(PrestoMediaTypes.PRESTO_PAGES)
    public Response abortResults(@PathParam("taskId") String taskId, @PathParam("outputId") String outputId)
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(outputId, "outputId is null");

        try {
            taskManager.abortTaskResults(taskId, outputId);
            return Response.noContent().build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @POST
    @Path("{taskId}/source/{sourceId}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void addSplit(@PathParam("taskId") String taskId, @PathParam("sourceId") String sourceId, Split split)
    {
        checkNotNull(split, "split is null");
        taskManager.addSplit(taskId, new PlanNodeId(sourceId), split);
    }

    @PUT
    @Path("{taskId}/source/{sourceId}/complete")
    @Consumes(MediaType.APPLICATION_JSON)
    public void noMoreSplits(@PathParam("taskId") String taskId, @PathParam("sourceId") String sourceId, boolean isComplete)
    {
        if (isComplete) {
            taskManager.noMoreSplits(taskId, sourceId);
        }
    }
}
