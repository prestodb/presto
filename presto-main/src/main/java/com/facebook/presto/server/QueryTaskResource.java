/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
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
import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/v1/presto/task")
public class QueryTaskResource
{
    private static final int DEFAULT_MAX_PAGE_COUNT = 10;
    private static final Duration DEFAULT_MAX_WAIT_TIME = new Duration(1, SECONDS);

    private final QueryTaskManager queryTaskManager;

    @Inject
    public QueryTaskResource(QueryTaskManager queryTaskManager)
    {
        this.queryTaskManager = checkNotNull(queryTaskManager, "queryTaskManager is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<QueryTaskInfo> getAllQueryTaskInfo()
            throws InterruptedException
    {
        return queryTaskManager.getAllQueryTaskInfo();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response create(QueryFragmentRequest queryFragmentRequest, @Context UriInfo uriInfo)
    {
        checkNotNull(queryFragmentRequest, "queryFragmentRequest is null");

        QueryTask queryTask = queryTaskManager.createQueryTask(queryFragmentRequest.getFragment(), queryFragmentRequest.getFragmentSources());
        URI pagesUri = uriBuilderFrom(uriInfo.getRequestUri()).appendPath(queryTask.getTaskId()).build();
        return Response.created(pagesUri).entity(queryTask.getQueryTaskInfo()).build();
    }

    @GET
    @Path("{taskId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getQueryTaskInfo(@PathParam("taskId") String taskId)
    {
        checkNotNull(taskId, "taskId is null");

        QueryTaskInfo queryTaskInfo = queryTaskManager.getQueryTaskInfo(taskId);
        return Response.ok(queryTaskInfo).build();
    }

    @DELETE
    @Path("{taskId}")
    public void cancelQueryTask(@PathParam("taskId") String taskId)
    {
        checkNotNull(taskId, "taskId is null");

        queryTaskManager.cancelQueryTask(taskId);
    }

    @GET
    @Path("{taskId}/results/{outputId}")
    @Produces(PrestoMediaTypes.PRESTO_PAGES)
    public Response getResults(@PathParam("taskId") String taskId, @PathParam("outputId") String outputId)
            throws InterruptedException
    {
        checkNotNull(taskId, "taskId is null");
        checkNotNull(outputId, "outputId is null");

        List<Page> pages = queryTaskManager.getQueryTaskResults(taskId, outputId, DEFAULT_MAX_PAGE_COUNT, DEFAULT_MAX_WAIT_TIME);
        if (pages.isEmpty()) {
            return Response.status(Status.GONE).build();
        }
        GenericEntity<?> entity = new GenericEntity<>(pages, new TypeToken<List<Page>>() {}.getType());
        return Response.ok(entity).build();
    }
}
