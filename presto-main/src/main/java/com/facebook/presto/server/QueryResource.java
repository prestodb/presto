/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.facebook.presto.server.QueryState.State;
import com.google.common.base.Preconditions;
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
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

@Path("/v1/presto/query")
public class QueryResource
{
    private static final int DEFAULT_MAX_PAGE_COUNT = 10;
    private static final Duration DEFAULT_MAX_WAIT = new Duration(1, TimeUnit.SECONDS);
    private final QueryManager queryManager;

    @Inject
    public QueryResource(QueryManager queryManager)
    {
        this.queryManager = queryManager;
    }

    @GET
    public List<QueryInfo> getAllStatus()
            throws InterruptedException
    {
        return queryManager.getAllQueryInfo();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response create(String query, @Context UriInfo uriInfo)
    {
        QueryInfo queryInfo = queryManager.createQuery(query);
        URI pagesUri = uriBuilderFrom(uriInfo.getRequestUri()).appendPath(queryInfo.getQueryId()).build();
        return Response.created(pagesUri).entity(queryInfo).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response create(QueryFragmentRequest queryFragmentRequest, @Context UriInfo uriInfo)
    {
        QueryInfo queryInfo = queryManager.createQueryFragment(queryFragmentRequest.getSourceSplits(), queryFragmentRequest.getPlanFragment());
        URI pagesUri = uriBuilderFrom(uriInfo.getRequestUri()).appendPath(queryInfo.getQueryId()).build();
        return Response.created(pagesUri).entity(queryInfo).build();
    }

    @GET
    @Path("{operatorId: .+}")
    public Response getResults(@PathParam("operatorId") String operatorId)
            throws InterruptedException
    {
        Preconditions.checkNotNull(operatorId, "operatorId is null");

        List<Page> pages = queryManager.getQueryResults(operatorId, DEFAULT_MAX_PAGE_COUNT, DEFAULT_MAX_WAIT);
        if (pages.isEmpty()) {
            State queryStatus = queryManager.getQueryStatus(operatorId);
            if (queryStatus == State.PREPARING || queryStatus == State.RUNNING) {
                return Response.status(Status.NO_CONTENT).build();
            } else {
                return Response.status(Status.GONE).build();
            }
        }
        GenericEntity<?> entity = new GenericEntity<>(pages, new TypeToken<List<Page>>() {}.getType());
        return Response.ok(entity).build();
    }

    @GET
    @Path("{operatorId: .+}/info")
    public Response getQueryInfo(@PathParam("operatorId") String operatorId)
            throws InterruptedException
    {
        Preconditions.checkNotNull(operatorId, "operatorId is null");

        try {
            QueryInfo queryInfo = queryManager.getQueryInfo(operatorId);
            return Response.ok(queryInfo).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @DELETE
    @Path("{operatorId: .+}")
    public void destroy(@PathParam("operatorId") String operatorId)
    {
        queryManager.destroyQuery(operatorId);
    }
}
