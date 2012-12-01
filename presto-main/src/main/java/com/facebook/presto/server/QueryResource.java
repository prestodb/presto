/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

@Path("/v1/presto/query")
public class QueryResource
{
    private final QueryManager queryManager;

    @Inject
    public QueryResource(QueryManager queryManager)
    {
        this.queryManager = checkNotNull(queryManager, "queryManager is null");
    }

    @GET
    public List<QueryInfo> getAllQueryInfo()
            throws InterruptedException
    {
        return queryManager.getAllQueryInfo();
    }

    @GET
    @Path("{queryId}")
    public Response getQueryInfo(@PathParam("queryId") String queryId)
            throws InterruptedException
    {
        checkNotNull(queryId, "queryId is null");

        try {
            QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            return Response.ok(queryInfo).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response createQuery(String query, @Context UriInfo uriInfo)
    {
        checkNotNull(query, "query is null");

        QueryInfo queryInfo = queryManager.createQuery(query);
        URI pagesUri = uriBuilderFrom(uriInfo.getRequestUri()).appendPath(queryInfo.getQueryId()).build();
        return Response.created(pagesUri).entity(queryInfo).build();
    }

    @DELETE
    @Path("{queryId}")
    public void cancelQuery(@PathParam("queryId") String queryId)
    {
        checkNotNull(queryId, "queryId is null");
        queryManager.cancelQuery(queryId);
    }
}
