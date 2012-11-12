/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import javax.inject.Inject;
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

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

@Path("/v1/presto/query")
public class QueryResource
{
    private static final int DEFAULT_MAX_PAGE_COUNT = 10;
    private final QueryManager queryManager;

    @Inject
    public QueryResource(QueryManager queryManager)
    {
        this.queryManager = queryManager;
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response create(String query, @Context UriInfo uriInfo)
    {
        QueryInfo queryInfo = queryManager.createQuery(query);
        URI pagesUri = uriBuilderFrom(uriInfo.getRequestUri()).appendPath(queryInfo.getQueryId()).build();
        return Response.created(pagesUri).entity(queryInfo).build();
    }

    @GET
    @Path("{operatorId: .+}")
    public Response getResults(@PathParam("operatorId") String operatorId)
            throws InterruptedException
    {
        Preconditions.checkNotNull(operatorId, "operatorId is null");

        List<Page> pages = queryManager.getQueryResults(operatorId, DEFAULT_MAX_PAGE_COUNT);
        if (pages.isEmpty()) {
            return Response.status(Status.GONE).build();
        }
        GenericEntity<?> entity = new GenericEntity<>(pages, new TypeToken<List<Page>>() {}.getType());
        return Response.ok(entity).build();
    }

    @DELETE
    @Path("{operatorId: .+}")
    public void destroy(@PathParam("operatorId") String operatorId)
    {
        queryManager.destroyQuery(operatorId);
    }
}
