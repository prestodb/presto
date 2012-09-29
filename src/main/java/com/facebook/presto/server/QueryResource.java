/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

@Path("/v1/presto/query")
public class QueryResource
{
    private final QueryManager queryManager;

    @Inject
    public QueryResource(QueryManager queryManager)
    {
        this.queryManager = queryManager;
    }

    @POST
    public Response create(String query, @Context UriInfo uriInfo)
    {
        String operatorId = queryManager.createQuery(query);
        URI blocksUri = uriBuilderFrom(uriInfo.getRequestUri()).appendPath(operatorId).build();
        return Response.created(blocksUri).build();
    }

    @GET
    @Path("{operatorId: .+}")
    public Response getResults(@PathParam("operatorId") String operatorId)
            throws InterruptedException
    {
        Preconditions.checkNotNull(operatorId, "operatorId is null");

        List<UncompressedBlock> blocks = queryManager.getQueryResults(operatorId, 10);
        if (blocks.isEmpty()) {
            return Response.status(Status.GONE).build();
        }
        GenericEntity<?> entity = new GenericEntity<>(blocks, new TypeToken<List<UncompressedBlock>>() {}.getType());
        return Response.ok(entity).build();
    }

    @DELETE
    @Path("{operatorId: .+}")
    public void destroy(@PathParam("operatorId") String operatorId)
    {
        queryManager.destroyQuery(operatorId);
    }
}
