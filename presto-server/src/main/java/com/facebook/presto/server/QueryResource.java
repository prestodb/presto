/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.sql.analyzer.Session;
import com.google.common.base.Preconditions;
import com.google.common.net.HttpHeaders;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
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

import static com.facebook.presto.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_CATALOG;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_SCHEMA;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

/**
 * Manage queries scheduled on this node
 */
@Path("/v1/query")
public class QueryResource
{
    private static final String NO_CACHE = "no-cache";
    private final QueryManager queryManager;

    @Inject
    public QueryResource(QueryManager queryManager)
    {
        this.queryManager = checkNotNull(queryManager, "queryManager is null");
    }

    @GET
    public List<QueryInfo> getAllQueryInfo()
    {
        return queryManager.getAllQueryInfo();
    }

    @GET
    @Path("{queryId}")
    public Response getQueryInfo(@PathParam("queryId") String queryId, @HeaderParam(HttpHeaders.CACHE_CONTROL) String cacheControl)
    {
        checkNotNull(queryId, "queryId is null");

        try {
            QueryInfo queryInfo = queryManager.getQueryInfo(queryId, NO_CACHE.equals(cacheControl));
            return Response.ok(queryInfo).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response createQuery(String query,
            @HeaderParam(PRESTO_USER) String user,
            @HeaderParam(PRESTO_CATALOG) @DefaultValue(DEFAULT_CATALOG) String catalog,
            @HeaderParam(PRESTO_SCHEMA) @DefaultValue(DEFAULT_SCHEMA) String schema,
            @Context UriInfo uriInfo)
    {
        checkNotNull(query, "query is null");
        checkNotNull(catalog, "catalog is null");
        checkNotNull(schema, "schema is null");

        QueryInfo queryInfo = queryManager.createQuery(new Session(user, catalog, schema), query);
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

    @DELETE
    @Path("{queryId}/stage/{stageId}")
    public void cancelStage(@PathParam("queryId") String queryId, @PathParam("stageId") String stageId)
    {
        checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        queryManager.cancelStage(queryId, stageId);
    }
}
