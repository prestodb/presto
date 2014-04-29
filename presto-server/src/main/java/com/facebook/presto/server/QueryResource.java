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

import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.TimeZoneNotSupportedException;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
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
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.TimeZone;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_LANGUAGE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.spi.Session.DEFAULT_CATALOG;
import static com.facebook.presto.spi.Session.DEFAULT_SCHEMA;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.net.HttpHeaders.USER_AGENT;
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
    public List<BasicQueryInfo> getAllQueryInfo()
    {
        return extractBasicQueryInfo(queryManager.getAllQueryInfo());
    }

    private List<BasicQueryInfo> extractBasicQueryInfo(List<QueryInfo> allQueryInfo)
    {
        ImmutableList.Builder<BasicQueryInfo> basicQueryInfo = ImmutableList.builder();
        for (QueryInfo queryInfo : allQueryInfo) {
            basicQueryInfo.add(new BasicQueryInfo(queryInfo));
        }
        return basicQueryInfo.build();
    }

    @GET
    @Path("{queryId}")
    public Response getQueryInfo(@PathParam("queryId") QueryId queryId)
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
    public Response createQuery(String query,
            @HeaderParam(PRESTO_USER) String user,
            @HeaderParam(PRESTO_SOURCE) String source,
            @HeaderParam(PRESTO_CATALOG) @DefaultValue(DEFAULT_CATALOG) String catalog,
            @HeaderParam(PRESTO_SCHEMA) @DefaultValue(DEFAULT_SCHEMA) String schema,
            @HeaderParam(PRESTO_TIME_ZONE) String timeZoneId,
            @HeaderParam(PRESTO_LANGUAGE) String language,
            @HeaderParam(USER_AGENT) String userAgent,
            @Context HttpServletRequest requestContext,
            @Context UriInfo uriInfo)
    {
        checkNotNull(query, "query is null");
        checkArgument(!query.isEmpty(), "query is empty");
        checkNotNull(catalog, "catalog is null");
        checkNotNull(schema, "schema is null");

        if (timeZoneId == null) {
            timeZoneId = TimeZone.getDefault().getID();
        }

        Locale locale = Locale.getDefault();
        if (language != null) {
            locale = Locale.forLanguageTag(language);
        }

        String remoteUserAddress = requestContext.getRemoteAddr();

        Session session;
        try {
            session = new Session(user, source, catalog, schema, TimeZoneKey.getTimeZoneKey(timeZoneId), locale, remoteUserAddress, userAgent);
        }
        catch (TimeZoneNotSupportedException e) {
            return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
        }

        QueryInfo queryInfo = queryManager.createQuery(session, query);
        URI pagesUri = uriBuilderFrom(uriInfo.getRequestUri()).appendPath(queryInfo.getQueryId().toString()).build();
        return Response.created(pagesUri).entity(queryInfo).build();
    }

    @DELETE
    @Path("{queryId}")
    public void cancelQuery(@PathParam("queryId") QueryId queryId)
    {
        checkNotNull(queryId, "queryId is null");
        queryManager.cancelQuery(queryId);
    }

    @DELETE
    @Path("stage/{stageId}")
    public void cancelStage(@PathParam("stageId") StageId stageId)
    {
        checkNotNull(stageId, "stageId is null");
        queryManager.cancelStage(stageId);
    }
}
