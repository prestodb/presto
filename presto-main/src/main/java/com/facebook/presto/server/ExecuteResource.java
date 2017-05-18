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

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.AbstractIterator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.server.ResourceUtil.assertRequest;
import static com.facebook.presto.server.ResourceUtil.createClientSessionForRequest;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.transform;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.status;

@Path("/v1/execute")
public class ExecuteResource
{
    private final HttpServerInfo serverInfo;
    private final HttpClient httpClient;
    private final JsonCodec<QueryResults> queryResultsCodec;

    @Inject
    public ExecuteResource(
            HttpServerInfo serverInfo,
            @ForExecute HttpClient httpClient,
            JsonCodec<QueryResults> queryResultsCodec)
    {
        this.serverInfo = requireNonNull(serverInfo, "serverInfo is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.queryResultsCodec = requireNonNull(queryResultsCodec, "queryResultsCodec is null");
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response createQuery(String query, @Context HttpServletRequest servletRequest)
    {
        assertRequest(!isNullOrEmpty(query), "SQL query is empty");

        ClientSession clientSession = createClientSessionForRequest(servletRequest, serverUri(), new Duration(2, MINUTES));
        StatementClient client = new StatementClient(httpClient, queryResultsCodec, clientSession, query);

        List<Column> columns = getColumns(client);
        Iterator<List<Object>> iterator = flatten(new ResultsPageIterator(client));
        SimpleQueryResults results = new SimpleQueryResults(columns, iterator);

        return Response.ok(results, MediaType.APPLICATION_JSON_TYPE).build();
    }

    private URI serverUri()
    {
        checkState(serverInfo.getHttpUri() != null, "No HTTP URI for this server (HTTP disabled?)");
        return serverInfo.getHttpUri();
    }

    private static List<Column> getColumns(StatementClient client)
    {
        while (client.isValid()) {
            List<Column> columns = client.current().getColumns();
            if (columns != null) {
                return columns;
            }
            client.advance();
        }

        if (!client.isFailed()) {
            throw internalServerError("No columns");
        }
        throw internalServerError(failureMessage(client.finalResults()));
    }

    @SuppressWarnings("RedundantTypeArguments")
    private static <T> Iterator<T> flatten(Iterator<Iterable<T>> iterator)
    {
        // the explicit type argument is required by the Eclipse compiler
        return concat(transform(iterator, Iterable<T>::iterator));
    }

    private static class ResultsPageIterator
            extends AbstractIterator<Iterable<List<Object>>>
    {
        private final StatementClient client;

        private ResultsPageIterator(StatementClient client)
        {
            this.client = requireNonNull(client, "client is null");
        }

        @Override
        protected Iterable<List<Object>> computeNext()
        {
            while (client.isValid()) {
                Iterable<List<Object>> data = client.current().getData();
                client.advance();
                if (data != null) {
                    return data;
                }
            }

            if (client.isFailed()) {
                throw internalServerError(failureMessage(client.finalResults()));
            }

            return endOfData();
        }
    }

    private static WebApplicationException internalServerError(String message)
    {
        return new WebApplicationException(status(INTERNAL_SERVER_ERROR).entity(message).build());
    }

    private static String failureMessage(QueryResults results)
    {
        return format("Query failed (#%s): %s", results.getId(), results.getError().getMessage());
    }

    public static class SimpleQueryResults
    {
        private final List<Column> columns;
        private final Iterator<List<Object>> data;

        public SimpleQueryResults(List<Column> columns, Iterator<List<Object>> data)
        {
            this.columns = requireNonNull(columns, "columns is null");
            this.data = requireNonNull(data, "data is null");
        }

        @JsonProperty
        public List<Column> getColumns()
        {
            return columns;
        }

        @JsonProperty
        public Iterator<List<Object>> getData()
        {
            return data;
        }
    }
}
