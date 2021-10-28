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
package com.facebook.presto.utils;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.QueryStateInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;

import java.net.URI;
import java.util.List;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class QueryExecutionClientUtil
{
    public static final String DEFAULT_TEST_USER = "user";

    private QueryExecutionClientUtil()
    {
    }

    private static QueryResults postQuery(HttpClient client, String sql, URI uri, String user)
    {
        Request request = preparePost()
                .setHeader(PRESTO_USER, user)
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .build();
        return client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
    }

    public static void runToCompletion(HttpClient client, TestingPrestoServer server, String sql)
    {
        runToCompletion(client, server, sql, DEFAULT_TEST_USER);
    }

    public static void runToCompletion(HttpClient client, TestingPrestoServer server, String sql, String user)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(client, sql, uri, user);
        while (queryResults.getNextUri() != null) {
            queryResults = getQueryResults(client, queryResults, user);
        }
    }

    public static void runToFirstResult(HttpClient client, TestingPrestoServer server, String sql)
    {
        runToFirstResult(client, server, sql, DEFAULT_TEST_USER);
    }

    public static void runToFirstResult(HttpClient client, TestingPrestoServer server, String sql, String user)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(client, sql, uri, user);
        while (queryResults.getData() == null) {
            queryResults = getQueryResults(client, queryResults, user);
        }
    }

    public static void runToQueued(HttpClient client, TestingPrestoServer server, String sql)
    {
        runToQueued(client, server, sql, DEFAULT_TEST_USER);
    }

    public static void runToQueued(HttpClient client, TestingPrestoServer server, String sql, String user)
    {
        runToState(client, server, sql, QUEUED, user);
    }

    public static void runToExecuting(HttpClient client, TestingPrestoServer server, String sql)
    {
        runToState(client, server, sql, RUNNING, DEFAULT_TEST_USER);
    }

    private static void runToState(HttpClient client, TestingPrestoServer server, String sql, QueryState queryState, String user)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(client, sql, uri, user);
        while (!queryState.toString().equals(queryResults.getStats().getState()) &&
                QueryState.valueOf(queryResults.getStats().getState()).getValue() < queryState.getValue()) {
            queryResults = getQueryResults(client, queryResults, user);
        }
        //throw exception if input state cannot be reached
        if (QueryState.valueOf(queryResults.getStats().getState()).getValue() > queryState.getValue()) {
            throw new IllegalArgumentException("Failed to run to state " + queryState + ", current query state =" + queryResults.getStats().getState());
        }
        getQueryResults(client, queryResults, user);
    }

    public static QueryResults getQueryResults(HttpClient client, QueryResults queryResults, String user)
    {
        requireNonNull(queryResults.getNextUri(), "uri is null, query state : " + queryResults.getStats().getState());
        Request request = prepareGet()
                .setHeader(PRESTO_USER, user)
                .setUri(queryResults.getNextUri())
                .build();
        queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        return queryResults;
    }

    public static List<BasicQueryInfo> getQueryInfos(HttpClient client, TestingPrestoServer server, String path)
    {
        Request request = prepareGet().setUri(server.resolve(path)).build();
        return client.execute(request, createJsonResponseHandler(listJsonCodec(BasicQueryInfo.class)));
    }

    public static List<QueryStateInfo> getQueryStateInfos(HttpClient client, TestingPrestoServer server, String path)
    {
        Request request = prepareGet().setUri(server.resolve(path)).build();
        return client.execute(request, createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));
    }

    public static <T> T getResponseEntity(HttpClient client, TestingPrestoServer server, String path, JsonCodec<T> codec)
    {
        Request request = prepareGet().setUri(server.resolve(path)).build();
        return client.execute(request, createJsonResponseHandler(codec));
    }

    public static QueryStateInfo getQueryStateInfo(HttpClient client, TestingPrestoServer server, String path)
    {
        Request request = prepareGet().setUri(server.resolve(path)).build();
        return client.execute(request, createJsonResponseHandler(jsonCodec(QueryStateInfo.class)));
    }
}
