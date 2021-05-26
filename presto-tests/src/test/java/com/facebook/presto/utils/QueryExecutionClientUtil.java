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
import com.facebook.presto.client.QueryResults;
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
import static java.nio.charset.StandardCharsets.UTF_8;

public class QueryExecutionClientUtil
{
    private QueryExecutionClientUtil()
    {
    }

    public static QueryResults postQuery(HttpClient client, String sql, URI uri)
    {
        Request request = preparePost()
                .setHeader(PRESTO_USER, "user")
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .build();
        return client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
    }

    public static void runToCompletion(HttpClient client, TestingPrestoServer server, String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(client, sql, uri);
        while (queryResults.getNextUri() != null) {
            queryResults = getQueryResults(client, queryResults);
        }
    }

    public static void runToFirstResult(HttpClient client, TestingPrestoServer server, String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(client, sql, uri);
        while (queryResults.getData() == null) {
            queryResults = getQueryResults(client, queryResults);
        }
    }

    public static void runToQueued(HttpClient client, TestingPrestoServer server, String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(client, sql, uri);
        while (!"QUEUED".equals(queryResults.getStats().getState())) {
            queryResults = getQueryResults(client, queryResults);
        }
        getQueryResults(client, queryResults);
    }

    public static QueryResults getQueryResults(HttpClient client, QueryResults queryResults)
    {
        Request request = prepareGet()
                .setHeader(PRESTO_USER, "user")
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

    public static QueryStateInfo getQueryStateInfo(HttpClient client, TestingPrestoServer server, String path)
    {
        Request request = prepareGet().setUri(server.resolve(path)).build();
        return client.execute(request, createJsonResponseHandler(jsonCodec(QueryStateInfo.class)));
    }
}
