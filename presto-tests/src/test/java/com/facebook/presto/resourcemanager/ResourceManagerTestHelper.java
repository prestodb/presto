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
package com.facebook.presto.resourcemanager;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.QueryStateInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;

import java.io.Closeable;
import java.io.IOException;
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

public class ResourceManagerTestHelper
        implements Closeable
{
    public static final String DEFAULT_TEST_USER = "user";
    private HttpClient client;

    public ResourceManagerTestHelper(HttpClient client)
    {
        this.client = client;
    }

    public String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    private QueryResults postQuery(String sql, URI uri, String user)
    {
        Request request = preparePost()
                .setHeader(PRESTO_USER, user)
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .build();
        return client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
    }

    public void runToCompletion(TestingPrestoServer server, String sql)
    {
        runToCompletion(server, sql, DEFAULT_TEST_USER);
    }

    public void runToCompletion(TestingPrestoServer server, String sql, String user)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(sql, uri, user);
        while (queryResults.getNextUri() != null) {
            queryResults = getQueryResults(queryResults, user);
        }
    }

    public void runToFirstResult(TestingPrestoServer server, String sql)
    {
        runToFirstResult(server, sql, DEFAULT_TEST_USER);
    }

    public void runToFirstResult(TestingPrestoServer server, String sql, String user)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(sql, uri, user);
        while (queryResults.getData() == null) {
            queryResults = getQueryResults(queryResults, user);
        }
    }

    public void runToQueued(TestingPrestoServer server, String sql)
    {
        runToQueued(server, sql, DEFAULT_TEST_USER);
    }

    public void runToQueued(TestingPrestoServer server, String sql, String user)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(sql, uri, user);
        while (!"QUEUED".equals(queryResults.getStats().getState())) {
            queryResults = getQueryResults(queryResults, user);
        }
        getQueryResults(queryResults, user);
    }

    private QueryResults getQueryResults(QueryResults queryResults, String user)
    {
        Request request = prepareGet()
                .setHeader(PRESTO_USER, user)
                .setUri(queryResults.getNextUri())
                .build();
        queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        return queryResults;
    }

    public List<BasicQueryInfo> getQueryInfos(TestingPrestoServer server, String path)
    {
        Request request = prepareGet().setUri(server.resolve(path)).build();
        return client.execute(request, createJsonResponseHandler(listJsonCodec(BasicQueryInfo.class)));
    }

    public List<QueryStateInfo> getQueryStateInfos(TestingPrestoServer server, String path)
    {
        Request request = prepareGet().setUri(server.resolve(path)).build();
        return client.execute(request, createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));
    }

    public <T> T getEntity(TestingPrestoServer server, String path, JsonCodec<T> codec)
    {
        Request request = prepareGet().setUri(server.resolve(path)).build();
        return client.execute(request, createJsonResponseHandler(codec));
    }

    public QueryStateInfo getQueryStateInfo(TestingPrestoServer server, String path)
    {
        Request request = prepareGet().setUri(server.resolve(path)).build();
        return client.execute(request, createJsonResponseHandler(jsonCodec(QueryStateInfo.class)));
    }

    @Override
    public void close()
            throws IOException
    {
        this.client.close();
    }
}
