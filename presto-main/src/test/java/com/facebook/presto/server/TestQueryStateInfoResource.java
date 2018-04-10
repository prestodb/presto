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

import com.facebook.presto.client.QueryResults;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.testing.Closeables.closeQuietly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestQueryStateInfoResource
{
    private TestingPrestoServer server;
    private HttpClient client;
    private List<QueryResults> queryResults;

    TestQueryStateInfoResource()
            throws Exception
    {
        server = new TestingPrestoServer();
        client = new JettyHttpClient();
    }

    @BeforeClass
    public void setup()
    {
        ImmutableList.Builder<QueryResults> builder = ImmutableList.builder();
        builder.add(startQuery("user1", server, client));
        builder.add(startQuery("user2", server, client));
        queryResults = builder.build();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        // gracefully drain the output and shutdown the server
        for (QueryResults results : queryResults) {
            while (results.getNextUri() != null) {
                results = client.execute(prepareGet().setUri(results.getNextUri()).build(), createJsonResponseHandler(jsonCodec(QueryResults.class)));
            }
        }
        closeQuietly(server);
        closeQuietly(client);
    }

    @Test
    public void testGetAllQueryStateInfos()
    {
        List<QueryStateInfo> infos = client.execute(
                prepareGet().setUri(server.resolve("/v1/queryState")).build(),
                createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));

        assertEquals(infos.size(), 2);
    }

    @Test
    public void testGetQueryStateInfosForUser()
    {
        List<QueryStateInfo> infos = client.execute(
                prepareGet().setUri(server.resolve("/v1/queryState?user=user2")).build(),
                createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));

        assertEquals(infos.size(), 1);
    }

    @Test
    public void testGetQueryStateInfosForUserNoResult()
    {
        List<QueryStateInfo> infos = client.execute(
                prepareGet().setUri(server.resolve("/v1/queryState?user=user3")).build(),
                createJsonResponseHandler(listJsonCodec(QueryStateInfo.class)));

        assertTrue(infos.isEmpty());
    }

    @Test
    public void testGetQueryStateInfo()
    {
        QueryStateInfo info = client.execute(
                prepareGet().setUri(server.resolve("/v1/queryState/" + queryResults.get(0).getId())).build(),
                createJsonResponseHandler(jsonCodec(QueryStateInfo.class)));

        assertNotNull(info);
    }

    @Test(expectedExceptions = {UnexpectedResponseException.class}, expectedExceptionsMessageRegExp = ".*404: Not Found")
    public void testGetQueryStateInfoNo()
    {
        client.execute(
                prepareGet().setUri(server.resolve("/v1/queryState/123")).build(),
                createJsonResponseHandler(jsonCodec(QueryStateInfo.class)));
    }

    private static QueryResults startQuery(String user, TestingPrestoServer server, HttpClient client)
    {
        // Send a query that can produce some result that is more than 1 page.
        // Stop pulling data once the first page is received so that the query is in running state.
        QueryResults queryResults;
        Request request = preparePost()
                .setUri(HttpUriBuilder.uriBuilderFrom(server.getBaseUrl()).replacePath("/v1/statement").build())
                .setBodyGenerator(createStaticBodyGenerator("select * from (select repeat(coordinator, 4000) as array from system.runtime.nodes) cross join unnest(array)", UTF_8))
                .setHeader(PRESTO_USER, user)
                .setHeader(PRESTO_MAX_SIZE, "1B")
                .build();
        queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));

        while (queryResults.getStats().getState().equals(QueryState.QUEUED.name()) && queryResults.getNextUri() != null) {
            request = prepareGet().setUri(queryResults.getNextUri()).build();
            queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        }
        return queryResults;
    }
}
