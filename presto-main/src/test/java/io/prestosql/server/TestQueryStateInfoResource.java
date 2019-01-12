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
package io.prestosql.server;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.prestosql.client.QueryResults;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.server.testing.TestingPrestoServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.testing.Closeables.closeQuietly;
import static io.prestosql.client.PrestoHeaders.PRESTO_USER;
import static io.prestosql.execution.QueryState.RUNNING;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestQueryStateInfoResource
{
    private static final String LONG_LASTING_QUERY = "SELECT * FROM tpch.sf1.lineitem";
    private static final JsonCodec<QueryResults> QUERY_RESULTS_JSON_CODEC = jsonCodec(QueryResults.class);

    private TestingPrestoServer server;
    private HttpClient client;
    private QueryResults queryResults;

    TestQueryStateInfoResource()
            throws Exception
    {
        server = new TestingPrestoServer();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        client = new JettyHttpClient();
    }

    @BeforeClass
    public void setup()
    {
        Request request1 = preparePost()
                .setUri(uriBuilderFrom(server.getBaseUrl()).replacePath("/v1/statement").build())
                .setBodyGenerator(createStaticBodyGenerator(LONG_LASTING_QUERY, UTF_8))
                .setHeader(PRESTO_USER, "user1")
                .build();
        queryResults = client.execute(request1, createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));
        client.execute(prepareGet().setUri(queryResults.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));

        Request request2 = preparePost()
                .setUri(uriBuilderFrom(server.getBaseUrl()).replacePath("/v1/statement").build())
                .setBodyGenerator(createStaticBodyGenerator(LONG_LASTING_QUERY, UTF_8))
                .setHeader(PRESTO_USER, "user2")
                .build();
        QueryResults queryResults2 = client.execute(request2, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        client.execute(prepareGet().setUri(queryResults2.getNextUri()).build(), createJsonResponseHandler(QUERY_RESULTS_JSON_CODEC));

        // queries are started in the background, so they may not all be immediately visible
        while (true) {
            List<BasicQueryInfo> queryInfos = client.execute(
                    prepareGet().setUri(uriBuilderFrom(server.getBaseUrl()).replacePath("/v1/query").build()).build(),
                    createJsonResponseHandler(listJsonCodec(BasicQueryInfo.class)));
            if ((queryInfos.size() == 2) && queryInfos.stream().allMatch(info -> info.getState() == RUNNING)) {
                break;
            }
        }
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
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
                prepareGet().setUri(server.resolve("/v1/queryState/" + queryResults.getId())).build(),
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
}
