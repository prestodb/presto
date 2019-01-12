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
import io.airlift.http.client.jetty.JettyHttpClient;
import io.prestosql.client.QueryResults;
import io.prestosql.server.testing.TestingPrestoServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
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
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestQueryResource
{
    private final HttpClient client = new JettyHttpClient();
    private TestingPrestoServer server;

    public TestQueryResource()
            throws Exception
    {
        server = new TestingPrestoServer();
    }

    @BeforeClass
    public void setup()
    {
        runToCompletion("SELECT 1");
        runToCompletion("SELECT 2");
        runToCompletion("SELECT x FROM y");
    }

    private void runToCompletion(String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        Request request = preparePost()
                .setHeader(PRESTO_USER, "user")
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .build();
        QueryResults queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        while (queryResults.getNextUri() != null) {
            request = prepareGet()
                    .setHeader(PRESTO_USER, "user")
                    .setUri(queryResults.getNextUri())
                    .build();
            queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        }
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
        closeQuietly(client);
    }

    @Test
    public void testGetQueryInfos()
    {
        List<BasicQueryInfo> infos = getQueryInfos("/v1/query");
        assertEquals(infos.size(), 3);
        assertStateCounts(infos, 2, 1, 0);

        infos = getQueryInfos("/v1/query?state=finished");
        assertEquals(infos.size(), 2);
        assertStateCounts(infos, 2, 0, 0);

        infos = getQueryInfos("/v1/query?state=failed");
        assertEquals(infos.size(), 1);
        assertStateCounts(infos, 0, 1, 0);

        infos = getQueryInfos("/v1/query?state=running");
        assertEquals(infos.size(), 0);
        assertStateCounts(infos, 0, 0, 0);
    }

    private List<BasicQueryInfo> getQueryInfos(String path)
    {
        Request request = prepareGet().setUri(server.resolve(path)).build();
        return client.execute(request, createJsonResponseHandler(listJsonCodec(BasicQueryInfo.class)));
    }

    private void assertStateCounts(List<BasicQueryInfo> infos, int expectedFinished, int expectedFailed, int expectedRunning)
    {
        int failed = 0;
        int finished = 0;
        int running = 0;
        for (BasicQueryInfo info : infos) {
            switch (info.getState()) {
                case FINISHED:
                    finished++;
                    break;
                case FAILED:
                    failed++;
                    break;
                case RUNNING:
                    running++;
                    break;
                default:
                    fail("Unexpected query state " + info.getState());
            }
        }
        assertEquals(failed, expectedFailed);
        assertEquals(finished, expectedFinished);
        assertEquals(running, expectedRunning);
    }
}
