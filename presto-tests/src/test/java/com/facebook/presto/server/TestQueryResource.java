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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestQueryResource
{
    private HttpClient client;
    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        client = new JettyHttpClient();
        DistributedQueryRunner runner = createQueryRunner(ImmutableMap.of("query.client.timeout", "10s"));
        server = runner.getCoordinator();
        server.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
        server.getResourceGroupManager().get()
                .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_simple.json")));
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
        closeQuietly(client);
        server = null;
        client = null;
    }

    @Test(timeOut = 60_000, enabled = false)
    public void testGetQueryInfos()
            throws Exception
    {
        runToCompletion("SELECT 1");
        runToCompletion("SELECT 2");
        runToCompletion("SELECT x FROM y");
        runToFirstResult("SELECT * from tpch.sf100.orders -- 1");
        runToFirstResult("SELECT * from tpch.sf100.orders -- 2");
        runToFirstResult("SELECT * from tpch.sf100.orders -- 3");
        runToQueued("SELECT 3");

        // Sleep to allow query to make some progress
        sleep(SECONDS.toMillis(5));

        List<BasicQueryInfo> infos = getQueryInfos("/v1/query");
        assertEquals(infos.size(), 7);
        assertStateCounts(infos, 2, 1, 3, 1);

        assertThatThrownBy(() -> getQueryInfos("/v1/query?limit=-1"))
                .hasMessageMatching(".*Bad Request.*");

        infos = getQueryInfos("/v1/query?limit=5");
        assertEquals(infos.size(), 5);
        assertEquals(infos.get(0).getQuery(), "SELECT * from tpch.sf100.orders -- 1");
        assertEquals(infos.get(1).getQuery(), "SELECT * from tpch.sf100.orders -- 2");
        assertEquals(infos.get(2).getQuery(), "SELECT * from tpch.sf100.orders -- 3");
        assertEquals(infos.get(3).getQuery(), "SELECT 3");
        assertEquals(infos.get(4).getQuery(), "SELECT x FROM y");
        assertStateCounts(infos, 0, 1, 3, 1);

        infos = getQueryInfos("/v1/query?state=finished");
        assertEquals(infos.size(), 2);
        assertStateCounts(infos, 2, 0, 0, 0);

        infos = getQueryInfos("/v1/query?state=failed");
        assertEquals(infos.size(), 1);
        assertStateCounts(infos, 0, 1, 0, 0);

        infos = getQueryInfos("/v1/query?state=running");
        assertEquals(infos.size(), 3);
        assertStateCounts(infos, 0, 0, 3, 0);

        infos = getQueryInfos("/v1/query?state=running&limit=2");
        assertEquals(infos.size(), 2);
        assertStateCounts(infos, 0, 0, 2, 0);

        infos = getQueryInfos("/v1/query?state=queued");
        assertEquals(infos.size(), 1);
        assertStateCounts(infos, 0, 0, 0, 1);

        // Sleep to trigger client query expiration
        sleep(SECONDS.toMillis(10));

        infos = getQueryInfos("/v1/query?state=failed");
        assertEquals(infos.size(), 5);
        assertStateCounts(infos, 0, 5, 0, 0);
    }

    private List<BasicQueryInfo> getQueryInfos(String path)
    {
        Request request = prepareGet().setUri(server.resolve(path)).build();
        return client.execute(request, createJsonResponseHandler(listJsonCodec(BasicQueryInfo.class)));
    }

    private void runToCompletion(String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(sql, uri);
        while (queryResults.getNextUri() != null) {
            queryResults = getQueryResults(queryResults);
        }
    }

    private void runToFirstResult(String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(sql, uri);
        while (queryResults.getData() == null) {
            queryResults = getQueryResults(queryResults);
        }
    }

    private void runToQueued(String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(sql, uri);
        while (!"QUEUED".equals(queryResults.getStats().getState())) {
            queryResults = getQueryResults(queryResults);
        }
        getQueryResults(queryResults);
    }

    private QueryResults postQuery(String sql, URI uri)
    {
        Request request = preparePost()
                .setHeader(PRESTO_USER, "user")
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .build();
        return client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
    }

    private QueryResults getQueryResults(QueryResults queryResults)
    {
        Request request = prepareGet()
                .setHeader(PRESTO_USER, "user")
                .setUri(queryResults.getNextUri())
                .build();
        queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        return queryResults;
    }

    private void assertStateCounts(List<BasicQueryInfo> infos, int expectedFinished, int expectedFailed, int expectedRunning, int expectedQueued)
    {
        int failed = 0;
        int finished = 0;
        int running = 0;
        int queued = 0;
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
                case QUEUED:
                    queued++;
                    break;
                default:
                    fail("Unexpected query state " + info.getState());
            }
        }
        assertEquals(failed, expectedFailed);
        assertEquals(finished, expectedFinished);
        assertEquals(running, expectedRunning);
        assertEquals(queued, expectedQueued);
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}
