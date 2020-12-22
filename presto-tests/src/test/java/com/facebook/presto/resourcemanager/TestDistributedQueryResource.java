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
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestDistributedQueryResource
{
    private HttpClient client;
    private TestingPrestoServer coordinator1;
    private TestingPrestoServer coordinator2;
    private TestingPrestoServer resourceManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        client = new JettyHttpClient();
        DistributedQueryRunner runner = createQueryRunner(ImmutableMap.of("query.client.timeout", "200s"), 2);
        coordinator1 = runner.getCoordinators().get(0);
        coordinator2 = runner.getCoordinators().get(1);
        Optional<TestingPrestoServer> resourceManager = runner.getResourceManager();
        checkState(resourceManager.isPresent(), "resource manager not present");
        this.resourceManager = resourceManager.get();
        coordinator1.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
        coordinator1.getResourceGroupManager().get()
                .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_simple.json")));
        coordinator2.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
        coordinator2.getResourceGroupManager().get()
                .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_simple.json")));
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(coordinator1);
        closeQuietly(coordinator2);
        closeQuietly(resourceManager);
        closeQuietly(client);
        coordinator1 = null;
        coordinator2 = null;
        resourceManager = null;
        client = null;
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    @Test(timeOut = 220_000)
    public void testGetQueryInfos()
            throws Exception
    {
        runToCompletion(coordinator1, "SELECT 1");
        runToCompletion(coordinator2, "SELECT 2");
        runToCompletion(coordinator1, "SELECT x FROM y");
        runToFirstResult(coordinator1, "SELECT * from tpch.sf100.orders");
        runToFirstResult(coordinator1, "SELECT * from tpch.sf101.orders");
        runToFirstResult(coordinator1, "SELECT * from tpch.sf102.orders");
        runToQueued(coordinator1, "SELECT 3");

        // Sleep to allow query to make some progress
        sleep(SECONDS.toMillis(1));

        List<BasicQueryInfo> infos = getQueryInfos(coordinator1, "/v1/query");
        assertEquals(infos.size(), 7);
        assertStateCounts(infos, 2, 1, 3, 1);

        infos = getQueryInfos(coordinator2, "/v1/query?state=finished");
        assertEquals(infos.size(), 2);
        assertStateCounts(infos, 2, 0, 0, 0);

        infos = getQueryInfos(coordinator1, "/v1/query?state=failed");
        assertEquals(infos.size(), 1);
        assertStateCounts(infos, 0, 1, 0, 0);

        infos = getQueryInfos(coordinator2, "/v1/query?state=running");
        assertEquals(infos.size(), 3);
        assertStateCounts(infos, 0, 0, 3, 0);

        infos = getQueryInfos(coordinator1, "/v1/query?state=queued");
        assertEquals(infos.size(), 1);
        assertStateCounts(infos, 0, 0, 0, 1);

        // Sleep to trigger client query expiration
        sleep(SECONDS.toMillis(20));

        infos = getQueryInfos(coordinator2, "/v1/query?state=failed");
        assertEquals(infos.size(), 5);
        assertStateCounts(infos, 0, 5, 0, 0);
    }

    private List<BasicQueryInfo> getQueryInfos(TestingPrestoServer server, String path)
    {
        Request request = prepareGet().setUri(server.resolve(path)).build();
        return client.execute(request, createJsonResponseHandler(listJsonCodec(BasicQueryInfo.class)));
    }

    private void runToCompletion(TestingPrestoServer server, String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(sql, uri);
        while (queryResults.getNextUri() != null) {
            queryResults = getQueryResults(queryResults);
        }
    }

    private void runToFirstResult(TestingPrestoServer server, String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(sql, uri);
        while (queryResults.getData() == null) {
            queryResults = getQueryResults(queryResults);
        }
    }

    private void runToQueued(TestingPrestoServer server, String sql)
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
                case RUNNING:
                case FINISHING:
                    running++;
                    break;
                case WAITING_FOR_RESOURCES:
                case PLANNING:
                case DISPATCHING:
                case QUEUED:
                    queued++;
                    break;
                case FINISHED:
                    finished++;
                    break;
                case FAILED:
                    failed++;
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
}
