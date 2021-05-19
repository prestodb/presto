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
import com.facebook.presto.server.ClusterStatsResource;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestDistributedClusterStatsResource
{
    private HttpClient client;
    private TestingPrestoServer coordinator;
    private TestingPrestoServer resourceManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        client = new JettyHttpClient();
        DistributedQueryRunner runner = createQueryRunner(ImmutableMap.of("query.client.timeout", "10s"), 2);
        coordinator = runner.getCoordinators().get(0);
        Optional<TestingPrestoServer> resourceManager = runner.getResourceManager();
        checkState(resourceManager.isPresent(), "resource manager not present");
        this.resourceManager = resourceManager.get();
        coordinator.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
        coordinator.getResourceGroupManager().get()
                .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_simple.json")));
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(coordinator);
        closeQuietly(resourceManager);
        closeQuietly(client);
        coordinator = null;
        resourceManager = null;
        client = null;
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    @Test(timeOut = 60_000)
    public void testClusterStatsRedirectToResourceManager()
            throws Exception
    {
        URI uri = uriBuilderFrom(coordinator.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery("SELECT * from tpch.sf100.orders", uri);
        queryResults = getQueryResults(queryResults);
        queryResults = getQueryResults(queryResults);

        // Sleep to allow query to make some progress
        sleep(SECONDS.toMillis(1));

        ClusterStatsResource.ClusterStats clusterStats = getClusterStats(true);

        assertNotNull(clusterStats);
        assertTrue(clusterStats.getActiveWorkers() > 0);
        assertEquals(clusterStats.getRunningQueries(), 1);
        assertEquals(clusterStats.getBlockedQueries(), 0);
        assertEquals(clusterStats.getQueuedQueries(), 0);
    }

    private ClusterStatsResource.ClusterStats getClusterStats(boolean followRedirects)
    {
        Request request = prepareGet()
                .setHeader(PRESTO_USER, "user")
                .setUri(uriBuilderFrom(coordinator.getBaseUrl().resolve("/v1/cluster")).build())
                .setFollowRedirects(followRedirects)
                .build();

        return client.execute(request, createJsonResponseHandler(jsonCodec(ClusterStatsResource.ClusterStats.class)));
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
}
