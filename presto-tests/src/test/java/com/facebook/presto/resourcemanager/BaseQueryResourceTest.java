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
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

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
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class BaseQueryResourceTest
{
    protected HttpClient client;
    protected TestingPrestoServer coordinator1;
    protected TestingPrestoServer coordinator2;
    protected TestingPrestoServer resourceManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        client = new JettyHttpClient();
        DistributedQueryRunner runner = createQueryRunner(ImmutableMap.of("query.client.timeout", "20s"), 2);
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

    protected QueryResults postQuery(String sql, URI uri)
    {
        Request request = preparePost()
                .setHeader(PRESTO_USER, "user")
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .build();
        return client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
    }

    protected void runToCompletion(TestingPrestoServer server, String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(sql, uri);
        while (queryResults.getNextUri() != null) {
            queryResults = getQueryResults(queryResults);
        }
    }

    protected void runToFirstResult(TestingPrestoServer server, String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(sql, uri);
        while (queryResults.getData() == null) {
            queryResults = getQueryResults(queryResults);
        }
    }

    protected void runToQueued(TestingPrestoServer server, String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        QueryResults queryResults = postQuery(sql, uri);
        while (!"QUEUED".equals(queryResults.getStats().getState())) {
            queryResults = getQueryResults(queryResults);
        }
        getQueryResults(queryResults);
    }

    protected QueryResults getQueryResults(QueryResults queryResults)
    {
        Request request = prepareGet()
                .setHeader(PRESTO_USER, "user")
                .setUri(queryResults.getNextUri())
                .build();
        queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        return queryResults;
    }
}
