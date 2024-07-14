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
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.facebook.presto.utils.QueryExecutionClientUtil.runToExecuting;
import static com.facebook.presto.utils.ResourceUtils.getResourceFilePath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test
public class TestResourceGroupStateInfoResource
{
    private HttpClient client;
    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        client = new JettyHttpClient();
        DistributedQueryRunner runner = createQueryRunner(ImmutableMap.of("query.client.timeout", "20s", "cluster-resource-group-state-info-expiration-duration", "20s"));
        server = runner.getCoordinator();
        server.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
        server.getResourceGroupManager().get()
                .forceSetConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_simple.json")));
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
        closeQuietly(client);
        server = null;
        client = null;
    }

    @Test
    public void testResourceGroupStateInfo()
    {
        runToExecuting(client, server, "SELECT * from tpch.sf101.orders");

        ResourceGroupInfo resourceGroupInfo = getGlobalResourceGroupStateInfo(false);

        assertNotNull(resourceGroupInfo);
        assertEquals(resourceGroupInfo.getNumRunningQueries(), 1);

        runToExecuting(client, server, "SELECT * from tpch.sf101.orders");
        resourceGroupInfo = getGlobalResourceGroupStateInfo(false);

        assertNotNull(resourceGroupInfo);
        //Result will be served from cache so running queries count should remain 1
        assertEquals(resourceGroupInfo.getNumRunningQueries(), 1);
    }

    private ResourceGroupInfo getGlobalResourceGroupStateInfo(boolean followRedirects)
    {
        Request.Builder builder = prepareGet();
        Request request = builder
                .setHeader(PRESTO_USER, "user")
                .setUri(uriBuilderFrom(server.getBaseUrl().resolve("/v1/resourceGroupState/global")).build())
                .setFollowRedirects(followRedirects)
                .build();

        return client.execute(request, createJsonResponseHandler(jsonCodec(ResourceGroupInfo.class)));
    }
}
