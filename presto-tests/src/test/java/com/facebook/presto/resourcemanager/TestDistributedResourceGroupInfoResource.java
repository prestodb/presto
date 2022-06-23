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

import com.facebook.airlift.http.client.UnexpectedResponseException;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.ResourceGroupInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.facebook.presto.utils.QueryExecutionClientUtil.getResponseEntity;
import static com.facebook.presto.utils.QueryExecutionClientUtil.runToCompletion;
import static com.facebook.presto.utils.QueryExecutionClientUtil.runToFirstResult;
import static com.facebook.presto.utils.ResourceUtils.getResourceFilePath;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDistributedResourceGroupInfoResource
{
    private static final int COORDINATOR_COUNT = 2;
    private JettyHttpClient client;
    private TestingPrestoServer coordinator1;
    private TestingPrestoServer coordinator2;
    private TestingPrestoServer resourceManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        client = new JettyHttpClient();
        DistributedQueryRunner runner = createQueryRunner(ImmutableMap.of("query.client.timeout", "20s"), COORDINATOR_COUNT);
        coordinator1 = runner.getCoordinator(0);
        coordinator2 = runner.getCoordinator(1);
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

    @Test(timeOut = 220_000)
    public void testGetResourceGroupInfo()
            throws InterruptedException, TimeoutException
    {
        runToCompletion(client, coordinator1, "SELECT 1", "user1");
        runToFirstResult(client, coordinator2, "SELECT * from tpch.sf100.orders", "user1");
        runToFirstResult(client, coordinator1, "SELECT * from tpch.sf101.orders", "user2");
        waitUntilCoordinatorsDiscoveredHealthyInRM(SECONDS.toMillis(15));
        ResourceGroupInfo resourceGroupInfo = getResponseEntity(client, coordinator1, "/v1/resourceGroupState/global", JsonCodec.jsonCodec(ResourceGroupInfo.class));
        assertEquals(resourceGroupInfo.getNumRunningQueries(), 2);

        Set<ResourceGroupId> subGroupResourceIdSet = resourceGroupInfo.getSubGroups().stream().map(a -> a.getId()).collect(Collectors.toSet());
        assertEquals(subGroupResourceIdSet.size(), 2);
        assertTrue(subGroupResourceIdSet.contains(new ResourceGroupId(resourceGroupInfo.getId(), "user-user1")));
        assertTrue(subGroupResourceIdSet.contains(new ResourceGroupId(resourceGroupInfo.getId(), "user-user2")));
    }

    @Test(expectedExceptions = UnexpectedResponseException.class, expectedExceptionsMessageRegExp = ".*404: Not Found")
    public void testResourceGroup404()
    {
        getResponseEntity(client, coordinator1, "/v1/resourceGroupState/global1", JsonCodec.jsonCodec(ResourceGroupInfo.class));
    }

    private void waitUntilCoordinatorsDiscoveredHealthyInRM(long timeoutInMillis)
            throws TimeoutException, InterruptedException
    {
        long deadline = System.currentTimeMillis() + timeoutInMillis;
        while (System.currentTimeMillis() < deadline) {
            AllNodes allNodes = this.resourceManager.refreshNodes();
            if (allNodes.getActiveCoordinators().size() == COORDINATOR_COUNT) {
                return;
            }
            sleep(100);
        }
        throw new TimeoutException(format("one of the nodes is still missing after: %s ms", timeoutInMillis));
    }
}
