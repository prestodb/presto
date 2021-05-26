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

import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.QueryStateInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestDistributedQueryInfoResource
{
    private ResourceManagerTestHelper rmTestHelper;
    private TestingPrestoServer coordinator1;
    private TestingPrestoServer coordinator2;
    private TestingPrestoServer resourceManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        rmTestHelper = new ResourceManagerTestHelper(new JettyHttpClient());
        DistributedQueryRunner runner = createQueryRunner(ImmutableMap.of("query.client.timeout", "20s"), 2);
        coordinator1 = runner.getCoordinators().get(0);
        coordinator2 = runner.getCoordinators().get(1);
        Optional<TestingPrestoServer> resourceManager = runner.getResourceManager();
        checkState(resourceManager.isPresent(), "resource manager not present");
        this.resourceManager = resourceManager.get();
        coordinator1.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
        coordinator1.getResourceGroupManager().get()
                .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", rmTestHelper.getResourceFilePath("resource_groups_config_simple.json")));
        coordinator2.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
        coordinator2.getResourceGroupManager().get()
                .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", rmTestHelper.getResourceFilePath("resource_groups_config_simple.json")));
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(coordinator1);
        closeQuietly(coordinator2);
        closeQuietly(resourceManager);
        closeQuietly(rmTestHelper);
        coordinator1 = null;
        coordinator2 = null;
        resourceManager = null;
        rmTestHelper = null;
    }

    @Test(timeOut = 220_000)
    public void testGetAllQueryInfo()
            throws Exception
    {
        rmTestHelper.runToCompletion(coordinator1, "SELECT 1");
        rmTestHelper.runToCompletion(coordinator2, "SELECT 2");
        rmTestHelper.runToCompletion(coordinator1, "SELECT 3");
        rmTestHelper.runToFirstResult(coordinator1, "SELECT * from tpch.sf100.orders");
        rmTestHelper.runToFirstResult(coordinator1, "SELECT * from tpch.sf101.orders");
        rmTestHelper.runToFirstResult(coordinator1, "SELECT * from tpch.sf102.orders");
        rmTestHelper.runToFirstResult(coordinator2, "SELECT * from tpch.sf100.orders");
        rmTestHelper.runToQueued(coordinator1, "SELECT 4");
        sleep(SECONDS.toMillis(5));
        List<QueryStateInfo> queryInfos = rmTestHelper.getQueryStateInfos(coordinator1, "/v1/queryState");
        assertEquals(queryInfos.size(), 5);
        for (QueryStateInfo inputQueryInfo : queryInfos) {
            QueryStateInfo queryInfoResult = rmTestHelper.getQueryStateInfo(coordinator1, "/v1/queryState/" + inputQueryInfo.getQueryId().getId());
            assertNotNull(queryInfoResult);
            assertEquals(queryInfoResult.getQueryId(), inputQueryInfo.getQueryId());
        }
    }
}
