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
import com.facebook.airlift.http.client.UnexpectedResponseException;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.facebook.presto.utils.QueryExecutionClientUtil.getResponseEntity;
import static com.facebook.presto.utils.QueryExecutionClientUtil.runToCompletion;
import static com.facebook.presto.utils.QueryExecutionClientUtil.runToFirstResult;
import static com.facebook.presto.utils.ResourceUtils.getResourceFilePath;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class TestDistributedTaskInfoResource
{
    private static final int COORDINATOR_COUNT = 2;
    private HttpClient client;
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
                .forceSetConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_simple.json")));
        coordinator2.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
        coordinator2.getResourceGroupManager().get()
                .forceSetConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_simple.json")));
    }

    @Test(timeOut = 220_000)
    public void testDistributedGetTaskInfo()
            throws Exception
    {
        sleep(SECONDS.toMillis(5));
        waitUntilCoordinatorsDiscoveredHealthyInRM(SECONDS.toMillis(15));
        runToCompletion(client, coordinator1, "SELECT 1");
        runToFirstResult(client, coordinator1, "SELECT * from tpch.sf101.orders");

        Map<ResourceGroupId, ResourceGroupRuntimeInfo> resourceGroupRuntimeInfoSnapshot;
        int globalRunningQueries = 0;
        do {
            MILLISECONDS.sleep(100);
            resourceGroupRuntimeInfoSnapshot = coordinator2.getResourceGroupManager().get().getResourceGroupRuntimeInfosSnapshot();
            ResourceGroupRuntimeInfo resourceGroupRuntimeInfo = resourceGroupRuntimeInfoSnapshot.get(new ResourceGroupId("global"));
            if (resourceGroupRuntimeInfo != null) {
                globalRunningQueries = resourceGroupRuntimeInfo.getDescendantRunningQueries();
            }
        } while (globalRunningQueries != 1);

        for (TaskInfo actualTaskInfo : coordinator1.getTaskManager().getAllTaskInfo()) {
            TaskId actualTaskId = actualTaskInfo.getTaskId();
            TaskInfo proxiedTaskInfo = getResponseEntity(client, coordinator2, "/v1/taskInfo/" + actualTaskId, jsonCodec(TaskInfo.class));
            assertNotNull(proxiedTaskInfo);
            assertEquals(actualTaskInfo.getTaskId(), proxiedTaskInfo.getTaskId());
        }

        try {
            getResponseEntity(client, coordinator2, "/v1/taskInfo/invalidTaskId", jsonCodec(TaskInfo.class));
            fail("Retrieving TaskInfo for an invalid TaskId should fail with a 404");
        }
        catch (UnexpectedResponseException expected) {
            assertEquals(expected.getStatusCode(), 404);
        }

        try {
            getResponseEntity(client, coordinator2, "/v1/taskInfo/20221102_075648_00000_8ybuj.9.0.0", jsonCodec(TaskInfo.class));
            fail("Retrieving TaskInfo for an invalid TaskId should fail with a 404");
        }
        catch (UnexpectedResponseException expected) {
            assertEquals(expected.getStatusCode(), 404);
        }
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
}
