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

import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestResourceManagerResourceGroupService
{
    @Test
    public void testGetResourceGroupInfo()
            throws Exception
    {
        TestingResourceManagerClient resourceManagerClient = new TestingResourceManagerClient();
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        ResourceManagerConfig resourceManagerConfig = new ResourceManagerConfig();
        ResourceManagerResourceGroupService service = new ResourceManagerResourceGroupService((addressSelectionContext, headers) -> resourceManagerClient, resourceManagerConfig, nodeManager);
        List<ResourceGroupRuntimeInfo> resourceGroupInfos = service.getResourceGroupInfo();
        assertNotNull(resourceGroupInfos);
        assertTrue(resourceGroupInfos.isEmpty());
        assertEquals(resourceManagerClient.getResourceGroupInfoCalls("local"), 1);

        resourceManagerClient.setResourceGroupRuntimeInfos(ImmutableList.of(new ResourceGroupRuntimeInfo(new ResourceGroupId("global"), 1, 2, 3, 0, 1)));

        Thread.sleep(SECONDS.toMillis(2));

        resourceGroupInfos = service.getResourceGroupInfo();
        assertNotNull(resourceGroupInfos);
        assertEquals(resourceGroupInfos.size(), 1);
        assertEquals(resourceManagerClient.getResourceGroupInfoCalls("local"), 2);
    }
}
