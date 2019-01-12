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
package io.prestosql.execution.resourceGroups;

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.prestosql.server.ResourceGroupInfo;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.tests.DistributedQueryRunner;
import io.prestosql.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

import java.util.List;

import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestResourceGroupIntegration
{
    @Test
    public void testMemoryFraction()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().build()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            getResourceGroupManager(queryRunner).setConfigurationManager("file", ImmutableMap.of(
                    "resource-groups.config-file", getResourceFilePath("resource_groups_memory_percentage.json")));

            queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
            waitForGlobalResourceGroup(queryRunner);
        }
    }

    @Test
    public void testPathToRoot()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder().build()) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            InternalResourceGroupManager<?> manager = getResourceGroupManager(queryRunner);
            manager.setConfigurationManager("file", ImmutableMap.of(
                    "resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            queryRunner.execute(testSessionBuilder().setCatalog("tpch").setSchema("tiny").setSource("dashboard-foo").build(), "SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
            List<ResourceGroupInfo> path = manager.getPathToRoot(new ResourceGroupId(new ResourceGroupId(new ResourceGroupId("global"), "user-user"), "dashboard-user"));
            assertEquals(path.size(), 3);
            assertTrue(path.get(1).getSubGroups() != null);
            assertEquals(path.get(2).getId(), new ResourceGroupId("global"));
            assertEquals(path.get(2).getHardConcurrencyLimit(), 100);
            assertEquals(path.get(2).getRunningQueries(), null);
        }
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    public static void waitForGlobalResourceGroup(DistributedQueryRunner queryRunner)
            throws InterruptedException
    {
        long startTime = System.nanoTime();
        while (true) {
            SECONDS.sleep(1);
            ResourceGroupInfo global = getResourceGroupManager(queryRunner).getResourceGroupInfo(new ResourceGroupId("global"));
            if (global.getSoftMemoryLimit().toBytes() > 0) {
                break;
            }
            assertLessThan(nanosSince(startTime).roundTo(SECONDS), 60L);
        }
    }

    private static InternalResourceGroupManager<?> getResourceGroupManager(DistributedQueryRunner queryRunner)
    {
        return queryRunner.getCoordinator().getResourceGroupManager()
                .orElseThrow(() -> new IllegalArgumentException("no resource group manager"));
    }
}
