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
package com.facebook.presto.execution.resourceGroups;

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.resourceGroups.ResourceGroupManagerPlugin;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.Duration.nanosSince;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestResourceGroupIntegration
{
    @Test
    public void testMemoryFraction()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of(), ImmutableMap.of("experimental.resource-groups-enabled", "true"))) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_memory_percentage.json")));

            queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
            long startTime = System.nanoTime();
            while (true) {
                SECONDS.sleep(1);
                ResourceGroupInfo global = getResourceGroupInfo(queryRunner, new ResourceGroupId("global"));
                if (global.getSoftMemoryLimit().toBytes() > 0) {
                    break;
                }
                assertLessThan(nanosSince(startTime).roundTo(SECONDS), 60L);
            }
        }
    }

    @Test
    public void testResourceGroupInfo()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(ImmutableMap.of(), ImmutableMap.of("experimental.resource-groups-enabled", "true"))) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_info.json")));

            CompletableFuture<MaterializedResult> future = runAsync(queryRunner, "SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
            while (true) {
                List<QueryInfo> queries = queryRunner.getCoordinator().getQueryManager().getAllQueryInfo();
                if (!queries.isEmpty() && queries.get(0).getState().isDone()) {
                    break;
                }
                if (queries.isEmpty() || !queries.get(0).getState().equals(QueryState.RUNNING)) {
                    MILLISECONDS.sleep(1);
                    continue;
                }
                ResourceGroupInfo global = getResourceGroupInfo(queryRunner, new ResourceGroupId("global"));
                assertEquals(global.getQueryIds(), ImmutableSet.of(queries.get(0).getQueryId()));
            }
            ResourceGroupInfo global = getResourceGroupInfo(queryRunner, new ResourceGroupId("global"));
            assertGreaterThan(global.getCpuUsage(), 0L);
            future.get();
            assertTrue(future.isDone());
        }
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    private static CompletableFuture<MaterializedResult> runAsync(DistributedQueryRunner queryRunner, String query)
    {
        return CompletableFuture.supplyAsync(() -> queryRunner.execute(query));
    }

    private static ResourceGroupInfo getResourceGroupInfo(DistributedQueryRunner queryRunner, ResourceGroupId id)
    {
        return queryRunner.getCoordinator().getResourceGroupManager().get().getResourceGroupInfo(id);
    }
}
