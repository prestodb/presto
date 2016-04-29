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

import com.facebook.presto.execution.resourceGroups.FileResourceGroupConfigurationManager.ManagerSpec;
import com.facebook.presto.execution.resourceGroups.ResourceGroup.RootResourceGroup;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static com.facebook.presto.execution.resourceGroups.ResourceGroup.DEFAULT_WEIGHT;
import static com.facebook.presto.execution.resourceGroups.ResourceGroup.SubGroupSchedulingPolicy.FAIR;
import static com.facebook.presto.execution.resourceGroups.ResourceGroup.SubGroupSchedulingPolicy.WEIGHTED;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestFileResourceGroupConfigurationManager
{
    @Test(timeOut = 60_000)
    public void testMemoryFraction()
            throws Exception
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put("experimental.resource-groups-enabled", "true");
        builder.put("resource-groups.config-file", getResourceFilePath("resource_groups_memory_percentage.json"));
        Map<String, String> properties = builder.build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(properties)) {
            queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
            while (true) {
                TimeUnit.SECONDS.sleep(1);
                ResourceGroupInfo global = queryRunner.getCoordinator().getResourceGroupManager().get().getResourceGroupInfo(ResourceGroupId.fromString("global"));
                if (global.getSoftMemoryLimit().toBytes() > 0) {
                    break;
                }
            }
        }
    }

    @Test
    public void testInvalid()
    {
        assertFails("resource_groups_config_bad_root.json", "Duplicated root group: global");
        assertFails("resource_groups_config_bad_sub_group.json", "Duplicated sub group: sub");
        assertFails("resource_groups_config_bad_group_id.json", "Invalid resource group name. 'glo.bal' contains a '.'");
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "No matching configuration found for: missing")
    public void testMissing()
    {
        ResourceGroupConfigurationManager manager = parse("resource_groups_config.json");
        ResourceGroup missing = new RootResourceGroup("missing", (group, export) -> { }, directExecutor());
        manager.configure(missing, new SelectionContext(true, "user", Optional.empty()));
    }

    @Test
    public void testConfiguration()
    {
        ResourceGroupConfigurationManager manager = parse("resource_groups_config.json");
        AtomicBoolean exported = new AtomicBoolean();
        ResourceGroup global = new RootResourceGroup("global", (group, export) -> exported.set(export), directExecutor());
        manager.configure(global, new SelectionContext(true, "user", Optional.empty()));
        assertEquals(global.getSoftMemoryLimit(), new DataSize(1, MEGABYTE));
        assertEquals(global.getMaxQueuedQueries(), 1000);
        assertEquals(global.getMaxRunningQueries(), 100);
        assertEquals(global.getSchedulingPolicy(), WEIGHTED);
        assertEquals(global.getSchedulingWeight(), DEFAULT_WEIGHT);
        assertEquals(global.getJmxExport(), true);
        assertEquals(exported.get(), true);
        exported.set(false);
        ResourceGroup sub = global.getOrCreateSubGroup("sub");
        manager.configure(sub, new SelectionContext(true, "user", Optional.empty()));
        assertEquals(sub.getSoftMemoryLimit(), new DataSize(2, MEGABYTE));
        assertEquals(sub.getMaxRunningQueries(), 3);
        assertEquals(sub.getMaxQueuedQueries(), 4);
        assertEquals(sub.getSchedulingPolicy(), FAIR);
        assertEquals(sub.getSchedulingWeight(), 5);
        assertEquals(sub.getJmxExport(), false);
        assertEquals(exported.get(), false);
    }

    private FileResourceGroupConfigurationManager parse(String fileName)
    {
        ResourceGroupConfig config = new ResourceGroupConfig();
        config.setConfigFile(getResourceFilePath(fileName));
        return new FileResourceGroupConfigurationManager(
                (poolId, listener) -> { },
                config,
                jsonCodec(ManagerSpec.class));
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    private void assertFails(String fileName, String expectedPattern)
    {
        try {
            parse(fileName);
            fail("Expected parsing to fail");
        }
        catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof JsonMappingException);
            assertTrue(e.getCause().getCause() instanceof IllegalArgumentException);
            assertTrue(Pattern.matches(expectedPattern, e.getCause().getCause().getMessage()),
                    "\nExpected (re) :" + expectedPattern + "\nActual        :" + e.getCause().getCause().getMessage());
        }
    }
}
