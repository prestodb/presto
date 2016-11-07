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
package com.facebook.presto.resourceGroups;

import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestFileResourceGroupConfigurationManager
{
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
        ResourceGroup missing = new TestingResourceGroup(new ResourceGroupId("missing"));
        manager.configure(missing, new SelectionContext(true, "user", Optional.empty(), 1));
    }

    @Test
    public void testConfiguration()
    {
        ResourceGroupConfigurationManager manager = parse("resource_groups_config.json");
        ResourceGroup global = new TestingResourceGroup(new ResourceGroupId("global"));
        manager.configure(global, new SelectionContext(true, "user", Optional.empty(), 1));
        assertEquals(global.getSoftMemoryLimit(), new DataSize(1, MEGABYTE));
        assertEquals(global.getSoftCpuLimit(), new Duration(1, HOURS));
        assertEquals(global.getHardCpuLimit(), new Duration(1, DAYS));
        assertEquals(global.getCpuQuotaGenerationMillisPerSecond(), 1000 * 24);
        assertEquals(global.getMaxQueuedQueries(), 1000);
        assertEquals(global.getMaxRunningQueries(), 100);
        assertEquals(global.getSchedulingPolicy(), WEIGHTED);
        assertEquals(global.getSchedulingWeight(), 0);
        assertEquals(global.getJmxExport(), true);

        ResourceGroup sub = new TestingResourceGroup(new ResourceGroupId(new ResourceGroupId("global"), "sub"));
        manager.configure(sub, new SelectionContext(true, "user", Optional.empty(), 1));
        assertEquals(sub.getSoftMemoryLimit(), new DataSize(2, MEGABYTE));
        assertEquals(sub.getMaxRunningQueries(), 3);
        assertEquals(sub.getMaxQueuedQueries(), 4);
        assertEquals(sub.getSchedulingPolicy(), null);
        assertEquals(sub.getSchedulingWeight(), 5);
        assertEquals(sub.getJmxExport(), false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Selector refers to nonexistent group: a.b.c.X")
    public void testNonExistentGroup()
    {
        parse("resource_groups_config_bad_selector.json");
    }

    private FileResourceGroupConfigurationManager parse(String fileName)
    {
        FileResourceGroupConfig config = new FileResourceGroupConfig();
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
