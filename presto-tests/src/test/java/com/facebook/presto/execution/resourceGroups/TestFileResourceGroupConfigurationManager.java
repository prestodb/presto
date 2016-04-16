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
import com.fasterxml.jackson.databind.JsonMappingException;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.regex.Pattern;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
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
        ResourceGroup missing = new RootResourceGroup("missing", directExecutor());
        manager.configure(missing, testSessionBuilder().build().toSessionRepresentation());
    }

    @Test
    public void testConfiguration()
    {
        ResourceGroupConfigurationManager manager = parse("resource_groups_config.json");
        ResourceGroup global = new RootResourceGroup("global", directExecutor());
        manager.configure(global, testSessionBuilder().build().toSessionRepresentation());
        assertEquals(global.getSoftMemoryLimit(), new DataSize(1, MEGABYTE));
        assertEquals(global.getMaxQueuedQueries(), 1000);
        assertEquals(global.getMaxRunningQueries(), 100);
    }

    private FileResourceGroupConfigurationManager parse(String fileName)
    {
        String path = this.getClass().getClassLoader().getResource(fileName).getPath();
        ResourceGroupConfig config = new ResourceGroupConfig();
        config.setConfigFile(path);
        return new FileResourceGroupConfigurationManager(config, jsonCodec(ManagerSpec.class));
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
