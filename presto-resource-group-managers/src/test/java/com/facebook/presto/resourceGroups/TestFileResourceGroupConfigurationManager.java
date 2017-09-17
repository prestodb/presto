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
import com.facebook.presto.spi.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;
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
        assertFails("resource_groups_config_bad_query_priority_scheduling_policy.json", "Must use \"weighted\" scheduling policy when using scheduling weight");
        assertFails("resource_groups_config_bad_weighted_scheduling_policy.json", "Must specify scheduling weight for each sub group when using \"weighted\" scheduling policy");
        assertFails("resource_groups_config_unused_field.json", "Unknown property at line 8:6: maxFoo");
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "No matching configuration found for: missing")
    public void testMissing()
    {
        ResourceGroupConfigurationManager manager = parse("resource_groups_config.json");
        ResourceGroup missing = new TestingResourceGroup(new ResourceGroupId("missing"));
        manager.configure(missing, new SelectionContext(true, "user", Optional.empty(), ImmutableSet.of(), 1, Optional.empty()));
    }

    @Test
    public void testQueryTypeConfiguration()
    {
        ResourceGroupConfigurationManager manager = parse("resource_groups_config_query_type.json");
        List<ResourceGroupSelector> selectors = manager.getSelectors();
        assertMatch(selectors, new SelectionContext(true, "test_user", Optional.empty(), ImmutableSet.of(), 1, Optional.of("select")), "global.select");
        assertMatch(selectors, new SelectionContext(true, "test_user", Optional.empty(), ImmutableSet.of(), 1, Optional.of("explain")), "global.explain");
        assertMatch(selectors, new SelectionContext(true, "test_user", Optional.empty(), ImmutableSet.of(), 1, Optional.of("insert")), "global.insert");
        assertMatch(selectors, new SelectionContext(true, "test_user", Optional.empty(), ImmutableSet.of(), 1, Optional.of("delete")), "global.delete");
        assertMatch(selectors, new SelectionContext(true, "test_user", Optional.empty(), ImmutableSet.of(), 1, Optional.of("describe")), "global.describe");
        assertMatch(selectors, new SelectionContext(true, "test_user", Optional.empty(), ImmutableSet.of(), 1, Optional.of("data_definition")), "global.data_definition");
        assertMatch(selectors, new SelectionContext(true, "test_user", Optional.empty(), ImmutableSet.of(), 1, Optional.of("sth_else")), "global.other");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Selector specifies an invalid query type: invalid_query_type")
    public void testInvalidQueryTypeConfiguration()
    {
        parse("resource_groups_config_bad_query_type.json");
    }

    private void assertMatch(List<ResourceGroupSelector> selectors, SelectionContext context, String expectedResourceGroup)
    {
        Optional<ResourceGroupId> group = tryMatch(selectors, context);
        assertTrue(group.isPresent(), "match expected");
        assertEquals(group.get().toString(), expectedResourceGroup, format("Expected: '%s' resource group, found: %s", expectedResourceGroup, group.get()));
    }

    private Optional<ResourceGroupId> tryMatch(List<ResourceGroupSelector> selectors, SelectionContext context)
    {
        for (ResourceGroupSelector selector : selectors) {
            Optional<ResourceGroupId> group = selector.match(context);
            if (group.isPresent()) {
                return group;
            }
        }
        return Optional.empty();
    }

    @Test
    public void testConfiguration()
    {
        ResourceGroupConfigurationManager manager = parse("resource_groups_config.json");
        ResourceGroup global = new TestingResourceGroup(new ResourceGroupId("global"));
        manager.configure(global, new SelectionContext(true, "user", Optional.empty(), ImmutableSet.of(), 1, Optional.empty()));
        assertEquals(global.getSoftMemoryLimit(), new DataSize(1, MEGABYTE));
        assertEquals(global.getSoftCpuLimit(), new Duration(1, HOURS));
        assertEquals(global.getHardCpuLimit(), new Duration(1, DAYS));
        assertEquals(global.getCpuQuotaGenerationMillisPerSecond(), 1000 * 24);
        assertEquals(global.getMaxQueuedQueries(), 1000);
        assertEquals(global.getHardConcurrencyLimit(), 100);
        assertEquals(global.getSchedulingPolicy(), WEIGHTED);
        assertEquals(global.getSchedulingWeight(), 0);
        assertEquals(global.getJmxExport(), true);
        assertEquals(global.getQueuedTimeLimit(), new Duration(1, HOURS));
        assertEquals(global.getRunningTimeLimit(), new Duration(1, HOURS));

        ResourceGroup sub = new TestingResourceGroup(new ResourceGroupId(new ResourceGroupId("global"), "sub"));
        manager.configure(sub, new SelectionContext(true, "user", Optional.empty(), ImmutableSet.of(), 1, Optional.empty()));
        assertEquals(sub.getSoftMemoryLimit(), new DataSize(2, MEGABYTE));
        assertEquals(sub.getHardConcurrencyLimit(), 3);
        assertEquals(sub.getMaxQueuedQueries(), 4);
        assertEquals(sub.getSchedulingPolicy(), null);
        assertEquals(sub.getSchedulingWeight(), 5);
        assertEquals(sub.getJmxExport(), false);
        assertEquals(global.getQueuedTimeLimit(), new Duration(1, HOURS));
        assertEquals(global.getRunningTimeLimit(), new Duration(1, HOURS));
    }

    @Test
    public void testLegacyConfiguration()
    {
        ResourceGroupConfigurationManager manager = parse("resource_groups_config_legacy.json");
        ResourceGroup global = new TestingResourceGroup(new ResourceGroupId("global"));
        manager.configure(global, new SelectionContext(true, "user", Optional.empty(), ImmutableSet.of(), 1, Optional.empty()));
        assertEquals(global.getSoftMemoryLimit(), new DataSize(3, MEGABYTE));
        assertEquals(global.getMaxQueuedQueries(), 99);
        assertEquals(global.getHardConcurrencyLimit(), 42);
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
        return new FileResourceGroupConfigurationManager((poolId, listener) -> {}, config);
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
            assertThat(e.getMessage()).matches(expectedPattern);
        }
    }
}
