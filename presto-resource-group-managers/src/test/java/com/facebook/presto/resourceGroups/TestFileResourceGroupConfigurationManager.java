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
import com.facebook.presto.spi.resourceGroups.SelectionCriteria;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED;
import static com.google.common.io.Resources.getResource;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFileResourceGroupConfigurationManager
{
    private static final ResourceEstimates EMPTY_RESOURCE_ESTIMATES = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    @Test
    public void testInvalid()
    {
        assertFails("resource_groups_config_bad_root.json", "Duplicated root group: global");
        assertFails("resource_groups_config_bad_sub_group.json", "Duplicated sub group: sub");
        assertFails("resource_groups_config_bad_group_id.json", "Invalid resource group name. 'glo.bal' contains a '.'");
        assertFails("resource_groups_config_bad_weighted_scheduling_policy.json", "Must specify scheduling weight for all sub-groups of 'requests' or none of them");
        assertFails("resource_groups_config_unused_field.json", "Unknown property at line 8:6: maxFoo");
        assertFails("resource_groups_config_bad_query_priority_scheduling_policy.json",
                "Must use 'weighted' or 'weighted_fair' scheduling policy if specifying scheduling weight for 'requests'");
        assertFails("resource_groups_config_bad_extract_variable.json", "Invalid resource group name.*");
    }

    @Test
    public void testQueryTypeConfiguration()
    {
        FileResourceGroupConfigurationManager manager = parse("resource_groups_config_query_type.json");
        List<ResourceGroupSelector> selectors = manager.getSelectors();
        assertMatch(selectors, new SelectionCriteria(true, "test_user", Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.of("select")), "global.select");
        assertMatch(selectors, new SelectionCriteria(true, "test_user", Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.of("explain")), "global.explain");
        assertMatch(selectors, new SelectionCriteria(true, "test_user", Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.of("insert")), "global.insert");
        assertMatch(selectors, new SelectionCriteria(true, "test_user", Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.of("delete")), "global.delete");
        assertMatch(selectors, new SelectionCriteria(true, "test_user", Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.of("describe")), "global.describe");
        assertMatch(selectors, new SelectionCriteria(true, "test_user", Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.of("data_definition")), "global.data_definition");
        assertMatch(selectors, new SelectionCriteria(true, "test_user", Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.of("sth_else")), "global.other");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Selector specifies an invalid query type: invalid_query_type")
    public void testInvalidQueryTypeConfiguration()
    {
        parse("resource_groups_config_bad_query_type.json");
    }

    private static void assertMatch(List<ResourceGroupSelector> selectors, SelectionCriteria context, String expectedResourceGroup)
    {
        Optional<ResourceGroupId> group = tryMatch(selectors, context);
        assertTrue(group.isPresent(), "match expected");
        assertEquals(group.get().toString(), expectedResourceGroup, format("Expected: '%s' resource group, found: %s", expectedResourceGroup, group.get()));
    }

    private static Optional<ResourceGroupId> tryMatch(List<ResourceGroupSelector> selectors, SelectionCriteria context)
    {
        for (ResourceGroupSelector selector : selectors) {
            Optional<SelectionContext<VariableMap>> group = selector.match(context);
            if (group.isPresent()) {
                return group.map(SelectionContext::getResourceGroupId);
            }
        }
        return Optional.empty();
    }

    @SuppressWarnings("SimplifiedTestNGAssertion")
    @Test
    public void testConfiguration()
    {
        ResourceGroupConfigurationManager<VariableMap> manager = parse("resource_groups_config.json");
        ResourceGroupId globalId = new ResourceGroupId("global");
        ResourceGroup global = new TestingResourceGroup(globalId);
        manager.configure(global, new SelectionContext<>(globalId, new VariableMap(ImmutableMap.of("USER", "user"))));
        assertEquals(global.getPerQueryLimits().getExecutionTimeLimit(), Optional.of(new Duration(1, HOURS)));
        assertEquals(global.getPerQueryLimits().getTotalMemoryLimit(), Optional.of(new DataSize(1, MEGABYTE)));
        assertEquals(global.getPerQueryLimits().getCpuTimeLimit(), Optional.of(new Duration(1, HOURS)));
        assertEquals(global.getCpuQuotaGenerationMillisPerSecond(), 1000 * 24);
        assertEquals(global.getMaxQueuedQueries(), 1000);
        assertEquals(global.getHardConcurrencyLimit(), 100);
        assertEquals(global.getSchedulingPolicy(), WEIGHTED);
        assertEquals(global.getSchedulingWeight(), 0);
        assertEquals(global.getJmxExport(), true);

        ResourceGroupId subId = new ResourceGroupId(globalId, "sub");
        ResourceGroup sub = new TestingResourceGroup(subId);
        manager.configure(sub, new SelectionContext<>(subId, new VariableMap(ImmutableMap.of("USER", "user"))));
        assertEquals(sub.getSoftMemoryLimit(), new DataSize(2, MEGABYTE));
        assertEquals(sub.getHardConcurrencyLimit(), 3);
        assertEquals(sub.getMaxQueuedQueries(), 4);
        assertEquals(sub.getSchedulingPolicy(), null);
        assertEquals(sub.getSchedulingWeight(), 5);
        assertEquals(sub.getJmxExport(), false);
        assertEquals(sub.getPerQueryLimits().getExecutionTimeLimit(), Optional.empty());
        assertEquals(sub.getPerQueryLimits().getTotalMemoryLimit(), Optional.empty());
        assertEquals(sub.getPerQueryLimits().getCpuTimeLimit(), Optional.empty());
    }

    @Test
    public void testExtractVariableConfiguration()
    {
        ResourceGroupConfigurationManager<VariableMap> manager = parse("resource_groups_config_extract_variable.json");

        VariableMap variableMap = new VariableMap(ImmutableMap.of("USER", "user", "domain", "prestodb", "region", "us_east", "cluster", "12"));

        ResourceGroupId globalId = new ResourceGroupId("global");
        manager.configure(new TestingResourceGroup(globalId), new SelectionContext<>(globalId, variableMap));

        ResourceGroupId childId = new ResourceGroupId(new ResourceGroupId("global"), "prestodb:us_east:12");
        TestingResourceGroup child = new TestingResourceGroup(childId);
        manager.configure(child, new SelectionContext<>(childId, variableMap));

        assertEquals(child.getHardConcurrencyLimit(), 3);
    }

    @Test
    public void testLegacyConfiguration()
    {
        ResourceGroupConfigurationManager<VariableMap> manager = parse("resource_groups_config_legacy.json");
        ResourceGroupId globalId = new ResourceGroupId("global");
        ResourceGroup global = new TestingResourceGroup(globalId);
        manager.configure(global, new SelectionContext<>(globalId, new VariableMap(ImmutableMap.of("USER", "user"))));
        assertEquals(global.getSoftMemoryLimit(), new DataSize(3, MEGABYTE));
        assertEquals(global.getMaxQueuedQueries(), 99);
        assertEquals(global.getHardConcurrencyLimit(), 42);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Selector refers to nonexistent group: a.b.c.X")
    public void testNonExistentGroup()
    {
        parse("resource_groups_config_bad_selector.json");
    }

    private static FileResourceGroupConfigurationManager parse(String fileName)
    {
        FileResourceGroupConfig config = new FileResourceGroupConfig();
        config.setConfigFile(getResource(fileName).getPath());
        return new FileResourceGroupConfigurationManager((poolId, listener) -> {}, config);
    }

    private static void assertFails(String fileName, String expectedPattern)
    {
        assertThatThrownBy(() -> parse(fileName)).hasMessageMatching(expectedPattern);
    }
}
