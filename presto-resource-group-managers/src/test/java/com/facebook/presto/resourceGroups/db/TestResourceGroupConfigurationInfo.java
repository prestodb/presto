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
package com.facebook.presto.resourceGroups.db;

import com.facebook.presto.resourceGroups.ResourceGroupConfigurationInfo;
import com.facebook.presto.resourceGroups.ResourceGroupIdTemplate;
import com.facebook.presto.resourceGroups.ResourceGroupNameTemplate;
import com.facebook.presto.resourceGroups.ResourceGroupSpec;
import com.facebook.presto.resourceGroups.SelectorSpec;
import com.facebook.presto.resourceGroups.TestingResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.presto.resourceGroups.db.TestDbResourceGroupConfigurationManager.setup;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;

public class TestResourceGroupConfigurationInfo
{
    @Test
    public void testResourceGroupConfigurationInfo()
            throws Exception
    {
        H2DaoProvider daoProvider = setup("test_configuration");
        H2ResourceGroupsDao dao = daoProvider.get();
        ResourceGroupConfigurationInfo configurationInfo = new ResourceGroupConfigurationInfo();
        // Create resource group configuration manager just to populate configurationInfo
        DbResourceGroupConfigurationManager manager = getManager(configurationInfo, dao);
        manager.start();
        try {
            assertEquals(configurationInfo.getCpuQuotaPeriod(), Optional.of(Duration.valueOf("1h")));
            SelectorSpec selectorSpec = new SelectorSpec(
                    Optional.of(Pattern.compile("user")),
                    Optional.of(Pattern.compile("test_source")),
                    Optional.empty(),
                    new ResourceGroupIdTemplate("global.sub"));
            assertEquals(getOnlyElement(configurationInfo.getSelectorSpecs()), selectorSpec);
            Map<ResourceGroupIdTemplate, ResourceGroupSpec> specs = configurationInfo.getResourceGroupSpecs();
            assertEquals(specs.size(), 2);
            ResourceGroupSpec actual = specs.get(new ResourceGroupIdTemplate("global"));
            assertEquals(actual, getExpectedResourceGroupSpec());
            // Update and insert resource groups
            dao.updateResourceGroup(2, "sub", "2MB", 40, 30, null, 6, null, null, null, "1h", "1h", 1L);
            dao.insertResourceGroup(3, "sub2", "2MB", 40, 30, null, 6, null, null, null, "1h", "1h", 1L);
            // Add a new selector
            dao.insertSelector(3, "user", "test_source");
            // Wait for the db to be polled
            Thread.sleep(2000);
            specs = configurationInfo.getResourceGroupSpecs();
            assertEquals(specs.size(), 3);
            actual = specs.get(new ResourceGroupIdTemplate("global"));
            assertEquals(actual, getUpdatedResourceGroupSpec());
            SelectorSpec selectorSpec2 = new SelectorSpec(
                    Optional.of(Pattern.compile("user")),
                    Optional.of(Pattern.compile("test_source")),
                    Optional.empty(),
                    new ResourceGroupIdTemplate("global.sub2"));
            assertEquals(
                    ImmutableSet.copyOf(configurationInfo.getSelectorSpecs()),
                    ImmutableSet.of(selectorSpec, selectorSpec2));
        }
        finally {
            manager.destroy();
        }
    }

    public static ResourceGroupSpec getExpectedResourceGroupSpec()
    {
        return new ResourceGroupSpec(
                new ResourceGroupNameTemplate("global"),
                "1MB",
                1000,
                100,
                Optional.of("weighted"),
                Optional.empty(),
                Optional.of(ImmutableList.of(
                        new ResourceGroupSpec(
                                new ResourceGroupNameTemplate("sub"),
                                "2MB",
                                4,
                                3,
                                Optional.empty(),
                                Optional.of(5),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(Duration.valueOf("1h")),
                                Optional.of(Duration.valueOf("1h"))))),
                Optional.of(true),
                Optional.of(Duration.valueOf("1h")),
                Optional.of(Duration.valueOf("1d")),
                Optional.of(Duration.valueOf("1h")),
                Optional.of(Duration.valueOf("1h")));
    }

    public static ResourceGroupSpec getUpdatedResourceGroupSpec()
    {
        return new ResourceGroupSpec(
                new ResourceGroupNameTemplate("global"),
                "1MB",
                1000,
                100,
                Optional.of("weighted"),
                Optional.empty(),
                Optional.of(ImmutableList.of(
                        new ResourceGroupSpec(
                                new ResourceGroupNameTemplate("sub"),
                                "2MB",
                                40,
                                30,
                                Optional.empty(),
                                Optional.of(6),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(Duration.valueOf("1h")),
                                Optional.of(Duration.valueOf("1h"))),
                        new ResourceGroupSpec(
                                new ResourceGroupNameTemplate("sub2"),
                                "2MB",
                                40,
                                30,
                                Optional.empty(),
                                Optional.of(6),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(Duration.valueOf("1h")),
                                Optional.of(Duration.valueOf("1h"))))),
                Optional.of(true),
                Optional.of(Duration.valueOf("1h")),
                Optional.of(Duration.valueOf("1d")),
                Optional.of(Duration.valueOf("1h")),
                Optional.of(Duration.valueOf("1h")));
    }
    @Test
    public void testConfiguredGroups()
    {
        H2DaoProvider daoProvider = setup("test_configuration");
        H2ResourceGroupsDao dao = daoProvider.get();
        ResourceGroupConfigurationInfo configurationInfo = new ResourceGroupConfigurationInfo();
        DbResourceGroupConfigurationManager manager = getManager(configurationInfo, dao);
        SelectionContext selectionContext = new SelectionContext(
                true,
                "user",
                Optional.empty(),
                1,
                Optional.empty());
        ResourceGroup global = new TestingResourceGroup(new ResourceGroupId("global"));
        manager.configure(global, selectionContext);
        ResourceGroup sub = new TestingResourceGroup(new ResourceGroupId(new ResourceGroupId("global"), "sub"));
        manager.configure(sub, selectionContext);
        assertEquals(configurationInfo.getConfiguredGroups(),
                ImmutableMap.of(
                        new ResourceGroupId("global"),
                        new ResourceGroupIdTemplate("global"),
                        new ResourceGroupId(new ResourceGroupId("global"), "sub"),
                        new ResourceGroupIdTemplate("global.sub")));
    }

    private static DbResourceGroupConfigurationManager getManager(ResourceGroupConfigurationInfo configurationInfo, H2ResourceGroupsDao dao)
    {
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        // Populate with some initial values
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, "weighted", null, true, "1h", "1d", "1h", "1h", null);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, null, 5, null, null, null, "1h", "1h", 1L);
        dao.insertSelector(2, "user", "test_source");
        return new DbResourceGroupConfigurationManager((poolId, listener) -> { },
                dao, configurationInfo);
    }
}
