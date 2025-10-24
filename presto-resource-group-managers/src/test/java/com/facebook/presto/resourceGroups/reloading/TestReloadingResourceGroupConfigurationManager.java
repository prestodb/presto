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
package com.facebook.presto.resourceGroups.reloading;

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.execution.ClusterOverloadConfig;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroup;
import com.facebook.presto.execution.scheduler.clusterOverload.ClusterOverloadPolicy;
import com.facebook.presto.execution.scheduler.clusterOverload.ClusterResourceChecker;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.resourceGroups.VariableMap;
import com.facebook.presto.resourceGroups.db.DbManagerSpecProvider;
import com.facebook.presto.resourceGroups.db.DbResourceGroupConfig;
import com.facebook.presto.resourceGroups.db.DbSourceExactMatchSelector;
import com.facebook.presto.resourceGroups.db.H2DaoProvider;
import com.facebook.presto.resourceGroups.db.H2ResourceGroupsDao;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.resourceGroups.ResourceGroupQueryLimits;
import com.facebook.presto.spi.resourceGroups.SchedulingPolicy;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.execution.resourceGroups.InternalResourceGroup.DEFAULT_WEIGHT;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupQueryLimits.NO_LIMITS;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.FAIR;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestReloadingResourceGroupConfigurationManager
{
    private static final String ENVIRONMENT = "test";

    static H2DaoProvider setup(String prefix)
    {
        DbResourceGroupConfig config = new DbResourceGroupConfig().setConfigDbUrl("jdbc:h2:mem:test_" + prefix + System.nanoTime() + "_" + ThreadLocalRandom.current().nextInt());
        return new H2DaoProvider(config);
    }

    @Test
    public void testConfiguration()
    {
        H2DaoProvider daoProvider = setup("test_configuration");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", "1h", "1MB", "1h", 0, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, null, null, null, 0, 1L, ENVIRONMENT);
        dao.insertSelector(2, 1, null, null, null, null, null, null);
        DbManagerSpecProvider dbManagerSpecProvider = new DbManagerSpecProvider(daoProvider.get(), ENVIRONMENT, new ReloadingResourceGroupConfig());
        ReloadingResourceGroupConfigurationManager manager = new ReloadingResourceGroupConfigurationManager((poolId, listener) -> {}, new ReloadingResourceGroupConfig(), dbManagerSpecProvider);
        AtomicBoolean exported = new AtomicBoolean();
        InternalResourceGroup global = new InternalResourceGroup.RootInternalResourceGroup("global", (group, export) -> exported.set(export), directExecutor(), ignored -> Optional.empty(), rg -> false, new InMemoryNodeManager(), createClusterResourceChecker());
        manager.configure(global, new SelectionContext<>(global.getId(), new VariableMap(ImmutableMap.of("USER", "user"))));
        assertEqualsResourceGroup(global, "1MB", 1000, 100, 100, WEIGHTED, DEFAULT_WEIGHT, true, new Duration(1, HOURS), new Duration(1, DAYS), new ResourceGroupQueryLimits(Optional.of(new Duration(1, HOURS)), Optional.of(new DataSize(1, MEGABYTE)), Optional.of(new Duration(1, HOURS))));
        exported.set(false);
        InternalResourceGroup sub = global.getOrCreateSubGroup("sub", true);
        manager.configure(sub, new SelectionContext<>(sub.getId(), new VariableMap(ImmutableMap.of("USER", "user"))));
        assertEqualsResourceGroup(sub, "2MB", 4, 3, 3, FAIR, 5, false, new Duration(Long.MAX_VALUE, MILLISECONDS), new Duration(Long.MAX_VALUE, MILLISECONDS), NO_LIMITS);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "No matching configuration found for: missing")
    public void testMissing()
    {
        H2DaoProvider daoProvider = setup("test_missing");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", "1h", "1MB", "1h", 0, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, null, null, null, 0, 1L, ENVIRONMENT);
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        dao.insertSelector(2, 1, null, null, null, null, null, null);
        DbManagerSpecProvider dbManagerSpecProvider = new DbManagerSpecProvider(daoProvider.get(), ENVIRONMENT, new ReloadingResourceGroupConfig());
        ReloadingResourceGroupConfigurationManager manager = new ReloadingResourceGroupConfigurationManager((poolId, listener) -> {}, new ReloadingResourceGroupConfig(), dbManagerSpecProvider);
        InternalResourceGroup missing = new InternalResourceGroup.RootInternalResourceGroup("missing", (group, export) -> {}, directExecutor(), ignored -> Optional.empty(), rg -> false, new InMemoryNodeManager(), createClusterResourceChecker());
        manager.configure(missing, new SelectionContext<>(missing.getId(), new VariableMap(ImmutableMap.of("USER", "user"))));
    }

    @Test(timeOut = 60_000)
    public void testReconfig()
            throws Exception
    {
        H2DaoProvider daoProvider = setup("test_reconfig");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", "1h", "1MB", "1h", 0, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, null, null, null, 0, 1L, ENVIRONMENT);
        dao.insertSelector(2, 1, null, null, null, null, null, null);
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        DbManagerSpecProvider dbManagerSpecProvider = new DbManagerSpecProvider(daoProvider.get(), ENVIRONMENT, new ReloadingResourceGroupConfig());
        ReloadingResourceGroupConfigurationManager manager = new ReloadingResourceGroupConfigurationManager((poolId, listener) -> {}, new ReloadingResourceGroupConfig(), dbManagerSpecProvider);
        manager.start();
        AtomicBoolean exported = new AtomicBoolean();
        InternalResourceGroup global = new InternalResourceGroup.RootInternalResourceGroup("global", (group, export) -> exported.set(export), directExecutor(), ignored -> Optional.empty(), rg -> false, new InMemoryNodeManager(), createClusterResourceChecker());
        manager.configure(global, new SelectionContext<>(global.getId(), new VariableMap(ImmutableMap.of("USER", "user"))));
        InternalResourceGroup globalSub = global.getOrCreateSubGroup("sub", true);
        manager.configure(globalSub, new SelectionContext<>(globalSub.getId(), new VariableMap(ImmutableMap.of("USER", "user"))));
        // Verify record exists
        assertEqualsResourceGroup(globalSub, "2MB", 4, 3, 3, FAIR, 5, false, new Duration(Long.MAX_VALUE, MILLISECONDS), new Duration(Long.MAX_VALUE, MILLISECONDS), NO_LIMITS);
        dao.updateResourceGroup(2, "sub", "3MB", 2, 1, 1, "weighted", 6, true, "1h", "1d", "1h", "1MB", "30m", 0, 1L, ENVIRONMENT);
        do {
            MILLISECONDS.sleep(500);
        }
        while (globalSub.getJmxExport() == false);
        // Verify update
        assertEqualsResourceGroup(globalSub, "3MB", 2, 1, 1, WEIGHTED, 6, true, new Duration(1, HOURS), new Duration(1, DAYS), new ResourceGroupQueryLimits(Optional.of(new Duration(1, HOURS)), Optional.of(new DataSize(1, MEGABYTE)), Optional.of(new Duration(30, MINUTES))));
        // Verify delete
        dao.deleteSelectors(2);
        dao.deleteResourceGroup(2);
        do {
            MILLISECONDS.sleep(500);
        }
        while (globalSub.getMaxQueuedQueries() != 0 || globalSub.getHardConcurrencyLimit() != 0);
    }

    @Test
    public void testExactMatchSelector()
    {
        H2DaoProvider daoProvider = setup("test_exact_match_selector");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.createExactMatchSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, null, null, 0, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, null, null, null, 0, 1L, ENVIRONMENT);
        dao.insertSelector(2, 1, null, null, null, null, null, null);
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        ReloadingResourceGroupConfig config = new ReloadingResourceGroupConfig();
        config.setExactMatchSelectorEnabled(true);
        DbManagerSpecProvider dbManagerSpecProvider = new DbManagerSpecProvider(daoProvider.get(), ENVIRONMENT, config);
        ReloadingResourceGroupConfigurationManager manager = new ReloadingResourceGroupConfigurationManager((poolId, listener) -> {}, config, dbManagerSpecProvider);
        manager.load();
        assertEquals(manager.getSelectors().size(), 2);
        assertTrue(manager.getSelectors().get(0) instanceof DbSourceExactMatchSelector);

        config.setExactMatchSelectorEnabled(false);
        manager = new ReloadingResourceGroupConfigurationManager((poolId, listener) -> {}, config, dbManagerSpecProvider);
        manager.load();
        assertEquals(manager.getSelectors().size(), 1);
        assertFalse(manager.getSelectors().get(0) instanceof DbSourceExactMatchSelector);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No selectors are configured")
    public void testInvalidConfiguration()
    {
        H2DaoProvider daoProvider = setup("selectors");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "100%", 100, 100, 100, null, null, null, null, null, null, null, null, 0, null, ENVIRONMENT);

        DbManagerSpecProvider dbManagerSpecProvider = new DbManagerSpecProvider(daoProvider.get(), ENVIRONMENT, new ReloadingResourceGroupConfig());
        ReloadingResourceGroupConfigurationManager manager = new ReloadingResourceGroupConfigurationManager(
                (poolId, listener) -> {},
                new ReloadingResourceGroupConfig().setMaxRefreshInterval(Duration.valueOf("1ms")),
                dbManagerSpecProvider);

        manager.getSelectors();
    }

    @Test
    public void testRefreshInterval()
    {
        H2DaoProvider daoProvider = setup("selectors");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "100%", 100, 100, 100, null, null, null, null, null, null, null, null, 0, null, ENVIRONMENT);

        DbManagerSpecProvider dbManagerSpecProvider = new DbManagerSpecProvider(daoProvider.get(), ENVIRONMENT, new ReloadingResourceGroupConfig());
        ReloadingResourceGroupConfigurationManager manager = new ReloadingResourceGroupConfigurationManager(
                (poolId, listener) -> {},
                new ReloadingResourceGroupConfig().setMaxRefreshInterval(Duration.valueOf("1ms")),
                dbManagerSpecProvider);

        dao.dropSelectorsTable();
        manager.load();

        try {
            manager.getSelectors();
            fail("Expected unavailable configuration exception");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Resource group configuration cannot be fetched from source."));
        }

        try {
            manager.getRootGroups();
            fail("Expected unavailable configuration exception");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Resource group configuration cannot be fetched from source."));
        }

        manager.destroy();
    }

    private static void assertEqualsResourceGroup(
            InternalResourceGroup group,
            String softMemoryLimit,
            int maxQueued,
            int hardConcurrencyLimit,
            int softConcurrencyLimit,
            SchedulingPolicy schedulingPolicy,
            int schedulingWeight,
            boolean jmxExport,
            Duration softCpuLimit,
            Duration hardCpuLimit,
            ResourceGroupQueryLimits perQueryLimits)
    {
        assertEquals(group.getSoftMemoryLimit(), DataSize.valueOf(softMemoryLimit));
        assertEquals(group.getMaxQueuedQueries(), maxQueued);
        assertEquals(group.getHardConcurrencyLimit(), hardConcurrencyLimit);
        assertEquals(group.getSoftConcurrencyLimit(), softConcurrencyLimit);
        assertEquals(group.getSchedulingPolicy(), schedulingPolicy);
        assertEquals(group.getSchedulingWeight(), schedulingWeight);
        assertEquals(group.getJmxExport(), jmxExport);
        assertEquals(group.getSoftCpuLimit(), softCpuLimit);
        assertEquals(group.getHardCpuLimit(), hardCpuLimit);
        assertEquals(group.getPerQueryLimits(), perQueryLimits);
    }

    private static ClusterResourceChecker createClusterResourceChecker()
    {
        // Create a mock cluster overload policy that never reports overload
        ClusterOverloadPolicy mockPolicy = new ClusterOverloadPolicy()
        {
            @Override
            public boolean isClusterOverloaded(InternalNodeManager nodeManager)
            {
                return false; // Never overloaded for tests
            }

            @Override
            public String getName()
            {
                return "test-policy";
            }
        };

        // Create a config with throttling disabled for tests
        ClusterOverloadConfig config = new ClusterOverloadConfig()
                .setClusterOverloadThrottlingEnabled(false);

        return new ClusterResourceChecker(mockPolicy, config, new InMemoryNodeManager());
    }
}
