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

import com.facebook.presto.execution.resourceGroups.InternalResourceGroup;
import com.facebook.presto.resourceGroups.ResourceGroupSpec;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.spi.resourceGroups.SchedulingPolicy;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.execution.resourceGroups.InternalResourceGroup.DEFAULT_WEIGHT;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.FAIR;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDbResourceGroupConfigurationManager
{
    private static final String ENVIRONMENT = "test";

    static H2DaoProvider setup(String prefix)
    {
        DbResourceGroupConfig config = new DbResourceGroupConfig().setConfigDbUrl("jdbc:h2:mem:test_" + prefix + System.nanoTime());
        return new H2DaoProvider(config);
    }

    @Test
    public void testEnvironments()
    {
        H2DaoProvider daoProvider = setup("test_configuration");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        String prodEnvironment = "prod";
        String devEnvironment = "dev";
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        // two resource groups are the same except the group for the prod environment has a larger softMemoryLimit
        dao.insertResourceGroup(1, "prod_global", "10MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", "1h", "1h", null, prodEnvironment);
        dao.insertResourceGroup(2, "dev_global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", "1h", "1h", null, devEnvironment);
        dao.insertSelector(1, ".*prod_user.*", null, null);
        dao.insertSelector(2, ".*dev_user.*", null, null);

        // check the prod configuration
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {}, new DbResourceGroupConfig(), daoProvider.get(), prodEnvironment);
        List<ResourceGroupSpec> groups = manager.getRootGroups();
        assertEquals(groups.size(), 1);
        InternalResourceGroup prodGlobal = new InternalResourceGroup.RootInternalResourceGroup("prod_global", (group, export) -> {}, directExecutor());
        manager.configure(prodGlobal, new SelectionContext(true, "user", Optional.empty(), ImmutableSet.of(), 1, Optional.empty()));
        assertEqualsResourceGroup(prodGlobal, "10MB", 1000, 100, 100, WEIGHTED, DEFAULT_WEIGHT, true, new Duration(1, HOURS), new Duration(1, DAYS), new Duration(1, HOURS), new Duration(1, HOURS));
        assertEquals(manager.getSelectors().size(), 1);
        ResourceGroupSelector prodSelector = manager.getSelectors().get(0);
        Optional<ResourceGroupId> prodResourceGroupId = prodSelector.match(new SelectionContext(true, "prod_user", Optional.empty(), ImmutableSet.of(), 1, Optional.empty()));
        assertTrue(prodResourceGroupId.isPresent());
        assertEquals(prodResourceGroupId.get().toString(), "prod_global");

        // check the dev configuration
        manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {}, new DbResourceGroupConfig(), daoProvider.get(), devEnvironment);
        assertEquals(groups.size(), 1);
        InternalResourceGroup devGlobal = new InternalResourceGroup.RootInternalResourceGroup("dev_global", (group, export) -> {}, directExecutor());
        manager.configure(devGlobal, new SelectionContext(true, "user", Optional.empty(), ImmutableSet.of(), 1, Optional.empty()));
        assertEqualsResourceGroup(devGlobal, "1MB", 1000, 100, 100, WEIGHTED, DEFAULT_WEIGHT, true, new Duration(1, HOURS), new Duration(1, DAYS), new Duration(1, HOURS), new Duration(1, HOURS));
        assertEquals(manager.getSelectors().size(), 1);
        ResourceGroupSelector devSelector = manager.getSelectors().get(0);
        Optional<ResourceGroupId> devResourceGroupId = devSelector.match(new SelectionContext(true, "dev_user", Optional.empty(), ImmutableSet.of(), 1, Optional.empty()));
        assertTrue(devResourceGroupId.isPresent());
        assertEquals(devResourceGroupId.get().toString(), "dev_global");
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
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", "1h", "1h", null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, "1h", "1h", 1L, ENVIRONMENT);
        dao.insertSelector(2, null, null, null);
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {}, new DbResourceGroupConfig(), daoProvider.get(), ENVIRONMENT);
        AtomicBoolean exported = new AtomicBoolean();
        InternalResourceGroup global = new InternalResourceGroup.RootInternalResourceGroup("global", (group, export) -> exported.set(export), directExecutor());
        manager.configure(global, new SelectionContext(true, "user", Optional.empty(), ImmutableSet.of(), 1, Optional.empty()));
        assertEqualsResourceGroup(global, "1MB", 1000, 100, 100, WEIGHTED, DEFAULT_WEIGHT, true, new Duration(1, HOURS), new Duration(1, DAYS), new Duration(1, HOURS), new Duration(1, HOURS));
        exported.set(false);
        InternalResourceGroup sub = global.getOrCreateSubGroup("sub");
        manager.configure(sub, new SelectionContext(true, "user", Optional.empty(), ImmutableSet.of(), 1, Optional.empty()));
        assertEqualsResourceGroup(sub, "2MB", 4, 3, 3, FAIR, 5, false, new Duration(Long.MAX_VALUE, MILLISECONDS), new Duration(Long.MAX_VALUE, MILLISECONDS), new Duration(1, HOURS), new Duration(1, HOURS));
    }

    @Test
    public void testDuplicates()
    {
        H2DaoProvider daoProvider = setup("test_dup_roots");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, ENVIRONMENT);
        try {
            dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, ENVIRONMENT);
            fail("Expected to fail");
        }
        catch (RuntimeException ex) {
            assertTrue(ex instanceof UnableToExecuteStatementException);
            assertTrue(ex.getCause() instanceof org.h2.jdbc.JdbcSQLException);
            assertTrue(ex.getCause().getMessage().startsWith("Unique index or primary key violation"));
        }
        dao.insertSelector(1, null, null, null);
        daoProvider = setup("test_dup_subs");
        dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, 1L, ENVIRONMENT);
        try {
            dao.insertResourceGroup(2, "sub", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, 1L, ENVIRONMENT);
        }
        catch (RuntimeException ex) {
            assertTrue(ex instanceof UnableToExecuteStatementException);
            assertTrue(ex.getCause() instanceof org.h2.jdbc.JdbcSQLException);
            assertTrue(ex.getCause().getMessage().startsWith("Unique index or primary key violation"));
        }

        dao.insertSelector(2, null, null, null);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "No matching configuration found for: missing")
    public void testMissing()
    {
        H2DaoProvider daoProvider = setup("test_missing");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, null, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, null, null, 1L, ENVIRONMENT);
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        dao.insertSelector(2, null, null, null);
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {}, new DbResourceGroupConfig(), daoProvider.get(), ENVIRONMENT);
        InternalResourceGroup missing = new InternalResourceGroup.RootInternalResourceGroup("missing", (group, export) -> {}, directExecutor());
        manager.configure(missing, new SelectionContext(true, "user", Optional.empty(), ImmutableSet.of(), 1, Optional.empty()));
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
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, null, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, null, null, 1L, ENVIRONMENT);
        dao.insertSelector(2, null, null, null);
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {}, new DbResourceGroupConfig(), daoProvider.get(), ENVIRONMENT);
        manager.start();
        AtomicBoolean exported = new AtomicBoolean();
        InternalResourceGroup global = new InternalResourceGroup.RootInternalResourceGroup("global", (group, export) -> exported.set(export), directExecutor());
        manager.configure(global, new SelectionContext(true, "user", Optional.empty(), ImmutableSet.of(), 1, Optional.empty()));
        InternalResourceGroup globalSub = global.getOrCreateSubGroup("sub");
        manager.configure(globalSub, new SelectionContext(true, "user", Optional.empty(), ImmutableSet.of(), 1, Optional.empty()));
        // Verify record exists
        assertEqualsResourceGroup(globalSub, "2MB", 4, 3, 3, FAIR, 5, false, new Duration(Long.MAX_VALUE, MILLISECONDS), new Duration(Long.MAX_VALUE, MILLISECONDS), new Duration(Long.MAX_VALUE, MILLISECONDS), new Duration(Long.MAX_VALUE, MILLISECONDS));
        dao.updateResourceGroup(2, "sub", "3MB", 2, 1, 1, "weighted", 6, true, "1h", "1d", null, null, 1L, ENVIRONMENT);
        do {
            MILLISECONDS.sleep(500);
        }
        while (globalSub.getJmxExport() == false);
        // Verify update
        assertEqualsResourceGroup(globalSub, "3MB", 2, 1, 1, WEIGHTED, 6, true, new Duration(1, HOURS), new Duration(1, DAYS), new Duration(Long.MAX_VALUE, MILLISECONDS), new Duration(Long.MAX_VALUE, MILLISECONDS));
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
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, null, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, null, null, 1L, ENVIRONMENT);
        dao.insertSelector(2, null, null, null);
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        DbResourceGroupConfig config = new DbResourceGroupConfig();
        config.setExactMatchSelectorEnabled(true);
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {}, config, daoProvider.get(), ENVIRONMENT);
        manager.load();
        assertEquals(manager.getSelectors().size(), 2);
        assertTrue(manager.getSelectors().get(0) instanceof DbSourceExactMatchSelector);

        config.setExactMatchSelectorEnabled(false);
        manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {}, config, daoProvider.get(), ENVIRONMENT);
        manager.load();
        assertEquals(manager.getSelectors().size(), 1);
        assertFalse(manager.getSelectors().get(0) instanceof DbSourceExactMatchSelector);
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
            Duration queuedTimeLimit,
            Duration runningTimeLimit)
    {
        assertEquals(group.getSoftMemoryLimit(), DataSize.valueOf(softMemoryLimit));
        assertEquals(group.getInfo().getMaxQueuedQueries(), maxQueued);
        assertEquals(group.getInfo().getHardConcurrencyLimit(), hardConcurrencyLimit);
        assertEquals(group.getInfo().getSoftConcurrencyLimit(), softConcurrencyLimit);
        assertEquals(group.getSchedulingPolicy(), schedulingPolicy);
        assertEquals(group.getSchedulingWeight(), schedulingWeight);
        assertEquals(group.getJmxExport(), jmxExport);
        assertEquals(group.getSoftCpuLimit(), softCpuLimit);
        assertEquals(group.getHardCpuLimit(), hardCpuLimit);
        assertEquals(group.getQueuedTimeLimit(), queuedTimeLimit);
        assertEquals(group.getRunningTimeLimit(), runningTimeLimit);
    }
}
