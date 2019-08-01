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
import com.facebook.presto.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.resourceGroups.ResourceGroupSpec;
import com.facebook.presto.resourceGroups.StaticSelector;
import com.facebook.presto.resourceGroups.VariableMap;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.SchedulingPolicy;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import com.facebook.presto.spi.resourceGroups.SelectionCriteria;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

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
    private static final ResourceEstimates EMPTY_RESOURCE_ESTIMATES = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty());

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
        dao.insertResourceGroup(1, "prod_global", "10MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, prodEnvironment);
        dao.insertResourceGroup(2, "dev_global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, devEnvironment);
        dao.insertSelector(1, 1, ".*prod_user.*", null, null, null, null);
        dao.insertSelector(2, 2, ".*dev_user.*", null, null, null, null);

        // check the prod configuration
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {}, new DbResourceGroupConfig(), daoProvider.get(), prodEnvironment);
        List<ResourceGroupSpec> groups = manager.getRootGroups();
        assertEquals(groups.size(), 1);
        InternalResourceGroup prodGlobal = new InternalResourceGroup.RootInternalResourceGroup("prod_global", (group, export) -> {}, directExecutor());
        manager.configure(prodGlobal, new SelectionContext<>(prodGlobal.getId(), new VariableMap(ImmutableMap.of("USER", "user"))));
        assertEqualsResourceGroup(prodGlobal, "10MB", 1000, 100, 100, WEIGHTED, DEFAULT_WEIGHT, true, new Duration(1, HOURS), new Duration(1, DAYS));
        assertEquals(manager.getSelectors().size(), 1);
        ResourceGroupSelector prodSelector = manager.getSelectors().get(0);
        ResourceGroupId prodResourceGroupId = prodSelector.match(new SelectionCriteria(true, "prod_user", Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.empty())).get().getResourceGroupId();
        assertEquals(prodResourceGroupId.toString(), "prod_global");

        // check the dev configuration
        manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {}, new DbResourceGroupConfig(), daoProvider.get(), devEnvironment);
        assertEquals(groups.size(), 1);
        InternalResourceGroup devGlobal = new InternalResourceGroup.RootInternalResourceGroup("dev_global", (group, export) -> {}, directExecutor());
        manager.configure(devGlobal, new SelectionContext<>(prodGlobal.getId(), new VariableMap(ImmutableMap.of("USER", "user"))));
        assertEqualsResourceGroup(devGlobal, "1MB", 1000, 100, 100, WEIGHTED, DEFAULT_WEIGHT, true, new Duration(1, HOURS), new Duration(1, DAYS));
        assertEquals(manager.getSelectors().size(), 1);
        ResourceGroupSelector devSelector = manager.getSelectors().get(0);
        ResourceGroupId devResourceGroupId = devSelector.match(new SelectionCriteria(true, "dev_user", Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.empty())).get().getResourceGroupId();
        assertEquals(devResourceGroupId.toString(), "dev_global");
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
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, 1L, ENVIRONMENT);
        dao.insertSelector(2, 1, null, null, null, null, null);
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {}, new DbResourceGroupConfig(), daoProvider.get(), ENVIRONMENT);
        AtomicBoolean exported = new AtomicBoolean();
        InternalResourceGroup global = new InternalResourceGroup.RootInternalResourceGroup("global", (group, export) -> exported.set(export), directExecutor());
        manager.configure(global, new SelectionContext<>(global.getId(), new VariableMap(ImmutableMap.of("USER", "user"))));
        assertEqualsResourceGroup(global, "1MB", 1000, 100, 100, WEIGHTED, DEFAULT_WEIGHT, true, new Duration(1, HOURS), new Duration(1, DAYS));
        exported.set(false);
        InternalResourceGroup sub = global.getOrCreateSubGroup("sub");
        manager.configure(sub, new SelectionContext<>(sub.getId(), new VariableMap(ImmutableMap.of("USER", "user"))));
        assertEqualsResourceGroup(sub, "2MB", 4, 3, 3, FAIR, 5, false, new Duration(Long.MAX_VALUE, MILLISECONDS), new Duration(Long.MAX_VALUE, MILLISECONDS));
    }

    @Test
    public void testDuplicates()
    {
        H2DaoProvider daoProvider = setup("test_dup_roots");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, null, null, null, null, null, null, ENVIRONMENT);
        try {
            dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, null, null, null, null, null, null, ENVIRONMENT);
            fail("Expected to fail");
        }
        catch (RuntimeException ex) {
            assertTrue(ex instanceof UnableToExecuteStatementException);
            assertTrue(ex.getCause() instanceof org.h2.jdbc.JdbcSQLException);
            assertTrue(ex.getCause().getMessage().startsWith("Unique index or primary key violation"));
        }
        dao.insertSelector(1, 1, null, null, null, null, null);
        daoProvider = setup("test_dup_subs");
        dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "1MB", 1000, 100, 100, null, null, null, null, null, 1L, ENVIRONMENT);
        try {
            dao.insertResourceGroup(2, "sub", "1MB", 1000, 100, 100, null, null, null, null, null, 1L, ENVIRONMENT);
        }
        catch (RuntimeException ex) {
            assertTrue(ex instanceof UnableToExecuteStatementException);
            assertTrue(ex.getCause() instanceof org.h2.jdbc.JdbcSQLException);
            assertTrue(ex.getCause().getMessage().startsWith("Unique index or primary key violation"));
        }

        dao.insertSelector(2, 2, null, null, null, null, null);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "No matching configuration found for: missing")
    public void testMissing()
    {
        H2DaoProvider daoProvider = setup("test_missing");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, 1L, ENVIRONMENT);
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        dao.insertSelector(2, 1, null, null, null, null, null);
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {}, new DbResourceGroupConfig(), daoProvider.get(), ENVIRONMENT);
        InternalResourceGroup missing = new InternalResourceGroup.RootInternalResourceGroup("missing", (group, export) -> {}, directExecutor());
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
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, 1L, ENVIRONMENT);
        dao.insertSelector(2, 1, null, null, null, null, null);
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {}, new DbResourceGroupConfig(), daoProvider.get(), ENVIRONMENT);
        manager.start();
        AtomicBoolean exported = new AtomicBoolean();
        InternalResourceGroup global = new InternalResourceGroup.RootInternalResourceGroup("global", (group, export) -> exported.set(export), directExecutor());
        manager.configure(global, new SelectionContext<>(global.getId(), new VariableMap(ImmutableMap.of("USER", "user"))));
        InternalResourceGroup globalSub = global.getOrCreateSubGroup("sub");
        manager.configure(globalSub, new SelectionContext<>(globalSub.getId(), new VariableMap(ImmutableMap.of("USER", "user"))));
        // Verify record exists
        assertEqualsResourceGroup(globalSub, "2MB", 4, 3, 3, FAIR, 5, false, new Duration(Long.MAX_VALUE, MILLISECONDS), new Duration(Long.MAX_VALUE, MILLISECONDS));
        dao.updateResourceGroup(2, "sub", "3MB", 2, 1, 1, "weighted", 6, true, "1h", "1d", 1L, ENVIRONMENT);
        do {
            MILLISECONDS.sleep(500);
        }
        while (globalSub.getJmxExport() == false);
        // Verify update
        assertEqualsResourceGroup(globalSub, "3MB", 2, 1, 1, WEIGHTED, 6, true, new Duration(1, HOURS), new Duration(1, DAYS));
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
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, 3, null, 5, null, null, null, 1L, ENVIRONMENT);
        dao.insertSelector(2, 1, null, null, null, null, null);
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

    @Test
    public void testSelectorPriority()
    {
        H2DaoProvider daoProvider = setup("selectors");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "100%", 100, 100, 100, null, null, null, null, null, null, ENVIRONMENT);

        final int numberOfUsers = 100;
        List<String> expectedUsers = new ArrayList<>();

        int[] randomPriorities = ThreadLocalRandom.current()
                .ints(0, 1000)
                .distinct()
                .limit(numberOfUsers)
                .toArray();

        // insert several selectors with unique random priority where userRegex is equal to the priority
        for (int i = 0; i < numberOfUsers; i++) {
            int priority = randomPriorities[i];
            String user = String.valueOf(priority);
            dao.insertSelector(1, priority, user, ".*", null, null, null);
            expectedUsers.add(user);
        }

        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {}, new DbResourceGroupConfig(), daoProvider.get(), ENVIRONMENT);
        manager.load();

        List<ResourceGroupSelector> selectors = manager.getSelectors();
        assertEquals(selectors.size(), expectedUsers.size());

        // when we load the selectors we expect the selector list to be ordered by priority
        expectedUsers.sort(Comparator.<String>comparingInt(Integer::parseInt).reversed());

        for (int i = 0; i < numberOfUsers; i++) {
            Optional<Pattern> user = ((StaticSelector) selectors.get(i)).getUserRegex();
            assertTrue(user.isPresent());
            assertEquals(user.get().pattern(), expectedUsers.get(i));
        }
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No selectors are configured")
    public void testInvalidConfiguration()
    {
        H2DaoProvider daoProvider = setup("selectors");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "100%", 100, 100, 100, null, null, null, null, null, null, ENVIRONMENT);

        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(
                (poolId, listener) -> {},
                new DbResourceGroupConfig().setMaxRefreshInterval(Duration.valueOf("1ms")),
                daoProvider.get(),
                ENVIRONMENT);

        manager.getSelectors();
    }

    @Test
    public void testRefreshInterval()
    {
        H2DaoProvider daoProvider = setup("selectors");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "100%", 100, 100, 100, null, null, null, null, null, null, ENVIRONMENT);

        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(
                (poolId, listener) -> {},
                new DbResourceGroupConfig().setMaxRefreshInterval(Duration.valueOf("1ms")),
                daoProvider.get(),
                ENVIRONMENT);

        dao.dropSelectorsTable();
        manager.load();

        try {
            manager.getSelectors();
            fail("Expected unavailable configuration exception");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Resource group configuration cannot be fetched from database."));
        }

        try {
            manager.getRootGroups();
            fail("Expected unavailable configuration exception");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Resource group configuration cannot be fetched from database."));
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
            Duration hardCpuLimit)
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
    }
}
