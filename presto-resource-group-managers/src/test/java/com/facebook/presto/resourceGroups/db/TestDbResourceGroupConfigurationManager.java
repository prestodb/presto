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
import com.facebook.presto.spi.resourceGroups.SchedulingPolicy;
import com.facebook.presto.spi.resourceGroups.SelectionContext;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.testng.annotations.Test;

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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDbResourceGroupConfigurationManager
{
    static H2DaoProvider setup(String prefix)
    {
        DbResourceGroupConfig config = new DbResourceGroupConfig().setConfigDbUrl("jdbc:h2:mem:test_" + prefix + System.nanoTime());
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
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, "weighted", null, true, "1h", "1d", null);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, null, 5, null, null, null, 1L);
        dao.insertSelector(2, null, null);
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager((poolId, listener) -> { },
                daoProvider.get());
        AtomicBoolean exported = new AtomicBoolean();
        InternalResourceGroup global = new InternalResourceGroup.RootInternalResourceGroup("global", (group, export) -> exported.set(export), directExecutor());
        manager.configure(global, new SelectionContext(true, "user", Optional.empty(), 1));
        assertEqualsResourceGroup(global, "1MB", 1000, 100, WEIGHTED, DEFAULT_WEIGHT, true,  new Duration(1, HOURS), new Duration(1, DAYS));
        exported.set(false);
        InternalResourceGroup sub = global.getOrCreateSubGroup("sub");
        manager.configure(sub, new SelectionContext(true, "user", Optional.empty(), 1));
        assertEqualsResourceGroup(sub, "2MB", 4, 3, FAIR, 5, false, new Duration(Long.MAX_VALUE, MILLISECONDS), new Duration(Long.MAX_VALUE, MILLISECONDS));
    }

    @Test
    public void testDuplicates()
    {
        H2DaoProvider daoProvider = setup("test_dup_roots");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, null, null, null, null, null, null);
        try {
            dao.insertResourceGroup(1, "global", "1MB", 1000, 100, null, null, null, null, null, null);
            fail("Expected to fail");
        }
        catch (RuntimeException ex) {
            assertTrue(ex instanceof UnableToExecuteStatementException);
            assertTrue(ex.getCause() instanceof org.h2.jdbc.JdbcSQLException);
            assertTrue(ex.getCause().getMessage().startsWith("Unique index or primary key violation"));
        }
        dao.insertSelector(1, null, null);
        daoProvider = setup("test_dup_subs");
        dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, null, null, null, null, null, null);
        dao.insertResourceGroup(2, "sub", "1MB", 1000, 100, null, null, null, null, null, 1L);
        try {
            dao.insertResourceGroup(2, "sub", "1MB", 1000, 100, null, null, null, null, null, 1L);
        }
        catch (RuntimeException ex) {
            assertTrue(ex instanceof UnableToExecuteStatementException);
            assertTrue(ex.getCause() instanceof org.h2.jdbc.JdbcSQLException);
            assertTrue(ex.getCause().getMessage().startsWith("Unique index or primary key violation"));
        }

        dao.insertSelector(2, null, null);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "No matching configuration found for: missing")
    public void testMissing()
    {
        H2DaoProvider daoProvider = setup("test_missing");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, "weighted", null, true, "1h", "1d", null);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, null, 5, null, null, null, 1L);
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        dao.insertSelector(2, null, null);
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager((poolId, listener) -> {
        },
                daoProvider.get());
        InternalResourceGroup missing = new InternalResourceGroup.RootInternalResourceGroup("missing", (group, export) -> { }, directExecutor());
        manager.configure(missing, new SelectionContext(true, "user", Optional.empty(), 1));
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
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, "weighted", null, true, "1h", "1d", null);
        dao.insertResourceGroup(2, "sub", "2MB", 4, 3, null, 5, null, null, null, 1L);
        dao.insertSelector(2, null, null);
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        DbResourceGroupConfigurationManager manager = new DbResourceGroupConfigurationManager(
                (poolId, listener) -> { },
                daoProvider.get());
        manager.start();
        AtomicBoolean exported = new AtomicBoolean();
        InternalResourceGroup global = new InternalResourceGroup.RootInternalResourceGroup("global", (group, export) -> exported.set(export), directExecutor());
        manager.configure(global, new SelectionContext(true, "user", Optional.empty(), 1));
        InternalResourceGroup globalSub = global.getOrCreateSubGroup("sub");
        manager.configure(globalSub, new SelectionContext(true, "user", Optional.empty(), 1));
        // Verify record exists
        assertEqualsResourceGroup(globalSub, "2MB", 4, 3, FAIR, 5, false, new Duration(Long.MAX_VALUE, MILLISECONDS), new Duration(Long.MAX_VALUE, MILLISECONDS));
        dao.updateResourceGroup(2, "sub", "3MB", 2, 1, "weighted", 6, true, "1h", "1d", 1L);
        do {
            MILLISECONDS.sleep(500);
        } while(globalSub.getJmxExport() == false);
        // Verify update
        assertEqualsResourceGroup(globalSub, "3MB", 2, 1, WEIGHTED, 6, true, new Duration(1, HOURS), new Duration(1, DAYS));
        // Verify delete
        dao.deleteSelectors(2);
        dao.deleteResourceGroup(2);
        do {
            MILLISECONDS.sleep(500);
        } while(globalSub.getMaxQueuedQueries() != 0 || globalSub.getMaxRunningQueries() != 0);
    }

    private static void assertEqualsResourceGroup(
            InternalResourceGroup group,
            String softMemoryLimit,
            int maxQueued,
            int maxRunning,
            SchedulingPolicy schedulingPolicy,
            int schedulingWeight,
            boolean jmxExport,
            Duration softCpuLimit,
            Duration hardCpuLimit)
    {
        assertEquals(group.getSoftMemoryLimit(), DataSize.valueOf(softMemoryLimit));
        assertEquals(group.getInfo().getMaxQueuedQueries(), maxQueued);
        assertEquals(group.getInfo().getMaxRunningQueries(), maxRunning);
        assertEquals(group.getSchedulingPolicy(), schedulingPolicy);
        assertEquals(group.getSchedulingWeight(), schedulingWeight);
        assertEquals(group.getJmxExport(), jmxExport);
        assertEquals(group.getSoftCpuLimit(), softCpuLimit);
        assertEquals(group.getHardCpuLimit(), hardCpuLimit);
    }
}
