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

import com.facebook.presto.resourceGroups.ManagerSpec;
import com.facebook.presto.resourceGroups.SelectorSpec;
import com.facebook.presto.resourceGroups.reloading.ReloadingResourceGroupConfig;
import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDbManagerSpecProvider
{
    private static final String ENVIRONMENT = "test";

    private static H2DaoProvider setup(String prefix)
    {
        DbResourceGroupConfig config = new DbResourceGroupConfig().setConfigDbUrl("jdbc:h2:mem:test_" + prefix + System.nanoTime() + "_" + ThreadLocalRandom.current().nextInt());
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
        dao.insertResourceGroup(1, "prod_global", "10MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", "1h", "1MB", "1h", null, prodEnvironment);
        dao.insertResourceGroup(2, "dev_global", "1MB", 1000, 100, 100, "weighted", null, true, "1h", "1d", "1h", "1MB", "1h", null, devEnvironment);
        dao.insertSelector(1, 1, ".*prod_user.*", null, null, null, null);
        dao.insertSelector(2, 2, ".*dev_user.*", null, null, null, null);

        // check the prod configuration
        DbManagerSpecProvider dbManagerSpecProvider = new DbManagerSpecProvider(daoProvider.get(), prodEnvironment, new ReloadingResourceGroupConfig());
        ManagerSpec managerSpec = dbManagerSpecProvider.getManagerSpec();
        assertEquals(managerSpec.getRootGroups().size(), 1);
        assertEquals(managerSpec.getSelectors().size(), 1);
        SelectorSpec prodSelector = managerSpec.getSelectors().get(0);
        assertEquals(prodSelector.getGroup().toString(), "prod_global");

        // check the dev configuration
        dbManagerSpecProvider = new DbManagerSpecProvider(daoProvider.get(), devEnvironment, new ReloadingResourceGroupConfig());
        managerSpec = dbManagerSpecProvider.getManagerSpec();
        assertEquals(managerSpec.getRootGroups().size(), 1);
        assertEquals(managerSpec.getSelectors().size(), 1);
        prodSelector = managerSpec.getSelectors().get(0);
        assertEquals(prodSelector.getGroup().toString(), "dev_global");
    }

    @Test
    public void testSelectorPriority()
    {
        H2DaoProvider daoProvider = setup("selectors");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "100%", 100, 100, 100, null, null, null, null, null, null, null, null, null, ENVIRONMENT);

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

        DbManagerSpecProvider dbManagerSpecProvider = new DbManagerSpecProvider(daoProvider.get(), ENVIRONMENT, new ReloadingResourceGroupConfig());
        List<SelectorSpec> selectors = dbManagerSpecProvider.getManagerSpec().getSelectors();
        assertEquals(selectors.size(), expectedUsers.size());

        // when we load the selectors we expect the selector list to be ordered by priority
        expectedUsers.sort(Comparator.<String>comparingInt(Integer::parseInt).reversed());

        for (int i = 0; i < numberOfUsers; i++) {
            Optional<Pattern> user = selectors.get(i).getUserRegex();
            assertTrue(user.isPresent());
            assertEquals(user.get().pattern(), expectedUsers.get(i));
        }
    }

    @Test
    public void testDuplicates()
    {
        H2DaoProvider daoProvider = setup("test_dup_roots");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, null, ENVIRONMENT);
        try {
            dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, null, ENVIRONMENT);
            fail("Expected to fail");
        }
        catch (RuntimeException ex) {
            assertTrue(ex instanceof UnableToExecuteStatementException);
            assertTrue(ex.getCause() instanceof JdbcSQLIntegrityConstraintViolationException);
            assertTrue(ex.getCause().getMessage().startsWith("Unique index or primary key violation"));
        }
        dao.insertSelector(1, 1, null, null, null, null, null);
        daoProvider = setup("test_dup_subs");
        dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "sub", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, 1L, ENVIRONMENT);
        try {
            dao.insertResourceGroup(2, "sub", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, 1L, ENVIRONMENT);
        }
        catch (RuntimeException ex) {
            assertTrue(ex instanceof UnableToExecuteStatementException);
            assertTrue(ex.getCause() instanceof JdbcSQLIntegrityConstraintViolationException);
            assertTrue(ex.getCause().getMessage().startsWith("Unique index or primary key violation"));
        }

        dao.insertSelector(2, 2, null, null, null, null, null);
    }

    @Test
    public void testSubgroups()
    {
        H2DaoProvider daoProvider = setup("test_dup_roots");
        H2ResourceGroupsDao dao = daoProvider.get();
        dao.createResourceGroupsGlobalPropertiesTable();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.insertResourceGroup(1, "global", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, null, ENVIRONMENT);
        dao.insertResourceGroup(2, "subTo1-1", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, 1L, ENVIRONMENT);
        dao.insertResourceGroup(3, "subTo1-2", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, 1L, ENVIRONMENT);
        dao.insertResourceGroup(4, "subTo2-1", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, 2L, ENVIRONMENT);
        dao.insertResourceGroup(5, "subTo2-2", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, 2L, ENVIRONMENT);
        dao.insertResourceGroup(6, "subTo3", "1MB", 1000, 100, 100, null, null, null, null, null, null, null, null, 3L, ENVIRONMENT);
        DbManagerSpecProvider dbManagerSpecProvider = new DbManagerSpecProvider(daoProvider.get(), ENVIRONMENT, new ReloadingResourceGroupConfig());
        ManagerSpec managerSpec = dbManagerSpecProvider.getManagerSpec();
        assertEquals(managerSpec.getRootGroups().size(), 1);
        assertEquals(managerSpec.getRootGroups().get(0).getName().toString(), "global");
        assertEquals(managerSpec.getRootGroups().get(0).getSubGroups().size(), 2);
        assertEquals(managerSpec.getRootGroups().get(0).getSubGroups().get(0).getName().toString(), "subTo1-1");
        assertEquals(managerSpec.getRootGroups().get(0).getSubGroups().get(0).getSubGroups().size(), 2);
        assertEquals(managerSpec.getRootGroups().get(0).getSubGroups().get(0).getSubGroups().get(1).getName().toString(), "subTo2-2");
    }
}
