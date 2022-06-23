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
package com.facebook.presto.execution.resourceGroups.db;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.resourceGroups.db.DbResourceGroupConfig;
import com.facebook.presto.resourceGroups.db.H2DaoProvider;
import com.facebook.presto.resourceGroups.db.H2ResourceGroupsDao;
import com.facebook.presto.resourceGroups.reloading.ReloadingResourceGroupConfigurationManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryState.TERMINAL_QUERY_STATES;
import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static com.facebook.presto.spi.resourceGroups.QueryType.EXPLAIN;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class H2TestUtil
{
    private static final String CONFIGURATION_MANAGER_TYPE = "h2";
    public static final String TEST_ENVIRONMENT = "test_environment";
    public static final String TEST_ENVIRONMENT_2 = "test_environment_2";
    public static final JsonCodec<List<String>> CLIENT_TAGS_CODEC = listJsonCodec(String.class);

    private H2TestUtil() {}

    public static Session adhocSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource("adhoc")
                .build();
    }

    public static Session testSession(Identity identity)
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource("abc")
                .setIdentity(identity)
                .build();
    }

    public static Session dashboardSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource("dashboard")
                .build();
    }

    public static Session rejectingSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource("reject")
                .build();
    }

    public static void waitForCompleteQueryCount(DistributedQueryRunner queryRunner, int expectedCount)
            throws InterruptedException
    {
        waitForQueryCount(queryRunner, TERMINAL_QUERY_STATES, expectedCount);
    }

    public static void waitForRunningQueryCount(DistributedQueryRunner queryRunner, int expectedCount)
            throws InterruptedException
    {
        waitForQueryCount(queryRunner, ImmutableSet.of(RUNNING), expectedCount);
    }

    public static void waitForQueryCount(DistributedQueryRunner queryRunner, Set<QueryState> countingStates, int expectedCount)
            throws InterruptedException
    {
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        while (queryManager.getQueries().stream()
                .filter(q -> countingStates.contains(q.getState())).count() != expectedCount) {
            MILLISECONDS.sleep(500);
        }
    }

    public static String getDbConfigUrl()
    {
        return "jdbc:h2:mem:test_" + System.nanoTime() + "_" + ThreadLocalRandom.current().nextInt();
    }

    public static H2ResourceGroupsDao getDao(String url)
    {
        DbResourceGroupConfig dbResourceGroupConfig = new DbResourceGroupConfig()
                .setConfigDbUrl(url);
        H2ResourceGroupsDao dao = new H2DaoProvider(dbResourceGroupConfig).get();
        dao.createResourceGroupsTable();
        dao.createSelectorsTable();
        dao.createResourceGroupsGlobalPropertiesTable();
        return dao;
    }

    public static DistributedQueryRunner createQueryRunner(String dbConfigUrl, H2ResourceGroupsDao dao)
            throws Exception
    {
        return createQueryRunner(dbConfigUrl, dao, TEST_ENVIRONMENT, ImmutableMap.of(), 1);
    }

    public static DistributedQueryRunner createQueryRunner(String dbConfigUrl, H2ResourceGroupsDao dao, int coordinatorCount)
            throws Exception
    {
        return createQueryRunner(dbConfigUrl, dao, TEST_ENVIRONMENT, ImmutableMap.of(), coordinatorCount);
    }

    public static DistributedQueryRunner createQueryRunner(String dbConfigUrl, H2ResourceGroupsDao dao, Map<String, String> coordinatorProperties, int coordinatorCount)
            throws Exception
    {
        return createQueryRunner(dbConfigUrl, dao, TEST_ENVIRONMENT, coordinatorProperties, coordinatorCount);
    }

    public static DistributedQueryRunner createQueryRunner(String dbConfigUrl, H2ResourceGroupsDao dao, String environment, Map<String, String> coordinatorProperties, int coordinatorCount)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner
                .builder(testSessionBuilder().setCatalog("tpch").setSchema("tiny").build())
                .setNodeCount(2)
                .setCoordinatorCount(coordinatorCount)
                .setEnvironment(environment)
                .setResourceManagerEnabled(true)
                .setCoordinatorProperties(coordinatorProperties)
                .build();
        try {
            Plugin h2ResourceGroupManagerPlugin = new H2ResourceGroupManagerPlugin();
            queryRunner.installPlugin(h2ResourceGroupManagerPlugin);
            for (int coordinator = 0; coordinator < coordinatorCount; coordinator++) {
                queryRunner.getCoordinator(coordinator).getResourceGroupManager().get()
                        .setConfigurationManager(CONFIGURATION_MANAGER_TYPE, ImmutableMap.of("resource-groups.config-db-url", dbConfigUrl, "node.environment", environment));
            }
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            setup(queryRunner, dao, environment);
            queryRunner.waitForClusterToGetReady();
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public static DistributedQueryRunner getSimpleQueryRunner()
            throws Exception
    {
        String dbConfigUrl = getDbConfigUrl();
        H2ResourceGroupsDao dao = getDao(dbConfigUrl);
        return createQueryRunner(dbConfigUrl, dao);
    }

    private static void setup(DistributedQueryRunner queryRunner, H2ResourceGroupsDao dao, String environment)
            throws InterruptedException
    {
        dao.insertResourceGroupsGlobalProperties("cpu_quota_period", "1h");
        dao.insertResourceGroup(1, "global", "1MB", 100, 1000, 1000, null, null, null, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dao.insertResourceGroup(2, "bi-${USER}", "1MB", 3, 2, 2, null, null, null, null, null, null, null, null, 1L, TEST_ENVIRONMENT);
        dao.insertResourceGroup(3, "user-${USER}", "1MB", 3, 3, 3, null, null, null, null, null, null, null, null, 1L, TEST_ENVIRONMENT);
        dao.insertResourceGroup(4, "adhoc-${USER}", "1MB", 3, 3, 3, null, null, null, null, null, null, null, null, 3L, TEST_ENVIRONMENT);
        dao.insertResourceGroup(5, "dashboard-${USER}", "1MB", 1, 1, 1, null, null, null, null, null, null, null, null, 3L, TEST_ENVIRONMENT);
        dao.insertResourceGroup(6, "no-queueing", "1MB", 0, 1, 1, null, null, null, null, null, null, null, null, null, TEST_ENVIRONMENT_2);
        dao.insertResourceGroup(7, "explain", "1MB", 0, 1, 1, null, null, null, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dao.insertResourceGroup(8, "test", "1MB", 3, 3, 3, null, null, null, null, null, null, null, null, 1L, TEST_ENVIRONMENT);
        dao.insertResourceGroup(9, "test-${USER}", "1MB", 3, 3, 3, null, null, null, null, null, null, null, null, 8L, TEST_ENVIRONMENT);
        dao.insertSelector(2, 10_000, "user.*", "test", null, null, null);
        dao.insertSelector(4, 1_000, "user.*", "(?i).*adhoc.*", null, null, null);
        dao.insertSelector(5, 100, "user.*", "(?i).*dashboard.*", null, null, null);
        dao.insertSelector(4, 10, "user.*", null, null, CLIENT_TAGS_CODEC.toJson(ImmutableList.of("tag1", "tag2")), null);
        dao.insertSelector(2, 1, "user.*", null, null, CLIENT_TAGS_CODEC.toJson(ImmutableList.of("tag1")), null);
        dao.insertSelector(6, 6, ".*", ".*", null, null, null);
        dao.insertSelector(7, 100_000, null, null, EXPLAIN.name(), null, null);
        dao.insertSelector(9, 10_000, "user.*", "abc", null, null, null);

        int expectedSelectors = 7;
        if (environment.equals(TEST_ENVIRONMENT_2)) {
            expectedSelectors = 1;
        }

        // Selectors are loaded last
        for (int coordinator = 0; coordinator < queryRunner.getCoordinators().size(); coordinator++) {
            while (getSelectors(queryRunner, coordinator).size() != expectedSelectors) {
                MILLISECONDS.sleep(500);
            }
        }
    }

    public static List<ResourceGroupSelector> getSelectors(DistributedQueryRunner queryRunner)
    {
        checkState(queryRunner.getCoordinators().size() == 1, "Expected a single coordinator");
        return getSelectors(queryRunner, 0);
    }

    public static List<ResourceGroupSelector> getSelectors(DistributedQueryRunner queryRunner, int coordinator)
    {
        try {
            return ((ReloadingResourceGroupConfigurationManager) queryRunner.getCoordinator(coordinator).getResourceGroupManager().get().getConfigurationManager()).getSelectors();
        }
        catch (PrestoException e) {
            if (e.getErrorCode() == CONFIGURATION_INVALID.toErrorCode()) {
                return ImmutableList.of();
            }

            throw e;
        }
    }
}
