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

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.resourceGroups.db.DbResourceGroupConfig;
import com.facebook.presto.resourceGroups.db.H2DaoProvider;
import com.facebook.presto.resourceGroups.db.H2ResourceGroupsDao;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.resourceGroups.ResourceGroupSelector;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;

import java.util.List;
import java.util.Random;
import java.util.Set;

import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryState.TERMINAL_QUERY_STATES;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.json.JsonCodec.listJsonCodec;
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
        while (queryManager.getAllQueryInfo().stream()
                .filter(q -> countingStates.contains(q.getState())).count() != expectedCount) {
            MILLISECONDS.sleep(500);
        }
    }

    public static String getDbConfigUrl()
    {
        return "jdbc:h2:mem:test_" + Math.abs(new Random().nextLong());
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
        return createQueryRunner(dbConfigUrl, dao, TEST_ENVIRONMENT);
    }

    public static DistributedQueryRunner createQueryRunner(String dbConfigUrl, H2ResourceGroupsDao dao, String environment)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(
                testSessionBuilder().setCatalog("tpch").setSchema("tiny").build(),
                2,
                ImmutableMap.of("experimental.resource-groups-enabled", "true"),
                ImmutableMap.of(),
                new SqlParserOptions(),
                environment);
        try {
            Plugin h2ResourceGroupManagerPlugin = new H2ResourceGroupManagerPlugin();
            queryRunner.installPlugin(h2ResourceGroupManagerPlugin);
            queryRunner.getCoordinator().getResourceGroupManager().get()
                    .setConfigurationManager(CONFIGURATION_MANAGER_TYPE, ImmutableMap.of("resource-groups.config-db-url", dbConfigUrl, "node.environment", environment));
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            setup(queryRunner, dao, environment);
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
        dao.insertResourceGroup(1, "global", "1MB", 100, 1000, 1000, null, null, null, null, null, null, null, null, TEST_ENVIRONMENT);
        dao.insertResourceGroup(2, "bi-${USER}", "1MB", 3, 2, 2, null, null, null, null, null, null, null, 1L, TEST_ENVIRONMENT);
        dao.insertResourceGroup(3, "user-${USER}", "1MB", 3, 3, 3, null, null, null, null, null, null, null, 1L, TEST_ENVIRONMENT);
        dao.insertResourceGroup(4, "adhoc-${USER}", "1MB", 3, 3, 3, null, null, null, null, null, null, null, 3L, TEST_ENVIRONMENT);
        dao.insertResourceGroup(5, "dashboard-${USER}", "1MB", 1, 1, 1, null, null, null, null, null, null, null, 3L, TEST_ENVIRONMENT);
        dao.insertResourceGroup(6, "no-queueing", "1MB", 0, 1, 1, null, null, null, null, null, null, null, null, TEST_ENVIRONMENT_2);
        dao.insertSelector(2, "user.*", "test", null);
        dao.insertSelector(4, "user.*", "(?i).*adhoc.*", null);
        dao.insertSelector(5, "user.*", "(?i).*dashboard.*", null);
        dao.insertSelector(4, "user.*", null, CLIENT_TAGS_CODEC.toJson(ImmutableList.of("tag1", "tag2")));
        dao.insertSelector(2, "user.*", null, CLIENT_TAGS_CODEC.toJson(ImmutableList.of("tag1")));
        dao.insertSelector(6, ".*", ".*", null);

        int expectedSelectors = 5;
        if (environment.equals(TEST_ENVIRONMENT_2)) {
            expectedSelectors = 1;
        }

        // Selectors are loaded last
        while (getSelectors(queryRunner).size() != expectedSelectors) {
            MILLISECONDS.sleep(500);
        }
    }

    public static List<ResourceGroupSelector> getSelectors(DistributedQueryRunner queryRunner)
    {
        return queryRunner.getCoordinator().getResourceGroupManager().get().getConfigurationManager().getSelectors();
    }
}
