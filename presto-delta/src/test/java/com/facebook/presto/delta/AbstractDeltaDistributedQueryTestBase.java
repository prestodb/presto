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
package com.facebook.presto.delta;

import com.facebook.presto.Session;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Path;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.US;

public abstract class AbstractDeltaDistributedQueryTestBase
        extends AbstractTestQueryFramework
{
    public static final String DELTA_CATALOG = "delta";
    public static final String HIVE_CATALOG = "hive";
    public static final String PATH_SCHEMA = "$path$";
    public static final String DELTA_SCHEMA = "deltaTables"; // Schema in Hive which has test Delta tables

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaQueryRunner(ImmutableMap.of());
    }

    /**
     * Helper method to
     * - registering Delta table given location as a Hive table and
     * - run the test query, expected results query and compare the results.
     */
    protected void assertDeltaQuery(String deltaTable, String testQuery, String expResultsQuery)
    {
        try {
            registerDeltaTableInHMS(deltaTable, deltaTable);
            assertQuery(testQuery, expResultsQuery);
        }
        finally {
            unregisterDeltaTableInHMS(deltaTable);
        }
    }

    /**
     * Helper method to
     * - registering Delta table given location as a Hive table and
     * - run the test query, expect the query to fail and failure contains the expected message
     */
    protected void assertDeltaQueryFails(String deltaTable, String testQuery, String expFailure)
    {
        try {
            registerDeltaTableInHMS(deltaTable, deltaTable);
            assertQueryFails(testQuery, expFailure);
        }
        finally {
            unregisterDeltaTableInHMS(deltaTable);
        }
    }

    protected static String goldenTablePath(String tableName)
    {
        return TestDeltaIntegration.class.getClassLoader().getResource(tableName).toString();
    }

    private static DistributedQueryRunner createDeltaQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(DELTA_SCHEMA.toLowerCase(US))
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .build();

        // Install the TPCH plugin for test data (not in Delta format)
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_metadata");
        Path catalogDir = dataDir.getParent().resolve("catalog");

        // Install a Delta connector catalog
        queryRunner.installPlugin(new DeltaPlugin());
        Map<String, String> deltaProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDir.toFile().toURI().toString())
                .build();
        queryRunner.createCatalog(DELTA_CATALOG, "delta", deltaProperties);

        // Install a Hive connector catalog that uses the same metastore as Delta
        // This catalog will be used to create tables in metastore as the Delta connector doesn't
        // support creating tables yet.
        queryRunner.installPlugin(new HivePlugin("hive"));
        Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDir.toFile().toURI().toString())
                .put("hive.allow-drop-table", "true")
                .put("hive.security", "legacy")
                .build();
        queryRunner.createCatalog(HIVE_CATALOG, "hive", hiveProperties);
        queryRunner.execute(format("CREATE SCHEMA %s.%s", HIVE_CATALOG, DELTA_SCHEMA));

        return queryRunner;
    }

    /**
     * Register the given <i>deltaTableName</i> as <i>hiveTableName</i> in HMS using the Hive storage catalog.
     * Hive and Delta catalogs share the same HMS in this test. Hive is used to register the tables as Delta
     * connector doesn't have the write support yet.
     *
     * @param deltaTableName Name of the delta table which is on the classpath.
     * @param hiveTableName Name of the Hive table that the Delta table is to be registered as in HMS
     */
    private void registerDeltaTableInHMS(String deltaTableName, String hiveTableName)
    {
        getQueryRunner().execute(format(
                "CREATE TABLE %s.\"%s\".\"%s\" (dummyColumn INT) WITH (external_location = '%s')",
                HIVE_CATALOG,
                DELTA_SCHEMA,
                hiveTableName,
                goldenTablePath(deltaTableName)));
    }

    /**
     * Drop the given table from HMS
     */
    private void unregisterDeltaTableInHMS(String hiveTableName)
    {
        getQueryRunner().execute(
                format("DROP TABLE IF EXISTS %s.\"%s\".\"%s\"", HIVE_CATALOG, DELTA_SCHEMA, hiveTableName));
    }
}
