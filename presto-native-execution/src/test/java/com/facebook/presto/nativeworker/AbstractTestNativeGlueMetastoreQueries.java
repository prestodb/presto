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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * AbstractTestNativeGlueMetastoreQueries requires AWS credentials to be correctly set up in order to access Glue catalog.
 * You can configure credentials locally in ~/.aws/credentials before running the test. Alternatively, you can set up
 * AWS access/secret keys or IAM role in `setUpGlueCatalog` method in the test suite using the Glue config properties
 * from https://prestodb.io/docs/current/connector/hive.html#aws-glue-catalog-configuration-properties.
 */
abstract class AbstractTestNativeGlueMetastoreQueries
        extends AbstractTestQueryFramework
{
    private static final String TEMPORARY_PREFIX = "tmp_presto_glue_test_";
    // Catalog name used in the tests, it is also hardcoded in HiveExternalWorkerQueryRunner so workers can register
    // the connector so if you want to change the name, also update it in HiveExternalWorkerQueryRunner.
    private static final String CATALOG_NAME = "test_glue";

    // Warehouse directory for the Glue catalog.
    private File tempWarehouseDir;
    // Fully-qualified schema name in format [catalog].[schema] used in tests.
    private String schemaName;

    private final boolean useThrift;

    protected AbstractTestNativeGlueMetastoreQueries(boolean useThrift)
    {
        this.useThrift = useThrift;
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(useThrift);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner();
    }

    @BeforeClass
    public void setUp() throws IOException
    {
        tempWarehouseDir = Files.createTempDir();
        setUpGlueCatalog(getQueryRunner(), tempWarehouseDir);
        setUpGlueCatalog((QueryRunner) getExpectedQueryRunner(), tempWarehouseDir);

        schemaName = tempSchema("test_schema");
        getQueryRunner().execute("CREATE SCHEMA " + schemaName);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws IOException
    {
        deleteRecursively(tempWarehouseDir.toPath(), ALLOW_INSECURE);
        if (schemaName != null) {
            getQueryRunner().execute("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test
    public void testShowCatalogs()
    {
        // Glue catalog should be configured correctly.
        MaterializedResult result = getQueryRunner().execute("show catalogs like '%" + CATALOG_NAME + "%'");
        assertEquals(result.getMaterializedRows().size(), 1);
        for (MaterializedRow row : result.getMaterializedRows()) {
            assertEquals(row.getField(0), CATALOG_NAME);
        }
    }

    @Test
    public void testCreateDropSchema()
    {
        String schemaName = tempSchema("default");
        getQueryRunner().execute("CREATE SCHEMA " + schemaName);
        getQueryRunner().execute("DROP SCHEMA " + schemaName);
    }

    @Test
    public void testCreateInsertDropTable()
    {
        String tableName = tempTable(schemaName, "test_table");
        Session session = getSession();

        try {
            getQueryRunner().execute(session, "CREATE TABLE " + tableName + " (a int, b int) WITH (format = 'DWRF')");
            getQueryRunner().execute(session, "INSERT INTO " + tableName + " VALUES (1, 2)");
            assertQuery(session, "SELECT * FROM " + tableName);
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateView()
    {
        String viewName = tempTable(schemaName, "test_view");
        Session session = getSession();

        try {
            getQueryRunner().execute(session, "CREATE VIEW " + viewName + " AS SELECT * FROM customer");
            assertQuery(session, "SELECT * FROM " + viewName);
        }
        finally {
            getQueryRunner().execute(session, "DROP VIEW IF EXISTS " + viewName);
        }
    }

    @Test
    public void testRenameTable()
    {
        String tableName = tempTable(schemaName, "test_table");
        String newTableName = tempTable(schemaName, "renamed_table");
        Session session = getSession();

        try {
            getQueryRunner().execute(session, "CREATE TABLE " + tableName + " AS SELECT * FROM customer");
            assertQueryFails(session,
                    "ALTER TABLE " + tableName + " RENAME TO " + newTableName,
                    "Table rename is not yet supported by Glue service");
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testAddRenameDropColumn()
    {
        String tableName = tempTable(schemaName, "test_table");
        Session session = getSession();

        try {
            // Add column.
            getQueryRunner().execute(session, "CREATE TABLE " + tableName + " WITH (format = 'DWRF') AS SELECT acctbal, custkey FROM customer");
            getQueryRunner().execute(session, "ALTER TABLE " + tableName + " ADD COLUMN tmp_col INT COMMENT 'test column'");
            assertQuery(session, "SELECT * FROM " + tableName);
            assertTableColumnNames(tableName, "acctbal", "custkey", "tmp_col");

            // Rename column.
            getQueryRunner().execute(session, "ALTER TABLE " + tableName + " RENAME COLUMN tmp_col TO tmp_col2");
            assertQuery(session, "SELECT * FROM " + tableName);
            assertTableColumnNames(tableName, "acctbal", "custkey", "tmp_col2");

            // Drop column.
            getQueryRunner().execute(session, "ALTER TABLE " + tableName + " DROP COLUMN tmp_col2");
            assertQuery(session, "SELECT * FROM " + tableName);
            assertTableColumnNames(tableName, "acctbal", "custkey");
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCreateInsertDropPartitionedTable()
    {
        String tableName = tempTable(schemaName, "test_table");
        Session session = getSession();

        try {
            getQueryRunner().execute(session, "CREATE TABLE " + tableName + " (name VARCHAR, address VARCHAR, mktsegment VARCHAR) WITH (format = 'DWRF', partitioned_by = ARRAY['mktsegment'])");
            getQueryRunner().execute(session, "INSERT INTO " + tableName + " SELECT name, address, mktsegment FROM customer");
            assertQuery(session, "SELECT * FROM " + tableName);
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testPartitionFilters()
    {
        String tableName = tempTable(schemaName, "test_table");
        Session session = getSession();

        try {
            getQueryRunner().execute(session,
                    "CREATE TABLE " + tableName + "(" +
                            "orderkey bigint, " +
                            "custkey bigint, " +
                            "orderstatus varchar(1), " +
                            "totalprice double, " +
                            "orderdate varchar, " +
                            "shippriority int" +
                            ") WITH (format = 'DWRF', partitioned_by = ARRAY['orderdate', 'shippriority'])");
            getQueryRunner().execute(session,
                    "INSERT INTO " + tableName + " SELECT " +
                            "orderkey, custkey, orderstatus, totalprice, orderdate, shippriority FROM orders LIMIT 10");

            assertQuery(session, "SELECT * FROM " + tableName);
            assertQuery(session, "SELECT * FROM " + tableName + " WHERE orderdate = '1996-04-02'");
            assertQuery(session, "SELECT * FROM " + tableName + " WHERE orderdate = '1996-04-02' AND shippriority = 0");
            assertQuery(session, "SELECT * FROM " + tableName + " WHERE orderdate = '1996-04-02' OR orderdate = '1996-06-26'");

            assertQuery(session, "SELECT * FROM " + tableName + " WHERE shippriority = 1");
            assertQuery(session, "SELECT * FROM " + tableName + " WHERE orderdate = '2023-03-01' OR orderdate = '1990-01-01'");
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testAnalyzeTable()
    {
        String tableName = tempTable(schemaName, "test_table");
        Session session = getSession();

        try {
            getQueryRunner().execute(session,
                    "CREATE TABLE " + tableName + " WITH (format = 'DWRF', partitioned_by = ARRAY['orderdate', 'shippriority']) " +
                    "AS SELECT orderkey, custkey, orderstatus, totalprice, orderdate, shippriority FROM orders LIMIT 10");
            getQueryRunner().execute(session, "ANALYZE " + tableName);
        }
        finally {
            getQueryRunner().execute(session, "DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                // This option is required to avoid the error:
                // "Unknown plan node type com.facebook.presto.sql.planner.plan.TableWriterMergeNode".
                // TableWriterMergeNode is not supported by Velox yet.
                .setSystemProperty("table_writer_merge_operator_enabled", "false")
                .build();
    }

    private static void setUpGlueCatalog(QueryRunner queryRunner, File tempWarehouseDir) throws IOException
    {
        assertNotNull(queryRunner, "Query runner must be provided");
        assertNotNull(tempWarehouseDir, "Temp warehouse directory must be provided");

        queryRunner.installPlugin(new HivePlugin("glue"));
        Map<String, String> glueProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "glue")
                .put("hive.metastore.glue.region", "us-east-1")
                .put("hive.metastore.glue.default-warehouse-dir", tempWarehouseDir.getCanonicalPath())
                .put("hive.storage-format", "DWRF")
                // This option is required to avoid the error:
                // "Table scan with filter pushdown disabled is not supported".
                // Velox requires this flag to be set for scans.
                .put("hive.pushdown-filter-enabled", "true")
                // The options below are required to avoid "Access Denied" errors when performing operations such
                // as DROP TABLE.
                .put("hive.max-partitions-per-writers", "999")
                .put("hive.allow-drop-table", "true")
                .put("hive.allow-rename-table", "true")
                .put("hive.allow-rename-column", "true")
                .put("hive.allow-add-column", "true")
                .put("hive.allow-drop-column", "true")
                .build();

        queryRunner.createCatalog("test_glue", "glue", glueProperties);
    }

    private static String getRandomName(String prefix)
    {
        String randomName = UUID.randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        return TEMPORARY_PREFIX + prefix + randomName;
    }

    /**
     * Returns the fully-qualified schema name.
     */
    private static String tempSchema(String prefix)
    {
        return CATALOG_NAME + "." + getRandomName(prefix);
    }

    /**
     * Returns the fully-qualified table name.
     */
    private static String tempTable(String schemaName, String prefix)
    {
        return schemaName + "." + getRandomName(prefix);
    }
}
