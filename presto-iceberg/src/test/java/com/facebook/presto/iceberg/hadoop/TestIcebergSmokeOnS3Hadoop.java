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
package com.facebook.presto.iceberg.hadoop;

import com.facebook.presto.Session;
import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.iceberg.IcebergNativeCatalogFactory;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.container.IcebergMinIODataLake;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeIcebergTable;
import static com.facebook.presto.iceberg.container.IcebergMinIODataLake.ACCESS_KEY;
import static com.facebook.presto.iceberg.container.IcebergMinIODataLake.SECRET_KEY;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestIcebergSmokeOnS3Hadoop
        extends IcebergDistributedSmokeTestBase
{
    static final String WAREHOUSE_DATA_DIR = "warehouse_data/";
    final String bucketName;
    final String catalogWarehouseDir;

    private IcebergMinIODataLake dockerizedS3DataLake;
    HostAndPort hostAndPort;

    public TestIcebergSmokeOnS3Hadoop()
            throws IOException
    {
        super(HADOOP);
        bucketName = "forhadoop-" + randomTableSuffix();
        catalogWarehouseDir = createTempDirectory(bucketName).toUri().toString();
    }

    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .setExtraConnectorProperties(ImmutableMap.of(
                        "iceberg.catalog.warehouse", catalogWarehouseDir,
                        "iceberg.catalog.hadoop.warehouse.datadir", getCatalogDataDirectory().toString(),
                        "hive.s3.aws-access-key", ACCESS_KEY,
                        "hive.s3.aws-secret-key", SECRET_KEY,
                        "hive.s3.endpoint", format("http://%s:%s", hostAndPort.getHost(), hostAndPort.getPort()),
                        "hive.s3.path-style-access", "true"))
                .build().getQueryRunner();
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        this.dockerizedS3DataLake = new IcebergMinIODataLake(bucketName, WAREHOUSE_DATA_DIR);
        this.dockerizedS3DataLake.start();
        hostAndPort = this.dockerizedS3DataLake.getMinio().getMinioApiEndpoint();
        super.init();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (dockerizedS3DataLake != null) {
            dockerizedS3DataLake.stop();
        }
    }

    @Test
    public void testShowCreateTableWithSpecifiedWriteDataLocation()
    {
        String tableName = "test_table_with_specified_write_data_location";
        String dataWriteLocation = getPathBasedOnDataDirectory("test-" + randomTableSuffix());
        try {
            assertUpdate(format("CREATE TABLE %s(a int, b varchar) with (\"write.data.path\" = '%s')", tableName, dataWriteLocation));
            String schemaName = getSession().getSchema().get();
            String location = getLocation(schemaName, tableName);
            String createTableSql = "CREATE TABLE iceberg.%s.%s (\n" +
                    "   \"a\" integer,\n" +
                    "   \"b\" varchar\n" +
                    ")\n" +
                    "WITH (\n" +
                    "   delete_mode = 'merge-on-read',\n" +
                    "   format = 'PARQUET',\n" +
                    "   format_version = '2',\n" +
                    "   location = '%s',\n" +
                    "   metadata_delete_after_commit = false,\n" +
                    "   metadata_previous_versions_max = 100,\n" +
                    "   metrics_max_inferred_column = 100,\n" +
                    "   \"read.split.target-size\" = 134217728,\n" +
                    "   \"write.data.path\" = '%s',\n" +
                    "   \"write.update.mode\" = 'merge-on-read'\n" +
                    ")";
            assertThat(computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue())
                    .isEqualTo(format(createTableSql, schemaName, tableName, location, dataWriteLocation));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testTableWithSpecifiedWriteDataLocation()
    {
        String tableName = "test_table_with_specified_write_data_location2";
        String dataWriteLocation = getPathBasedOnDataDirectory("test-" + randomTableSuffix());
        try {
            assertUpdate(format("create table %s(a int, b varchar) with (\"write.data.path\" = '%s')", tableName, dataWriteLocation));
            assertUpdate(format("insert into %s values(1, '1001'), (2, '1002'), (3, '1003')", tableName), 3);
            assertQuery("select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");
            assertUpdate(format("delete from %s where a > 2", tableName), 1);
            assertQuery("select * from " + tableName, "values(1, '1001'), (2, '1002')");
        }
        finally {
            assertUpdate("drop table if exists " + tableName);
        }
    }

    @Test
    public void testPartitionedTableWithSpecifiedWriteDataLocation()
    {
        String tableName = "test_table_with_specified_write_data_location3";
        String dataWriteLocation = getPathBasedOnDataDirectory("test-" + randomTableSuffix());
        try {
            assertUpdate(format("create table %s(a int, b varchar) with (partitioning = ARRAY['a'], \"write.data.path\" = '%s')", tableName, dataWriteLocation));
            assertUpdate(format("insert into %s values(1, '1001'), (2, '1002'), (3, '1003')", tableName), 3);
            assertQuery("select * from " + tableName, "values(1, '1001'), (2, '1002'), (3, '1003')");
            assertUpdate(format("delete from %s where a > 2", tableName), 1);
            assertQuery("select * from " + tableName, "values(1, '1001'), (2, '1002')");
        }
        finally {
            assertUpdate("drop table if exists " + tableName);
        }
    }

    @Override
    protected void testCreatePartitionedTableAs(Session session, FileFormat fileFormat)
    {
        String tableName = "test_create_partitioned_table_as_" + fileFormat.toString().toLowerCase(ENGLISH);
        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + fileFormat + "', " +
                "partitioning = ARRAY['ORDER_STATUS', 'Ship_Priority', 'Bucket(order_key,9)']" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(session, createTable, "SELECT count(*) from orders");

        String createTableSql = "" +
                "CREATE TABLE %s.%s.%s (\n" +
                "   \"order_key\" bigint,\n" +
                "   \"ship_priority\" integer,\n" +
                "   \"order_status\" varchar\n" +
                ")\n" +
                "WITH (\n" +
                "   delete_mode = 'merge-on-read',\n" +
                "   format = '" + fileFormat + "',\n" +
                "   format_version = '2',\n" +
                "   location = '%s',\n" +
                "   metadata_delete_after_commit = false,\n" +
                "   metadata_previous_versions_max = 100,\n" +
                "   metrics_max_inferred_column = 100,\n" +
                "   partitioning = ARRAY['order_status','ship_priority','bucket(order_key, 9)'],\n" +
                "   \"read.split.target-size\" = 134217728,\n" +
                "   \"write.data.path\" = '%s',\n" +
                "   \"write.update.mode\" = 'merge-on-read'\n" +
                ")";

        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()),
                format(createTableSql,
                        getSession().getCatalog().get(),
                        getSession().getSchema().get(),
                        tableName,
                        getLocation(getSession().getSchema().get(), tableName),
                        getPathBasedOnDataDirectory(getSession().getSchema().get() + "/" + tableName)));

        assertQuery(session, "SELECT * from " + tableName, "SELECT orderkey, shippriority, orderstatus FROM orders");

        dropTable(session, tableName);
    }

    @Override
    protected void testCreateTableLike()
    {
        Session session = getSession();
        String schemaName = session.getSchema().get();

        String tablePropertiesString = "WITH (\n" +
                "   delete_mode = 'merge-on-read',\n" +
                "   format = 'PARQUET',\n" +
                "   format_version = '2',\n" +
                "   location = '%s',\n" +
                "   metadata_delete_after_commit = false,\n" +
                "   metadata_previous_versions_max = 100,\n" +
                "   metrics_max_inferred_column = 100,\n" +
                "   partitioning = ARRAY['adate'],\n" +
                "   \"read.split.target-size\" = 134217728,\n" +
                "   \"write.data.path\" = '%s',\n" +
                "   \"write.update.mode\" = 'merge-on-read'\n" +
                ")";
        assertUpdate(session, "CREATE TABLE test_create_table_like_original (col1 INTEGER, aDate DATE) WITH(format = 'PARQUET', partitioning = ARRAY['aDate'])");
        assertEquals(getTablePropertiesString("test_create_table_like_original"),
                format(tablePropertiesString,
                        getLocation(schemaName, "test_create_table_like_original"),
                        getPathBasedOnDataDirectory(schemaName + "/test_create_table_like_original")));

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy0 (LIKE test_create_table_like_original, col2 INTEGER)");
        assertUpdate(session, "INSERT INTO test_create_table_like_copy0 (col1, aDate, col2) VALUES (1, CAST('1950-06-28' AS DATE), 3)", 1);
        assertQuery(session, "SELECT * from test_create_table_like_copy0", "VALUES(1, CAST('1950-06-28' AS DATE), 3)");
        dropTable(session, "test_create_table_like_copy0");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy1 (LIKE test_create_table_like_original)");
        tablePropertiesString = "WITH (\n" +
                "   delete_mode = 'merge-on-read',\n" +
                "   format = 'PARQUET',\n" +
                "   format_version = '2',\n" +
                "   location = '%s',\n" +
                "   metadata_delete_after_commit = false,\n" +
                "   metadata_previous_versions_max = 100,\n" +
                "   metrics_max_inferred_column = 100,\n" +
                "   \"read.split.target-size\" = 134217728,\n" +
                "   \"write.data.path\" = '%s',\n" +
                "   \"write.update.mode\" = 'merge-on-read'\n" +
                ")";
        assertEquals(getTablePropertiesString("test_create_table_like_copy1"),
                format(tablePropertiesString,
                        getLocation(schemaName, "test_create_table_like_copy1"),
                        getPathBasedOnDataDirectory(schemaName + "/test_create_table_like_copy1")));
        dropTable(session, "test_create_table_like_copy1");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy2 (LIKE test_create_table_like_original EXCLUDING PROPERTIES)");
        tablePropertiesString = "WITH (\n" +
                "   delete_mode = 'merge-on-read',\n" +
                "   format = 'PARQUET',\n" +
                "   format_version = '2',\n" +
                "   location = '%s',\n" +
                "   metadata_delete_after_commit = false,\n" +
                "   metadata_previous_versions_max = 100,\n" +
                "   metrics_max_inferred_column = 100,\n" +
                "   \"read.split.target-size\" = 134217728,\n" +
                "   \"write.data.path\" = '%s',\n" +
                "   \"write.update.mode\" = 'merge-on-read'\n" +
                ")";
        assertEquals(getTablePropertiesString("test_create_table_like_copy2"),
                format(tablePropertiesString,
                        getLocation(schemaName, "test_create_table_like_copy2"),
                        getPathBasedOnDataDirectory(schemaName + "/test_create_table_like_copy2")));
        dropTable(session, "test_create_table_like_copy2");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy5 (LIKE test_create_table_like_original INCLUDING PROPERTIES)" +
                " WITH (location = '', \"write.data.path\" = '', format = 'ORC')");
        tablePropertiesString = "WITH (\n" +
                "   delete_mode = 'merge-on-read',\n" +
                "   format = 'ORC',\n" +
                "   format_version = '2',\n" +
                "   location = '%s',\n" +
                "   metadata_delete_after_commit = false,\n" +
                "   metadata_previous_versions_max = 100,\n" +
                "   metrics_max_inferred_column = 100,\n" +
                "   partitioning = ARRAY['adate'],\n" +
                "   \"read.split.target-size\" = 134217728,\n" +
                "   \"write.data.path\" = '%s',\n" +
                "   \"write.update.mode\" = 'merge-on-read'\n" +
                ")";
        assertEquals(getTablePropertiesString("test_create_table_like_copy5"),
                format(tablePropertiesString,
                        getLocation(schemaName, "test_create_table_like_copy5"),
                        getPathBasedOnDataDirectory(schemaName + "/test_create_table_like_copy5")));
        dropTable(session, "test_create_table_like_copy5");

        assertQueryFails(session, "CREATE TABLE test_create_table_like_copy6 (LIKE test_create_table_like_original INCLUDING PROPERTIES)",
                "Cannot set a custom location for a path-based table.*");

        dropTable(session, "test_create_table_like_original");
    }

    @Override
    protected void testCreateTableWithFormatVersion(String formatVersion, String defaultDeleteMode)
    {
        String tableName = "test_create_table_with_format_version_" + formatVersion;
        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = 'PARQUET', " +
                "format_version = '" + formatVersion + "'" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        Session session = getSession();

        assertUpdate(session, createTable, "SELECT count(*) from orders");

        String createTableSql = "" +
                "CREATE TABLE %s.%s.%s (\n" +
                "   \"order_key\" bigint,\n" +
                "   \"ship_priority\" integer,\n" +
                "   \"order_status\" varchar\n" +
                ")\n" +
                "WITH (\n" +
                "   delete_mode = '%s',\n" +
                "   format = 'PARQUET',\n" +
                "   format_version = '%s',\n" +
                "   location = '%s',\n" +
                "   metadata_delete_after_commit = false,\n" +
                "   metadata_previous_versions_max = 100,\n" +
                "   metrics_max_inferred_column = 100,\n" +
                "   \"read.split.target-size\" = 134217728,\n" +
                "   \"write.data.path\" = '%s',\n" +
                "   \"write.update.mode\" = '%s'\n" +
                ")";

        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE " + tableName);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()),
                format(createTableSql,
                        getSession().getCatalog().get(),
                        getSession().getSchema().get(),
                        tableName,
                        defaultDeleteMode,
                        formatVersion,
                        getLocation(getSession().getSchema().get(), tableName),
                        getPathBasedOnDataDirectory(getSession().getSchema().get() + "/" + tableName),
                        defaultDeleteMode));

        dropTable(session, tableName);
    }

    @Override
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().get();
        String createTableSql = "CREATE TABLE iceberg.%s.orders (\n" +
                "   \"orderkey\" bigint,\n" +
                "   \"custkey\" bigint,\n" +
                "   \"orderstatus\" varchar,\n" +
                "   \"totalprice\" double,\n" +
                "   \"orderdate\" date,\n" +
                "   \"orderpriority\" varchar,\n" +
                "   \"clerk\" varchar,\n" +
                "   \"shippriority\" integer,\n" +
                "   \"comment\" varchar\n" +
                ")\n" +
                "WITH (\n" +
                "   delete_mode = 'merge-on-read',\n" +
                "   format = 'PARQUET',\n" +
                "   format_version = '2',\n" +
                "   location = '%s',\n" +
                "   metadata_delete_after_commit = false,\n" +
                "   metadata_previous_versions_max = 100,\n" +
                "   metrics_max_inferred_column = 100,\n" +
                "   \"read.split.target-size\" = 134217728,\n" +
                "   \"write.data.path\" = '%s',\n" +
                "   \"write.update.mode\" = 'merge-on-read'\n" +
                ")";
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo(format(createTableSql,
                        schemaName,
                        getLocation(schemaName, "orders"),
                        getPathBasedOnDataDirectory(schemaName + "/orders")));
    }

    @Test
    public void testTableComments()
    {
        Session session = getSession();
        String schemaName = session.getSchema().get();

        @Language("SQL") String createTable = "" +
                "CREATE TABLE iceberg.%s.test_table_comments (\n" +
                "   \"_x\" bigint\n" +
                ")\n" +
                "COMMENT '%s'\n" +
                "WITH (\n" +
                "   format = 'ORC',\n" +
                "   format_version = '2'\n" +
                ")";

        assertUpdate(format(createTable, schemaName, "test table comment"));

        String createTableSql = "" +
                "CREATE TABLE iceberg.%s.test_table_comments (\n" +
                "   \"_x\" bigint\n" +
                ")\n" +
                "COMMENT '%s'\n" +
                "WITH (\n" +
                "   delete_mode = 'merge-on-read',\n" +
                "   format = 'ORC',\n" +
                "   format_version = '2',\n" +
                "   location = '%s',\n" +
                "   metadata_delete_after_commit = false,\n" +
                "   metadata_previous_versions_max = 100,\n" +
                "   metrics_max_inferred_column = 100,\n" +
                "   \"read.split.target-size\" = 134217728,\n" +
                "   \"write.data.path\" = '%s',\n" +
                "   \"write.update.mode\" = 'merge-on-read'\n" +
                ")";

        MaterializedResult resultOfCreate = computeActual("SHOW CREATE TABLE test_table_comments");
        assertEquals(getOnlyElement(resultOfCreate.getOnlyColumnAsSet()),
                format(createTableSql, schemaName, "test table comment",
                        getLocation(schemaName, "test_table_comments"),
                        getPathBasedOnDataDirectory(schemaName + "/test_table_comments")));

        dropTable(session, "test_table_comments");
    }

    @Override
    protected String getLocation(String schema, String table)
    {
        Path tempLocation = getCatalogDirectory();
        return format("%s/%s/%s", tempLocation.toUri(), schema, table);
    }

    @Override
    protected Table getIcebergTable(ConnectorSession session, String schema, String tableName)
    {
        IcebergConfig icebergConfig = new IcebergConfig();
        icebergConfig.setCatalogType(HADOOP);
        icebergConfig.setCatalogWarehouse(getCatalogDirectory().toString());

        HiveS3Config hiveS3Config = new HiveS3Config()
                .setS3AwsAccessKey(ACCESS_KEY)
                .setS3AwsSecretKey(SECRET_KEY)
                .setS3PathStyleAccess(true)
                .setS3Endpoint(format("http://%s:%s", hostAndPort.getHost(), hostAndPort.getPort()));

        IcebergNativeCatalogFactory catalogFactory = new IcebergNativeCatalogFactory(icebergConfig,
                new IcebergCatalogName(ICEBERG_CATALOG),
                new PrestoS3ConfigurationUpdater(hiveS3Config),
                new HiveGcsConfigurationInitializer(new HiveGcsConfig()));

        return getNativeIcebergTable(catalogFactory,
                session,
                SchemaTableName.valueOf(schema + "." + tableName));
    }

    protected Path getCatalogDirectory()
    {
        return new Path(catalogWarehouseDir);
    }

    private Path getCatalogDataDirectory()
    {
        return new Path(URI.create(format("s3://%s/%s", bucketName, WAREHOUSE_DATA_DIR)));
    }

    private String getPathBasedOnDataDirectory(String name)
    {
        return new Path(getCatalogDataDirectory(), name).toString();
    }
}
