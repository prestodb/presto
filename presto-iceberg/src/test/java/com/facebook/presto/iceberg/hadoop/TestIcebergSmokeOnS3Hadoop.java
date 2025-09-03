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
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
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
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;

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
        catalogWarehouseDir = new Path(createTempDirectory(bucketName).toUri()).toString();
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

            validateShowCreateTable(tableName,
                    ImmutableList.of(columnDefinition("a", "integer"), columnDefinition("b", "varchar")),
                    getCustomizedTableProperties(ImmutableMap.of(
                            "location", "'" + location + "'",
                            "write.data.path", "'" + dataWriteLocation + "'")));
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

        validateShowCreateTable(tableName,
                ImmutableList.of(
                        columnDefinition("order_key", "bigint"),
                        columnDefinition("ship_priority", "integer"),
                        columnDefinition("order_status", "varchar")),
                getCustomizedTableProperties(ImmutableMap.of(
                        "write.format.default", "'" + fileFormat + "'",
                        "location", "'" + getLocation(getSession().getSchema().get(), tableName) + "'",
                        "partitioning", "ARRAY['order_status','ship_priority','bucket(order_key, 9)']",
                        "write.data.path", "'" + getPathBasedOnDataDirectory(getSession().getSchema().get() + "/" + tableName) + "'")));

        assertQuery(session, "SELECT * from " + tableName, "SELECT orderkey, shippriority, orderstatus FROM orders");

        dropTable(session, tableName);
    }

    @Override
    protected void testCreateTableLike()
    {
        Session session = getSession();
        String schemaName = session.getSchema().get();

        assertUpdate(session, "CREATE TABLE test_create_table_like_original (col1 INTEGER, aDate DATE) WITH(format = 'PARQUET', partitioning = ARRAY['aDate'])");
        validatePropertiesForShowCreateTable("test_create_table_like_original",
                getCustomizedTableProperties(ImmutableMap.of(
                        "location", "'" + getLocation(schemaName, "test_create_table_like_original") + "'",
                        "partitioning", "ARRAY['adate']",
                        "write.data.path", "'" + getPathBasedOnDataDirectory(schemaName + "/test_create_table_like_original") + "'")));

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy0 (LIKE test_create_table_like_original, col2 INTEGER)");
        assertUpdate(session, "INSERT INTO test_create_table_like_copy0 (col1, aDate, col2) VALUES (1, CAST('1950-06-28' AS DATE), 3)", 1);
        assertQuery(session, "SELECT * from test_create_table_like_copy0", "VALUES(1, CAST('1950-06-28' AS DATE), 3)");
        dropTable(session, "test_create_table_like_copy0");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy1 (LIKE test_create_table_like_original)");

        validatePropertiesForShowCreateTable("test_create_table_like_copy1",
                getCustomizedTableProperties(ImmutableMap.of(
                                "location", "'" + getLocation(schemaName, "test_create_table_like_copy1") + "'",
                                "write.data.path", "'" + getPathBasedOnDataDirectory(schemaName + "/test_create_table_like_copy1") + "'")));
        dropTable(session, "test_create_table_like_copy1");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy2 (LIKE test_create_table_like_original EXCLUDING PROPERTIES)");
        validatePropertiesForShowCreateTable("test_create_table_like_copy2",
                getCustomizedTableProperties(ImmutableMap.of(
                                "location", "'" + getLocation(schemaName, "test_create_table_like_copy2") + "'",
                                "write.data.path", "'" + getPathBasedOnDataDirectory(schemaName + "/test_create_table_like_copy2") + "'")));
        dropTable(session, "test_create_table_like_copy2");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy5 (LIKE test_create_table_like_original INCLUDING PROPERTIES)" +
                " WITH (location = '', \"write.data.path\" = '', format = 'ORC')");

        validatePropertiesForShowCreateTable("test_create_table_like_copy5",
                getCustomizedTableProperties(ImmutableMap.of(
                        "write.format.default", "'ORC'",
                        "location", "'" + getLocation(schemaName, "test_create_table_like_copy5") + "'",
                        "partitioning", "ARRAY['adate']",
                        "write.data.path", "'" + getPathBasedOnDataDirectory(schemaName + "/test_create_table_like_copy5") + "'")));
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

        validateShowCreateTable(
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                tableName,
                ImmutableList.of(
                        columnDefinition("order_key", "bigint"),
                        columnDefinition("ship_priority", "integer"),
                        columnDefinition("order_status", "varchar")),
                null,
                getCustomizedTableProperties(ImmutableMap.of(
                        "write.delete.mode", "'" + defaultDeleteMode + "'",
                        "format-version", "'" + formatVersion + "'",
                        "location", "'" + getLocation(getSession().getSchema().get(), tableName) + "'",
                        "write.data.path", "'" + getPathBasedOnDataDirectory(getSession().getSchema().get() + "/" + tableName) + "'",
                        "write.update.mode", "'" + defaultDeleteMode + "'")));

        dropTable(session, tableName);
    }

    @Override
    public void testShowCreateTable()
    {
        String schemaName = getSession().getSchema().get();
        validateShowCreateTable(
                "iceberg",
                schemaName,
                "orders",
                ImmutableList.of(
                        columnDefinition("orderkey", "bigint"),
                        columnDefinition("custkey", "bigint"),
                        columnDefinition("orderstatus", "varchar"),
                        columnDefinition("totalprice", "double"),
                        columnDefinition("orderdate", "date"),
                        columnDefinition("orderpriority", "varchar"),
                        columnDefinition("clerk", "varchar"),
                        columnDefinition("shippriority", "integer"),
                        columnDefinition("comment", "varchar")),
                null,
                getCustomizedTableProperties(ImmutableMap.of(
                                "location", "'" + getLocation(schemaName, "orders") + "'",
                                "write.data.path", "'" + getPathBasedOnDataDirectory(schemaName + "/orders") + "'")));
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

        validateShowCreateTable("iceberg", schemaName, "test_table_comments",
                ImmutableList.of(columnDefinition("_x", "bigint")),
                "test table comment",
                getCustomizedTableProperties(ImmutableMap.of(
                        "write.format.default", "'ORC'",
                        "location", "'" + getLocation(schemaName, "test_table_comments") + "'",
                        "write.data.path", "'" + getPathBasedOnDataDirectory(schemaName + "/test_table_comments") + "'")));

        dropTable(session, "test_table_comments");
    }

    @Test
    public void testAddColumnWithMultiplePartitionTransforms()
    {
        Session session = getSession();
        String catalog = session.getCatalog().get();
        String schema = format("\"%s\"", session.getSchema().get());

        assertQuerySucceeds("create table add_multiple_partition_column(a int)");
        assertUpdate("insert into add_multiple_partition_column values 1", 1);

        validateShowCreateTable(catalog, schema, "add_multiple_partition_column",
                ImmutableList.of(columnDefinition("a", "integer")),
                null,
                getCustomizedTableProperties(ImmutableMap.of(
                        "location", "'" + getLocation(session.getSchema().get(), "add_multiple_partition_column") + "'",
                        "write.data.path", "'" + getPathBasedOnDataDirectory(session.getSchema().get() + "/add_multiple_partition_column") + "'")));

        // Add a varchar column with partition transforms `ARRAY['bucket(4)', 'truncate(2)', 'identity']`
        assertQuerySucceeds("alter table add_multiple_partition_column add column b varchar with(partitioning = ARRAY['bucket(4)', 'truncate(2)', 'identity'])");
        assertUpdate("insert into add_multiple_partition_column values(2, '1002')", 1);

        validateShowCreateTable(catalog, schema, "add_multiple_partition_column",
                ImmutableList.of(
                        columnDefinition("a", "integer"),
                        columnDefinition("b", "varchar")),
                null,
                getCustomizedTableProperties(ImmutableMap.of(
                        "location", "'" + getLocation(session.getSchema().get(), "add_multiple_partition_column") + "'",
                        "partitioning", "ARRAY['bucket(b, 4)','truncate(b, 2)','b']",
                        "write.data.path", "'" + getPathBasedOnDataDirectory(session.getSchema().get() + "/add_multiple_partition_column") + "'")));

        // Add a date column with partition transforms `ARRAY['year', 'bucket(8)', 'identity']`
        assertQuerySucceeds("alter table add_multiple_partition_column add column c date with(partitioning = ARRAY['year', 'bucket(8)', 'identity'])");
        assertUpdate("insert into add_multiple_partition_column values(3, '1003', date '1984-12-08')", 1);

        validateShowCreateTable(catalog, schema, "add_multiple_partition_column",
                ImmutableList.of(
                        columnDefinition("a", "integer"),
                        columnDefinition("b", "varchar"),
                        columnDefinition("c", "date")),
                null,
                getCustomizedTableProperties(ImmutableMap.of(
                        "location", "'" + getLocation(session.getSchema().get(), "add_multiple_partition_column") + "'",
                        "partitioning", "ARRAY['bucket(b, 4)','truncate(b, 2)','b','year(c)','bucket(c, 8)','c']",
                        "write.data.path", "'" + getPathBasedOnDataDirectory(session.getSchema().get() + "/add_multiple_partition_column") + "'")));

        assertQuery("select * from add_multiple_partition_column",
                "values(1, null, null), (2, '1002', null), (3, '1003', date '1984-12-08')");
        dropTable(getSession(), "add_multiple_partition_column");
    }

    @Test
    public void testAddColumnWithRedundantOrDuplicatedPartitionTransforms()
    {
        Session session = getSession();
        String catalog = session.getCatalog().get();
        String schema = format("\"%s\"", session.getSchema().get());

        assertQuerySucceeds("create table add_redundant_partition_column(a int)");

        // Specify duplicated transforms would fail
        assertQueryFails("alter table add_redundant_partition_column add column b varchar with(partitioning = ARRAY['bucket(4)', 'truncate(2)', 'bucket(4)'])",
                "Cannot add duplicate partition field: .*");
        assertQueryFails("alter table add_redundant_partition_column add column b varchar with(partitioning = ARRAY['identity', 'identity'])",
                "Cannot add duplicate partition field: .*");

        // Specify redundant transforms would fail
        assertQueryFails("alter table add_redundant_partition_column add column c date with(partitioning = ARRAY['year', 'month'])",
                "Cannot add redundant partition field: .*");
        assertQueryFails("alter table add_redundant_partition_column add column c timestamp with(partitioning = ARRAY['day', 'hour'])",
                "Cannot add redundant partition field: .*");

        validateShowCreateTable(catalog, schema, "add_redundant_partition_column",
                ImmutableList.of(columnDefinition("a", "integer")),
                null,
                getCustomizedTableProperties(ImmutableMap.of(
                        "location", "'" + getLocation(session.getSchema().get(), "add_redundant_partition_column") + "'",
                        "write.data.path", "'" + getPathBasedOnDataDirectory(session.getSchema().get() + "/add_redundant_partition_column") + "'")));

        dropTable(getSession(), "add_redundant_partition_column");
    }

    @Test
    public void testAddColumnWithUnsupportedPropertyValueTypes()
    {
        Session session = getSession();
        String catalog = session.getCatalog().get();
        String schema = format("\"%s\"", session.getSchema().get());

        assertQuerySucceeds("create table add_invalid_partition_column(a int)");

        assertQueryFails("alter table add_invalid_partition_column add column b varchar with(partitioning = 123)",
                "Invalid value for column property 'partitioning': Cannot convert '123' to array\\(varchar\\) or any of \\[varchar]");
        assertQueryFails("alter table add_invalid_partition_column add column b varchar with(partitioning = ARRAY[123, 234])",
                "Invalid value for column property 'partitioning': Cannot convert 'ARRAY\\[123,234]' to array\\(varchar\\) or any of \\[varchar]");

        validateShowCreateTable(catalog, schema, "add_invalid_partition_column",
                ImmutableList.of(columnDefinition("a", "integer")),
                null,
                getCustomizedTableProperties(ImmutableMap.of(
                        "location", "'" + getLocation(session.getSchema().get(), "add_invalid_partition_column") + "'",
                        "write.data.path", "'" + getPathBasedOnDataDirectory(session.getSchema().get() + "/add_invalid_partition_column") + "'")));

        dropTable(getSession(), "add_invalid_partition_column");
    }

    @Override
    protected String getLocation(String schema, String table)
    {
        Path tempLocation = getCatalogDirectory();
        return format("%s/%s/%s", tempLocation.toString(), schema, table);
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
