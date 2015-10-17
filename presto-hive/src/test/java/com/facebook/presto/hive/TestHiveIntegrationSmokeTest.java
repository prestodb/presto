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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.TPCH_SCHEMA;
import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.facebook.presto.hive.HiveQueryRunner.createSampledSession;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestHiveIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    public TestHiveIntegrationSmokeTest()
            throws Exception
    {
        super(createQueryRunner(ORDERS), createSampledSession());
    }

    @Test
    public void testInformationSchemaTablesWithoutEqualityConstraint()
            throws Exception
    {
        @Language("SQL") String actual = "" +
                "SELECT lower(table_name) " +
                "FROM information_schema.tables " +
                "WHERE table_catalog = 'hive' AND table_schema LIKE 'tpch' AND table_name LIKE '%orders'";

        @Language("SQL") String expected = "" +
                "SELECT lower(table_name) " +
                "FROM information_schema.tables " +
                "WHERE table_name LIKE '%ORDERS'";

        assertQuery(actual, expected);
    }

    @Test
    public void testInformationSchemaColumnsWithoutEqualityConstraint()
            throws Exception
    {
        @Language("SQL") String actual = "" +
                "SELECT lower(table_name), lower(column_name) " +
                "FROM information_schema.columns " +
                "WHERE table_catalog = 'hive' AND table_schema = 'tpch' AND table_name LIKE '%orders%'";

        @Language("SQL") String expected = "" +
                "SELECT lower(table_name), lower(column_name) " +
                "FROM information_schema.columns " +
                "WHERE table_name LIKE '%ORDERS%'";

        assertQuery(actual, expected);
    }

    @Test
    public void createTableWithEveryType()
            throws Exception
    {
        @Language("SQL") String query = "" +
                "CREATE TABLE test_types_table AS " +
                "SELECT" +
                " 'foo' _varchar" +
                ", cast('bar' as varbinary) _varbinary" +
                ", 1 _bigint" +
                ", 3.14 _double" +
                ", true _boolean" +
                ", DATE '1980-05-07' _date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' _timestamp";

        assertQuery(query, "SELECT 1");

        MaterializedResult results = queryRunner.execute(getSession(), "SELECT * FROM test_types_table").toJdbcTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 3.14);
        assertEquals(row.getField(4), true);
        assertEquals(row.getField(5), new Date(new DateTime(1980, 5, 7, 0, 0, 0, UTC).getMillis()));
        assertEquals(row.getField(6), new Timestamp(new DateTime(1980, 5, 7, 11, 22, 33, 456, UTC).getMillis()));
        assertQueryTrue("DROP TABLE test_types_table");

        assertFalse(queryRunner.tableExists(getSession(), "test_types_table"));
    }

    @Test
    public void createPartitionedTable()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : HiveStorageFormat.values()) {
            createPartitionedTable(storageFormat);
        }
    }

    public void createPartitionedTable(HiveStorageFormat storageFormat)
            throws Exception
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table (" +
                "  _partition_varchar VARCHAR" +
                ", _partition_bigint BIGINT" +
                ", _varchar VARCHAR" +
                ", _varbinary VARBINARY" +
                ", _bigint BIGINT" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ '_partition_varchar', '_partition_bigint' ]" +
                ") ";

        assertQuery(createTable, "SELECT 1");

        TableMetadata tableMetadata = getTableMetadata("test_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> partitionedBy = ImmutableList.of("_partition_varchar", "_partition_bigint");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            assertEquals(columnMetadata.isPartitionKey(), partitionedBy.contains(columnMetadata.getName()));
        }

        MaterializedResult result = computeActual("SELECT * from test_partitioned_table");
        assertEquals(result.getRowCount(), 0);

        assertQueryTrue("DROP TABLE test_partitioned_table");

        assertFalse(queryRunner.tableExists(getSession(), "test_partitioned_table"));
    }

    @Test
    public void createTableAs()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : HiveStorageFormat.values()) {
            createTableAs(storageFormat);
        }
    }

    public void createTableAs(HiveStorageFormat storageFormat)
            throws Exception
    {
        @Language("SQL") String select = "SELECT" +
                " 'foo' _varchar" +
                ", 1 _bigint" +
                ", 3.14 _double" +
                ", true _boolean";

        String createTableAs = String.format("CREATE TABLE test_format_table WITH (format = '%s') AS %s", storageFormat, select);

        assertQuery(createTableAs, "SELECT 1");

        TableMetadata tableMetadata = getTableMetadata("test_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertQuery("SELECT * from test_format_table", select);

        assertQueryTrue("DROP TABLE test_format_table");

        assertFalse(queryRunner.tableExists(getSession(), "test_format_table"));
    }

    @Test
    public void createPartitionedTableAs()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : HiveStorageFormat.values()) {
            createPartitionedTableAs(storageFormat);
        }
    }

    public void createPartitionedTableAs(HiveStorageFormat storageFormat)
            throws Exception
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertQuery(createTable, "SELECT count(*) from orders");

        TableMetadata tableMetadata = getTableMetadata("test_create_partitioned_table_as");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> partitionedBy = ImmutableList.of("ship_priority", "order_status");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            assertEquals(columnMetadata.isPartitionKey(), partitionedBy.contains(columnMetadata.getName()));
        }

        List<HivePartition> partitions = getPartitions("test_create_partitioned_table_as");
        assertEquals(partitions.size(), 3);

        // Hive will reorder the partition keys to the end
        assertQuery("SELECT * from test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertQueryTrue("DROP TABLE test_create_partitioned_table_as");

        assertFalse(queryRunner.tableExists(getSession(), "test_create_partitioned_table_as"));
    }

    @Test
    public void insertTable()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : HiveStorageFormat.values()) {
            insertTable(storageFormat);
        }
    }

    public void insertTable(HiveStorageFormat storageFormat)
            throws Exception
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_insert_format_table " +
                "(" +
                "  _varchar VARCHAR," +
                "  _bigint BIGINT," +
                "  _doube DOUBLE," +
                "  _boolean BOOLEAN" +
                ") " +
                "WITH (format = '" + storageFormat + "') ";

        assertQuery(createTable, "SELECT 1");

        TableMetadata tableMetadata = getTableMetadata("test_insert_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        @Language("SQL") String select = "SELECT" +
                " 'foo' _varchar" +
                ", 1 _bigint" +
                ", 3.14 _double" +
                ", true _boolean";

        assertQuery("INSERT INTO test_insert_format_table " + select, "SELECT 1");

        assertQuery("SELECT * from test_insert_format_table", select);

        assertQueryTrue("DROP TABLE test_insert_format_table");

        assertFalse(queryRunner.tableExists(getSession(), "test_insert_format_table"));
    }

    @Test
    public void insertPartitionedTable()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : HiveStorageFormat.values()) {
            insertPartitionedTable(storageFormat);
        }
    }

    public void insertPartitionedTable(HiveStorageFormat storageFormat)
            throws Exception
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_insert_partitioned_table " +
                "(" +
                "  ORDER_STATUS VARCHAR," +
                "  SHIP_PRIORITY BIGINT," +
                "  ORDER_KEY BIGINT" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") ";

        assertQuery(createTable, "SELECT 1");

        TableMetadata tableMetadata = getTableMetadata("test_insert_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        // Hive will reorder the partition keys, so we must insert into the table assuming the partition keys have been moved to the end
        assertQuery("" +
                        "INSERT INTO test_insert_partitioned_table " +
                        "SELECT orderkey, shippriority, orderstatus " +
                        "FROM tpch.tiny.orders",
                "SELECT count(*) from orders");

        // verify the partitions
        List<HivePartition> partitions = getPartitions("test_insert_partitioned_table");
        assertEquals(partitions.size(), 3);

        assertQuery("SELECT * from test_insert_partitioned_table", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertQueryTrue("DROP TABLE test_insert_partitioned_table");

        assertFalse(queryRunner.tableExists(getSession(), "test_insert_partitioned_table"));
    }

    @Test
    public void testDeleteFromUnpartitionedTable()
            throws Exception
    {
        assertQuery("CREATE TABLE test_delete_unpartitioned (x bigint, y varchar)", "SELECT 1");

        try {
            queryRunner.execute("DELETE FROM test_delete_unpartitioned");
            fail("expected exception");
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "This connector only supports delete where one or more partitions are deleted entirely");
        }
    }

    @Test
    public void testMetadataDelete()
            throws Exception
    {
        for (HiveStorageFormat storageFormat : HiveStorageFormat.values()) {
            testMetadataDelete(storageFormat);
        }
    }

    private void testMetadataDelete(HiveStorageFormat storageFormat)
            throws Exception
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_metadata_delete " +
                "(" +
                "  ORDER_STATUS VARCHAR," +
                "  SHIP_PRIORITY BIGINT," +
                "  ORDER_KEY BIGINT" +
                ") " +
                "WITH (" +
                STORAGE_FORMAT_PROPERTY + " = '" + storageFormat + "', " +
                PARTITIONED_BY_PROPERTY + " = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") ";

        assertQuery(createTable, "SELECT 1");

        // Hive will reorder the partition keys, so we must insert into the table assuming the partition keys have been moved to the end
        assertQuery("" +
                        "INSERT INTO test_metadata_delete " +
                        "SELECT orderkey, shippriority, orderstatus " +
                        "FROM tpch.tiny.orders",
                "SELECT count(*) from orders");

        // Delete returns number of rows deleted, or null if obtaining the number is hard or impossible.
        // Currently, Hive implementation always returns null.
        assertQuery("DELETE FROM test_metadata_delete WHERE ORDER_STATUS='O'", "SELECT null");

        assertQuery("SELECT * from test_metadata_delete", "SELECT orderkey, shippriority, orderstatus FROM orders WHERE orderstatus<>'O'");

        try {
            queryRunner.execute("DELETE FROM test_metadata_delete WHERE ORDER_KEY=1");
            fail("expected exception");
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "This connector only supports delete where one or more partitions are deleted entirely");
        }

        assertQuery("SELECT * from test_metadata_delete", "SELECT orderkey, shippriority, orderstatus FROM orders WHERE orderstatus<>'O'");

        assertQueryTrue("DROP TABLE test_metadata_delete");

        assertFalse(queryRunner.tableExists(getSession(), "test_metadata_delete"));
    }

    private TableMetadata getTableMetadata(String tableName)
    {
        Session session = getSession();
        Metadata metadata = ((DistributedQueryRunner) queryRunner).getCoordinator().getMetadata();
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, new QualifiedTableName(HIVE_CATALOG, TPCH_SCHEMA, tableName));
        assertTrue(tableHandle.isPresent());
        return metadata.getTableMetadata(session, tableHandle.get());
    }

    private List<HivePartition> getPartitions(String tableName)
    {
        Session session = getSession();
        Metadata metadata = ((DistributedQueryRunner) queryRunner).getCoordinator().getMetadata();
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, new QualifiedTableName(HIVE_CATALOG, TPCH_SCHEMA, tableName));
        assertTrue(tableHandle.isPresent());

        List<TableLayoutResult> layouts = metadata.getLayouts(session, tableHandle.get(), Constraint.alwaysTrue(), Optional.empty());
        TableLayout layout = Iterables.getOnlyElement(layouts).getLayout();
        List<HivePartition> partitions = ((HiveTableLayoutHandle) layout.getHandle().getConnectorHandle()).getPartitions().get();
        return partitions;
    }

    // TODO: These should be moved to another class, when more connectors support arrays
    @Test
    public void testArrays()
            throws Exception
    {
        assertQuery("CREATE TABLE tmp_array1 AS SELECT ARRAY[1, 2, NULL] AS col", "SELECT 1");
        assertQuery("SELECT col[2] FROM tmp_array1", "SELECT 2");
        assertQuery("SELECT col[3] FROM tmp_array1", "SELECT NULL");

        assertQuery("CREATE TABLE tmp_array2 AS SELECT ARRAY[1.0, 2.5, 3.5] AS col", "SELECT 1");
        assertQuery("SELECT col[2] FROM tmp_array2", "SELECT 2.5");

        assertQuery("CREATE TABLE tmp_array3 AS SELECT ARRAY['puppies', 'kittens', NULL] AS col", "SELECT 1");
        assertQuery("SELECT col[2] FROM tmp_array3", "SELECT 'kittens'");
        assertQuery("SELECT col[3] FROM tmp_array3", "SELECT NULL");

        assertQuery("CREATE TABLE tmp_array4 AS SELECT ARRAY[TRUE, NULL] AS col", "SELECT 1");
        assertQuery("SELECT col[1] FROM tmp_array4", "SELECT TRUE");
        assertQuery("SELECT col[2] FROM tmp_array4", "SELECT NULL");

        assertQuery("CREATE TABLE tmp_array5 AS SELECT ARRAY[ARRAY[1, 2], NULL, ARRAY[3, 4]] AS col", "SELECT 1");
        assertQuery("SELECT col[1][2] FROM tmp_array5", "SELECT 2");

        assertQuery("CREATE TABLE tmp_array6 AS SELECT ARRAY[ARRAY['\"hi\"'], NULL, ARRAY['puppies']] AS col", "SELECT 1");
        assertQuery("SELECT col[1][1] FROM tmp_array6", "SELECT '\"hi\"'");
        assertQuery("SELECT col[3][1] FROM tmp_array6", "SELECT 'puppies'");
    }

    @Test
    public void testTemporalArrays()
            throws Exception
    {
        assertQuery("CREATE TABLE tmp_array7 AS SELECT ARRAY[DATE '2014-09-30'] AS col", "SELECT 1");
        assertOneNotNullResult("SELECT col[1] FROM tmp_array7");
        assertQuery("CREATE TABLE tmp_array8 AS SELECT ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", "SELECT 1");
        assertOneNotNullResult("SELECT col[1] FROM tmp_array8");
    }

    @Test
    public void testMaps()
            throws Exception
    {
        assertQuery("CREATE TABLE tmp_map1 AS SELECT MAP(ARRAY[0,1], ARRAY[2,NULL]) AS col", "SELECT 1");
        assertQuery("SELECT col[0] FROM tmp_map1", "SELECT 2");
        assertQuery("SELECT col[1] FROM tmp_map1", "SELECT NULL");

        assertQuery("CREATE TABLE tmp_map2 AS SELECT MAP(ARRAY[1.0], ARRAY[2.5]) AS col", "SELECT 1");
        assertQuery("SELECT col[1.0] FROM tmp_map2", "SELECT 2.5");

        assertQuery("CREATE TABLE tmp_map3 AS SELECT MAP(ARRAY['puppies'], ARRAY['kittens']) AS col", "SELECT 1");
        assertQuery("SELECT col['puppies'] FROM tmp_map3", "SELECT 'kittens'");

        assertQuery("CREATE TABLE tmp_map4 AS SELECT MAP(ARRAY[TRUE], ARRAY[FALSE]) AS col", "SELECT 1");
        assertQuery("SELECT col[TRUE] FROM tmp_map4", "SELECT FALSE");

        assertQuery("CREATE TABLE tmp_map5 AS SELECT MAP(ARRAY[1.0], ARRAY[ARRAY[1, 2]]) AS col", "SELECT 1");
        assertQuery("SELECT col[1.0][2] FROM tmp_map5", "SELECT 2");

        assertQuery("CREATE TABLE tmp_map6 AS SELECT MAP(ARRAY[DATE '2014-09-30'], ARRAY[DATE '2014-09-29']) AS col", "SELECT 1");
        assertOneNotNullResult("SELECT col[DATE '2014-09-30'] FROM tmp_map6");
        assertQuery("CREATE TABLE tmp_map7 AS SELECT MAP(ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'], ARRAY[TIMESTAMP '2001-08-22 03:04:05.321']) AS col", "SELECT 1");
        assertOneNotNullResult("SELECT col[TIMESTAMP '2001-08-22 03:04:05.321'] FROM tmp_map7");
    }

    @Test
    public void testRows()
            throws Exception
    {
        assertQuery("CREATE TABLE tmp_row1 AS SELECT test_row(1, CAST(NULL as BIGINT)) AS a",
                "SELECT 1");

        assertQuery(
                "SELECT a.col0, a.col1 FROM tmp_row1",
                "SELECT 1, cast(null as bigint)");
    }

    @Test
    public void testComplex()
            throws Exception
    {
        assertQuery("CREATE TABLE tmp_complex1 AS SELECT " +
                "ARRAY [MAP(ARRAY['a', 'b'], ARRAY[2.0, 4.0]), MAP(ARRAY['c', 'd'], ARRAY[12.0, 14.0])] AS a",
                "SELECT 1");

        assertQuery(
                "SELECT a[1]['a'], a[2]['d'] FROM tmp_complex1",
                "SELECT 2.0, 14.0");
    }

    private void assertOneNotNullResult(@Language("SQL") String query)
    {
        MaterializedResult results = queryRunner.execute(getSession(), query).toJdbcTypes();
        assertEquals(results.getRowCount(), 1);
        assertEquals(results.getMaterializedRows().get(0).getFieldCount(), 1);
        assertNotNull(results.getMaterializedRows().get(0).getField(0));
    }
}
