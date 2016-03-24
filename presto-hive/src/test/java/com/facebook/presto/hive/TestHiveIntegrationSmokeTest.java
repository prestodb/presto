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
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
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
import static com.facebook.presto.hive.HiveUtil.annotateColumnComment;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestHiveIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final String catalog;

    @SuppressWarnings("unused")
    public TestHiveIntegrationSmokeTest()
            throws Exception
    {
        this(createQueryRunner(ORDERS), createSampledSession(), HIVE_CATALOG);
    }

    protected TestHiveIntegrationSmokeTest(QueryRunner queryRunner, Session sampledSession, String catalog)
            throws Exception
    {
        super(queryRunner, sampledSession);
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    protected List<?> getPartitions(ConnectorTableLayoutHandle tableLayoutHandle)
    {
        return ((HiveTableLayoutHandle) tableLayoutHandle).getPartitions().get();
    }

    @Test
    public void testInformationSchemaTablesWithoutEqualityConstraint()
            throws Exception
    {
        @Language("SQL") String actual = "" +
                "SELECT lower(table_name) " +
                "FROM information_schema.tables " +
                "WHERE table_catalog = '" + catalog + "' AND table_schema LIKE 'tpch' AND table_name LIKE '%orders'";

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
                "WHERE table_catalog = '" + catalog + "' AND table_schema = 'tpch' AND table_name LIKE '%orders%'";

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

        assertUpdate(query, 1);

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
        assertUpdate("DROP TABLE test_types_table");

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
                "  _varchar VARCHAR" +
                ", _varbinary VARBINARY" +
                ", _bigint BIGINT" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                ", _partition_varchar VARCHAR" +
                ", _partition_bigint BIGINT" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ '_partition_varchar', '_partition_bigint' ]" +
                ") ";

        assertUpdate(createTable);

        TableMetadata tableMetadata = getTableMetadata("test_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> partitionedBy = ImmutableList.of("_partition_varchar", "_partition_bigint");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            boolean partitionKey = partitionedBy.contains(columnMetadata.getName());
            assertEquals(columnMetadata.getComment(), annotateColumnComment(null, partitionKey));
        }

        MaterializedResult result = computeActual("SELECT * from test_partitioned_table");
        assertEquals(result.getRowCount(), 0);

        assertUpdate("DROP TABLE test_partitioned_table");

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

        assertUpdate(createTableAs, 1);

        TableMetadata tableMetadata = getTableMetadata("test_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertQuery("SELECT * from test_format_table", select);

        assertUpdate("DROP TABLE test_format_table");

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

        assertUpdate(createTable, "SELECT count(*) from orders");

        TableMetadata tableMetadata = getTableMetadata("test_create_partitioned_table_as");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> partitionedBy = ImmutableList.of("ship_priority", "order_status");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            boolean partitionKey = partitionedBy.contains(columnMetadata.getName());
            assertEquals(columnMetadata.getComment(), annotateColumnComment(null, partitionKey));
        }

        List<?> partitions = getPartitions("test_create_partitioned_table_as");
        assertEquals(partitions.size(), 3);

        assertQuery("SELECT * from test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertUpdate("DROP TABLE test_create_partitioned_table_as");

        assertFalse(queryRunner.tableExists(getSession(), "test_create_partitioned_table_as"));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition keys must be the last columns in the table and in the same order as the table properties.*")
    public void testCreatePartitionedTableInvalidColumnOrdering()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_invalid_column_ordering\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['apple'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition keys must be the last columns in the table and in the same order as the table properties.*")
    public void testCreatePartitionedTableAsInvalidColumnOrdering()
            throws Exception
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_as_invalid_column_ordering " +
                "WITH (partitioned_by = ARRAY['SHIP_PRIORITY', 'ORDER_STATUS']) " +
                "AS " +
                "SELECT shippriority AS ship_priority, orderkey AS order_key, orderstatus AS order_status " +
                "FROM tpch.tiny.orders");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Table contains only partition columns")
    public void testCreateTableOnlyPartitionColumns()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_only_partition_columns\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['grape', 'apple', 'orange', 'pear'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition columns .* not present in schema")
    public void testCreateTableNonExistentPartitionColumns()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_nonexistent_partition_columns\n" +
                "(grape bigint, apple varchar, orange bigint, pear varchar)\n" +
                "WITH (partitioned_by = ARRAY['dragonfruit'])");
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
                "  _double DOUBLE," +
                "  _boolean BOOLEAN" +
                ") " +
                "WITH (format = '" + storageFormat + "') ";

        assertUpdate(createTable);

        TableMetadata tableMetadata = getTableMetadata("test_insert_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        @Language("SQL") String select = "SELECT" +
                " 'foo' _varchar" +
                ", 1 _bigint" +
                ", 3.14 _double" +
                ", true _boolean";

        assertUpdate("INSERT INTO test_insert_format_table " + select, 1);

        assertQuery("SELECT * from test_insert_format_table", select);

        assertUpdate("INSERT INTO test_insert_format_table (_bigint, _double) SELECT 2, 14.3", 1);

        assertQuery("SELECT * from test_insert_format_table where _bigint = 2", "SELECT null, 2, 14.3, null");

        assertUpdate("INSERT INTO test_insert_format_table (_double, _bigint) SELECT 2.72, 3", 1);

        assertQuery("SELECT * from test_insert_format_table where _bigint = 3", "SELECT null, 3, 2.72, null");

        assertUpdate("DROP TABLE test_insert_format_table");

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
                "  ORDER_KEY BIGINT," +
                "  SHIP_PRIORITY BIGINT," +
                "  ORDER_STATUS VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") ";

        assertUpdate(createTable);

        TableMetadata tableMetadata = getTableMetadata("test_insert_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        assertQuery(
                "SHOW PARTITIONS FROM test_insert_partitioned_table",
                "SELECT shippriority, orderstatus FROM orders LIMIT 0");

        // Hive will reorder the partition keys, so we must insert into the table assuming the partition keys have been moved to the end
        assertUpdate("" +
                        "INSERT INTO test_insert_partitioned_table " +
                        "SELECT orderkey, shippriority, orderstatus " +
                        "FROM tpch.tiny.orders",
                "SELECT count(*) from orders");

        // verify the partitions
        List<?> partitions = getPartitions("test_insert_partitioned_table");
        assertEquals(partitions.size(), 3);

        assertQuery("SELECT * from test_insert_partitioned_table", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertQuery(
                "SHOW PARTITIONS FROM test_insert_partitioned_table",
                "SELECT DISTINCT shippriority, orderstatus FROM orders");

        assertQuery(
                "SHOW PARTITIONS FROM test_insert_partitioned_table ORDER BY order_status LIMIT 2",
                "SELECT DISTINCT shippriority, orderstatus FROM orders ORDER BY orderstatus LIMIT 2");

        assertQuery(
                "SHOW PARTITIONS FROM test_insert_partitioned_table WHERE order_status = 'O'",
                "SELECT DISTINCT shippriority, orderstatus FROM orders WHERE orderstatus = 'O'");

        assertUpdate("DROP TABLE test_insert_partitioned_table");

        assertFalse(queryRunner.tableExists(getSession(), "test_insert_partitioned_table"));
    }

    @Test
    public void testDeleteFromUnpartitionedTable()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_delete_unpartitioned (x bigint, y varchar)");

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
                "  ORDER_KEY BIGINT," +
                "  LINE_NUMBER BIGINT," +
                "  LINE_STATUS VARCHAR" +
                ") " +
                "WITH (" +
                STORAGE_FORMAT_PROPERTY + " = '" + storageFormat + "', " +
                PARTITIONED_BY_PROPERTY + " = ARRAY[ 'LINE_NUMBER', 'LINE_STATUS' ]" +
                ") ";

        assertUpdate(createTable);

        assertUpdate("" +
                        "INSERT INTO test_metadata_delete " +
                        "SELECT orderkey, linenumber, linestatus " +
                        "FROM tpch.tiny.lineitem",
                "SELECT count(*) from lineitem");

        // Delete returns number of rows deleted, or null if obtaining the number is hard or impossible.
        // Currently, Hive implementation always returns null.
        assertUpdate("DELETE FROM test_metadata_delete WHERE LINE_STATUS='F' and LINE_NUMBER=3");

        assertQuery("SELECT * from test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'F' or linenumber<>3");

        assertUpdate("DELETE FROM test_metadata_delete WHERE LINE_STATUS='O'");

        assertQuery("SELECT * from test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'O' and linenumber<>3");

        try {
            queryRunner.execute("DELETE FROM test_metadata_delete WHERE ORDER_KEY=1");
            fail("expected exception");
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "This connector only supports delete where one or more partitions are deleted entirely");
        }

        assertQuery("SELECT * from test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'O' and linenumber<>3");

        assertUpdate("DROP TABLE test_metadata_delete");

        assertFalse(queryRunner.tableExists(getSession(), "test_metadata_delete"));
    }

    private TableMetadata getTableMetadata(String tableName)
    {
        Session session = getSession();
        Metadata metadata = ((DistributedQueryRunner) queryRunner).getCoordinator().getMetadata();

        return transaction(queryRunner.getTransactionManager())
                .readOnly()
                .execute(session, transactionSession -> {
                    Optional<TableHandle> tableHandle = metadata.getTableHandle(transactionSession, new QualifiedObjectName(catalog, TPCH_SCHEMA, tableName));
                    assertTrue(tableHandle.isPresent());
                    return metadata.getTableMetadata(transactionSession, tableHandle.get());
                });
    }

    private List<?> getPartitions(String tableName)
    {
        Session session = getSession();
        Metadata metadata = ((DistributedQueryRunner) queryRunner).getCoordinator().getMetadata();

        return transaction(queryRunner.getTransactionManager())
                .readOnly()
                .execute(session, transactionSession -> {
                    Optional<TableHandle> tableHandle = metadata.getTableHandle(transactionSession, new QualifiedObjectName(catalog, TPCH_SCHEMA, tableName));
                    assertTrue(tableHandle.isPresent());

                    List<TableLayoutResult> layouts = metadata.getLayouts(transactionSession, tableHandle.get(), Constraint.alwaysTrue(), Optional.empty());
                    TableLayout layout = Iterables.getOnlyElement(layouts).getLayout();
                    return getPartitions(layout.getHandle().getConnectorHandle());
                });
    }

    @Test
    public void testShowColumnsPartitionKey()
    {
        assertUpdate("" +
                "CREATE TABLE test_show_columns_partition_key\n" +
                "(grape bigint, orange bigint, pear varchar, apple varchar)\n" +
                "WITH (partitioned_by = ARRAY['apple'])");

        MaterializedResult actual = computeActual("SHOW COLUMNS FROM test_show_columns_partition_key");
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR)
                .row("grape", "bigint", "")
                .row("orange", "bigint", "")
                .row("pear", "varchar", "")
                .row("apple", "varchar", "Partition Key")
                .build();
        assertEquals(actual, expected);
    }

    // TODO: These should be moved to another class, when more connectors support arrays
    @Test
    public void testArrays()
            throws Exception
    {
        assertUpdate("CREATE TABLE tmp_array1 AS SELECT ARRAY[1, 2, NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array1", "SELECT 2");
        assertQuery("SELECT col[3] FROM tmp_array1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array2 AS SELECT ARRAY[1.0, 2.5, 3.5] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array2", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_array3 AS SELECT ARRAY['puppies', 'kittens', NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array3", "SELECT 'kittens'");
        assertQuery("SELECT col[3] FROM tmp_array3", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array4 AS SELECT ARRAY[TRUE, NULL] AS col", 1);
        assertQuery("SELECT col[1] FROM tmp_array4", "SELECT TRUE");
        assertQuery("SELECT col[2] FROM tmp_array4", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array5 AS SELECT ARRAY[ARRAY[1, 2], NULL, ARRAY[3, 4]] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array5", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array6 AS SELECT ARRAY[ARRAY['\"hi\"'], NULL, ARRAY['puppies']] AS col", 1);
        assertQuery("SELECT col[1][1] FROM tmp_array6", "SELECT '\"hi\"'");
        assertQuery("SELECT col[3][1] FROM tmp_array6", "SELECT 'puppies'");
    }

    @Test
    public void testTemporalArrays()
            throws Exception
    {
        assertUpdate("CREATE TABLE tmp_array7 AS SELECT ARRAY[DATE '2014-09-30'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array7");
        assertUpdate("CREATE TABLE tmp_array8 AS SELECT ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array8");
    }

    @Test
    public void testMaps()
            throws Exception
    {
        assertUpdate("CREATE TABLE tmp_map1 AS SELECT MAP(ARRAY[0,1], ARRAY[2,NULL]) AS col", 1);
        assertQuery("SELECT col[0] FROM tmp_map1", "SELECT 2");
        assertQuery("SELECT col[1] FROM tmp_map1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_map2 AS SELECT MAP(ARRAY[1.0], ARRAY[2.5]) AS col", 1);
        assertQuery("SELECT col[1.0] FROM tmp_map2", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_map3 AS SELECT MAP(ARRAY['puppies'], ARRAY['kittens']) AS col", 1);
        assertQuery("SELECT col['puppies'] FROM tmp_map3", "SELECT 'kittens'");

        assertUpdate("CREATE TABLE tmp_map4 AS SELECT MAP(ARRAY[TRUE], ARRAY[FALSE]) AS col", 1);
        assertQuery("SELECT col[TRUE] FROM tmp_map4", "SELECT FALSE");

        assertUpdate("CREATE TABLE tmp_map5 AS SELECT MAP(ARRAY[1.0], ARRAY[ARRAY[1, 2]]) AS col", 1);
        assertQuery("SELECT col[1.0][2] FROM tmp_map5", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map6 AS SELECT MAP(ARRAY[DATE '2014-09-30'], ARRAY[DATE '2014-09-29']) AS col", 1);
        assertOneNotNullResult("SELECT col[DATE '2014-09-30'] FROM tmp_map6");
        assertUpdate("CREATE TABLE tmp_map7 AS SELECT MAP(ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'], ARRAY[TIMESTAMP '2001-08-22 03:04:05.321']) AS col", 1);
        assertOneNotNullResult("SELECT col[TIMESTAMP '2001-08-22 03:04:05.321'] FROM tmp_map7");
    }

    @Test
    public void testRows()
            throws Exception
    {
        assertUpdate("CREATE TABLE tmp_row1 AS SELECT test_row(1, CAST(NULL as BIGINT)) AS a", 1);

        assertQuery(
                "SELECT a.col0, a.col1 FROM tmp_row1",
                "SELECT 1, cast(null as bigint)");
    }

    @Test
    public void testComplex()
            throws Exception
    {
        assertUpdate("CREATE TABLE tmp_complex1 AS SELECT " +
                "ARRAY [MAP(ARRAY['a', 'b'], ARRAY[2.0, 4.0]), MAP(ARRAY['c', 'd'], ARRAY[12.0, 14.0])] AS a",
                1);

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
