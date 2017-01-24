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
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.apache.hadoop.fs.Path;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.TPCH_SCHEMA;
import static com.facebook.presto.hive.HiveQueryRunner.createBucketedSession;
import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.facebook.presto.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveUtil.columnExtraInfo;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.FileAssert.assertFile;

public class TestHiveIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final String catalog;
    private final Session bucketedSession;
    private final TypeManager typeManager;
    private final TypeTranslator typeTranslator;

    @SuppressWarnings("unused")
    public TestHiveIntegrationSmokeTest()
            throws Exception
    {
        this(createQueryRunner(ORDERS, CUSTOMER), createBucketedSession(), HIVE_CATALOG, new HiveTypeTranslator());
    }

    protected TestHiveIntegrationSmokeTest(QueryRunner queryRunner, Session bucketedSession, String catalog, TypeTranslator typeTranslator)
    {
        super(queryRunner);
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.bucketedSession = requireNonNull(bucketedSession, "bucketSession is null");
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");

        this.typeManager = new TypeRegistry();
    }

    protected List<?> getPartitions(HiveTableLayoutHandle tableLayoutHandle)
    {
        return tableLayoutHandle.getPartitions().get();
    }

    @Test
    public void testSchemaOperations()
    {
        assertUpdate("CREATE SCHEMA new_schema");

        assertUpdate("CREATE TABLE new_schema.test (x bigint)");

        assertQueryFails("DROP SCHEMA new_schema", "Schema not empty: new_schema");

        assertUpdate("DROP TABLE new_schema.test");

        assertUpdate("DROP SCHEMA new_schema");
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
                ", cast(1 as bigint) _bigint" +
                ", 2 _integer" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", DATE '1980-05-07' _date" +
                ", TIMESTAMP '1980-05-07 11:22:33.456' _timestamp" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                ", CAST('bar' AS CHAR(10)) _char";

        assertUpdate(query, 1);

        MaterializedResult results = queryRunner.execute(getSession(), "SELECT * FROM test_types_table").toJdbcTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 2);
        assertEquals(row.getField(4), 3.14);
        assertEquals(row.getField(5), true);
        assertEquals(row.getField(6), new Date(new DateTime(1980, 5, 7, 0, 0, 0, UTC).getMillis()));
        assertEquals(row.getField(7), new Timestamp(new DateTime(1980, 5, 7, 11, 22, 33, 456, UTC).getMillis()));
        assertEquals(row.getField(8), new BigDecimal("3.14"));
        assertEquals(row.getField(9), new BigDecimal("12345678901234567890.0123456789"));
        assertEquals(row.getField(10), "bar       ");
        assertUpdate("DROP TABLE test_types_table");

        assertFalse(queryRunner.tableExists(getSession(), "test_types_table"));
    }

    @Test
    public void createPartitionedTable()
            throws Exception
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            if (insertOperationsSupported(storageFormat.getFormat())) {
                createPartitionedTable(storageFormat.getSession(), storageFormat.getFormat());
            }
        }
    }

    public void createPartitionedTable(Session session, HiveStorageFormat storageFormat)
            throws Exception
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table (" +
                "  _string VARCHAR" +
                ",  _varchar VARCHAR(65535)" +
                ", _char CHAR(10)" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _smallint SMALLINT" +
                ", _tinyint TINYINT" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                ", _decimal_short DECIMAL(3,2)" +
                ", _decimal_long DECIMAL(30,10)" +
                ", _partition_string VARCHAR" +
                ", _partition_varchar VARCHAR(65535)" +
                ", _partition_char CHAR(10)" +
                ", _partition_tinyint TINYINT" +
                ", _partition_smallint SMALLINT" +
                ", _partition_integer INTEGER" +
                ", _partition_bigint BIGINT" +
                ", _partition_decimal_short DECIMAL(3,2)" +
                ", _partition_decimal_long DECIMAL(30,10)" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ '_partition_string', '_partition_varchar', '_partition_char', '_partition_tinyint', '_partition_smallint', '_partition_integer', '_partition_bigint', '_partition_decimal_short', '_partition_decimal_long' ]" +
                ") ";

        if (storageFormat == HiveStorageFormat.AVRO) {
            createTable = createTable.replace(" _smallint SMALLINT,", " _smallint INTEGER,");
            createTable = createTable.replace(" _tinyint TINYINT,", " _tinyint INTEGER,");
        }

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> partitionedBy = ImmutableList.of("_partition_string", "_partition_varchar", "_partition_char", "_partition_tinyint", "_partition_smallint", "_partition_integer", "_partition_bigint", "_partition_decimal_short", "_partition_decimal_long");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            boolean partitionKey = partitionedBy.contains(columnMetadata.getName());
            assertEquals(columnMetadata.getExtraInfo(), columnExtraInfo(partitionKey));
        }

        assertColumnType(tableMetadata, "_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_varchar", createVarcharType(65535));
        assertColumnType(tableMetadata, "_char", createCharType(10));
        assertColumnType(tableMetadata, "_partition_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_partition_varchar", createVarcharType(65535));

        MaterializedResult result = computeActual("SELECT * from test_partitioned_table");
        assertEquals(result.getRowCount(), 0);

        @Language("SQL") String select = "" +
                "SELECT" +
                " 'foo' _string" +
                ", 'bar' _varchar" +
                ", CAST('boo' AS CHAR(10)) _char" +
                ", CAST(1 AS BIGINT) _bigint" +
                ", 2 _integer" +
                ", CAST (3 AS SMALLINT) _smallint" +
                ", CAST (4 AS TINYINT) _tinyint" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                ", 'foo' _partition_string" +
                ", 'bar' _partition_varchar" +
                ", CAST('boo' AS CHAR(10)) _partition_char" +
                ", CAST(1 AS TINYINT) _partition_tinyint" +
                ", CAST(1 AS SMALLINT) _partition_smallint" +
                ", 1 _partition_integer" +
                ", CAST (1 AS BIGINT) _partition_bigint" +
                ", CAST('3.14' AS DECIMAL(3,2)) _partition_decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _partition_decimal_long";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (3 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (4 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        assertUpdate(session, "INSERT INTO test_partitioned_table " + select, 1);
        assertQuery(session, "SELECT * from test_partitioned_table", select);

        assertUpdate(session, "DROP TABLE test_partitioned_table");

        assertFalse(queryRunner.tableExists(session, "test_partitioned_table"));
    }

    @Test
    public void createTableLike()
            throws Exception
    {
        createTableLike("", false);
        createTableLike("EXCLUDING PROPERTIES", false);
        createTableLike("INCLUDING PROPERTIES", true);
    }

    protected void createTableLike(String likeSuffix, boolean hasPartition)
            throws Exception
    {
        // Create a non-partitioned table
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_table_original (" +
                "  tinyint_col tinyint " +
                ", smallint_col smallint" +
                ")";
        assertUpdate(createTable);

        // Verify the table is correctly created
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_table_original");
        assertColumnType(tableMetadata, "tinyint_col", TINYINT);
        assertColumnType(tableMetadata, "smallint_col", SMALLINT);

        // Create a partitioned table
        @Language("SQL") String createPartitionedTable = "" +
                "CREATE TABLE test_partitioned_table_original (" +
                "  string_col VARCHAR" +
                ", decimal_long_col DECIMAL(30,10)" +
                ", partition_bigint BIGINT" +
                ", partition_decimal_long DECIMAL(30,10)" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY['partition_bigint', 'partition_decimal_long']" +
                ")";
        assertUpdate(createPartitionedTable);

        // Verify the table is correctly created
        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_original");

        // Verify the partition keys are correctly created
        List<String> partitionedBy = ImmutableList.of("partition_bigint", "partition_decimal_long");
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        // Create a table using only one LIKE
        @Language("SQL") String createTableSingleLike = "" +
                "CREATE TABLE test_partitioned_table_single_like (" +
                "LIKE test_partitioned_table_original " + likeSuffix +
                ")";
        assertUpdate(createTableSingleLike);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_single_like");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        @Language("SQL") String createTableLikeExtra = "" +
                "CREATE TABLE test_partitioned_table_like_extra (" +
                "  bigint_col BIGINT" +
                ", double_col DOUBLE" +
                ", LIKE test_partitioned_table_single_like " + likeSuffix +
                ")";
        assertUpdate(createTableLikeExtra);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_like_extra");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "bigint_col", BIGINT);
        assertColumnType(tableMetadata, "double_col", DOUBLE);
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        @Language("SQL") String createTableDoubleLike = "" +
                "CREATE TABLE test_partitioned_table_double_like (" +
                "  LIKE test_table_original " +
                ", LIKE test_partitioned_table_like_extra " + likeSuffix +
                ")";
        assertUpdate(createTableDoubleLike);

        tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table_double_like");

        // Verify the partitioned keys are correctly created if copying partition columns
        verifyPartition(hasPartition, tableMetadata, partitionedBy);

        // Verify the column types
        assertColumnType(tableMetadata, "tinyint_col", TINYINT);
        assertColumnType(tableMetadata, "smallint_col", SMALLINT);
        assertColumnType(tableMetadata, "string_col", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "partition_bigint", BIGINT);
        assertColumnType(tableMetadata, "partition_decimal_long", createDecimalType(30, 10));

        assertUpdate("DROP TABLE test_table_original");
        assertUpdate("DROP TABLE test_partitioned_table_original");
        assertUpdate("DROP TABLE test_partitioned_table_single_like");
        assertUpdate("DROP TABLE test_partitioned_table_like_extra");
        assertUpdate("DROP TABLE test_partitioned_table_double_like");
    }

    @Test
    public void createTableAs()
            throws Exception
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            if (insertOperationsSupported(storageFormat.getFormat())) {
                createTableAs(storageFormat.getSession(), storageFormat.getFormat());
            }
        }
    }

    public void createTableAs(Session session, HiveStorageFormat storageFormat)
            throws Exception
    {
        @Language("SQL") String select = "SELECT" +
                " 'foo' _varchar" +
                ", CAST('bar' AS CHAR(10)) _char" +
                ", CAST (1 AS BIGINT) _bigint" +
                ", 2 _integer" +
                ", CAST (3 AS SMALLINT) _smallint" +
                ", CAST (4 AS TINYINT) _tinyint" +
                ", CAST ('123.45' as REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (3 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (4 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        String createTableAs = format("CREATE TABLE test_format_table WITH (format = '%s') AS %s", storageFormat, select);

        assertUpdate(session, createTableAs, 1);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertColumnType(tableMetadata, "_varchar", createVarcharType(3));
        assertColumnType(tableMetadata, "_char", createCharType(10));

        // assure reader supports basic column reordering and pruning
        assertQuery(session, "SELECT _integer, _varchar, _integer from test_format_table", "SELECT 2, 'foo', 2");

        assertQuery(session, "SELECT * from test_format_table", select);

        assertUpdate(session, "DROP TABLE test_format_table");

        assertFalse(queryRunner.tableExists(session, "test_format_table"));
    }

    @Test
    public void createPartitionedTableAs()
            throws Exception
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            createPartitionedTableAs(storageFormat.getSession(), storageFormat.getFormat());
        }
    }

    public void createPartitionedTableAs(Session session, HiveStorageFormat storageFormat)
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

        assertUpdate(session, createTable, "SELECT count(*) from orders");

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_create_partitioned_table_as");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        List<?> partitions = getPartitions("test_create_partitioned_table_as");
        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * from test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE test_create_partitioned_table_as");

        assertFalse(queryRunner.tableExists(session, "test_create_partitioned_table_as"));
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

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported Hive type: varchar\\(65536\\)\\. Supported VARCHAR types: VARCHAR\\(<=65535\\), VARCHAR\\.")
    public void testCreateTableNonSupportedVarcharColumn()
    {
        assertUpdate("CREATE TABLE test_create_table_non_supported_varchar_column (apple varchar(65536))");
    }

    @Test
    public void testCreatePartitionedBucketedTableAsFewRows()
            throws Exception
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testCreatePartitionedBucketedTableAsFewRows(storageFormat.getSession(), storageFormat.getFormat());
        }
    }

    private void testCreatePartitionedBucketedTableAsFewRows(Session session, HiveStorageFormat storageFormat)
            throws Exception
    {
        String tableName = "test_create_partitioned_bucketed_table_as_few_rows";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'partition_key' ], " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT * " +
                "FROM (" +
                "VALUES " +
                "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                "  ('aa', 'bb', 'cc'), " +
                "  ('aaa', 'bbb', 'ccc')" +
                ") t(bucket_key, col, partition_key)";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getParallelWriteSession(),
                createTable,
                3);

        verifyPartitionedBucketedTableAsFewRows(storageFormat, tableName);

        try {
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('a0', 'b0', 'c')", 1);
            fail("expected failure");
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Cannot insert into existing partition of bucketed Hive table: partition_key=c");
        }

        assertUpdate(session, "DROP TABLE " + tableName);
        assertFalse(queryRunner.tableExists(session, tableName));
    }

    @Test
    public void testCreatePartitionedBucketedTableAs()
            throws Exception
    {
        testCreatePartitionedBucketedTableAs(HiveStorageFormat.RCBINARY);
    }

    private void testCreatePartitionedBucketedTableAs(HiveStorageFormat storageFormat)
            throws Exception
    {
        String tableName = "test_create_partitioned_bucketed_table_as";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getParallelWriteSession(),
                createTable,
                "SELECT count(*) from orders");

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(queryRunner.tableExists(getSession(), tableName));
    }

    @Test
    public void testCreatePartitionedBucketedTableAsWithUnionAll()
            throws Exception
    {
        testCreatePartitionedBucketedTableAsWithUnionAll(HiveStorageFormat.RCBINARY);
    }

    private void testCreatePartitionedBucketedTableAsWithUnionAll(HiveStorageFormat storageFormat)
            throws Exception
    {
        String tableName = "test_create_partitioned_bucketed_table_as_with_union_all";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11 " +
                ") " +
                "AS " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders " +
                "WHERE length(comment) % 2 = 0 " +
                "UNION ALL " +
                "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                "FROM tpch.tiny.orders " +
                "WHERE length(comment) % 2 = 1";

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getParallelWriteSession(),
                createTable,
                "SELECT count(*) from orders");

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(queryRunner.tableExists(getSession(), tableName));
    }

    private void verifyPartitionedBucketedTable(HiveStorageFormat storageFormat, String tableName)
            throws Exception
    {
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("orderstatus"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey", "custkey2"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        assertQuery("SELECT * from " + tableName, "SELECT custkey, custkey, comment, orderstatus FROM orders");

        for (int i = 1; i <= 30; i++) {
            assertQuery(
                    format("SELECT * from " + tableName + " where custkey = %d and custkey2 = %d", i, i),
                    format("SELECT custkey, custkey, comment, orderstatus FROM orders where custkey = %d", i));
        }

        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1, 'comment', 'O')", 1);
            fail("expected failure");
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Cannot insert into existing partition of bucketed Hive table: orderstatus=O");
        }
    }

    @Test
    public void testCreateInvalidBucketedTable()
            throws Exception
    {
        testCreateInvalidBucketedTable(HiveStorageFormat.RCBINARY);
    }

    private void testCreateInvalidBucketedTable(HiveStorageFormat storageFormat)
            throws Exception
    {
        String tableName = "test_create_invalid_bucketed_table";

        try {
            computeActual("" +
                    "CREATE TABLE " + tableName + " (" +
                    "  a BIGINT," +
                    "  b DOUBLE," +
                    "  p VARCHAR" +
                    ") WITH (" +
                    "format = '" + storageFormat + "', " +
                    "partitioned_by = ARRAY[ 'p' ], " +
                    "bucketed_by = ARRAY[ 'a', 'c' ], " +
                    "bucket_count = 11 " +
                    ")");
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Bucketing columns [c] not present in schema");
        }

        try {
            computeActual("" +
                    "CREATE TABLE " + tableName + " " +
                    "WITH (" +
                    "format = '" + storageFormat + "', " +
                    "partitioned_by = ARRAY[ 'orderstatus' ], " +
                    "bucketed_by = ARRAY[ 'custkey', 'custkey3' ], " +
                    "bucket_count = 11 " +
                    ") " +
                    "AS " +
                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                    "FROM tpch.tiny.orders");
            fail();
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "INSERT must write all distribution columns: [custkey, custkey3]");
        }

        assertFalse(queryRunner.tableExists(getSession(), tableName));
    }

    @Test
    public void testInsertPartitionedBucketedTableFewRows()
            throws Exception
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testInsertPartitionedBucketedTableFewRows(storageFormat.getSession(), storageFormat.getFormat());
        }
    }

    private void testInsertPartitionedBucketedTableFewRows(Session session, HiveStorageFormat storageFormat)
            throws Exception
    {
        String tableName = "test_insert_partitioned_bucketed_table_few_rows";

        assertUpdate(session, "" +
                "CREATE TABLE " + tableName + " (" +
                "  bucket_key varchar," +
                "  col varchar," +
                "  partition_key varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'partition_key' ], " +
                "bucketed_by = ARRAY[ 'bucket_key' ], " +
                "bucket_count = 11)");

        assertUpdate(
                // make sure that we will get one file per bucket regardless of writer count configured
                getParallelWriteSession(),
                "INSERT INTO " + tableName + " " +
                        "VALUES " +
                        "  (VARCHAR 'a', VARCHAR 'b', VARCHAR 'c'), " +
                        "  ('aa', 'bb', 'cc'), " +
                        "  ('aaa', 'bbb', 'ccc')",
                3);

        verifyPartitionedBucketedTableAsFewRows(storageFormat, tableName);

        try {
            assertUpdate(session, "INSERT INTO test_insert_partitioned_bucketed_table_few_rows VALUES ('a0', 'b0', 'c')", 1);
            fail("expected failure");
        }
        catch (Exception e) {
            assertEquals(e.getMessage(), "Cannot insert into existing partition of bucketed Hive table: partition_key=c");
        }

        assertUpdate(session, "DROP TABLE test_insert_partitioned_bucketed_table_few_rows");
        assertFalse(queryRunner.tableExists(session, tableName));
    }

    private void verifyPartitionedBucketedTableAsFewRows(HiveStorageFormat storageFormat, String tableName)
    {
        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("partition_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("bucket_key"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        MaterializedResult actual = computeActual("SELECT * from " + tableName);
        MaterializedResult expected = resultBuilder(getSession(), canonicalizeType(createUnboundedVarcharType()), canonicalizeType(createUnboundedVarcharType()), canonicalizeType(createUnboundedVarcharType()))
                .row("a", "b", "c")
                .row("aa", "bb", "cc")
                .row("aaa", "bbb", "ccc")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testInsertPartitionedBucketedTable()
            throws Exception
    {
        testInsertPartitionedBucketedTable(HiveStorageFormat.RCBINARY);
    }

    private void testInsertPartitionedBucketedTable(HiveStorageFormat storageFormat)
            throws Exception
    {
        String tableName = "test_insert_partitioned_bucketed_table";

        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  custkey bigint," +
                "  custkey2 bigint," +
                "  comment varchar," +
                "  orderstatus varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11)");

        ImmutableList<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String orderStatus = orderStatusList.get(i);
            assertUpdate(
                    // make sure that we will get one file per bucket regardless of writer count configured
                    getParallelWriteSession(),
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s'",
                            orderStatus),
                    format("SELECT count(*) from orders where orderstatus = '%s'", orderStatus));
        }

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(queryRunner.tableExists(getSession(), tableName));
    }

    @Test
    public void testInsertPartitionedBucketedTableWithUnionAll()
            throws Exception
    {
        testInsertPartitionedBucketedTableWithUnionAll(HiveStorageFormat.RCBINARY);
    }

    private void testInsertPartitionedBucketedTableWithUnionAll(HiveStorageFormat storageFormat)
            throws Exception
    {
        String tableName = "test_insert_partitioned_bucketed_table_with_union_all";

        assertUpdate("" +
                "CREATE TABLE " + tableName + " (" +
                "  custkey bigint," +
                "  custkey2 bigint," +
                "  comment varchar," +
                "  orderstatus varchar)" +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'orderstatus' ], " +
                "bucketed_by = ARRAY[ 'custkey', 'custkey2' ], " +
                "bucket_count = 11)");

        ImmutableList<String> orderStatusList = ImmutableList.of("F", "O", "P");
        for (int i = 0; i < orderStatusList.size(); i++) {
            String orderStatus = orderStatusList.get(i);
            assertUpdate(
                    // make sure that we will get one file per bucket regardless of writer count configured
                    getParallelWriteSession(),
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s' and length(comment) %% 2 = 0 " +
                                    "UNION ALL " +
                                    "SELECT custkey, custkey AS custkey2, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderstatus = '%s' and length(comment) %% 2 = 1",
                            orderStatus, orderStatus),
                    format("SELECT count(*) from orders where orderstatus = '%s'", orderStatus));
        }

        verifyPartitionedBucketedTable(storageFormat, tableName);

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(queryRunner.tableExists(getSession(), tableName));
    }

    @Test
    public void insertTable()
            throws Exception
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            if (insertOperationsSupported(storageFormat.getFormat())) {
                insertTable(storageFormat.getSession(), storageFormat.getFormat());
            }
        }
    }

    public void insertTable(Session session, HiveStorageFormat storageFormat)
            throws Exception
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_insert_format_table " +
                "(" +
                "  _string VARCHAR," +
                "  _varchar VARCHAR(65535)," +
                "  _char CHAR(10)," +
                "  _bigint BIGINT," +
                "  _integer INTEGER," +
                "  _smallint SMALLINT," +
                "  _tinyint TINYINT," +
                "  _real REAL," +
                "  _double DOUBLE," +
                "  _boolean BOOLEAN," +
                "  _decimal_short DECIMAL(3,2)," +
                "  _decimal_long DECIMAL(30,10)" +
                ") " +
                "WITH (format = '" + storageFormat + "') ";

        if (storageFormat == HiveStorageFormat.AVRO) {
            createTable = createTable.replace(" _smallint SMALLINT,", " _smallint INTEGER,");
            createTable = createTable.replace(" _tinyint TINYINT,", " _tinyint INTEGER,");
        }

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_insert_format_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        assertColumnType(tableMetadata, "_string", createUnboundedVarcharType());
        assertColumnType(tableMetadata, "_varchar", createVarcharType(65535));
        assertColumnType(tableMetadata, "_char", createCharType(10));

        @Language("SQL") String select = "SELECT" +
                " 'foo' _string" +
                ", 'bar' _varchar" +
                ", CAST('boo' AS CHAR(10)) _char" +
                ", 1 _bigint" +
                ", CAST(42 AS INTEGER) _integer" +
                ", CAST(43 AS SMALLINT) _smallint" +
                ", CAST(44 AS TINYINT) _tinyint" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (43 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (44 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        assertUpdate(session, "INSERT INTO test_insert_format_table " + select, 1);

        assertQuery(session, "SELECT * from test_insert_format_table", select);

        assertUpdate(session, "INSERT INTO test_insert_format_table (_tinyint, _smallint, _integer, _bigint, _real, _double) SELECT CAST(1 AS TINYINT), CAST(2 AS SMALLINT), 3, 4, cast(14.3 as REAL), 14.3", 1);

        assertQuery(session, "SELECT * from test_insert_format_table where _bigint = 4", "SELECT null, null, null, 4, 3, 2, 1, 14.3, 14.3, null, null, null");

        assertQuery(session, "SELECT * from test_insert_format_table where _real = CAST(14.3 as REAL)", "SELECT null, null, null, 4, 3, 2, 1, 14.3, 14.3, null, null, null");

        assertUpdate(session, "INSERT INTO test_insert_format_table (_double, _bigint) SELECT 2.72, 3", 1);

        assertQuery(session, "SELECT * from test_insert_format_table where _bigint = 3", "SELECT null, null, null, 3, null, null, null, null, 2.72, null, null, null");

        assertUpdate(session, "INSERT INTO test_insert_format_table (_decimal_short, _decimal_long) SELECT DECIMAL '2.72', DECIMAL '98765432101234567890.0123456789'", 1);

        assertQuery(session, "SELECT * from test_insert_format_table where _decimal_long = DECIMAL '98765432101234567890.0123456789'", "SELECT null, null, null, null, null, null, null, null, null, null, 2.72, 98765432101234567890.0123456789");

        assertUpdate(session, "DROP TABLE test_insert_format_table");

        assertFalse(queryRunner.tableExists(session, "test_insert_format_table"));
    }

    @Test
    public void insertPartitionedTable()
            throws Exception
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            insertPartitionedTable(storageFormat.getSession(), storageFormat.getFormat());
        }
    }

    private void insertPartitionedTable(Session session, HiveStorageFormat storageFormat)
            throws Exception
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_insert_partitioned_table " +
                "(" +
                "  ORDER_KEY BIGINT," +
                "  SHIP_PRIORITY INTEGER," +
                "  ORDER_STATUS VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'SHIP_PRIORITY', 'ORDER_STATUS' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_insert_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("ship_priority", "order_status"));

        assertQuery(
                session,
                "SHOW PARTITIONS FROM test_insert_partitioned_table",
                "SELECT shippriority, orderstatus FROM orders LIMIT 0");

        // Hive will reorder the partition keys, so we must insert into the table assuming the partition keys have been moved to the end
        assertUpdate(
                session,
                "" +
                        "INSERT INTO test_insert_partitioned_table " +
                        "SELECT orderkey, shippriority, orderstatus " +
                        "FROM tpch.tiny.orders",
                "SELECT count(*) from orders");

        // verify the partitions
        List<?> partitions = getPartitions("test_insert_partitioned_table");
        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * from test_insert_partitioned_table", "SELECT orderkey, shippriority, orderstatus FROM orders");

        assertQuery(
                session,
                "SHOW PARTITIONS FROM test_insert_partitioned_table",
                "SELECT DISTINCT shippriority, orderstatus FROM orders");

        assertQuery(
                session,
                "SHOW PARTITIONS FROM test_insert_partitioned_table ORDER BY order_status LIMIT 2",
                "SELECT DISTINCT shippriority, orderstatus FROM orders ORDER BY orderstatus LIMIT 2");

        assertQuery(
                session,
                "SHOW PARTITIONS FROM test_insert_partitioned_table WHERE order_status = 'O'",
                "SELECT DISTINCT shippriority, orderstatus FROM orders WHERE orderstatus = 'O'");

        assertUpdate(session, "DROP TABLE test_insert_partitioned_table");

        assertFalse(queryRunner.tableExists(session, "test_insert_partitioned_table"));
    }

    @Test
    public void testInsertPartitionedTableExistingPartition()
            throws Exception
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testInsertPartitionedTableExistingPartition(storageFormat.getSession(), storageFormat.getFormat());
        }
    }

    private void testInsertPartitionedTableExistingPartition(Session session, HiveStorageFormat storageFormat)
            throws Exception
    {
        String tableName = "test_insert_partitioned_table_existing_partition";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'order_status' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("order_status"));

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) from orders where orderkey %% 3 = %d", i));
        }

        // verify the partitions
        List<?> partitions = getPartitions(tableName);
        assertEquals(partitions.size(), 3);

        assertQuery(
                session,
                "SELECT * from " + tableName,
                "SELECT orderkey, comment, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(queryRunner.tableExists(session, tableName));
    }

    @Test
    public void testInsertUnpartitionedTable()
            throws Exception
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testInsertUnpartitionedTable(storageFormat.getSession(), storageFormat.getFormat());
        }
    }

    private void testInsertUnpartitionedTable(Session session, HiveStorageFormat storageFormat)
            throws Exception
    {
        String tableName = "test_insert_unpartitioned_table";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  order_key BIGINT," +
                "  comment VARCHAR," +
                "  order_status VARCHAR" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "'" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        for (int i = 0; i < 3; i++) {
            assertUpdate(
                    session,
                    format(
                            "INSERT INTO " + tableName + " " +
                                    "SELECT orderkey, comment, orderstatus " +
                                    "FROM tpch.tiny.orders " +
                                    "WHERE orderkey %% 3 = %d",
                            i),
                    format("SELECT count(*) from orders where orderkey %% 3 = %d", i));
        }

        assertQuery(
                session,
                "SELECT * from " + tableName,
                "SELECT orderkey, comment, orderstatus FROM orders");

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(queryRunner.tableExists(session, tableName));
    }

    @Test
    public void testDeleteFromUnpartitionedTable()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_delete_unpartitioned AS SELECT orderstatus FROM tpch.tiny.orders", "SELECT count(*) from orders");

        assertUpdate("DELETE FROM test_delete_unpartitioned");

        MaterializedResult result = computeActual("SELECT * from test_delete_unpartitioned");
        assertEquals(result.getRowCount(), 0);

        assertUpdate("DROP TABLE test_delete_unpartitioned");

        assertFalse(queryRunner.tableExists(getSession(), "test_delete_unpartitioned"));
    }

    @Test
    public void testMetadataDelete()
            throws Exception
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_metadata_delete " +
                "(" +
                "  ORDER_KEY BIGINT," +
                "  LINE_NUMBER INTEGER," +
                "  LINE_STATUS VARCHAR" +
                ") " +
                "WITH (" +
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
        assertUpdate("DELETE FROM test_metadata_delete WHERE LINE_STATUS='F' and LINE_NUMBER=CAST(3 AS INTEGER)");

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

    private TableMetadata getTableMetadata(String catalog, String schema, String tableName)
    {
        Session session = getSession();
        Metadata metadata = ((DistributedQueryRunner) queryRunner).getCoordinator().getMetadata();

        return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    Optional<TableHandle> tableHandle = metadata.getTableHandle(transactionSession, new QualifiedObjectName(catalog, schema, tableName));
                    assertTrue(tableHandle.isPresent());
                    return metadata.getTableMetadata(transactionSession, tableHandle.get());
                });
    }

    private Object getHiveTableProperty(String tableName, Function<HiveTableLayoutHandle, Object> propertyGetter)
    {
        Session session = getSession();
        Metadata metadata = ((DistributedQueryRunner) queryRunner).getCoordinator().getMetadata();

        return transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    Optional<TableHandle> tableHandle = metadata.getTableHandle(transactionSession, new QualifiedObjectName(catalog, TPCH_SCHEMA, tableName));
                    assertTrue(tableHandle.isPresent());

                    List<TableLayoutResult> layouts = metadata.getLayouts(transactionSession, tableHandle.get(), Constraint.alwaysTrue(), Optional.empty());
                    TableLayout layout = getOnlyElement(layouts).getLayout();
                    return propertyGetter.apply((HiveTableLayoutHandle) layout.getHandle().getConnectorHandle());
                });
    }

    private List<?> getPartitions(String tableName)
    {
        return (List<?>) getHiveTableProperty(tableName, (HiveTableLayoutHandle table) -> getPartitions(table));
    }

    private int getBucketCount(String tableName)
    {
        return (int) getHiveTableProperty(tableName, (HiveTableLayoutHandle table) -> table.getBucketHandle().get().getBucketCount());
    }

    @Test
    public void testShowColumnsPartitionKey()
    {
        assertUpdate("" +
                "CREATE TABLE test_show_columns_partition_key\n" +
                "(grape bigint, orange bigint, pear varchar(65535), mango integer, lychee smallint, kiwi tinyint, apple varchar, pineapple varchar(65535))\n" +
                "WITH (partitioned_by = ARRAY['apple', 'pineapple'])");

        MaterializedResult actual = computeActual("SHOW COLUMNS FROM test_show_columns_partition_key");
        MaterializedResult expected = resultBuilder(getSession(), canonicalizeType(createUnboundedVarcharType()), canonicalizeType(createUnboundedVarcharType()), canonicalizeType(createUnboundedVarcharType()))
                .row("grape", canonicalizeTypeName("bigint"), "", "")
                .row("orange", canonicalizeTypeName("bigint"), "", "")
                .row("pear", canonicalizeTypeName("varchar(65535)"), "", "")
                .row("mango", canonicalizeTypeName("integer"), "", "")
                .row("lychee", canonicalizeTypeName("smallint"), "", "")
                .row("kiwi", canonicalizeTypeName("tinyint"), "", "")
                .row("apple", canonicalizeTypeName("varchar"), "partition key", "")
                .row("pineapple", canonicalizeTypeName("varchar(65535)"), "partition key", "")
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

        assertUpdate("CREATE TABLE tmp_array7 AS SELECT ARRAY[ARRAY[INTEGER'1', INTEGER'2'], NULL, ARRAY[INTEGER'3', INTEGER'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array7", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array8 AS SELECT ARRAY[ARRAY[SMALLINT'1', SMALLINT'2'], NULL, ARRAY[SMALLINT'3', SMALLINT'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array8", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array9 AS SELECT ARRAY[ARRAY[TINYINT'1', TINYINT'2'], NULL, ARRAY[TINYINT'3', TINYINT'4']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array9", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_array10 AS SELECT ARRAY[ARRAY[DECIMAL '3.14']] AS col1, ARRAY[ARRAY[DECIMAL '12345678901234567890.0123456789']] AS col2", 1);
        assertQuery("SELECT col1[1][1] FROM tmp_array10", "SELECT 3.14");
        assertQuery("SELECT col2[1][1] FROM tmp_array10", "SELECT 12345678901234567890.0123456789");

        assertUpdate("CREATE TABLE tmp_array13 AS SELECT ARRAY[ARRAY[REAL'1.234', REAL'2.345'], NULL, ARRAY[REAL'3.456', REAL'4.567']] AS col", 1);
        assertQuery("SELECT col[1][2] FROM tmp_array13", "SELECT 2.345");
    }

    @Test
    public void testTemporalArrays()
            throws Exception
    {
        assertUpdate("CREATE TABLE tmp_array11 AS SELECT ARRAY[DATE '2014-09-30'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array11");
        assertUpdate("CREATE TABLE tmp_array12 AS SELECT ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array12");
    }

    @Test
    public void testMaps()
            throws Exception
    {
        assertUpdate("CREATE TABLE tmp_map1 AS SELECT MAP(ARRAY[0,1], ARRAY[2,NULL]) AS col", 1);
        assertQuery("SELECT col[0] FROM tmp_map1", "SELECT 2");
        assertQuery("SELECT col[1] FROM tmp_map1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_map2 AS SELECT MAP(ARRAY[INTEGER'1'], ARRAY[INTEGER'2']) AS col", 1);
        assertQuery("SELECT col[INTEGER'1'] FROM tmp_map2", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map3 AS SELECT MAP(ARRAY[SMALLINT'1'], ARRAY[SMALLINT'2']) AS col", 1);
        assertQuery("SELECT col[SMALLINT'1'] FROM tmp_map3", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map4 AS SELECT MAP(ARRAY[TINYINT'1'], ARRAY[TINYINT'2']) AS col", 1);
        assertQuery("SELECT col[TINYINT'1'] FROM tmp_map4", "SELECT 2");

        assertUpdate("CREATE TABLE tmp_map5 AS SELECT MAP(ARRAY[1.0], ARRAY[2.5]) AS col", 1);
        assertQuery("SELECT col[1.0] FROM tmp_map5", "SELECT 2.5");

        assertUpdate("CREATE TABLE tmp_map6 AS SELECT MAP(ARRAY['puppies'], ARRAY['kittens']) AS col", 1);
        assertQuery("SELECT col['puppies'] FROM tmp_map6", "SELECT 'kittens'");

        assertUpdate("CREATE TABLE tmp_map7 AS SELECT MAP(ARRAY[TRUE], ARRAY[FALSE]) AS col", 1);
        assertQuery("SELECT col[TRUE] FROM tmp_map7", "SELECT FALSE");

        assertUpdate("CREATE TABLE tmp_map8 AS SELECT MAP(ARRAY[DATE '2014-09-30'], ARRAY[DATE '2014-09-29']) AS col", 1);
        assertOneNotNullResult("SELECT col[DATE '2014-09-30'] FROM tmp_map8");
        assertUpdate("CREATE TABLE tmp_map9 AS SELECT MAP(ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'], ARRAY[TIMESTAMP '2001-08-22 03:04:05.321']) AS col", 1);
        assertOneNotNullResult("SELECT col[TIMESTAMP '2001-08-22 03:04:05.321'] FROM tmp_map9");

        assertUpdate("CREATE TABLE tmp_map10 AS SELECT MAP(ARRAY[DECIMAL '3.14', DECIMAL '12345678901234567890.0123456789'], " +
                "ARRAY[DECIMAL '12345678901234567890.0123456789', DECIMAL '3.0123456789']) AS col", 1);
        assertQuery("SELECT col[DECIMAL '3.14'], col[DECIMAL '12345678901234567890.0123456789'] FROM tmp_map10", "SELECT 12345678901234567890.0123456789, 3.0123456789");

        assertUpdate("CREATE TABLE tmp_map11 AS SELECT MAP(ARRAY[REAL'1.234'], ARRAY[REAL'2.345']) AS col", 1);
        assertQuery("SELECT col[REAL'1.234'] FROM tmp_map11", "SELECT 2.345");
    }

    @Test
    public void testRows()
            throws Exception
    {
        assertUpdate("CREATE TABLE tmp_row1 AS SELECT cast(row(CAST(1 as BIGINT), CAST(NULL as BIGINT)) AS row(col0 bigint, col1 bigint)) AS a", 1);
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

    @Test
    public void testBucketedCatalog()
            throws Exception
    {
        String bucketedCatalog = bucketedSession.getCatalog().get();
        String bucketedSchema = bucketedSession.getSchema().get();

        TableMetadata ordersTableMetadata = getTableMetadata(bucketedCatalog, bucketedSchema, "orders");
        assertEquals(ordersTableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey"));
        assertEquals(ordersTableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);

        TableMetadata customerTableMetadata = getTableMetadata(bucketedCatalog, bucketedSchema, "customer");
        assertEquals(customerTableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("custkey"));
        assertEquals(customerTableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 11);
    }

    @Test
    public void testBucketedExecution()
            throws Exception
    {
        assertQuery(bucketedSession, "select count(*) a from orders t1 join orders t2 on t1.custkey=t2.custkey");
        assertQuery(bucketedSession, "select count(*) a from orders t1 join customer t2 on t1.custkey=t2.custkey", "SELECT count(*) from orders");
        assertQuery(bucketedSession, "select count(distinct custkey) from orders");

        assertQuery(
                Session.builder(bucketedSession).setSystemProperty("task_writer_count", "1").build(),
                "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
        assertQuery(
                Session.builder(bucketedSession).setSystemProperty("task_writer_count", "4").build(),
                "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testShowCreateTable()
            throws Exception
    {
        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint,\n" +
                        "   c2 double,\n" +
                        "   \"c 3\" varchar,\n" +
                        "   \"c'4\" array(bigint),\n" +
                        "   c5 map(bigint, varchar)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_show_create_table");

        assertUpdate(createTableSql);
        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_show_create_table");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   c1 bigint,\n" +
                        "   \"c 2\" varchar,\n" +
                        "   \"c'3\" array(bigint),\n" +
                        "   c4 map(bigint, varchar) COMMENT 'comment test4',\n" +
                        "   c5 double COMMENT 'comment test5'\n)\n" +
                        "WITH (\n" +
                        "   format = 'ORC',\n" +
                        "   partitioned_by = ARRAY['c4','c5']\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "\"test_show_create_table'2\"");
        assertUpdate(createTableSql);
        actualResult = computeActual("SHOW CREATE TABLE \"test_show_create_table'2\"");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);
    }

    @Test
    public void testCreateExternalTable()
            throws Exception
    {
        File tempDir = createTempDir();
        File dataFile = new File(tempDir, "test.txt");
        Files.write("hello\nworld\n", dataFile, UTF_8);

        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.%s.test_create_external (\n" +
                        "   name varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   external_location = '%s',\n" +
                        "   format = 'TEXTFILE'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                new Path(tempDir.toURI().toASCIIString()).toString());

        assertUpdate(createTableSql);
        MaterializedResult actual = computeActual("SHOW CREATE TABLE test_create_external");
        assertEquals(actual.getOnlyValue(), createTableSql);

        actual = computeActual("SELECT name FROM test_create_external");
        assertEquals(actual.getOnlyColumnAsSet(), ImmutableSet.of("hello", "world"));

        assertUpdate("DROP TABLE test_create_external");

        // file should still exist after drop
        assertFile(dataFile);

        deleteRecursively(tempDir);
    }

    @Test
    public void testPathHiddenColumn()
            throws Exception
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            doTestPathHiddenColumn(storageFormat.getSession(), storageFormat.getFormat());
        }
    }

    private void doTestPathHiddenColumn(Session session, HiveStorageFormat storageFormat)
    {
        @Language("SQL") String createTable = "CREATE TABLE test_path " +
                "WITH (" +
                "format = '" + storageFormat + "'," +
                "partitioned_by = ARRAY['col1']" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 0), (3, 0), (6, 0), " +
                "(1, 1), (4, 1), (7, 1), " +
                "(2, 2), (5, 2) " +
                " ) t(col0, col1) ";
        assertUpdate(session, createTable, 8);
        assertTrue(queryRunner.tableExists(getSession(), "test_path"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_path");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(PATH_COLUMN_NAME)) {
                // $path should be hidden column
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getPartitions("test_path").size(), 3);

        MaterializedResult results = computeActual(session, format("SELECT *, \"%s\" FROM test_path", PATH_COLUMN_NAME));
        Map<Integer, String> partitionPathMap = new HashMap<>();
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            String pathName = (String) row.getField(2);
            String parentDirectory = new Path(pathName).getParent().toString();

            assertTrue(pathName.length() > 0);
            assertEquals((int) (col0 % 3), col1);
            if (partitionPathMap.containsKey(col1)) {
                // the rows in the same partition should be in the same partition directory
                assertEquals(partitionPathMap.get(col1), parentDirectory);
            }
            else {
                partitionPathMap.put(col1, parentDirectory);
            }
        }
        assertEquals(partitionPathMap.size(), 3);

        assertUpdate(session, "DROP TABLE test_path");
        assertFalse(queryRunner.tableExists(session, "test_path"));
    }

    @Test
    public void testBucketHiddenColumn()
            throws Exception
    {
        @Language("SQL") String createTable = "CREATE TABLE test_bucket_hidden_column " +
                "WITH (" +
                "bucketed_by = ARRAY['col0']," +
                "bucket_count = 2" +
                ") AS " +
                "SELECT * FROM (VALUES " +
                "(0, 11), (1, 12), (2, 13), " +
                "(3, 14), (4, 15), (5, 16), " +
                "(6, 17), (7, 18), (8, 19)" +
                " ) t (col0, col1) ";
        assertUpdate(createTable, 9);
        assertTrue(queryRunner.tableExists(getSession(), "test_bucket_hidden_column"));

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_bucket_hidden_column");
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKETED_BY_PROPERTY), ImmutableList.of("col0"));
        assertEquals(tableMetadata.getMetadata().getProperties().get(BUCKET_COUNT_PROPERTY), 2);

        List<String> columnNames = ImmutableList.of("col0", "col1", PATH_COLUMN_NAME, BUCKET_COLUMN_NAME);
        List<ColumnMetadata> columnMetadatas = tableMetadata.getColumns();
        assertEquals(columnMetadatas.size(), columnNames.size());
        for (int i = 0; i < columnMetadatas.size(); i++) {
            ColumnMetadata columnMetadata = columnMetadatas.get(i);
            assertEquals(columnMetadata.getName(), columnNames.get(i));
            if (columnMetadata.getName().equals(BUCKET_COLUMN_NAME)) {
                // $bucket_number should be hidden column
                assertTrue(columnMetadata.isHidden());
            }
        }
        assertEquals(getBucketCount("test_bucket_hidden_column"), 2);

        MaterializedResult results = computeActual(format("SELECT *, \"%1$s\" FROM test_bucket_hidden_column WHERE \"%1$s\" = 1",
                BUCKET_COLUMN_NAME));
        for (int i = 0; i < results.getRowCount(); i++) {
            MaterializedRow row = results.getMaterializedRows().get(i);
            int col0 = (int) row.getField(0);
            int col1 = (int) row.getField(1);
            int bucket = (int) row.getField(2);

            assertEquals(col1, col0 + 11);
            assertTrue(col1 % 2 == 0);

            // Because Hive's hash function for integer n is h(n) = n.
            assertEquals(bucket, col0 % 2);
        }
        assertEquals(results.getRowCount(), 4);

        assertUpdate("DROP TABLE test_bucket_hidden_column");
        assertFalse(queryRunner.tableExists(getSession(), "test_bucket_hidden_column"));
    }

    @Test
    public void testDeleteAndInsert()
    {
        Session session = getSession();

        // Partition 1 is untouched
        // Partition 2 is altered (dropped and then added back)
        // Partition 3 is added
        // Partition 4 is dropped

        assertUpdate(
                session,
                "CREATE TABLE tmp_delete_insert WITH (partitioned_by=array ['z']) AS " +
                        "SELECT * from (VALUES (CAST (101 AS BIGINT), CAST (1 AS BIGINT)), (201, 2), (202, 2), (401, 4), (402, 4), (403, 4)) t(a, z)",
                6);

        List<MaterializedRow> expectedBefore = MaterializedResult.resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(201L, 2L)
                .row(202L, 2L)
                .row(401L, 4L)
                .row(402L, 4L)
                .row(403L, 4L)
                .build()
                .getMaterializedRows();
        List<MaterializedRow> expectedAfter = MaterializedResult.resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(203L, 2L)
                .row(204L, 2L)
                .row(205L, 2L)
                .row(301L, 2L)
                .row(302L, 3L)
                .build()
                .getMaterializedRows();

        try {
            transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                    .execute(session, transactionSession -> {
                        assertUpdate(transactionSession, "DELETE FROM tmp_delete_insert WHERE z >= 2");
                        assertUpdate(transactionSession, "INSERT INTO tmp_delete_insert VALUES (203, 2), (204, 2), (205, 2), (301, 2), (302, 3)", 5);
                        MaterializedResult actualFromAnotherTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
                        assertEqualsIgnoreOrder(actualFromAnotherTransaction, expectedBefore);
                        MaterializedResult actualFromCurrentTransaction = computeActual(transactionSession, "SELECT * FROM tmp_delete_insert");
                        assertEqualsIgnoreOrder(actualFromCurrentTransaction, expectedAfter);
                        rollback();
                    });
        }
        catch (RollbackException e) {
            // ignore
        }

        MaterializedResult actualAfterRollback = computeActual(session, "SELECT * FROM tmp_delete_insert");
        assertEqualsIgnoreOrder(actualAfterRollback, expectedBefore);

        transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .execute(session, transactionSession -> {
                    assertUpdate(transactionSession, "DELETE FROM tmp_delete_insert WHERE z >= 2");
                    assertUpdate(transactionSession, "INSERT INTO tmp_delete_insert VALUES (203, 2), (204, 2), (205, 2), (301, 2), (302, 3)", 5);
                    MaterializedResult actualOutOfTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
                    assertEqualsIgnoreOrder(actualOutOfTransaction, expectedBefore);
                    MaterializedResult actualInTransaction = computeActual(transactionSession, "SELECT * FROM tmp_delete_insert");
                    assertEqualsIgnoreOrder(actualInTransaction, expectedAfter);
                });

        MaterializedResult actualAfterTransaction = computeActual(session, "SELECT * FROM tmp_delete_insert");
        assertEqualsIgnoreOrder(actualAfterTransaction, expectedAfter);
    }

    @Test
    public void testCreateAndInsert()
    {
        Session session = getSession();

        List<MaterializedRow> expected = MaterializedResult.resultBuilder(session, BIGINT, BIGINT)
                .row(101L, 1L)
                .row(201L, 2L)
                .row(202L, 2L)
                .row(301L, 3L)
                .row(302L, 3L)
                .build()
                .getMaterializedRows();

        transaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl())
                .execute(session, transactionSession -> {
                    assertUpdate(
                            transactionSession,
                            "CREATE TABLE tmp_create_insert WITH (partitioned_by=array ['z']) AS " +
                                    "SELECT * from (VALUES (CAST (101 AS BIGINT), CAST (1 AS BIGINT)), (201, 2), (202, 2)) t(a, z)",
                            3);
                    assertUpdate(transactionSession, "INSERT INTO tmp_create_insert VALUES (301, 3), (302, 3)", 2);
                    MaterializedResult actualFromCurrentTransaction = computeActual(transactionSession, "SELECT * FROM tmp_create_insert");
                    assertEqualsIgnoreOrder(actualFromCurrentTransaction, expected);
                });

        MaterializedResult actualAfterTransaction = computeActual(session, "SELECT * FROM tmp_create_insert");
        assertEqualsIgnoreOrder(actualAfterTransaction, expected);
    }

    @Test
    public void testRenameColumn()
            throws Exception
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_rename_column\n" +
                "WITH (\n" +
                "  partitioned_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN orderkey TO new_orderkey");
        assertQuery("SELECT new_orderkey, orderstatus FROM test_rename_column", "SELECT orderkey, orderstatus FROM orders");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN orderstatus TO new_orderstatus", "Renaming partition columns is not supported");
        assertQuery("SELECT new_orderkey, orderstatus FROM test_rename_column", "SELECT orderkey, orderstatus FROM orders");
        assertUpdate("DROP TABLE test_rename_column");
    }

    @Test
    public void testAvroTypeValidation()
    {
        assertQueryFails("CREATE TABLE test_avro_types (x map(bigint, bigint)) WITH (format = 'AVRO')", "Column x has a non-varchar map key, which is not supported by Avro");
        assertQueryFails("CREATE TABLE test_avro_types (x tinyint) WITH (format = 'AVRO')", "Column x is tinyint, which is not supported by Avro. Use integer instead.");
        assertQueryFails("CREATE TABLE test_avro_types (x smallint) WITH (format = 'AVRO')", "Column x is smallint, which is not supported by Avro. Use integer instead.");

        assertQueryFails("CREATE TABLE test_avro_types WITH (format = 'AVRO') AS SELECT cast(42 AS smallint) z", "Column z is smallint, which is not supported by Avro. Use integer instead.");
    }

    private Session getParallelWriteSession()
    {
        return Session.builder(getSession())
                .setSystemProperty("task_writer_count", "4")
                .build();
    }

    private void assertOneNotNullResult(@Language("SQL") String query)
    {
        MaterializedResult results = queryRunner.execute(getSession(), query).toJdbcTypes();
        assertEquals(results.getRowCount(), 1);
        assertEquals(results.getMaterializedRows().get(0).getFieldCount(), 1);
        assertNotNull(results.getMaterializedRows().get(0).getField(0));
    }

    private boolean insertOperationsSupported(HiveStorageFormat storageFormat)
    {
        return storageFormat != HiveStorageFormat.DWRF;
    }

    private Type canonicalizeType(Type type)
    {
        HiveType hiveType = HiveType.toHiveType(typeTranslator, type);
        return typeManager.getType(hiveType.getTypeSignature());
    }

    private String canonicalizeTypeName(String type)
    {
        TypeSignature typeSignature = TypeSignature.parseTypeSignature(type);
        return canonicalizeType(typeManager.getType(typeSignature)).toString();
    }

    private void assertColumnType(TableMetadata tableMetadata, String columnName, Type expectedType)
    {
        assertEquals(tableMetadata.getColumn(columnName).getType(), canonicalizeType(expectedType));
    }

    private void verifyPartition(boolean hasPartition, TableMetadata tableMetadata, List<String> partitionKeys)
    {
        Object partitionByProperty = tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY);
        if (hasPartition) {
            assertEquals(partitionByProperty, partitionKeys);
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                boolean partitionKey = partitionKeys.contains(columnMetadata.getName());
                assertEquals(columnMetadata.getExtraInfo(), columnExtraInfo(partitionKey));
            }
        }
        else {
            assertNull(partitionByProperty);
        }
    }

    private void rollback()
    {
        throw new RollbackException();
    }

    private static class RollbackException
        extends RuntimeException
    {
    }

    private List<TestingHiveStorageFormat> getAllTestingHiveStorageFormat()
    {
        Session session = getSession();
        ImmutableList.Builder<TestingHiveStorageFormat> formats = ImmutableList.builder();
        for (HiveStorageFormat hiveStorageFormat : HiveStorageFormat.values()) {
            formats.add(new TestingHiveStorageFormat(session, hiveStorageFormat));
        }
        formats.add(new TestingHiveStorageFormat(
                Session.builder(session).setCatalogSessionProperty(session.getCatalog().get(), "rcfile_optimized_reader_enabled", "true").build(),
                HiveStorageFormat.RCBINARY));
        formats.add(new TestingHiveStorageFormat(
                Session.builder(session).setCatalogSessionProperty(session.getCatalog().get(), "rcfile_optimized_reader_enabled", "true").build(),
                HiveStorageFormat.RCTEXT));
        formats.add(new TestingHiveStorageFormat(
                Session.builder(session).setCatalogSessionProperty(session.getCatalog().get(), "parquet_optimized_reader_enabled", "true").build(),
                HiveStorageFormat.PARQUET));
        return formats.build();
    }

    private static class TestingHiveStorageFormat
    {
        private final Session session;
        private final HiveStorageFormat format;

        public TestingHiveStorageFormat(Session session, HiveStorageFormat format)
        {
            this.session = requireNonNull(session, "session is null");
            this.format = requireNonNull(format, "format is null");
        }

        public Session getSession()
        {
            return session;
        }

        public HiveStorageFormat getFormat()
        {
            return format;
        }
    }
}
