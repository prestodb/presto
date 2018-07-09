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
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.hive.HiveSessionProperties.InsertExistingPartitionsBehavior;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.apache.hadoop.fs.Path;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.LongStream;

import static com.facebook.presto.SystemSessionProperties.COLOCATED_JOIN;
import static com.facebook.presto.SystemSessionProperties.CONCURRENT_LIFESPANS_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_JOIN;
import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION_FOR_AGGREGATION;
import static com.facebook.presto.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.TPCH_SCHEMA;
import static com.facebook.presto.hive.HiveQueryRunner.createBucketedSession;
import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.facebook.presto.hive.HiveSessionProperties.RCFILE_OPTIMIZED_WRITER_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.getInsertExistingPartitionsBehavior;
import static com.facebook.presto.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveTestUtils.TYPE_MANAGER;
import static com.facebook.presto.hive.HiveUtil.columnExtraInfo;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    private final TypeTranslator typeTranslator;

    @SuppressWarnings("unused")
    public TestHiveIntegrationSmokeTest()
    {
        this(() -> createQueryRunner(ORDERS, CUSTOMER), createBucketedSession(), HIVE_CATALOG, new HiveTypeTranslator());
    }

    protected TestHiveIntegrationSmokeTest(QueryRunnerSupplier queryRunnerSupplier, Session bucketedSession, String catalog, TypeTranslator typeTranslator)
    {
        super(queryRunnerSupplier);
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.bucketedSession = requireNonNull(bucketedSession, "bucketSession is null");
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");
    }

    private List<?> getPartitions(HiveTableLayoutHandle tableLayoutHandle)
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
    public void createTableWithEveryType()
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

        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM test_types_table").toTestTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), "bar".getBytes(UTF_8));
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 2);
        assertEquals(row.getField(4), 3.14);
        assertEquals(row.getField(5), true);
        assertEquals(row.getField(6), LocalDate.of(1980, 5, 7));
        assertEquals(row.getField(7), LocalDateTime.of(1980, 5, 7, 11, 22, 33, 456_000_000));
        assertEquals(row.getField(8), new BigDecimal("3.14"));
        assertEquals(row.getField(9), new BigDecimal("12345678901234567890.0123456789"));
        assertEquals(row.getField(10), "bar       ");
        assertUpdate("DROP TABLE test_types_table");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_types_table"));
    }

    @Test
    public void createPartitionedTable()
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            if (insertOperationsSupported(storageFormat.getFormat())) {
                createPartitionedTable(storageFormat.getSession(), storageFormat.getFormat());
            }
        }
    }

    private void createPartitionedTable(Session session, HiveStorageFormat storageFormat)
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
                ", _partition_boolean BOOLEAN" +
                ", _partition_decimal_short DECIMAL(3,2)" +
                ", _partition_decimal_long DECIMAL(30,10)" +
                ", _partition_date DATE" +
                ", _partition_timestamp TIMESTAMP" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ '_partition_string', '_partition_varchar', '_partition_char', '_partition_tinyint', '_partition_smallint', '_partition_integer', '_partition_bigint', '_partition_boolean', '_partition_decimal_short', '_partition_decimal_long', '_partition_date', '_partition_timestamp']" +
                ") ";

        if (storageFormat == HiveStorageFormat.AVRO) {
            createTable = createTable.replace(" _smallint SMALLINT,", " _smallint INTEGER,");
            createTable = createTable.replace(" _tinyint TINYINT,", " _tinyint INTEGER,");
        }

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, "test_partitioned_table");
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);

        List<String> partitionedBy = ImmutableList.of(
                "_partition_string",
                "_partition_varchar",
                "_partition_char",
                "_partition_tinyint",
                "_partition_smallint",
                "_partition_integer",
                "_partition_bigint",
                "_partition_boolean",
                "_partition_decimal_short",
                "_partition_decimal_long",
                "_partition_date",
                "_partition_timestamp");
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
                ", true _partition_boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _partition_decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _partition_decimal_long" +
                ", CAST('2017-05-01' AS DATE) _partition_date" +
                ", CAST('2017-05-01 10:12:34' AS TIMESTAMP) _partition_timestamp";

        if (storageFormat == HiveStorageFormat.AVRO) {
            select = select.replace(" CAST (3 AS SMALLINT) _smallint,", " 3 _smallint,");
            select = select.replace(" CAST (4 AS TINYINT) _tinyint,", " 4 _tinyint,");
        }

        assertUpdate(session, "INSERT INTO test_partitioned_table " + select, 1);
        assertQuery(session, "SELECT * from test_partitioned_table", select);
        assertQuery(session,
                "SELECT * from test_partitioned_table WHERE" +
                        " 'foo' = _partition_string" +
                        " AND 'bar' = _partition_varchar" +
                        " AND CAST('boo' AS CHAR(10)) = _partition_char" +
                        " AND CAST(1 AS TINYINT) = _partition_tinyint" +
                        " AND CAST(1 AS SMALLINT) = _partition_smallint" +
                        " AND 1 = _partition_integer" +
                        " AND CAST(1 AS BIGINT) = _partition_bigint" +
                        " AND true = _partition_boolean" +
                        " AND CAST('3.14' AS DECIMAL(3,2)) = _partition_decimal_short" +
                        " AND CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = _partition_decimal_long" +
                        " AND CAST('2017-05-01' AS DATE) = _partition_date" +
                        " AND CAST('2017-05-01 10:12:34' AS TIMESTAMP) = _partition_timestamp",
                select);

        assertUpdate(session, "DROP TABLE test_partitioned_table");

        assertFalse(getQueryRunner().tableExists(session, "test_partitioned_table"));
    }

    @Test
    public void createTableLike()
    {
        createTableLike("", false);
        createTableLike("EXCLUDING PROPERTIES", false);
        createTableLike("INCLUDING PROPERTIES", true);
    }

    private void createTableLike(String likeSuffix, boolean hasPartition)
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
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            if (insertOperationsSupported(storageFormat.getFormat())) {
                createTableAs(storageFormat.getSession(), storageFormat.getFormat());
            }
        }
    }

    private void createTableAs(Session session, HiveStorageFormat storageFormat)
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

        assertFalse(getQueryRunner().tableExists(session, "test_format_table"));
    }

    @Test
    public void createPartitionedTableAs()
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            createPartitionedTableAs(storageFormat.getSession(), storageFormat.getFormat());
        }
    }

    private void createPartitionedTableAs(Session session, HiveStorageFormat storageFormat)
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

        assertFalse(getQueryRunner().tableExists(session, "test_create_partitioned_table_as"));
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

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported type .* for partition: .*")
    public void testCreateTableUnsupportedPartitionType()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_unsupported_partition_type " +
                "(foo bigint, bar ARRAY(varchar)) " +
                "WITH (partitioned_by = ARRAY['bar'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported type .* for partition: a")
    public void testCreateTableUnsupportedPartitionTypeAs()
    {
        assertUpdate("" +
                "CREATE TABLE test_create_table_unsupported_partition_type_as " +
                "WITH (partitioned_by = ARRAY['a']) " +
                "AS " +
                "SELECT 123 x, ARRAY ['foo'] a");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unsupported Hive type: varchar\\(65536\\)\\. Supported VARCHAR types: VARCHAR\\(<=65535\\), VARCHAR\\.")
    public void testCreateTableNonSupportedVarcharColumn()
    {
        assertUpdate("CREATE TABLE test_create_table_non_supported_varchar_column (apple varchar(65536))");
    }

    @Test
    public void testCreatePartitionedBucketedTableAsFewRows()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testCreatePartitionedBucketedTableAsFewRows(storageFormat.getSession(), storageFormat.getFormat());
        }
    }

    private void testCreatePartitionedBucketedTableAsFewRows(Session session, HiveStorageFormat storageFormat)
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

        assertThatThrownBy(() -> assertUpdate(session, "INSERT INTO " + tableName + " VALUES ('a0', 'b0', 'c')", 1))
                .hasMessage(getExpectedErrorMessageForInsertExistingBucketedTable(
                        getInsertExistingPartitionsBehavior(getConnectorSession(session)),
                        "partition_key=c"));

        assertUpdate(session, "DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testCreatePartitionedBucketedTableAs()
    {
        testCreatePartitionedBucketedTableAs(HiveStorageFormat.RCBINARY);
    }

    private void testCreatePartitionedBucketedTableAs(HiveStorageFormat storageFormat)
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
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testCreatePartitionedBucketedTableAsWithUnionAll()
    {
        testCreatePartitionedBucketedTableAsWithUnionAll(HiveStorageFormat.RCBINARY);
    }

    private void testCreatePartitionedBucketedTableAsWithUnionAll(HiveStorageFormat storageFormat)
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
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    private void verifyPartitionedBucketedTable(HiveStorageFormat storageFormat, String tableName)
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

        assertThatThrownBy(() -> assertUpdate("INSERT INTO " + tableName + " VALUES (1, 1, 'comment', 'O')", 1))
                .hasMessage(getExpectedErrorMessageForInsertExistingBucketedTable(
                        getInsertExistingPartitionsBehavior(getConnectorSession(getSession())),
                        "orderstatus=O"));
    }

    @Test
    public void testCreateInvalidBucketedTable()
    {
        testCreateInvalidBucketedTable(HiveStorageFormat.RCBINARY);
    }

    private void testCreateInvalidBucketedTable(HiveStorageFormat storageFormat)
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
            assertEquals(e.getMessage(), "Bucketing columns [custkey3] not present in schema");
        }

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsertPartitionedBucketedTableFewRows()
    {
        // go through all storage formats to make sure the empty buckets are correctly created
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testInsertPartitionedBucketedTableFewRows(storageFormat.getSession(), storageFormat.getFormat());
        }
    }

    private void testInsertPartitionedBucketedTableFewRows(Session session, HiveStorageFormat storageFormat)
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

        assertThatThrownBy(() -> assertUpdate(session, "INSERT INTO test_insert_partitioned_bucketed_table_few_rows VALUES ('a0', 'b0', 'c')", 1))
                .hasMessage(getExpectedErrorMessageForInsertExistingBucketedTable(
                        getInsertExistingPartitionsBehavior(getConnectorSession(session)),
                        "partition_key=c"));

        assertUpdate(session, "DROP TABLE test_insert_partitioned_bucketed_table_few_rows");
        assertFalse(getQueryRunner().tableExists(session, tableName));
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
    public void testCastNullToColumnTypes()
    {
        String tableName = "test_cast_null_to_column_types";
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, "orc_optimized_writer_enabled", "true")
                .build();

        assertUpdate(session, "" +
                "CREATE TABLE " + tableName + " (" +
                "  col1 bigint," +
                "  col2 map(bigint, bigint)," +
                "  partition_key varchar)" +
                "WITH (" +
                "  format = 'ORC', " +
                "  partitioned_by = ARRAY[ 'partition_key' ] " +
                ")");

        assertUpdate(session, format("INSERT INTO %s (col1) VALUES (1), (2), (3)", tableName), 3);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInsertPartitionedBucketedTable()
    {
        testInsertPartitionedBucketedTable(HiveStorageFormat.RCBINARY);
    }

    private void testInsertPartitionedBucketedTable(HiveStorageFormat storageFormat)
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
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsertPartitionedBucketedTableWithUnionAll()
    {
        testInsertPartitionedBucketedTableWithUnionAll(HiveStorageFormat.RCBINARY);
    }

    private void testInsertPartitionedBucketedTableWithUnionAll(HiveStorageFormat storageFormat)
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
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void insertTable()
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            if (insertOperationsSupported(storageFormat.getFormat())) {
                insertTable(storageFormat.getSession(), storageFormat.getFormat());
            }
        }
    }

    private void insertTable(Session session, HiveStorageFormat storageFormat)
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

        assertUpdate(session, "INSERT INTO test_insert_format_table (_tinyint, _smallint, _integer, _bigint, _real, _double) SELECT CAST(1 AS TINYINT), CAST(2 AS SMALLINT), 3, 4, cast(14.3E0 as REAL), 14.3E0", 1);

        assertQuery(session, "SELECT * from test_insert_format_table where _bigint = 4", "SELECT null, null, null, 4, 3, 2, 1, 14.3, 14.3, null, null, null");

        assertQuery(session, "SELECT * from test_insert_format_table where _real = CAST(14.3 as REAL)", "SELECT null, null, null, 4, 3, 2, 1, 14.3, 14.3, null, null, null");

        assertUpdate(session, "INSERT INTO test_insert_format_table (_double, _bigint) SELECT 2.72E0, 3", 1);

        assertQuery(session, "SELECT * from test_insert_format_table where _bigint = 3", "SELECT null, null, null, 3, null, null, null, null, 2.72, null, null, null");

        assertUpdate(session, "INSERT INTO test_insert_format_table (_decimal_short, _decimal_long) SELECT DECIMAL '2.72', DECIMAL '98765432101234567890.0123456789'", 1);

        assertQuery(session, "SELECT * from test_insert_format_table where _decimal_long = DECIMAL '98765432101234567890.0123456789'", "SELECT null, null, null, null, null, null, null, null, null, null, 2.72, 98765432101234567890.0123456789");

        assertUpdate(session, "DROP TABLE test_insert_format_table");

        assertFalse(getQueryRunner().tableExists(session, "test_insert_format_table"));
    }

    @Test
    public void insertPartitionedTable()
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            insertPartitionedTable(storageFormat.getSession(), storageFormat.getFormat());
        }
    }

    private void insertPartitionedTable(Session session, HiveStorageFormat storageFormat)
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

        String partitionsTable = "\"test_insert_partitioned_table$partitions\"";

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
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
                "SELECT * FROM " + partitionsTable,
                "SELECT DISTINCT shippriority, orderstatus FROM orders");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " ORDER BY order_status LIMIT 2",
                "SELECT DISTINCT shippriority, orderstatus FROM orders ORDER BY orderstatus LIMIT 2");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE order_status = 'O'",
                "SELECT DISTINCT shippriority, orderstatus FROM orders WHERE orderstatus = 'O'");

        assertQueryFails(session, "SELECT * FROM " + partitionsTable + " WHERE no_such_column = 1", "line \\S*: Column 'no_such_column' cannot be resolved");
        assertQueryFails(session, "SELECT * FROM " + partitionsTable + " WHERE orderkey = 1", "line \\S*: Column 'orderkey' cannot be resolved");

        assertUpdate(session, "DROP TABLE test_insert_partitioned_table");

        assertFalse(getQueryRunner().tableExists(session, "test_insert_partitioned_table"));
    }

    @Test
    public void testInsertPartitionedTableExistingPartition()
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testInsertPartitionedTableExistingPartition(storageFormat.getSession(), storageFormat.getFormat());
        }
    }

    private void testInsertPartitionedTableExistingPartition(Session session, HiveStorageFormat storageFormat)
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

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testInsertPartitionedTableOverwriteExistingPartition()
    {
        testInsertPartitionedTableOverwriteExistingPartition(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, "insert_existing_partitions_behavior", "OVERWRITE")
                        .build(),
                HiveStorageFormat.ORC);
    }

    private void testInsertPartitionedTableOverwriteExistingPartition(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_insert_partitioned_table_overwrite_existing_partition";

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

            // verify the partitions
            List<?> partitions = getPartitions(tableName);
            assertEquals(partitions.size(), 3);

            assertQuery(
                    session,
                    "SELECT * from " + tableName,
                    format("SELECT orderkey, comment, orderstatus FROM orders where orderkey %% 3 = %d", i));
        }
        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testNullPartitionValues()
    {
        assertUpdate("" +
                "CREATE TABLE test_null_partition (test VARCHAR, part VARCHAR)\n" +
                "WITH (partitioned_by = ARRAY['part'])");

        assertUpdate("INSERT INTO test_null_partition VALUES ('hello', 'test'), ('world', null)", 2);

        assertQuery(
                "SELECT * FROM test_null_partition",
                "VALUES ('hello', 'test'), ('world', null)");

        assertQuery(
                "SELECT * FROM \"test_null_partition$partitions\"",
                "VALUES 'test', null");

        assertUpdate("DROP TABLE test_null_partition");
    }

    @Test
    public void testPartitionPerScanLimit()
    {
        TestingHiveStorageFormat storageFormat = new TestingHiveStorageFormat(getSession(), HiveStorageFormat.DWRF);
        testPartitionPerScanLimit(storageFormat.getSession(), storageFormat.getFormat());
    }

    private void testPartitionPerScanLimit(Session session, HiveStorageFormat storageFormat)
    {
        String tableName = "test_partition_per_scan_limit";
        String partitionsTable = "\"" + tableName + "$partitions\"";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  foo VARCHAR," +
                "  part BIGINT" +
                ") " +
                "WITH (" +
                "format = '" + storageFormat + "', " +
                "partitioned_by = ARRAY[ 'part' ]" +
                ") ";

        assertUpdate(session, createTable);

        TableMetadata tableMetadata = getTableMetadata(catalog, TPCH_SCHEMA, tableName);
        assertEquals(tableMetadata.getMetadata().getProperties().get(STORAGE_FORMAT_PROPERTY), storageFormat);
        assertEquals(tableMetadata.getMetadata().getProperties().get(PARTITIONED_BY_PROPERTY), ImmutableList.of("part"));

        // insert 1200 partitions
        for (int i = 0; i < 12; i++) {
            int partStart = i * 100;
            int partEnd = (i + 1) * 100 - 1;

            @Language("SQL") String insertPartitions = "" +
                    "INSERT INTO " + tableName + " " +
                    "SELECT 'bar' foo, part " +
                    "FROM UNNEST(SEQUENCE(" + partStart + ", " + partEnd + ")) AS TMP(part)";

            assertUpdate(session, insertPartitions, 100);
        }

        // we are not constrained by hive.max-partitions-per-scan when listing partitions
        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE part > 490 and part <= 500",
                "VALUES 491, 492, 493, 494, 495, 496, 497, 498, 499, 500");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable + " WHERE part < 0",
                "SELECT null WHERE false");

        assertQuery(
                session,
                "SELECT * FROM " + partitionsTable,
                "VALUES " + LongStream.range(0, 1200)
                        .mapToObj(String::valueOf)
                        .collect(joining(",")));

        // verify can query 1000 partitions
        assertQuery(
                session,
                "SELECT count(foo) FROM " + tableName + " WHERE part < 1000",
                "SELECT 1000");

        // verify the rest 200 partitions are successfully inserted
        assertQuery(
                session,
                "SELECT count(foo) FROM " + tableName + " WHERE part >= 1000 AND part < 1200",
                "SELECT 200");

        // verify cannot query more than 1000 partitions
        assertQueryFails(
                session,
                "SELECT * from " + tableName + " WHERE part < 1001",
                format("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName));

        // verify cannot query all partitions
        assertQueryFails(
                session,
                "SELECT * from " + tableName,
                format("Query over table 'tpch.%s' can potentially read more than 1000 partitions", tableName));

        assertUpdate(session, "DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testShowColumnsFromPartitions()
    {
        String tableName = "test_show_columns_from_partitions";

        @Language("SQL") String createTable = "" +
                "CREATE TABLE " + tableName + " " +
                "(" +
                "  foo VARCHAR," +
                "  part1 BIGINT," +
                "  part2 VARCHAR" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY[ 'part1', 'part2' ]" +
                ") ";

        assertUpdate(getSession(), createTable);

        assertQuery(
                getSession(),
                "SHOW COLUMNS FROM \"" + tableName + "$partitions\"",
                "VALUES ('part1', 'bigint', '', ''), ('part2', 'varchar', '', '')");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"$partitions\"",
                ".*Table '.*\\.tpch\\.\\$partitions' does not exist");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"orders$partitions\"",
                ".*Table '.*\\.tpch\\.orders\\$partitions' does not exist");

        assertQueryFails(
                getSession(),
                "SHOW COLUMNS FROM \"blah$partitions\"",
                ".*Table '.*\\.tpch\\.blah\\$partitions' does not exist");
    }

    @Test
    public void testPartitionsTableInvalidAccess()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitions_invalid " +
                "(" +
                "  foo VARCHAR," +
                "  part1 BIGINT," +
                "  part2 VARCHAR" +
                ") " +
                "WITH (" +
                "partitioned_by = ARRAY[ 'part1', 'part2' ]" +
                ") ";

        assertUpdate(getSession(), createTable);

        assertQueryFails(
                getSession(),
                "SELECT * FROM \"test_partitions_invalid$partitions$partitions\"",
                ".*Table .*\\.tpch\\.test_partitions_invalid\\$partitions\\$partitions does not exist");

        assertQueryFails(
                getSession(),
                "SELECT * FROM \"non_existent$partitions\"",
                ".*Table .*\\.tpch\\.non_existent\\$partitions does not exist");
    }

    @Test
    public void testInsertUnpartitionedTable()
    {
        for (TestingHiveStorageFormat storageFormat : getAllTestingHiveStorageFormat()) {
            testInsertUnpartitionedTable(storageFormat.getSession(), storageFormat.getFormat());
        }
    }

    private void testInsertUnpartitionedTable(Session session, HiveStorageFormat storageFormat)
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

        assertFalse(getQueryRunner().tableExists(session, tableName));
    }

    @Test
    public void testDeleteFromUnpartitionedTable()
    {
        assertUpdate("CREATE TABLE test_delete_unpartitioned AS SELECT orderstatus FROM tpch.tiny.orders", "SELECT count(*) from orders");

        assertUpdate("DELETE FROM test_delete_unpartitioned");

        MaterializedResult result = computeActual("SELECT * from test_delete_unpartitioned");
        assertEquals(result.getRowCount(), 0);

        assertUpdate("DROP TABLE test_delete_unpartitioned");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_delete_unpartitioned"));
    }

    @Test
    public void testMetadataDelete()
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
            getQueryRunner().execute("DELETE FROM test_metadata_delete WHERE ORDER_KEY=1");
            fail("expected exception");
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "This connector only supports delete where one or more partitions are deleted entirely");
        }

        assertQuery("SELECT * from test_metadata_delete", "SELECT orderkey, linenumber, linestatus FROM lineitem WHERE linestatus<>'O' and linenumber<>3");

        assertUpdate("DROP TABLE test_metadata_delete");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_metadata_delete"));
    }

    private TableMetadata getTableMetadata(String catalog, String schema, String tableName)
    {
        Session session = getSession();
        Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
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
        Metadata metadata = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
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
        Type unboundedVarchar = canonicalizeType(VARCHAR);
        MaterializedResult expected = resultBuilder(getSession(), unboundedVarchar, unboundedVarchar, unboundedVarchar, unboundedVarchar)
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
    {
        assertUpdate("CREATE TABLE tmp_array1 AS SELECT ARRAY[1, 2, NULL] AS col", 1);
        assertQuery("SELECT col[2] FROM tmp_array1", "SELECT 2");
        assertQuery("SELECT col[3] FROM tmp_array1", "SELECT NULL");

        assertUpdate("CREATE TABLE tmp_array2 AS SELECT ARRAY[1.0E0, 2.5E0, 3.5E0] AS col", 1);
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
    {
        assertUpdate("CREATE TABLE tmp_array11 AS SELECT ARRAY[DATE '2014-09-30'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array11");
        assertUpdate("CREATE TABLE tmp_array12 AS SELECT ARRAY[TIMESTAMP '2001-08-22 03:04:05.321'] AS col", 1);
        assertOneNotNullResult("SELECT col[1] FROM tmp_array12");
    }

    @Test
    public void testMaps()
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

        assertUpdate("CREATE TABLE tmp_map12 AS SELECT MAP(ARRAY[1.0E0], ARRAY[ARRAY[1, 2]]) AS col", 1);
        assertQuery("SELECT col[1.0][2] FROM tmp_map12", "SELECT 2");
    }

    @Test
    public void testRows()
    {
        assertUpdate("CREATE TABLE tmp_row1 AS SELECT cast(row(CAST(1 as BIGINT), CAST(NULL as BIGINT)) AS row(col0 bigint, col1 bigint)) AS a", 1);
        assertQuery(
                "SELECT a.col0, a.col1 FROM tmp_row1",
                "SELECT 1, cast(null as bigint)");
    }

    @Test
    public void testComplex()
    {
        assertUpdate("CREATE TABLE tmp_complex1 AS SELECT " +
                        "ARRAY [MAP(ARRAY['a', 'b'], ARRAY[2.0E0, 4.0E0]), MAP(ARRAY['c', 'd'], ARRAY[12.0E0, 14.0E0])] AS a",
                1);

        assertQuery(
                "SELECT a[1]['a'], a[2]['d'] FROM tmp_complex1",
                "SELECT 2.0, 14.0");
    }

    @Test
    public void testBucketedCatalog()
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
    public void testScaleWriters()
    {
        try {
            // small table that will only have one writer
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty("scale_writers", "true")
                            .setSystemProperty("writer_min_size", "32MB")
                            .build(),
                    "CREATE TABLE scale_writers_small AS SELECT * FROM tpch.tiny.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.tiny.orders").getOnlyValue());

            assertEquals(computeActual("SELECT count(DISTINCT \"$path\") FROM scale_writers_small").getOnlyValue(), 1L);

            // large table that will scale writers to multiple machines
            assertUpdate(
                    Session.builder(getSession())
                            .setSystemProperty("scale_writers", "true")
                            .setSystemProperty("writer_min_size", "1MB")
                            .build(),
                    "CREATE TABLE scale_writers_large WITH (format = 'RCBINARY') AS SELECT * FROM tpch.sf1.orders",
                    (long) computeActual("SELECT count(*) FROM tpch.sf1.orders").getOnlyValue());

            long files = (long) computeScalar("SELECT count(DISTINCT \"$path\") FROM scale_writers_large");
            long workers = (long) computeScalar("SELECT count(*) FROM system.runtime.nodes");
            assertThat(files).isBetween(2L, workers);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS scale_writers_large");
            assertUpdate("DROP TABLE IF EXISTS scale_writers_small");
        }
    }

    @Test
    public void testShowCreateTable()
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
                        "COMMENT 'test'\n" +
                        "WITH (\n" +
                        "   bucket_count = 5,\n" +
                        "   bucketed_by = ARRAY['c1','c 2'],\n" +
                        "   format = 'ORC',\n" +
                        "   orc_bloom_filter_columns = ARRAY['c1','c2'],\n" +
                        "   orc_bloom_filter_fpp = 7E-1,\n" +
                        "   partitioned_by = ARRAY['c5'],\n" +
                        "   sorted_by = ARRAY['c1','c 2 DESC']\n" +
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

        deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testPathHiddenColumn()
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
        assertTrue(getQueryRunner().tableExists(getSession(), "test_path"));

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
        assertFalse(getQueryRunner().tableExists(session, "test_path"));
    }

    @Test
    public void testBucketHiddenColumn()
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
        assertTrue(getQueryRunner().tableExists(getSession(), "test_bucket_hidden_column"));

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
        assertFalse(getQueryRunner().tableExists(getSession(), "test_bucket_hidden_column"));
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
            transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
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

        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
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

        transaction(getQueryRunner().getTransactionManager(), getQueryRunner().getAccessControl())
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
    public void testAddColumn()
    {
        assertUpdate("CREATE TABLE test_add_column (a bigint COMMENT 'test comment AAA')");
        assertUpdate("ALTER TABLE test_add_column ADD COLUMN b bigint COMMENT 'test comment BBB'");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN a varchar", ".* Column 'a' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN c bad_type", ".* Unknown type 'bad_type' for column 'c'");
        assertQuery("SHOW COLUMNS FROM test_add_column", "VALUES ('a', 'bigint', '', 'test comment AAA'), ('b', 'bigint', '', 'test comment BBB')");
        assertUpdate("DROP TABLE test_add_column");
    }

    @Test
    public void testRenameColumn()
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
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN \"$path\" TO test", ".* Cannot rename hidden column");
        assertQueryFails("ALTER TABLE test_rename_column RENAME COLUMN orderstatus TO new_orderstatus", "Renaming partition columns is not supported");
        assertQuery("SELECT new_orderkey, orderstatus FROM test_rename_column", "SELECT orderkey, orderstatus FROM orders");
        assertUpdate("DROP TABLE test_rename_column");
    }

    @Test
    public void testDropColumn()
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_drop_column\n" +
                "WITH (\n" +
                "  partitioned_by = ARRAY ['orderstatus']\n" +
                ")\n" +
                "AS\n" +
                "SELECT custkey, orderkey, orderstatus FROM orders";

        assertUpdate(createTable, "SELECT count(*) FROM orders");
        assertQuery("SELECT orderkey, orderstatus FROM test_drop_column", "SELECT orderkey, orderstatus FROM orders");

        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN \"$path\"", ".* Cannot drop hidden column");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN orderstatus", "Cannot drop partition columns");
        assertUpdate("ALTER TABLE test_drop_column DROP COLUMN orderkey");
        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN custkey", "Cannot drop the only non-partition column in a table");
        assertQuery("SELECT * FROM test_drop_column", "SELECT custkey, orderstatus FROM orders");

        assertUpdate("DROP TABLE test_drop_column");
    }

    @Test
    public void testAvroTypeValidation()
    {
        assertQueryFails("CREATE TABLE test_avro_types (x map(bigint, bigint)) WITH (format = 'AVRO')", "Column x has a non-varchar map key, which is not supported by Avro");
        assertQueryFails("CREATE TABLE test_avro_types (x tinyint) WITH (format = 'AVRO')", "Column x is tinyint, which is not supported by Avro. Use integer instead.");
        assertQueryFails("CREATE TABLE test_avro_types (x smallint) WITH (format = 'AVRO')", "Column x is smallint, which is not supported by Avro. Use integer instead.");

        assertQueryFails("CREATE TABLE test_avro_types WITH (format = 'AVRO') AS SELECT cast(42 AS smallint) z", "Column z is smallint, which is not supported by Avro. Use integer instead.");
    }

    @Test
    public void testOrderByChar()
    {
        assertUpdate("CREATE TABLE char_order_by (c_char char(2))");
        assertUpdate("INSERT INTO char_order_by (c_char) VALUES" +
                "(CAST('a' as CHAR(2)))," +
                "(CAST('a\0' as CHAR(2)))," +
                "(CAST('a  ' as CHAR(2)))", 3);

        MaterializedResult actual = computeActual(getSession(),
                "SELECT * FROM char_order_by ORDER BY c_char ASC");

        assertUpdate("DROP TABLE char_order_by");

        MaterializedResult expected = resultBuilder(getSession(), createCharType(2))
                .row("a\0")
                .row("a ")
                .row("a ")
                .build();

        assertEquals(actual, expected);
    }

    /**
     * Tests correctness of comparison of char(x) and varchar pushed down to a table scan as a TupleDomain
     */
    @Test
    public void testPredicatePushDownToTableScan()
    {
        // Test not specific to Hive, but needs a connector supporting table creation

        assertUpdate("CREATE TABLE test_table_with_char (a char(20))");
        try {
            assertUpdate("INSERT INTO test_table_with_char (a) VALUES" +
                    "(cast('aaa' as char(20)))," +
                    "(cast('bbb' as char(20)))," +
                    "(cast('bbc' as char(20)))," +
                    "(cast('bbd' as char(20)))", 4);

            assertQuery(
                    "SELECT a, a <= 'bbc' FROM test_table_with_char",
                    "VALUES (cast('aaa' as char(20)), true), " +
                            "(cast('bbb' as char(20)), true), " +
                            "(cast('bbc' as char(20)), true), " +
                            "(cast('bbd' as char(20)), false)");

            assertQuery(
                    "SELECT a FROM test_table_with_char WHERE a <= 'bbc'",
                    "VALUES cast('aaa' as char(20)), " +
                            "cast('bbb' as char(20)), " +
                            "cast('bbc' as char(20))");
        }
        finally {
            assertUpdate("DROP TABLE test_table_with_char");
        }
    }

    @Test
    public void testGroupedExecution()
    {
        try {
            assertUpdate(
                    "CREATE TABLE test_grouped_join1\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key1']) AS\n" +
                            "SELECT orderkey key1, comment value1 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_join2\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key2']) AS\n" +
                            "SELECT orderkey key2, comment value2 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_join3\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['key3']) AS\n" +
                            "SELECT orderkey key3, comment value3 FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_joinN AS\n" +
                            "SELECT orderkey keyN, comment valueN FROM orders",
                    15000);
            assertUpdate(
                    "CREATE TABLE test_grouped_joinDual\n" +
                            "WITH (bucket_count = 13, bucketed_by = ARRAY['keyD']) AS\n" +
                            "SELECT orderkey keyD, comment valueD FROM orders CROSS JOIN UNNEST(repeat(NULL, 2))",
                    30000);

            // NOT grouped execution; default
            Session notColocated = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "false")
                    .setSystemProperty(GROUPED_EXECUTION_FOR_AGGREGATION, "false")
                    .build();
            // Co-located JOIN with all groups at once
            Session colocatedAllGroupsAtOnce = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION_FOR_AGGREGATION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "0")
                    .build();
            // Co-located JOIN, 1 group per worker at a time
            Session colocatedOneGroupAtATime = Session.builder(getSession())
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION_FOR_AGGREGATION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .build();
            // Broadcast JOIN, 1 group per worker at a time
            Session broadcastOneGroupAtATime = Session.builder(getSession())
                    .setSystemProperty(DISTRIBUTED_JOIN, "false")
                    .setSystemProperty(COLOCATED_JOIN, "true")
                    .setSystemProperty(GROUPED_EXECUTION_FOR_AGGREGATION, "true")
                    .setSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, "1")
                    .build();

            //
            // HASH JOIN
            // =========

            @Language("SQL") String joinThreeBucketedTable =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "JOIN test_grouped_join3\n" +
                            "ON key2 = key3";
            @Language("SQL") String joinThreeMixedTable =
                    "SELECT key1, value1, key2, value2, keyN, valueN\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "JOIN test_grouped_joinN\n" +
                            "ON key2 = keyN";
            @Language("SQL") String expectedJoinQuery = "SELECT orderkey, comment, orderkey, comment, orderkey, comment from orders";
            @Language("SQL") String leftJoinBucketedTable =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM test_grouped_join1\n" +
                            "LEFT JOIN (SELECT * FROM test_grouped_join2 WHERE key2 % 2 = 0)\n" +
                            "ON key1 = key2";
            @Language("SQL") String rightJoinBucketedTable =
                    "SELECT key1, value1, key2, value2\n" +
                            "FROM (SELECT * FROM test_grouped_join2 WHERE key2 % 2 = 0)\n" +
                            "RIGHT JOIN test_grouped_join1\n" +
                            "ON key1 = key2";
            @Language("SQL") String expectedOuterJoinQuery = "SELECT orderkey, comment, CASE mod(orderkey, 2) WHEN 0 THEN orderkey END, CASE mod(orderkey, 2) WHEN 0 THEN comment END from orders";

            assertQuery(notColocated, joinThreeBucketedTable, expectedJoinQuery);
            assertQuery(notColocated, leftJoinBucketedTable, expectedOuterJoinQuery);
            assertQuery(notColocated, rightJoinBucketedTable, expectedOuterJoinQuery);

            assertQuery(colocatedAllGroupsAtOnce, joinThreeBucketedTable, expectedJoinQuery);
            assertQuery(colocatedAllGroupsAtOnce, joinThreeMixedTable, expectedJoinQuery);
            assertQuery(colocatedOneGroupAtATime, joinThreeBucketedTable, expectedJoinQuery);
            assertQuery(colocatedOneGroupAtATime, joinThreeMixedTable, expectedJoinQuery);

            assertQuery(colocatedAllGroupsAtOnce, leftJoinBucketedTable, expectedOuterJoinQuery);
            assertQuery(colocatedAllGroupsAtOnce, rightJoinBucketedTable, expectedOuterJoinQuery);
            assertQuery(colocatedOneGroupAtATime, leftJoinBucketedTable, expectedOuterJoinQuery);
            assertQuery(colocatedOneGroupAtATime, rightJoinBucketedTable, expectedOuterJoinQuery);

            //
            // CROSS JOIN and HASH JOIN mixed
            // ==============================

            @Language("SQL") String crossJoin =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM test_grouped_join1\n" +
                            "JOIN test_grouped_join2\n" +
                            "ON key1 = key2\n" +
                            "CROSS JOIN (SELECT * FROM test_grouped_join3 WHERE key3 <= 3)";
            @Language("SQL") String expectedCrossJoinQuery =
                    "SELECT key1, value1, key1, value1, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT orderkey key1, comment value1 FROM orders)\n" +
                            "CROSS JOIN\n" +
                            "  (SELECT orderkey key3, comment value3 FROM orders where orderkey <= 3)";
            assertQuery(notColocated, crossJoin, expectedCrossJoinQuery);
            assertQuery(colocatedAllGroupsAtOnce, crossJoin, expectedCrossJoinQuery);
            assertQuery(colocatedOneGroupAtATime, crossJoin, expectedCrossJoinQuery);

            //
            // UNION ALL / GROUP BY
            // ====================

            @Language("SQL") String groupBySingleBucketed =
                    "SELECT\n" +
                            "  keyD,\n" +
                            "  count(valueD)\n" +
                            "FROM\n" +
                            "  test_grouped_joinDual\n" +
                            "GROUP BY keyD";
            @Language("SQL") String expectedSingleGroupByQuery = "SELECT orderkey, 2 from orders";
            @Language("SQL") String groupByOfUnionBucketed =
                    "SELECT\n" +
                            "  key\n" +
                            ", arbitrary(value1)\n" +
                            ", arbitrary(value2)\n" +
                            ", arbitrary(value3)\n" +
                            "FROM (\n" +
                            "  SELECT key1 key, value1, NULL value2, NULL value3\n" +
                            "  FROM test_grouped_join1\n" +
                            "UNION ALL\n" +
                            "  SELECT key2 key, NULL value1, value2, NULL value3\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE key2 % 2 = 0\n" +
                            "UNION ALL\n" +
                            "  SELECT key3 key, NULL value1, NULL value2, value3\n" +
                            "  FROM test_grouped_join3\n" +
                            "  WHERE key3 % 3 = 0\n" +
                            ")\n" +
                            "GROUP BY key";
            @Language("SQL") String groupByOfUnionMixed =
                    "SELECT\n" +
                            "  key\n" +
                            ", arbitrary(value1)\n" +
                            ", arbitrary(value2)\n" +
                            ", arbitrary(valueN)\n" +
                            "FROM (\n" +
                            "  SELECT key1 key, value1, NULL value2, NULL valueN\n" +
                            "  FROM test_grouped_join1\n" +
                            "UNION ALL\n" +
                            "  SELECT key2 key, NULL value1, value2, NULL valueN\n" +
                            "  FROM test_grouped_join2\n" +
                            "  WHERE key2 % 2 = 0\n" +
                            "UNION ALL\n" +
                            "  SELECT keyN key, NULL value1, NULL value2, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            "  WHERE keyN % 3 = 0\n" +
                            ")\n" +
                            "GROUP BY key";
            @Language("SQL") String expectedGroupByOfUnion = "SELECT orderkey, comment, CASE mod(orderkey, 2) WHEN 0 THEN comment END, CASE mod(orderkey, 3) WHEN 0 THEN comment END from orders";
            // In this case:
            // * left side can take advantage of bucketed execution
            // * right side does not have the necessary organization to allow its parent to take advantage of bucketed execution
            // In this scenario, we give up bucketed execution altogether. This can potentially be improved.
            //
            //       AGG(key)
            //           |
            //       UNION ALL
            //      /         \
            //  AGG(key)  Scan (not bucketed)
            //     |
            // Scan (bucketed on key)
            @Language("SQL") String groupByOfUnionOfGroupByMixed =
                    "SELECT\n" +
                            "  key, sum(cnt) cnt\n" +
                            "FROM (\n" +
                            "  SELECT keyD key, count(valueD) cnt\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            "UNION ALL\n" +
                            "  SELECT keyN key, 1 cnt\n" +
                            "  FROM test_grouped_joinN\n" +
                            ")\n" +
                            "group by key";
            @Language("SQL") String expectedGroupByOfUnionOfGroupBy = "SELECT orderkey, 3 from orders";

            // Eligible GROUP BYs run in the same fragment regardless of colocated_join flag
            assertQuery(colocatedAllGroupsAtOnce, groupBySingleBucketed, expectedSingleGroupByQuery);
            assertQuery(colocatedOneGroupAtATime, groupBySingleBucketed, expectedSingleGroupByQuery);
            assertQuery(colocatedAllGroupsAtOnce, groupByOfUnionBucketed, expectedGroupByOfUnion);
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionBucketed, expectedGroupByOfUnion);

            // cannot be executed in a grouped manner but should still produce correct result
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionMixed, expectedGroupByOfUnion);
            assertQuery(colocatedOneGroupAtATime, groupByOfUnionOfGroupByMixed, expectedGroupByOfUnionOfGroupBy);

            //
            // GROUP BY and JOIN mixed
            // ========================
            @Language("SQL") String joinGroupedWithGrouped =
                    "SELECT key1, count1, count2\n" +
                            "FROM (\n" +
                            "  SELECT keyD key1, count(valueD) count1\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ") JOIN (\n" +
                            "  SELECT keyD key2, count(valueD) count2\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ")\n" +
                            "ON key1 = key2";
            @Language("SQL") String expectedJoinGroupedWithGrouped = "SELECT orderkey, 2, 2 from orders";
            @Language("SQL") String joinGroupedWithUngrouped =
                    "SELECT keyD, countD, valueN\n" +
                            "FROM (\n" +
                            "  SELECT keyD, count(valueD) countD\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ") JOIN (\n" +
                            "  SELECT keyN, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            ")\n" +
                            "ON keyD = keyN";
            @Language("SQL") String expectedJoinGroupedWithUngrouped = "SELECT orderkey, 2, comment from orders";
            @Language("SQL") String joinUngroupedWithGrouped =
                    "SELECT keyN, valueN, countD\n" +
                            "FROM (\n" +
                            "  SELECT keyN, valueN\n" +
                            "  FROM test_grouped_joinN\n" +
                            ") JOIN (\n" +
                            "  SELECT keyD, count(valueD) countD\n" +
                            "  FROM test_grouped_joinDual\n" +
                            "  GROUP BY keyD\n" +
                            ")\n" +
                            "ON keyN = keyD";
            @Language("SQL") String expectedJoinUngroupedWithGrouped = "SELECT orderkey, comment, 2 from orders";
            @Language("SQL") String groupOnJoinResult =
                    "SELECT keyD, count(valueD), count(valueN)\n" +
                            "FROM\n" +
                            "  test_grouped_joinDual\n" +
                            "JOIN\n" +
                            "  test_grouped_joinN\n" +
                            "ON keyD=keyN\n" +
                            "GROUP BY keyD";
            @Language("SQL") String expectedGroupOnJoinResult = "SELECT orderkey, 2, 2 from orders";

            // Eligible GROUP BYs run in the same fragment regardless of colocated_join flag
            assertQuery(colocatedAllGroupsAtOnce, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped);
            assertQuery(colocatedOneGroupAtATime, joinGroupedWithGrouped, expectedJoinGroupedWithGrouped);
            assertQuery(colocatedAllGroupsAtOnce, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped);
            assertQuery(colocatedOneGroupAtATime, joinGroupedWithUngrouped, expectedJoinGroupedWithUngrouped);
            assertQuery(colocatedAllGroupsAtOnce, groupOnJoinResult, expectedGroupOnJoinResult);
            assertQuery(colocatedOneGroupAtATime, groupOnJoinResult, expectedGroupOnJoinResult);
            assertQuery(broadcastOneGroupAtATime, groupOnJoinResult, expectedGroupOnJoinResult);

            // cannot be executed in a grouped manner but should still produce correct result
            assertQuery(colocatedOneGroupAtATime, joinUngroupedWithGrouped, expectedJoinUngroupedWithGrouped);

            //
            // Outer JOIN
            // ==========

            // Chain on the probe side to test duplicating OperatorFactory
            @Language("SQL") String chainedOuterJoin =
                    "SELECT key1, value1, key2, value2, key3, value3\n" +
                            "FROM\n" +
                            "  (SELECT * FROM test_grouped_join1 where mod(key1, 2) = 0)\n" +
                            "RIGHT JOIN\n" +
                            "  (SELECT * FROM test_grouped_join2 where mod(key2, 3) = 0)\n" +
                            "ON key1 = key2\n" +
                            "FULL JOIN\n" +
                            "  (SELECT * FROM test_grouped_join3 where mod(key3, 5) = 0)\n" +
                            "ON key2 = key3";
            @Language("SQL") String expectedChainedOuterJoinResult = "SELECT\n" +
                    "  CASE WHEN mod(orderkey, 2 * 3) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 2 * 3) = 0 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 3) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 3) = 0 THEN comment END,\n" +
                    "  CASE WHEN mod(orderkey, 5) = 0 THEN orderkey END,\n" +
                    "  CASE WHEN mod(orderkey, 5) = 0 THEN comment END\n" +
                    "FROM ORDERS\n" +
                    "WHERE mod(orderkey, 3) = 0 OR mod(orderkey, 5) = 0";

            assertQuery(notColocated, chainedOuterJoin, expectedChainedOuterJoinResult);
            assertQuery(colocatedAllGroupsAtOnce, chainedOuterJoin, expectedChainedOuterJoinResult);
            assertQuery(colocatedOneGroupAtATime, chainedOuterJoin, expectedChainedOuterJoinResult);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join1");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join2");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_join3");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_joinN");
            assertUpdate("DROP TABLE IF EXISTS test_grouped_joinDual");
        }
    }

    @Test
    public void testRcTextCharDecoding()
    {
        testRcTextCharDecoding(false);
        testRcTextCharDecoding(true);
    }

    private void testRcTextCharDecoding(boolean rcFileOptimizedWriterEnabled)
    {
        String catalog = getSession().getCatalog().get();
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(catalog, RCFILE_OPTIMIZED_WRITER_ENABLED, Boolean.toString(rcFileOptimizedWriterEnabled))
                .build();

        assertUpdate(session, "CREATE TABLE test_table_with_char_rc WITH (format = 'RCTEXT') AS SELECT CAST('khaki' AS CHAR(7)) char_column", 1);
        try {
            assertQuery(session,
                    "SELECT * FROM test_table_with_char_rc WHERE char_column = 'khaki  '",
                    "VALUES (CAST('khaki' AS CHAR(7)))");
        }
        finally {
            assertUpdate(session, "DROP TABLE test_table_with_char_rc");
        }
    }

    @Test
    public void testInvalidPartitionValue()
    {
        assertUpdate("CREATE TABLE invalid_partition_value (a int, b varchar) WITH (partitioned_by = ARRAY['b'])");
        assertQueryFails(
                "INSERT INTO invalid_partition_value VALUES (4, 'test' || chr(13))",
                "\\QHive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: 74 65 73 74 0D\\E");
        assertUpdate("DROP TABLE invalid_partition_value");

        assertQueryFails(
                "CREATE TABLE invalid_partition_value (a, b) WITH (partitioned_by = ARRAY['b']) AS SELECT 4, chr(9731)",
                "\\QHive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: E2 98 83\\E");
    }

    @Test
    public void testCurrentUserInView()
    {
        checkState(getSession().getCatalog().isPresent(), "catalog is not set");
        checkState(getSession().getSchema().isPresent(), "schema is not set");
        String testAccountsUnqualifiedName = "test_accounts";
        String testAccountsViewUnqualifiedName = "test_accounts_view";
        String testAccountsViewFullyQualifiedName = format("%s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), testAccountsViewUnqualifiedName);
        assertUpdate(format("CREATE TABLE %s AS SELECT user_name, account_name" +
                "  FROM (VALUES ('user1', 'account1'), ('user2', 'account2'))" +
                "  t (user_name, account_name)", testAccountsUnqualifiedName), 2);
        assertUpdate(format("CREATE VIEW %s AS SELECT account_name FROM test_accounts WHERE user_name = CURRENT_USER", testAccountsViewUnqualifiedName));
        assertUpdate(format("GRANT SELECT ON %s TO user1", testAccountsViewFullyQualifiedName));
        assertUpdate(format("GRANT SELECT ON %s TO user2", testAccountsViewFullyQualifiedName));

        Session user1 = testSessionBuilder()
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setIdentity(new Identity("user1", getSession().getIdentity().getPrincipal()))
                .build();

        Session user2 = testSessionBuilder()
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setIdentity(new Identity("user2", getSession().getIdentity().getPrincipal()))
                .build();

        assertQuery(user1, "SELECT account_name FROM test_accounts_view", "VALUES 'account1'");
        assertQuery(user2, "SELECT account_name FROM test_accounts_view", "VALUES 'account2'");
        assertUpdate("DROP VIEW test_accounts_view");
        assertUpdate("DROP TABLE test_accounts");
    }

    private Session getParallelWriteSession()
    {
        return Session.builder(getSession())
                .setSystemProperty("task_writer_count", "4")
                .build();
    }

    private void assertOneNotNullResult(@Language("SQL") String query)
    {
        MaterializedResult results = getQueryRunner().execute(getSession(), query).toTestTypes();
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
        return TYPE_MANAGER.getType(hiveType.getTypeSignature());
    }

    private String canonicalizeTypeName(String type)
    {
        TypeSignature typeSignature = TypeSignature.parseTypeSignature(type);
        return canonicalizeType(TYPE_MANAGER.getType(typeSignature)).toString();
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

    private static String getExpectedErrorMessageForInsertExistingBucketedTable(InsertExistingPartitionsBehavior behavior, String partitionName)
    {
        if (behavior == InsertExistingPartitionsBehavior.APPEND) {
            return "Cannot insert into existing partition of bucketed Hive table: " + partitionName;
        }
        if (behavior == InsertExistingPartitionsBehavior.ERROR) {
            return "Cannot insert into an existing partition of Hive table: " + partitionName;
        }
        throw new IllegalArgumentException("Unexpected insertExistingPartitionsBehavior: " + behavior);
    }

    private static ConnectorSession getConnectorSession(Session session)
    {
        return session.toConnectorSession(new ConnectorId(session.getCatalog().get()));
    }

    private List<TestingHiveStorageFormat> getAllTestingHiveStorageFormat()
    {
        Session session = getSession();
        ImmutableList.Builder<TestingHiveStorageFormat> formats = ImmutableList.builder();
        for (HiveStorageFormat hiveStorageFormat : HiveStorageFormat.values()) {
            formats.add(new TestingHiveStorageFormat(session, hiveStorageFormat));
        }
        formats.add(new TestingHiveStorageFormat(
                Session.builder(session).setCatalogSessionProperty(session.getCatalog().get(), "orc_optimized_writer_enabled", "true").build(),
                HiveStorageFormat.ORC));
        formats.add(new TestingHiveStorageFormat(
                Session.builder(session).setCatalogSessionProperty(session.getCatalog().get(), "orc_optimized_writer_enabled", "true").build(),
                HiveStorageFormat.DWRF));
        formats.add(new TestingHiveStorageFormat(
                Session.builder(session).setCatalogSessionProperty(session.getCatalog().get(), "parquet_optimized_reader_enabled", "true").build(),
                HiveStorageFormat.PARQUET));
        return formats.build();
    }

    private static class TestingHiveStorageFormat
    {
        private final Session session;
        private final HiveStorageFormat format;

        TestingHiveStorageFormat(Session session, HiveStorageFormat format)
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
