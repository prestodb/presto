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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.assertions.Assert;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateProperties;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.TEST_CATALOG_DIRECTORY;
import static com.facebook.presto.iceberg.IcebergQueryRunner.TEST_DATA_DIRECTORY;
import static com.facebook.presto.iceberg.RegisterTableProcedure.METADATA_FOLDER_NAME;
import static com.facebook.presto.iceberg.TestIcebergRegisterProcedure.getMetadataFileLocation;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class IcebergDistributedSmokeTestBase
        extends AbstractTestIntegrationSmokeTest
{
    private final CatalogType catalogType;

    private static final Pattern WITH_CLAUSE_EXTRACTER = Pattern.compile(".*(WITH\\s*\\([^)]*\\))\\s*$", Pattern.DOTALL);

    protected IcebergDistributedSmokeTestBase(CatalogType catalogType)
    {
        this.catalogType = requireNonNull(catalogType, "catalogType is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(ImmutableMap.of(), catalogType);
    }

    @Test
    public void testTimestamp()
    {
        assertUpdate("CREATE TABLE test_timestamp (x timestamp)");
        assertUpdate("INSERT INTO test_timestamp VALUES (timestamp '2017-05-01 10:12:34')", 1);
        assertQuery("SELECT * FROM test_timestamp", "SELECT CAST('2017-05-01 10:12:34' AS TIMESTAMP)");
        dropTable(getSession(), "test_timestamp");
    }

    @Test
    public void testTimestampWithTimeZone()
    {
        assertQueryFails("CREATE TABLE test_timestamp_with_timezone (x timestamp with time zone)", "Iceberg column type timestamptz is not supported");
        assertUpdate("CREATE TABLE test_timestamp_with_timezone (x timestamp)");
        assertQueryFails("ALTER TABLE test_timestamp_with_timezone ADD COLUMN y timestamp with time zone", "Iceberg column type timestamptz is not supported");
        dropTable(getSession(), "test_timestamp_with_timezone");
    }

    @Test
    public void testTime()
    {
        assertUpdate("CREATE TABLE test_time (x time)");
        assertUpdate("INSERT INTO test_time VALUES (time '10:12:34')", 1);
        assertQuery("SELECT * FROM test_time", "SELECT CAST('10:12:34' AS TIME)");
        dropTable(getSession(), "test_time");
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        Assert.assertEquals(actualColumns, expectedColumns);
    }
    @Test
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo(format("CREATE TABLE iceberg.tpch.orders (\n" +
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
                        "   format = 'PARQUET',\n" +
                        "   format_version = '2',\n" +
                        "   location = '%s'\n" +
                        ")", getLocation("tpch", "orders")));
    }

    @Test
    public void testDecimal()
    {
        testWithAllFileFormats((session, format) -> testDecimalForFormat(session, format));
    }

    private void testDecimalForFormat(Session session, FileFormat format)
    {
        testDecimalWithPrecisionAndScale(session, format, 1, 0);
        testDecimalWithPrecisionAndScale(session, format, 8, 6);
        testDecimalWithPrecisionAndScale(session, format, 9, 8);
        testDecimalWithPrecisionAndScale(session, format, 10, 8);

        testDecimalWithPrecisionAndScale(session, format, 18, 1);
        testDecimalWithPrecisionAndScale(session, format, 18, 8);
        testDecimalWithPrecisionAndScale(session, format, 18, 17);

        testDecimalWithPrecisionAndScale(session, format, 17, 16);
        testDecimalWithPrecisionAndScale(session, format, 18, 17);
        testDecimalWithPrecisionAndScale(session, format, 24, 10);
        testDecimalWithPrecisionAndScale(session, format, 30, 10);
        testDecimalWithPrecisionAndScale(session, format, 37, 26);
        testDecimalWithPrecisionAndScale(session, format, 38, 37);

        testDecimalWithPrecisionAndScale(session, format, 38, 17);
        testDecimalWithPrecisionAndScale(session, format, 38, 37);
    }

    private void testDecimalWithPrecisionAndScale(Session session, FileFormat format, int precision, int scale)
    {
        checkArgument(precision >= 1 && precision <= 38, "Decimal precision (%s) must be between 1 and 38 inclusive", precision);
        checkArgument(scale < precision && scale >= 0, "Decimal scale (%s) must be less than the precision (%s) and non-negative", scale, precision);

        String tableName = format("test_decimal_p%d_s%d", precision, scale);
        String decimalType = format("DECIMAL(%d,%d)", precision, scale);
        String beforeTheDecimalPoint = "12345678901234567890123456789012345678".substring(0, precision - scale);
        String afterTheDecimalPoint = "09876543210987654321098765432109876543".substring(0, scale);
        String decimalValue = format("%s.%s", beforeTheDecimalPoint, afterTheDecimalPoint);

        assertUpdate(session, format("CREATE TABLE %s (x %s) WITH (format = '%s')", tableName, decimalType, format.name()));
        assertUpdate(session, format("INSERT INTO %s (x) VALUES (CAST('%s' AS %s))", tableName, decimalValue, decimalType), 1);
        assertQuery(session, format("SELECT * FROM %s", tableName), format("SELECT CAST('%s' AS %s)", decimalValue, decimalType));
        dropTable(session, tableName);
    }

    @Test
    public void testParquetPartitionByTimestamp()
    {
        // TODO
    }

    @Test
    public void testParquetSelectByTimestamp()
    {
        // TODO
    }

    @Test
    public void testOrcPartitionByTimestamp()
    {
        // TODO
    }

    @Test
    public void testOrcSelectByTimestamp()
    {
        // TODO
    }

    @Test
    public void testCreatePartitionedTable()
    {
        testWithAllFileFormats(this::testCreatePartitionedTable);
    }

    private void testCreatePartitionedTable(Session session, FileFormat fileFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table (" +
                "  _string VARCHAR" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                ", _decimal_short DECIMAL(3,2)" +
                ", _decimal_long DECIMAL(30,10)" +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "format = '" + fileFormat + "', " +
                "partitioning = ARRAY[" +
                "  '_string'," +
                "  '_integer'," +
                "  '_bigint'," +
                "  '_boolean'," +
                "  '_real'," +
                "  '_double'," +
                "  '_decimal_short', " +
                "  '_decimal_long'," +
                "  '_date']" +
                ")";

        assertUpdate(session, createTable);

        MaterializedResult result = computeActual("SELECT * from test_partitioned_table");
        assertEquals(result.getRowCount(), 0);

        @Language("SQL") String select = "" +
                "SELECT" +
                " 'foo' _string" +
                ", CAST(123 AS BIGINT) _bigint" +
                ", 456 _integer" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
                ", CAST('2017-05-01' AS DATE) _date";

        assertUpdate(session, "INSERT INTO test_partitioned_table " + select, 1);
        assertQuery(session, "SELECT * from test_partitioned_table", select);
        assertQuery(session, "" +
                        "SELECT * FROM test_partitioned_table WHERE" +
                        " 'foo' = _string" +
                        " AND 456 = _integer" +
                        " AND CAST(123 AS BIGINT) = _bigint" +
                        " AND true = _boolean" +
                        " AND CAST('3.14' AS DECIMAL(3,2)) = _decimal_short" +
                        " AND CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = _decimal_long" +
                        " AND CAST('2017-05-01' AS DATE) = _date",
                select);

        dropTable(session, "test_partitioned_table");
    }

    @Test
    public void testCreatePartitionedTableWithNestedTypes()
    {
        testWithAllFileFormats(this::testCreatePartitionedTableWithNestedTypes);
    }

    private void testCreatePartitionedTableWithNestedTypes(Session session, FileFormat fileFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table_nested_type (" +
                "  _string VARCHAR" +
                ", _struct ROW(_field1 INT, _field2 VARCHAR)" +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "format = '" + fileFormat + "', " +
                "partitioning = ARRAY['_date']" +
                ")";

        assertUpdate(session, createTable);

        dropTable(session, "test_partitioned_table_nested_type");
    }

    @Test
    public void testPartitionedTableWithNullValues()
    {
        testWithAllFileFormats(this::testPartitionedTableWithNullValues);
    }

    private void testPartitionedTableWithNullValues(Session session, FileFormat fileFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table_with_null_values (" +
                "  _string VARCHAR" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
                ", _decimal_short DECIMAL(3,2)" +
                ", _decimal_long DECIMAL(30,10)" +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "format = '" + fileFormat + "', " +
                "partitioning = ARRAY[" +
                "  '_string'," +
                "  '_integer'," +
                "  '_bigint'," +
                "  '_boolean'," +
                "  '_real'," +
                "  '_double'," +
                "  '_decimal_short', " +
                "  '_decimal_long'," +
                "  '_date']" +
                ")";

        assertUpdate(session, createTable);

        MaterializedResult result = computeActual("SELECT * from test_partitioned_table_with_null_values");
        assertEquals(result.getRowCount(), 0);

        @Language("SQL") String select = "" +
                "SELECT" +
                " null _string" +
                ", null _bigint" +
                ", null _integer" +
                ", null _real" +
                ", null _double" +
                ", null _boolean" +
                ", null _decimal_short" +
                ", null _decimal_long" +
                ", null _date";

        assertUpdate(session, "INSERT INTO test_partitioned_table_with_null_values " + select, 1);
        assertQuery(session, "SELECT * from test_partitioned_table_with_null_values", select);
        dropTable(session, "test_partitioned_table_with_null_values");
    }

    @Test
    public void testCreatePartitionedTableAs()
    {
        testWithAllFileFormats(this::testCreatePartitionedTableAs);
    }

    private void testCreatePartitionedTableAs(Session session, FileFormat fileFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as_" + fileFormat.toString().toLowerCase(ENGLISH) + " " +
                "WITH (" +
                "format = '" + fileFormat + "', " +
                "partitioning = ARRAY['ORDER_STATUS', 'Ship_Priority', 'Bucket(order_key,9)']" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(session, createTable, "SELECT count(*) from orders");

        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   \"order_key\" bigint,\n" +
                        "   \"ship_priority\" integer,\n" +
                        "   \"order_status\" varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = '" + fileFormat + "',\n" +
                        "   format_version = '2',\n" +
                        "   location = '%s',\n" +
                        "   partitioning = ARRAY['order_status','ship_priority','bucket(order_key, 9)']\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_create_partitioned_table_as_" + fileFormat.toString().toLowerCase(ENGLISH),
                getLocation(getSession().getSchema().get(), "test_create_partitioned_table_as_" + fileFormat.toString().toLowerCase(ENGLISH)));

        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_create_partitioned_table_as_" + fileFormat.toString().toLowerCase(ENGLISH));
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        assertQuery(session, "SELECT * from test_create_partitioned_table_as_" + fileFormat.toString().toLowerCase(ENGLISH), "SELECT orderkey, shippriority, orderstatus FROM orders");

        dropTable(session, "test_create_partitioned_table_as_" + fileFormat.toString().toLowerCase(ENGLISH));
    }

    @Test
    public void testColumnComments()
    {
        Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_column_comments (_bigint BIGINT COMMENT 'test column comment')");

        assertQuery(session, "SHOW COLUMNS FROM test_column_comments",
                "VALUES ('_bigint', 'bigint', '', 'test column comment')");

        assertUpdate("ALTER TABLE test_column_comments ADD COLUMN _varchar VARCHAR COMMENT 'test new column comment'");
        assertQuery(
                "SHOW COLUMNS FROM test_column_comments",
                "VALUES ('_bigint', 'bigint', '', 'test column comment'), ('_varchar', 'varchar', '', 'test new column comment')");

        dropTable(session, "test_column_comments");
    }

    @Test
    public void testTableComments()
    {
        Session session = getSession();

        @Language("SQL") String createTable = "" +
                "CREATE TABLE iceberg.tpch.test_table_comments (\n" +
                "   \"_x\" bigint\n" +
                ")\n" +
                "COMMENT '%s'\n" +
                "WITH (\n" +
                "   format = 'ORC',\n" +
                "   format_version = '2'\n" +
                ")";

        assertUpdate(format(createTable, "test table comment"));

        String createTableTemplate = "" +
                "CREATE TABLE iceberg.tpch.test_table_comments (\n" +
                "   \"_x\" bigint\n" +
                ")\n" +
                "COMMENT '%s'\n" +
                "WITH (\n" +
                "   format = 'ORC',\n" +
                "   format_version = '2',\n" +
                "   location = '%s'\n" +
                ")";
        String createTableSql = format(createTableTemplate, "test table comment", getLocation("tpch", "test_table_comments"));

        MaterializedResult resultOfCreate = computeActual("SHOW CREATE TABLE test_table_comments");
        assertEquals(getOnlyElement(resultOfCreate.getOnlyColumnAsSet()), createTableSql);

        dropTable(session, "test_table_comments");
    }

    @Test
    public void testRollbackSnapshot()
    {
        Session session = getSession();
        MaterializedResult result = computeActual("SHOW SCHEMAS FROM system");
        assertUpdate(session, "CREATE TABLE test_rollback AS SELECT * FROM (VALUES (123, CAST(321 AS BIGINT))) AS t (col0, col1)", 1);
        long afterCreateTableId = getLatestSnapshotId();

        assertUpdate(session, "INSERT INTO test_rollback (col0, col1) VALUES (123, CAST(987 AS BIGINT))", 1);
        long afterFirstInsertId = getLatestSnapshotId();

        assertUpdate(session, "INSERT INTO test_rollback (col0, col1) VALUES (456, CAST(654 AS BIGINT))", 1);
        assertQuery(session, "SELECT * FROM test_rollback ORDER BY col0",
                "VALUES (123, CAST(987 AS BIGINT)), (456, CAST(654 AS BIGINT)), (123, CAST(321 AS BIGINT))");

        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", afterFirstInsertId));
        assertQuery(session, "SELECT * FROM test_rollback ORDER BY col0",
                "VALUES (123, CAST(987 AS BIGINT)), (123, CAST(321 AS BIGINT))");

        assertUpdate(format("CALL system.rollback_to_snapshot('tpch', 'test_rollback', %s)", afterCreateTableId));
        assertEquals((long) computeActual(session, "SELECT COUNT(*) FROM test_rollback").getOnlyValue(), 1);

        dropTable(session, "test_rollback");
    }

    private long getLatestSnapshotId()
    {
        return (long) computeActual("SELECT snapshot_id FROM \"test_rollback$snapshots\" ORDER BY committed_at DESC LIMIT 1")
                .getOnlyValue();
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        // TODO: To support non-null column. (NOT_NULL_COLUMN_CONSTRAINT)
    }

    @Test
    public void testSchemaEvolution()
    {
        // TODO: Support schema evolution for PARQUET. Schema evolution should be id based.
        testSchemaEvolution(getSession(), FileFormat.ORC);
    }

    private void testSchemaEvolution(Session session, FileFormat fileFormat)
    {
        assertUpdate(session, "CREATE TABLE test_schema_evolution_drop_end (col0 INTEGER, col1 INTEGER, col2 INTEGER) WITH (format = '" + fileFormat + "')");
        assertUpdate(session, "INSERT INTO test_schema_evolution_drop_end VALUES (0, 1, 2)", 1);
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1, 2)");
        assertUpdate(session, "ALTER TABLE test_schema_evolution_drop_end DROP COLUMN col2");
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1)");
        assertUpdate(session, "ALTER TABLE test_schema_evolution_drop_end ADD COLUMN col2 INTEGER");
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1, NULL)");
        assertUpdate(session, "INSERT INTO test_schema_evolution_drop_end VALUES (3, 4, 5)", 1);
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1, NULL), (3, 4, 5)");
        dropTable(session, "test_schema_evolution_drop_end");

        assertUpdate(session, "CREATE TABLE test_schema_evolution_drop_middle (col0 INTEGER, col1 INTEGER, col2 INTEGER) WITH (format = '" + fileFormat + "')");
        assertUpdate(session, "INSERT INTO test_schema_evolution_drop_middle VALUES (0, 1, 2)", 1);
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 1, 2)");
        assertUpdate(session, "ALTER TABLE test_schema_evolution_drop_middle DROP COLUMN col1");
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 2)");
        assertUpdate(session, "ALTER TABLE test_schema_evolution_drop_middle ADD COLUMN col1 INTEGER");
        assertUpdate(session, "INSERT INTO test_schema_evolution_drop_middle VALUES (3, 4, 5)", 1);
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 2, NULL), (3, 4, 5)");
        dropTable(session, "test_schema_evolution_drop_middle");
    }

    @Test
    private void testCreateTableLike()
    {
        Session session = getSession();

        assertUpdate(session, "CREATE TABLE test_create_table_like_original (col1 INTEGER, aDate DATE) WITH(format = 'PARQUET', partitioning = ARRAY['aDate'])");
        assertEquals(getTablePropertiesString("test_create_table_like_original"), format("WITH (\n" +
                "   format = 'PARQUET',\n" +
                "   format_version = '2',\n" +
                "   location = '%s',\n" +
                "   partitioning = ARRAY['adate']\n" +
                ")", getLocation("tpch", "test_create_table_like_original")));

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy0 (LIKE test_create_table_like_original, col2 INTEGER)");
        assertUpdate(session, "INSERT INTO test_create_table_like_copy0 (col1, aDate, col2) VALUES (1, CAST('1950-06-28' AS DATE), 3)", 1);
        assertQuery(session, "SELECT * from test_create_table_like_copy0", "VALUES(1, CAST('1950-06-28' AS DATE), 3)");
        dropTable(session, "test_create_table_like_copy0");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy1 (LIKE test_create_table_like_original)");
        assertEquals(getTablePropertiesString("test_create_table_like_copy1"), format("WITH (\n" +
                "   format = 'PARQUET',\n" +
                "   format_version = '2',\n" +
                "   location = '%s'\n" +
                ")", getLocation("tpch", "test_create_table_like_copy1")));
        dropTable(session, "test_create_table_like_copy1");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy2 (LIKE test_create_table_like_original EXCLUDING PROPERTIES)");
        assertEquals(getTablePropertiesString("test_create_table_like_copy2"), format("WITH (\n" +
                "   format = 'PARQUET',\n" +
                "   format_version = '2',\n" +
                "   location = '%s'\n" +
                ")", getLocation("tpch", "test_create_table_like_copy2")));
        dropTable(session, "test_create_table_like_copy2");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy3 (LIKE test_create_table_like_original INCLUDING PROPERTIES)");
        assertEquals(getTablePropertiesString("test_create_table_like_copy3"), format("WITH (\n" +
                "   format = 'PARQUET',\n" +
                "   format_version = '2',\n" +
                "   location = '%s',\n" +
                "   partitioning = ARRAY['adate']\n" +
                ")", catalogType.equals(CatalogType.HIVE) ?
                getLocation("tpch", "test_create_table_like_original") :
                getLocation("tpch", "test_create_table_like_copy3")));
        dropTable(session, "test_create_table_like_copy3");

        assertUpdate(session, "CREATE TABLE test_create_table_like_copy4 (LIKE test_create_table_like_original INCLUDING PROPERTIES) WITH (format = 'ORC')");
        assertEquals(getTablePropertiesString("test_create_table_like_copy4"), format("WITH (\n" +
                "   format = 'ORC',\n" +
                "   format_version = '2',\n" +
                "   location = '%s',\n" +
                "   partitioning = ARRAY['adate']\n" +
                ")", catalogType.equals(CatalogType.HIVE) ?
                getLocation("tpch", "test_create_table_like_original") :
                getLocation("tpch", "test_create_table_like_copy4")));
        dropTable(session, "test_create_table_like_copy4");

        dropTable(session, "test_create_table_like_original");
    }

    @Test
    public void testCreateTableWithFormatVersion()
    {
        testWithAllFormatVersions(this::testCreateTableWithFormatVersion);
    }

    private void testCreateTableWithFormatVersion(Session session, String formatVersion)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_table_with_format_version_" + formatVersion + " " +
                "WITH (" +
                "format = 'PARQUET', " +
                "format_version = '" + formatVersion + "'" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(session, createTable, "SELECT count(*) from orders");

        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   \"order_key\" bigint,\n" +
                        "   \"ship_priority\" integer,\n" +
                        "   \"order_status\" varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'PARQUET',\n" +
                        "   format_version = '%s',\n" +
                        "   location = '%s'\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_create_table_with_format_version_" + formatVersion,
                formatVersion,
                getLocation(getSession().getSchema().get(), "test_create_table_with_format_version_" + formatVersion));

        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_create_table_with_format_version_" + formatVersion);
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

        dropTable(session, "test_create_table_with_format_version_" + formatVersion);
    }

    private void testWithAllFormatVersions(BiConsumer<Session, String> test)
    {
        test.accept(getSession(), "1");
        test.accept(getSession(), "2");
    }

    private String getTablePropertiesString(String tableName)
    {
        MaterializedResult showCreateTable = computeActual("SHOW CREATE TABLE " + tableName);
        String createTable = (String) getOnlyElement(showCreateTable.getOnlyColumnAsSet());
        Matcher matcher = WITH_CLAUSE_EXTRACTER.matcher(createTable);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        else {
            return null;
        }
    }

    @Test
    public void testPredicating()
    {
        testWithAllFileFormats(this::testPredicating);
    }

    private void testPredicating(Session session, FileFormat fileFormat)
    {
        assertUpdate(session, "CREATE TABLE test_predicating_on_real (col REAL) WITH (format = '" + fileFormat + "')");
        assertUpdate(session, "INSERT INTO test_predicating_on_real VALUES 1.2", 1);
        assertQuery(session, "SELECT * FROM test_predicating_on_real WHERE col = 1.2", "VALUES 1.2");
        dropTable(session, "test_predicating_on_real");
    }

    @Test
    public void testDateTransforms()
    {
        // TODO
    }

    @Test
    public void testTruncateTransform()
    {
        testWithAllFileFormats(this::testTruncateTransformsForFormat);
    }

    private void testTruncateTransformsForFormat(Session session, FileFormat format)
    {
        String select = "SELECT d_trunc, row_count, d.min AS d_min, d.max AS d_max, b.min AS b_min, b.max AS b_max FROM \"test_truncate_transform$partitions\"";

        assertUpdate(session, format("CREATE TABLE test_truncate_transform (d VARCHAR, b BIGINT)" +
                " WITH (format = '%s', partitioning = ARRAY['truncate(d, 2)'])", format.name()));

        String insertSql = "INSERT INTO test_truncate_transform VALUES" +
                "('abcd', 1)," +
                "('abxy', 2)," +
                "('ab598', 3)," +
                "('mommy', 4)," +
                "('moscow', 5)," +
                "('Greece', 6)," +
                "('Grozny', 7)";
        assertUpdate(session, insertSql, 7);

        assertQuery(session, "SELECT COUNT(*) FROM \"test_truncate_transform$partitions\"", "SELECT 3");

        assertQuery(session, "SELECT b FROM test_truncate_transform WHERE substr(d, 1, 2) = 'ab'", "SELECT b FROM (VALUES (1), (2), (3)) AS t(b)");
        assertQuery(session, select + " WHERE d_trunc = 'ab'", "VALUES('ab', 3, 'ab598', 'abxy', 1, 3)");

        assertQuery(session, "SELECT b FROM test_truncate_transform WHERE substr(d, 1, 2) = 'mo'", "SELECT b FROM (VALUES (4), (5)) AS t(b)");
        assertQuery(session, select + " WHERE d_trunc = 'mo'", "VALUES('mo', 2, 'mommy', 'moscow', 4, 5)");

        assertQuery(session, "SELECT b FROM test_truncate_transform WHERE substr(d, 1, 2) = 'Gr'", "SELECT b FROM (VALUES (6), (7)) AS t(b)");
        assertQuery(session, select + " WHERE d_trunc = 'Gr'", "VALUES('Gr', 2, 'Greece', 'Grozny', 6, 7)");

        dropTable(session, "test_truncate_transform");
    }

    @Test
    public void testBucketTransform()
    {
        testWithAllFileFormats(this::testBucketTransformsForFormat);
    }

    private void testBucketTransformsForFormat(Session session, FileFormat format)
    {
        String select = "SELECT d_bucket, row_count, d.min AS d_min, d.max AS d_max, b.min AS b_min, b.max AS b_max FROM \"test_bucket_transform$partitions\"";

        assertUpdate(session, format("CREATE TABLE test_bucket_transform (d VARCHAR, b BIGINT)" +
                " WITH (format = '%s', partitioning = ARRAY['bucket(d, 2)'])", format.name()));
        String insertSql = "INSERT INTO test_bucket_transform VALUES" +
                "('abcd', 1)," +
                "('abxy', 2)," +
                "('ab598', 3)," +
                "('mommy', 4)," +
                "('moscow', 5)," +
                "('Greece', 6)," +
                "('Grozny', 7)";
        assertUpdate(session, insertSql, 7);

        assertQuery(session, "SELECT COUNT(*) FROM \"test_bucket_transform$partitions\"", "SELECT 2");

        assertQuery(session, select + " WHERE d_bucket = 0", "VALUES(0, 3, 'Grozny', 'mommy', 1, 7)");

        assertQuery(session, select + " WHERE d_bucket = 1", "VALUES(1, 4, 'Greece', 'moscow', 2, 6)");

        dropTable(session, "test_bucket_transform");
    }

    private void testWithAllFileFormats(BiConsumer<Session, FileFormat> test)
    {
        test.accept(getSession(), FileFormat.PARQUET);
        test.accept(getSession(), FileFormat.ORC);
    }

    protected void dropTable(Session session, String table)
    {
        assertUpdate(session, "DROP TABLE " + table);
        assertFalse(getQueryRunner().tableExists(session, table));
    }

    protected void unregisterTable(String schemaName, String newTableName)
    {
        assertUpdate("CALL system.unregister_table('" + schemaName + "', '" + newTableName + "')");
    }

    @Test
    public void testCreateNestedPartitionedTable()
    {
        testWithAllFileFormats(this::testCreateNestedPartitionedTable);
    }

    public void testCreateNestedPartitionedTable(Session session, FileFormat fileFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_nested_table (" +
                " bool BOOLEAN" +
                ", int INTEGER" +
                ", arr ARRAY(VARCHAR)" +
                ", big BIGINT" +
                ", rl REAL" +
                ", dbl DOUBLE" +
                ", mp MAP(INTEGER, VARCHAR)" +
                ", dec DECIMAL(5,2)" +
                ", vc VARCHAR" +
                ", vb VARBINARY" +
                ", str ROW(id INTEGER , vc VARCHAR)" +
                ", dt DATE)" +
                " WITH (partitioning = ARRAY['int']," +
                " format = '" + fileFormat + "'" +
                ")";

        assertUpdate(session, createTable);

        assertUpdate(session, "INSERT INTO test_nested_table " +
                " select true, 1, array['uno', 'dos', 'tres'], BIGINT '1', REAL '1.0', DOUBLE '1.0', map(array[1,2,3,4], array['ek','don','teen','char'])," +
                " CAST(1.0 as DECIMAL(5,2))," +
                " 'one', VARBINARY 'binary0/1values',\n" +
                " (CAST(ROW(null, 'this is a random value') AS ROW(int, varchar))), current_date", 1);
        MaterializedResult result = computeActual("SELECT * from test_nested_table");
        assertEquals(result.getRowCount(), 1);

        dropTable(session, "test_nested_table");

        @Language("SQL") String createTable2 = "" +
                "CREATE TABLE test_nested_table (" +
                " int INTEGER" +
                ", arr ARRAY(ROW(id INTEGER, vc VARCHAR))" +
                ", big BIGINT" +
                ", rl REAL" +
                ", dbl DOUBLE" +
                ", mp MAP(INTEGER, ARRAY(VARCHAR))" +
                ", dec DECIMAL(5,2)" +
                ", str ROW(id INTEGER, vc VARCHAR, arr ARRAY(INTEGER))" +
                ", vc VARCHAR)" +
                " WITH (partitioning = ARRAY['int']," +
                " format = '" + fileFormat + "'" +
                ")";

        assertUpdate(session, createTable2);

        assertUpdate(session, "INSERT INTO test_nested_table " +
                " select 1, array[cast(row(1, null) as row(int, varchar)), cast(row(2, 'dos') as row(int, varchar))], BIGINT '1', REAL '1.0', DOUBLE '1.0', " +
                "map(array[1,2], array[array['ek', 'one'], array['don', 'do', 'two']]), CAST(1.0 as DECIMAL(5,2)), " +
                "CAST(ROW(1, 'this is a random value', null) AS ROW(int, varchar, array(int))), 'one'", 1);
        result = computeActual("SELECT * from test_nested_table");
        assertEquals(result.getRowCount(), 1);

        @Language("SQL") String createTable3 = "" +
                "CREATE TABLE test_nested_table2 WITH (partitioning = ARRAY['int']) as select * from test_nested_table";

        assertUpdate(session, createTable3, 1);

        result = computeActual("SELECT * from test_nested_table2");
        assertEquals(result.getRowCount(), 1);

        dropTable(session, "test_nested_table");
        dropTable(session, "test_nested_table2");
    }

    @Test
    public void testReadEmptyTable()
    {
        assertUpdate("CREATE TABLE test_read_empty (a bigint, b double, c varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_read_empty"));
        assertQuery("SELECT * FROM test_read_empty", "SELECT * FROM region WHERE name='not_exist'");

        assertUpdate("DROP TABLE test_read_empty");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_original"));
    }

    @Test
    public void testBasicTableStatistics()
    {
        Session session = getSession();
        String tableName = "test_basic_table_statistics";
        assertUpdate(format("CREATE TABLE %s (col REAL)", tableName));

        assertQuery(session, "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "  ('col', null, null, null, NULL, NULL, NULL), " +
                        "  (NULL, NULL, NULL, NULL, 0e0, NULL, NULL)");

        assertUpdate("INSERT INTO " + tableName + " VALUES -10", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES 100", 1);

        assertQuery(session, "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "  ('col', 96.0, NULL, 0.0, NULL, '-10.0', '100.0'), " +
                        "  (NULL, NULL, NULL, NULL, 2e0, NULL, NULL)");
        assertUpdate("INSERT INTO " + tableName + " VALUES 200", 1);
        assertQuery(session, "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "  ('col', 144.0, NULL, 0.0, NULL, '-10.0', '200.0'), " +
                        "  (NULL, NULL, NULL, NULL, 3e0, NULL, NULL)");

        dropTable(session, tableName);
    }

    @Test
    public void testPartitionedByRealWithNaNInf()
    {
        Session session = getSession();
        String tableName = "test_partitioned_by_real";

        assertUpdate("CREATE TABLE " + tableName + " (id integer, part real) WITH(partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, real 'NaN')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, real 'Infinity')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, real '-Infinity')", 1);

        assertQuery("SELECT part FROM " + tableName + " WHERE id = 1", "VALUES cast('NaN' as real)");
        assertQuery("SELECT part FROM " + tableName + " WHERE id = 2", "VALUES cast('Infinity' as real)");
        assertQuery("SELECT part FROM " + tableName + " WHERE id = 3", "VALUES cast('-Infinity' as real)");

        assertQuery("SELECT id FROM " + tableName + " WHERE is_nan(part)", "VALUES 1");
        assertQuery("SELECT id FROM " + tableName + " WHERE is_infinite(part) ORDER BY id", "VALUES (2),(3)");

        dropTable(session, tableName);
    }

    @Test
    public void testPartitionedByDoubleWithNaNInf()
    {
        Session session = getSession();
        String tableName = "test_partitioned_by_double";

        assertUpdate("CREATE TABLE " + tableName + " (id integer, part double) WITH(partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, double 'NaN')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, double 'Infinity')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, double '-Infinity')", 1);

        assertQuery("SELECT part FROM " + tableName + " WHERE id = 1", "VALUES cast('NaN' as double)");
        assertQuery("SELECT part FROM " + tableName + " WHERE id = 2", "VALUES cast('Infinity' as double)");
        assertQuery("SELECT part FROM " + tableName + " WHERE id = 3", "VALUES cast('-Infinity' as double)");

        assertQuery("SELECT id FROM " + tableName + " WHERE is_nan(part)", "VALUES 1");
        assertQuery("SELECT id FROM " + tableName + " WHERE is_infinite(part) ORDER BY id", "VALUES (2),(3)");

        dropTable(session, tableName);
    }

    @Test
    public void testFilterBySubfieldOfRowType()
    {
        Session session = getSession();
        String tableName = "test_filter_by_subfieldofrow";

        assertUpdate("CREATE TABLE " + tableName + " (id integer, r row(a integer, b varchar))");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, (1, '1001'))", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, (2, '1002'))", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, (3, '1003'))", 1);

        assertQuery("SELECT * FROM " + tableName + " WHERE r.a = 1", "VALUES (1, (1, '1001'))");
        assertQuery("SELECT * FROM " + tableName + " WHERE r.b = '1003'", "VALUES (3, (3, '1003'))");
        assertQuery("SELECT * FROM " + tableName + " WHERE r.a > 1 and r.b < '1003'", "VALUES (2, (2, '1002'))");
        dropTable(session, tableName);
    }

    protected String getLocation(String schema, String table)
    {
        return null;
    }

    protected Path getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory().resolve(TEST_DATA_DIRECTORY);
        return dataDirectory.getParent().resolve(TEST_CATALOG_DIRECTORY);
    }

    protected Table getIcebergTable(ConnectorSession session, String namespace, String tableName)
    {
        return null;
    }

    protected void createTableWithMergeOnRead(Session session, String schema, String tableName)
    {
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer) WITH (format_version = '2')");

        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(ICEBERG_CATALOG).get().getConnectorId();

        Table icebergTable = getIcebergTable(session.toConnectorSession(connectorId), schema, tableName);

        UpdateProperties updateProperties = icebergTable.updateProperties();
        updateProperties.set("write.merge.mode", "merge-on-read");
        updateProperties.commit();
    }

    protected void cleanupTableWithMergeOnRead(String tableName)
    {
        Session session = getSession();
        dropTable(session, tableName);
    }

    @Test
    public void testMergeOnReadEnabled()
    {
        String tableName = "test_merge_on_read_enabled";
        try {
            Session session = getSession();

            createTableWithMergeOnRead(session, "tpch", tableName);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES (1, 1)", 1);
            assertUpdate(session, "INSERT INTO " + tableName + " VALUES (2, 2)", 1);
            assertQuery(session, "SELECT * FROM " + tableName, "VALUES (1, 1), (2, 2)");
        }
        finally {
            cleanupTableWithMergeOnRead(tableName);
        }
    }

    @Test
    public void testMergeOnReadDisabled()
    {
        String tableName = "test_merge_on_read_disabled";
        @Language("RegExp") String errorMessage = "merge-on-read table mode not supported yet";
        try {
            Session session = Session.builder(getSession())
                    .setCatalogSessionProperty(ICEBERG_CATALOG, "merge_on_read_enabled", "false")
                    .build();

            createTableWithMergeOnRead(session, "tpch", tableName);
            assertQueryFails(session, "INSERT INTO " + tableName + " VALUES (1, 1)", errorMessage);
            assertQueryFails(session, "INSERT INTO " + tableName + " VALUES (2, 2)", errorMessage);
            assertQueryFails(session, "SELECT * FROM " + tableName, errorMessage);
        }
        finally {
            cleanupTableWithMergeOnRead(tableName);
        }
    }

    @Test
    public void testTableStatisticsTimestamp()
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(UTC_KEY)
                .build();
        String tableName = "test_table_statistics_timestamp";
        assertUpdate(session, format("CREATE TABLE %s (col TIMESTAMP)", tableName));

        assertQuery(session, "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "  ('col', null, null, null, NULL, NULL, NULL), " +
                        "  (NULL, NULL, NULL, NULL, 0e0, NULL, NULL)");

        assertUpdate(session, "INSERT INTO " + tableName + " VALUES TIMESTAMP '2021-01-02 09:04:05.321'", 1);
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES TIMESTAMP '2022-12-22 10:07:08.456'", 1);

        assertQuery(session, "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "  ('col', 113.0, NULL, 0.0, NULL, '2021-01-02 09:04:05.321', '2022-12-22 10:07:08.456'), " +
                        "  (NULL, NULL, NULL, NULL, 2e0, NULL, NULL)");
        dropTable(session, tableName);
    }

    @Test
    public void testDatePartitionedByYear()
    {
        Session session = getSession();
        String tableName = "test_date_partitioned_by_year";

        assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 date) WITH(partitioning = ARRAY['year(c2)'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, date '2022-10-01')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, date '2023-11-02')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, date '1980-01-01'), (4, date '1990-02-02')", 2);

        assertQuery("SELECT c2_year, row_count, file_count FROM " + "\"" + tableName + "$partitions\" ORDER BY c2_year",
                "VALUES (10, 1, 1), (20, 1, 1), (52, 1, 1), (53, 1, 1)");
        assertQuery("SELECT * FROM " + tableName + " WHERE year(c2) = 2023", "VALUES (2, '2023-11-02')");

        dropTable(session, tableName);
    }

    @Test
    public void testDatePartitionedByMonth()
    {
        Session session = getSession();
        String tableName = "test_date_partitioned_by_month";

        assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 date) WITH(partitioning = ARRAY['month(c2)'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, date '2022-01-01')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, date '2023-11-02')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, date '1970-02-02'), (4, date '1971-01-02')", 2);

        assertQuery("SELECT c2_month, row_count, file_count FROM " + "\"" + tableName + "$partitions\" ORDER BY c2_month",
                "VALUES (1, 1, 1), (12, 1, 1), (624, 1, 1), (646, 1, 1)");
        assertQuery("SELECT * FROM " + tableName + " WHERE month(c2) = 11", "VALUES (2, '2023-11-02')");

        dropTable(session, tableName);
    }

    @Test
    public void testDatePartitionedByDay()
    {
        Session session = getSession();
        String tableName = "test_date_partitioned_by_day";

        assertUpdate("CREATE TABLE " + tableName + " (c1 integer, c2 date) WITH(partitioning = ARRAY['day(c2)'])");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, date '2022-10-01')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, date '2023-11-02')", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (3, date '1970-10-01'), (4, date '1971-11-05')", 2);

        assertQuery("SELECT c2_day, row_count, file_count FROM " + "\"" + tableName + "$partitions\" ORDER BY c2_day",
                "VALUES ('1970-10-01', 1, 1), ('1971-11-05', 1, 1), ('2022-10-01', 1, 1), ('2023-11-02', 1, 1)");
        assertQuery("SELECT * FROM " + tableName + " WHERE day(c2) = 2", "VALUES (2, '2023-11-02')");

        dropTable(session, tableName);
    }

    @Test
    public void testTimestampPartitionedByYear()
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(UTC_KEY)
                .build();
        String tableName = "test_timestamp_partitioned_by_year";

        assertUpdate(session, "CREATE TABLE " + tableName + " (c1 integer, c2 timestamp) WITH(partitioning = ARRAY['year(c2)'])");
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (1, timestamp '2022-10-01 00:00:00.000')", 1);
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (2, timestamp '2023-11-02 12:10:31.315')", 1);
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (3, timestamp '1980-01-01 12:10:31.315'), (4, timestamp '1990-01-01 12:10:31.315')", 2);

        assertQuery(session, "SELECT c2_year, row_count, file_count FROM " + "\"" + tableName + "$partitions\" ORDER BY c2_year",
                "VALUES (10, 1, 1), (20, 1, 1), (52, 1, 1), (53, 1, 1)");
        assertQuery(session, "SELECT * FROM " + tableName + " WHERE year(c2) = 2023", "VALUES (2, '2023-11-02 12:10:31.315')");

        dropTable(session, tableName);
    }

    @Test
    public void testTimestampPartitionedByMonth()
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(UTC_KEY)
                .build();
        String tableName = "test_timestamp_partitioned_by_month";

        assertUpdate(session, "CREATE TABLE " + tableName + " (c1 integer, c2 timestamp) WITH(partitioning = ARRAY['month(c2)'])");
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (1, timestamp '2022-10-01 00:00:00.000')", 1);
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (2, timestamp '2023-11-02 12:10:31.315')", 1);
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (3, timestamp '1970-02-02 12:10:31.315'), (4, timestamp '1971-01-02 12:10:31.315')", 2);

        assertQuery(session, "SELECT c2_month, row_count, file_count FROM " + "\"" + tableName + "$partitions\" ORDER BY c2_month",
                "VALUES (1, 1, 1), (12, 1, 1), (633, 1, 1), (646, 1, 1)");
        assertQuery(session, "SELECT * FROM " + tableName + " WHERE month(c2) = 11", "VALUES (2, '2023-11-02 12:10:31.315')");

        dropTable(session, tableName);
    }

    @Test
    public void testTimestampPartitionedByDay()
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(UTC_KEY)
                .build();
        String tableName = "test_timestamp_partitioned_by_day";

        assertUpdate(session, "CREATE TABLE " + tableName + " (c1 integer, c2 timestamp) WITH(partitioning = ARRAY['day(c2)'])");
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (1, timestamp '2022-10-01 00:00:00.000')", 1);
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (2, timestamp '2023-11-02 12:10:31.315')", 1);
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (3, timestamp '1970-01-05 00:00:00.000'), (4, timestamp '1971-01-10 12:10:31.315')", 2);

        assertQuery(session, "SELECT c2_day, row_count, file_count FROM " + "\"" + tableName + "$partitions\" ORDER BY c2_day",
                "VALUES ('1970-01-05', 1, 1), ('1971-01-10', 1, 1), ('2022-10-01', 1, 1), ('2023-11-02', 1, 1)");
        assertQuery(session, "SELECT * FROM " + tableName + " WHERE day(c2) = 2", "VALUES (2, '2023-11-02 12:10:31.315')");

        dropTable(session, tableName);
    }

    @Test
    public void testTimestampPartitionedByHour()
    {
        Session session = Session.builder(getSession())
                .setTimeZoneKey(UTC_KEY)
                .build();
        String tableName = "test_timestamp_partitioned_by_hour";

        assertUpdate(session, "CREATE TABLE " + tableName + " (c1 integer, c2 timestamp) WITH(partitioning = ARRAY['hour(c2)'])");
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (1, timestamp '2022-10-01 10:00:00.000')", 1);
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (2, timestamp '2023-11-02 12:10:31.315')", 1);
        assertUpdate(session, "INSERT INTO " + tableName + " VALUES (3, timestamp '1970-01-01 10:10:31.315'), (4, timestamp '1970-01-02 10:10:31.315')", 2);

        assertQuery(session, "SELECT c2_hour, row_count, file_count FROM " + "\"" + tableName + "$partitions\" ORDER BY c2_hour",
                "VALUES (10, 1, 1), (34, 1, 1), (462394, 1, 1), (471924, 1, 1)");
        assertQuery(session, "SELECT * FROM " + tableName + " WHERE hour(c2) = 12", "VALUES (2, '2023-11-02 12:10:31.315')");

        dropTable(session, tableName);
    }

    @Test
    public void testRegisterTable()
    {
        String schemaName = getSession().getSchema().get();
        String tableName = "register";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 1)", 1);

        String metadataLocation = getLocation(schemaName, tableName);

        String newTableName = tableName + "_new";
        assertUpdate("CALL system.register_table('" + schemaName + "', '" + newTableName + "', '" + metadataLocation + "')");
        assertQuery("SELECT * FROM " + newTableName, "VALUES (1, 1)");

        unregisterTable(schemaName, newTableName);
        dropTable(getSession(), tableName);
    }

    @Test
    public void testRegisterTableAndInsert()
    {
        String schemaName = getSession().getSchema().get();
        String tableName = "register_insert";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 1)", 1);

        String metadataLocation = getLocation(schemaName, tableName);

        String newTableName = tableName + "_new";
        assertUpdate("CALL system.register_table('" + schemaName + "', '" + newTableName + "', '" + metadataLocation + "')");
        assertUpdate("INSERT INTO " + newTableName + " VALUES(2, 2)", 1);
        assertQuery("SELECT * FROM " + newTableName, "VALUES (1, 1), (2, 2)");

        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 1)");

        unregisterTable(schemaName, newTableName);
        dropTable(getSession(), tableName);
    }

    @Test
    public void testRegisterTableWithFileName()
    {
        String schemaName = getSession().getSchema().get();
        String tableName = "register_filename";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 1)", 1);

        String metadataLocation = getLocation(schemaName, tableName);
        String metadataFileName = getMetadataFileLocation(getSession().toConnectorSession(), schemaName, tableName, metadataLocation);

        // Register new table with procedure
        String newTableName = tableName + "_new";
        assertUpdate("CALL system.register_table('" + schemaName + "', '" + newTableName + "', '" + metadataLocation + "', '" + metadataFileName + "')");
        assertQuery("SELECT * FROM " + newTableName, "VALUES (1, 1)");

        unregisterTable(schemaName, newTableName);
        dropTable(getSession(), tableName);
    }

    @Test
    public void testRegisterTableWithInvalidLocation()
    {
        String schemaName = getSession().getSchema().get();
        String tableName = "register_invalid";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");
        assertUpdate("INSERT INTO " + tableName + " VALUES(1, 1)", 1);

        String metadataLocation = getLocation(schemaName, tableName).replace("//", "/") + "_invalid";

        @Language("RegExp") String errorMessage = format("Unable to find metadata at location %s/%s", metadataLocation, METADATA_FOLDER_NAME);
        assertQueryFails("CALL system.register_table ('" + schemaName + "', '" + tableName + "', '" + metadataLocation + "')", errorMessage);

        dropTable(getSession(), tableName);
    }

    @Test
    public void testUnregisterTable()
    {
        String schemaName = getSession().getSchema().get();
        String tableName = "unregister";
        assertUpdate("CREATE TABLE " + tableName + " (id integer, value integer)");

        // Unregister table with procedure
        assertUpdate("CALL system.unregister_table('" + schemaName + "', '" + tableName + "')");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }
}
