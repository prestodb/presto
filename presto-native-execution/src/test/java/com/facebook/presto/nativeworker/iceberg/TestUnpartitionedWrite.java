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
package com.facebook.presto.nativeworker.iceberg;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.time.LocalTime;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static org.testng.Assert.assertEquals;

public class TestUnpartitionedWrite
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Test
    public void testPrimitiveTypes()
    {
        String tableName = "insert_primitives";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "col_boolean BOOLEAN, " +
                    "col_integer INTEGER, " +
                    "col_bigint BIGINT, " +
                    "col_real REAL, " +
                    "col_double DOUBLE, " +
                    "col_decimal_short DECIMAL(10, 2), " +
                    "col_decimal_long DECIMAL(30, 10), " +
                    "col_varchar VARCHAR, " +
                    "col_varbinary VARBINARY, " +
                    "col_date DATE, " +
                    "col_timestamp TIMESTAMP" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "true, " +
                    "12345, " +
                    "1234567890123, " +
                    "REAL '3.14', " +
                    "2.718281828, " +
                    "DECIMAL '12345.67', " +
                    "DECIMAL '12345678901234567890.1234567890', " +
                    "'hello world', " +
                    "X'48656C6C6F', " +
                    "DATE '2025-01-15', " +
                    "TIMESTAMP '2024-01-15 14:30:00'" +
                    ")", tableName), 1);

            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", tableName), 1);

            assertQuery(String.format("SELECT col_boolean, col_integer, col_varchar FROM %s WHERE col_boolean IS NOT NULL", tableName),
                    "VALUES (true, 12345, 'hello world')");
            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE col_boolean IS NULL", tableName), "VALUES (BIGINT '1')");
            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '2')");
            assertQuery(String.format("SELECT col_bigint, col_date FROM %s WHERE col_boolean = true", tableName),
                    "VALUES (BIGINT '1234567890123', DATE '2025-01-15')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testTimeType()
    {
        String tableName = "insert_times";
        try {
            assertUpdate(String.format("CREATE TABLE %s (col_time TIME) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (TIME '11:12:13')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (NULL), (NULL)", tableName), 2);

            long count = (long) getExpectedQueryRunner().execute(getSession(),
                            String.format("SELECT count(*) from %s", tableName),
                            ImmutableList.of(BigintType.BIGINT))
                    .getOnlyValue();
            assertEquals(count, 3L);

            Object obj = getExpectedQueryRunner().execute(getSession(),
                            String.format("SELECT col_time from %s WHERE col_time IS NOT NULL", tableName),
                            ImmutableList.of(TimeType.TIME))
                    .getOnlyValue();
            assertEquals(obj, LocalTime.of(11, 12, 13));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testArrayType()
    {
        String tableName = "insert_array";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "int_array ARRAY(INTEGER), " +
                    "varchar_array ARRAY(VARCHAR), " +
                    "nested_array ARRAY(ARRAY(INTEGER))" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "1, " +
                    "ARRAY[1, 2, 3, 4, 5], " +
                    "ARRAY['a', 'b', 'c'], " +
                    "ARRAY[ARRAY[1, 2], ARRAY[3, 4]]" +
                    ")", tableName), 1);

            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "2, NULL, NULL, NULL)", tableName), 1);

            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "3, ARRAY[], ARRAY[], ARRAY[])", tableName), 1);

            assertQuery(String.format("SELECT id, int_array FROM %s WHERE id = 1", tableName),
                    "VALUES (1, ARRAY[1, 2, 3, 4, 5])");
            assertQuery(String.format("SELECT id FROM %s WHERE int_array IS NULL", tableName),
                    "VALUES (2)");
            assertQuery(String.format("SELECT id, cardinality(int_array) FROM %s WHERE id = 3", tableName),
                    "VALUES (3, BIGINT '0')");
            assertQuery(String.format("SELECT id, varchar_array FROM %s WHERE id = 1", tableName),
                    "VALUES (1, ARRAY['a', 'b', 'c'])");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testMapType()
    {
        String tableName = "insert_map";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "simple_map MAP(VARCHAR, INTEGER), " +
                    "nested_map MAP(VARCHAR, ARRAY(INTEGER))" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "1, " +
                    "MAP(ARRAY['key1', 'key2'], ARRAY[100, 200]), " +
                    "MAP(ARRAY['k1', 'k2'], ARRAY[ARRAY[1, 2], ARRAY[3, 4]])" +
                    ")", tableName), 1);

            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "2, NULL, NULL)", tableName), 1);

            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "3, MAP(), MAP())", tableName), 1);

            assertQuery(String.format("SELECT id, simple_map FROM %s WHERE id = 1", tableName));
            assertQuery(String.format("SELECT id FROM %s WHERE simple_map IS NULL", tableName),
                    "VALUES (2)");
            assertQuery(String.format("SELECT id, cardinality(simple_map) FROM %s WHERE id = 3", tableName),
                    "VALUES (3, BIGINT '0')");
            assertQuery(String.format("SELECT id, simple_map['key1'] FROM %s WHERE id = 1", tableName),
                    "VALUES (1, 100)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testRowType()
    {
        String tableName = "insert_row";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "person ROW(name VARCHAR, age INTEGER), " +
                    "nested_row ROW(id INTEGER, address ROW(city VARCHAR, zip INTEGER))" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "1, " +
                    "ROW('Alice', 30), " +
                    "ROW(100, ROW('New York', 10001))" +
                    ")", tableName), 1);

            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "2, NULL, NULL)", tableName), 1);

            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "3, ROW(NULL, NULL), ROW(NULL, NULL))", tableName), 1);

            assertQuery(String.format("SELECT id, person.name, person.age FROM %s WHERE id = 1", tableName),
                    "VALUES (1, 'Alice', 30)");
            assertQuery(String.format("SELECT id FROM %s WHERE person IS NULL", tableName),
                    "VALUES (2)");
            assertQuery(String.format("SELECT id, nested_row.address.city FROM %s WHERE id = 1", tableName),
                    "VALUES (1, 'New York')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testComplexNestedTypes()
    {
        String tableName = "insert_complex";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "array_of_rows ARRAY(ROW(name VARCHAR, value INTEGER)), " +
                    "map_of_arrays MAP(VARCHAR, ARRAY(INTEGER)), " +
                    "row_with_array ROW(id INTEGER, tags ARRAY(VARCHAR))" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "1, " +
                    "ARRAY[ROW('item1', 10), ROW('item2', 20)], " +
                    "MAP(ARRAY['key1', 'key2'], ARRAY[ARRAY[1, 2], ARRAY[3, 4]]), " +
                    "ROW(100, ARRAY['tag1', 'tag2', 'tag3'])" +
                    ")", tableName), 1);

            assertQuery(String.format("SELECT id FROM %s WHERE id = 1", tableName), "VALUES (1)");
            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '1')");
            assertQuery(String.format("SELECT id, cardinality(array_of_rows) FROM %s", tableName),
                    "VALUES (1, BIGINT '2')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testMultipleRows()
    {
        String tableName = "insert_multiple";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "name VARCHAR, " +
                    "value DOUBLE" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertUpdate(String.format("INSERT INTO %s VALUES " +
                    "(1, 'first', 1.1), " +
                    "(2, 'second', 2.2), " +
                    "(3, 'third', 3.3), " +
                    "(4, 'fourth', 4.4), " +
                    "(5, 'fifth', 5.5)", tableName), 5);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '5')");
            assertQuery(String.format("SELECT id, name FROM %s WHERE id = 3", tableName),
                    "VALUES (3, 'third')");
            assertQuery(String.format("SELECT SUM(value) FROM %s", tableName), "VALUES (DOUBLE '16.5')");
            assertQuery(String.format("SELECT name FROM %s ORDER BY id", tableName),
                    "VALUES ('first'), ('second'), ('third'), ('fourth'), ('fifth')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testInsertIntoSelect()
    {
        String sourceTable = "source";
        String targetTable = "target";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "name VARCHAR, " +
                    "value DOUBLE" +
                    ") WITH (format = 'PARQUET')", sourceTable));

            assertUpdate(String.format("INSERT INTO %s VALUES " +
                    "(1, 'Alice', 100.0), " +
                    "(2, 'Bob', 200.0), " +
                    "(3, 'Charlie', 300.0)", sourceTable), 3);

            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "name VARCHAR, " +
                    "value DOUBLE" +
                    ") WITH (format = 'PARQUET')", targetTable));

            assertUpdate(String.format("INSERT INTO %s SELECT * FROM %s", targetTable, sourceTable), 3);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '3')");
            assertQuery(String.format("SELECT id, name, value FROM %s ORDER BY id", targetTable),
                    "VALUES (1, 'Alice', DOUBLE '100.0'), (2, 'Bob', DOUBLE '200.0'), (3, 'Charlie', DOUBLE '300.0')");

            assertUpdate(String.format("INSERT INTO %s SELECT * FROM %s WHERE id > 1", targetTable, sourceTable), 2);
            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '5')");

            assertUpdate(String.format("INSERT INTO %s SELECT id, name, value * 2 FROM %s WHERE id = 1", targetTable, sourceTable), 1);
            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '6')");
            assertQuery(String.format("SELECT value FROM %s WHERE id = 1 ORDER BY value", targetTable),
                    "VALUES (DOUBLE '100.0'), (DOUBLE '200.0')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", sourceTable));
        }
    }

    @Test
    public void testInsertIntoSelectWithComplexTypes()
    {
        String sourceTable = "complex_source";
        String targetTable = "complex_target";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "int_array ARRAY(INTEGER), " +
                    "string_map MAP(VARCHAR, INTEGER), " +
                    "person ROW(name VARCHAR, age INTEGER)" +
                    ") WITH (format = 'PARQUET')", sourceTable));

            assertUpdate(String.format("INSERT INTO %s VALUES " +
                    "(1, ARRAY[1, 2, 3], MAP(ARRAY['a', 'b'], ARRAY[10, 20]), ROW('Alice', 30)), " +
                    "(2, ARRAY[4, 5, 6], MAP(ARRAY['c', 'd'], ARRAY[30, 40]), ROW('Bob', 40))", sourceTable), 2);

            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "int_array ARRAY(INTEGER), " +
                    "string_map MAP(VARCHAR, INTEGER), " +
                    "person ROW(name VARCHAR, age INTEGER)" +
                    ") WITH (format = 'PARQUET')", targetTable));

            assertUpdate(String.format("INSERT INTO %s SELECT * FROM %s", targetTable, sourceTable), 2);
            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '2')");
            assertQuery(String.format("SELECT id, int_array FROM %s WHERE id = 1", targetTable),
                    "VALUES (1, ARRAY[1, 2, 3])");
            assertQuery(String.format("SELECT id, person.name, person.age FROM %s WHERE id = 2", targetTable),
                    "VALUES (2, 'Bob', 40)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", sourceTable));
        }
    }

    @Test
    public void testColumnReordering()
    {
        String tableName = "column_reorder";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "col1 INTEGER, " +
                    "col2 VARCHAR, " +
                    "col3 DOUBLE, " +
                    "col4 BOOLEAN" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertUpdate(String.format("INSERT INTO %s (col3, col1, col4, col2) VALUES (3.14, 42, true, 'test')", tableName), 1);

            assertQuery(String.format("SELECT col1, col2, col3, col4 FROM %s", tableName),
                    "VALUES (42, 'test', DOUBLE '3.14', true)");

            assertUpdate(String.format("INSERT INTO %s (col2, col1, col3, col4) VALUES ('second', 100, 2.71, false)", tableName), 1);

            assertQuery(String.format("SELECT col1, col2, col3, col4 FROM %s WHERE col1 = 100", tableName),
                    "VALUES (100, 'second', DOUBLE '2.71', false)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testPartialColumns()
    {
        String tableName = "partial_columns";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "name VARCHAR, " +
                    "age INTEGER, " +
                    "email VARCHAR, " +
                    "score DOUBLE" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertUpdate(String.format("INSERT INTO %s (id, name) VALUES (1, 'Alice')", tableName), 1);

            assertQuery(String.format("SELECT id, name, age, email, score FROM %s WHERE id = 1", tableName),
                    "VALUES (1, 'Alice', NULL, NULL, NULL)");

            assertUpdate(String.format("INSERT INTO %s (id, age, score) VALUES (2, 30, 95.5)", tableName), 1);

            assertQuery(String.format("SELECT id, name, age, email, score FROM %s WHERE id = 2", tableName),
                    "VALUES (2, NULL, 30, NULL, DOUBLE '95.5')");

            assertUpdate(String.format("INSERT INTO %s (id, name, age, email, score) VALUES (3, 'Charlie', 25, 'charlie@example.com', 88.0)", tableName), 1);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '3')");
            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE name IS NULL", tableName), "VALUES (BIGINT '1')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testNullsInComplexTypes()
    {
        String tableName = "nulls_complex";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "nullable_array ARRAY(INTEGER), " +
                    "nullable_map MAP(VARCHAR, INTEGER), " +
                    "nullable_row ROW(name VARCHAR, value INTEGER)" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertUpdate(String.format("INSERT INTO %s VALUES (1, NULL, NULL, NULL)", tableName), 1);

            // Insert with empty collections (different from NULL)
            assertUpdate(String.format("INSERT INTO %s VALUES (2, ARRAY[], MAP(), ROW(NULL, NULL))", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (3, ARRAY[1, NULL, 3], MAP(ARRAY['a'], ARRAY[NULL]), ROW('test', NULL))", tableName), 1);
            assertQuery(String.format("SELECT id FROM %s WHERE nullable_array IS NULL", tableName), "VALUES (1)");

            // Verify empty vs NULL distinction
            assertQuery(String.format("SELECT id, cardinality(nullable_array) FROM %s WHERE id = 2", tableName),
                    "VALUES (2, BIGINT '0')");

            // Verify NULL elements within complex types
            assertQuery(String.format("SELECT id, nullable_row.name FROM %s WHERE id = 3", tableName),
                    "VALUES (3, 'test')");
            assertQuery(String.format("SELECT id FROM %s WHERE nullable_row.value IS NULL AND nullable_row.name = 'test'", tableName),
                    "VALUES (3)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSelectWithAggregation()
    {
        String sourceTable = "agg_source";
        String targetTable = "agg_target";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "category VARCHAR, " +
                    "amount DOUBLE, " +
                    "quantity INTEGER" +
                    ") WITH (format = 'PARQUET')", sourceTable));

            assertUpdate(String.format("INSERT INTO %s VALUES " +
                    "(1, 'Electronics', 100.0, 2), " +
                    "(2, 'Electronics', 200.0, 1), " +
                    "(3, 'Books', 50.0, 5), " +
                    "(4, 'Books', 30.0, 3), " +
                    "(5, 'Electronics', 150.0, 1)", sourceTable), 5);

            assertUpdate(String.format("CREATE TABLE %s (" +
                    "category VARCHAR, " +
                    "total_amount DOUBLE, " +
                    "total_quantity BIGINT, " +
                    "avg_amount DOUBLE, " +
                    "item_count BIGINT" +
                    ") WITH (format = 'PARQUET')", targetTable));

            assertUpdate(String.format("INSERT INTO %s " +
                    "SELECT category, SUM(amount), SUM(quantity), AVG(amount), COUNT(*) " +
                    "FROM %s GROUP BY category", targetTable, sourceTable), 2);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '2')");
            assertQuery(String.format("SELECT category, total_amount, total_quantity, item_count FROM %s WHERE category = 'Electronics'", targetTable),
                    "VALUES ('Electronics', DOUBLE '450.0', BIGINT '4', BIGINT '3')");
            assertQuery(String.format("SELECT category, total_amount, total_quantity, item_count FROM %s WHERE category = 'Books'", targetTable),
                    "VALUES ('Books', DOUBLE '80.0', BIGINT '8', BIGINT '2')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", sourceTable));
        }
    }

    @Test
    public void testInsertWithSpecialCharacters()
    {
        String tableName = "special_chars";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "text VARCHAR" +
                    ") WITH (format = 'PARQUET')", tableName));

            // Insert with Unicode characters
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'Hello 世界 🌍')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (2, 'O''Brien')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (3, 'Line1\nLine2\tTabbed')", tableName), 1);
            // Insert with various Unicode scripts
            assertUpdate(String.format("INSERT INTO %s VALUES (4, '江畔何人初见月？江月何年初照人？안녕하세요')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (5, '😀 😃 😄 😁')", tableName), 1);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '5')");
            assertQuery(String.format("SELECT text FROM %s WHERE id = 1", tableName), "VALUES ('Hello 世界 🌍')");
            assertQuery(String.format("SELECT text FROM %s WHERE id = 2", tableName), "VALUES ('O''Brien')");
            assertQuery(String.format("SELECT text FROM %s WHERE id = 4", tableName), "VALUES ('江畔何人初见月？江月何年初照人？안녕하세요')");
            assertQuery(String.format("SELECT text FROM %s WHERE id = 5", tableName), "VALUES ('😀 😃 😄 😁')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testInsertWithNumericEdgeCases()
    {
        String tableName = "test_numeric_edges";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "int_val INTEGER, " +
                    "bigint_val BIGINT, " +
                    "double_val DOUBLE, " +
                    "real_val REAL" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertUpdate(String.format("INSERT INTO %s VALUES (1, 2147483647, BIGINT '9223372036854775807', 1.7976931348623157E308, REAL '3.4028235E38')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (2, INT '-2147483648', BIGINT '-9223372036854775808', -1.7976931348623157E308, REAL '-3.4028235E38')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (3, INTEGER '0', BIGINT '0', 0.0, REAL '0.0')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (4, NULL, NULL, infinity(), REAL 'Infinity')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (5, NULL, NULL, -infinity(), REAL '-Infinity')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (6, NULL, NULL, nan(), REAL 'NaN')", tableName), 1);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '6')");
            assertQuery(String.format("SELECT int_val, bigint_val FROM %s WHERE id = 1", tableName),
                    "VALUES (2147483647, BIGINT '9223372036854775807')");
            assertQuery(String.format("SELECT int_val, bigint_val FROM %s WHERE id = 2", tableName),
                    "VALUES (INT '-2147483648', BIGINT '-9223372036854775808')");
            assertQuery(String.format("SELECT id FROM %s WHERE is_infinite(double_val) AND double_val > 0", tableName), "VALUES (4)");
            assertQuery(String.format("SELECT id FROM %s WHERE is_nan(double_val)", tableName), "VALUES (6)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSelectWithJoin()
    {
        String table1 = "join_table1";
        String table2 = "join_table2";
        String targetTable = "join_target";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "name VARCHAR" +
                    ") WITH (format = 'PARQUET')", table1));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", table1), 3);

            assertUpdate(String.format("CREATE TABLE %s (" +
                    "user_id INTEGER, " +
                    "score DOUBLE, " +
                    "department VARCHAR" +
                    ") WITH (format = 'PARQUET')", table2));

            assertUpdate(String.format("INSERT INTO %s VALUES (1, 95.5, 'Engineering'), (2, 88.0, 'Sales'), (3, 92.0, 'Marketing')", table2), 3);

            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "name VARCHAR, " +
                    "score DOUBLE, " +
                    "department VARCHAR" +
                    ") WITH (format = 'PARQUET')", targetTable));

            assertUpdate(String.format("INSERT INTO %s SELECT t1.id, t1.name, t2.score, t2.department " +
                    "FROM %s t1 JOIN %s t2 ON t1.id = t2.user_id", targetTable, table1, table2), 3);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '3')");
            assertQuery(String.format("SELECT name, score, department FROM %s WHERE id = 1", targetTable),
                    "VALUES ('Alice', DOUBLE '95.5', 'Engineering')");
            assertQuery(String.format("SELECT name, department FROM %s WHERE id = 2", targetTable),
                    "VALUES ('Bob', 'Sales')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", table2));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", table1));
        }
    }

    @Test
    public void testWithCast()
    {
        String sourceTable = "cast_source";
        String targetTable = "cast_target";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id VARCHAR, " +
                    "amount VARCHAR, " +
                    "is_active VARCHAR" +
                    ") WITH (format = 'PARQUET')", sourceTable));

            assertUpdate(String.format("INSERT INTO %s VALUES ('1', '100.5', 'true'), ('2', '200.75', 'false')", sourceTable), 2);

            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "amount DOUBLE, " +
                    "is_active BOOLEAN" +
                    ") WITH (format = 'PARQUET')", targetTable));

            assertUpdate(String.format("INSERT INTO %s SELECT CAST(id AS INTEGER), CAST(amount AS DOUBLE), CAST(is_active AS BOOLEAN) FROM %s",
                    targetTable, sourceTable), 2);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '2')");
            assertQuery(String.format("SELECT id, amount, is_active FROM %s WHERE id = 1", targetTable),
                    "VALUES (1, DOUBLE '100.5', true)");
            assertQuery(String.format("SELECT id, amount, is_active FROM %s WHERE id = 2", targetTable),
                    "VALUES (2, DOUBLE '200.75', false)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", sourceTable));
        }
    }

    @Test
    public void testDateTimeEdgeCases()
    {
        String tableName = "datetime_edges";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "date_val DATE, " +
                    "timestamp_val TIMESTAMP" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertUpdate(String.format("INSERT INTO %s VALUES (1, DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (2, DATE '2024-02-29', TIMESTAMP '2024-02-29 23:59:59')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (3, DATE '2100-12-31', TIMESTAMP '2100-12-31 23:59:59.999')", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (4, DATE '1900-01-01', TIMESTAMP '1900-01-01 00:00:00')", tableName), 1);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '4')");
            assertQuery(String.format("SELECT date_val FROM %s WHERE id = 1", tableName), "VALUES (DATE '1970-01-01')");
            assertQuery(String.format("SELECT date_val FROM %s WHERE id = 2", tableName), "VALUES (DATE '2024-02-29')");
            assertQuery(String.format("SELECT year(date_val), month(date_val), day(date_val) FROM %s WHERE id = 3", tableName),
                    "VALUES (BIGINT '2100', BIGINT '12', BIGINT '31')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSelectWithUnion()
    {
        String table1 = "union_table1";
        String table2 = "union_table2";
        String targetTable = "union_target";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "name VARCHAR, " +
                    "value DOUBLE" +
                    ") WITH (format = 'PARQUET')", table1));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)", table1), 2);

            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "name VARCHAR, " +
                    "value DOUBLE" +
                    ") WITH (format = 'PARQUET')", table2));
            assertUpdate(String.format("INSERT INTO %s VALUES (3, 'Charlie', 300.0), (4, 'David', 400.0)", table2), 2);

            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "name VARCHAR, " +
                    "value DOUBLE" +
                    ") WITH (format = 'PARQUET')", targetTable));

            assertUpdate(String.format("INSERT INTO %s SELECT * FROM %s UNION ALL SELECT * FROM %s",
                    targetTable, table1, table2), 4);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '4')");
            assertQuery(String.format("SELECT SUM(value) FROM %s", targetTable), "VALUES (DOUBLE '1000.0')");
            assertQuery(String.format("SELECT name FROM %s ORDER BY id", targetTable),
                    "VALUES ('Alice'), ('Bob'), ('Charlie'), ('David')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", table2));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", table1));
        }
    }

    @Test
    public void testMaximumLengthVarchar()
    {
        String tableName = "long_varchar";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "small_text VARCHAR, " +
                    "medium_text VARCHAR, " +
                    "large_text VARCHAR" +
                    ") WITH (format = 'PARQUET')", tableName));

            // Insert small text (1KB)
            assertUpdate(String.format("INSERT INTO %s (id, small_text) VALUES (1, rpad('A', 1024, 'A'))", tableName), 1);

            // Insert medium text (10KB)
            assertUpdate(String.format("INSERT INTO %s (id, medium_text) VALUES (2, rpad('B', 10240, 'B'))", tableName), 1);

            // Insert large text (100KB)
            assertUpdate(String.format("INSERT INTO %s (id, large_text) VALUES (3, rpad('C', 102400, 'C'))", tableName), 1);

            // Insert using concat and rpad
            assertUpdate(String.format("INSERT INTO %s (id, large_text) VALUES (4, concat('START-', rpad('XYZ', 30000, 'XYZ'), '-END'))", tableName), 1);

            // Insert using multiple string functions
            assertUpdate(String.format("INSERT INTO %s (id, medium_text) VALUES (5, upper(rpad('test', 10240, 'test')))", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s (id, small_text) VALUES (6, lpad('Z', 2048, 'Z'))", tableName), 1);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '6')");

            assertQuery(String.format("SELECT id, length(small_text) FROM %s WHERE id = 1", tableName),
                    "VALUES (1, BIGINT '1024')");
            assertQuery(String.format("SELECT id, length(medium_text) FROM %s WHERE id = 2", tableName),
                    "VALUES (2, BIGINT '10240')");
            assertQuery(String.format("SELECT id, length(large_text) FROM %s WHERE id = 3", tableName),
                    "VALUES (3, BIGINT '102400')");

            // Verify content integrity using substr
            assertQuery(String.format("SELECT substr(small_text, 1, 10) FROM %s WHERE id = 1", tableName),
                    "VALUES ('AAAAAAAAAA')");
            assertQuery(String.format("SELECT substr(large_text, 1, 6) FROM %s WHERE id = 4", tableName),
                    "VALUES ('START-')");
            assertQuery(String.format("SELECT substr(large_text, length(large_text) - 3, 4) FROM %s WHERE id = 4", tableName),
                    "VALUES ('-END')");

            // Verify all characters are correct using rpad comparison
            assertQuery(String.format("SELECT id FROM %s WHERE id = 1 AND small_text = rpad('A', 1024, 'A')", tableName),
                    "VALUES (1)");
            assertQuery(String.format("SELECT id FROM %s WHERE id = 2 AND medium_text = rpad('B', 10240, 'B')", tableName),
                    "VALUES (2)");
            assertQuery(String.format("SELECT id FROM %s WHERE id = 3 AND large_text = rpad('C', 102400, 'C')", tableName),
                    "VALUES (3)");
            assertQuery(String.format("SELECT id, length(small_text) FROM %s WHERE id = 6", tableName),
                    "VALUES (6, BIGINT '2048')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testDecimalTypeCastingOnInsert()
    {
        String sourceTable = "decimal_source";
        String targetTable = "decimal_target";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "short_decimal DECIMAL(10, 2), " +
                    "long_decimal DECIMAL(30, 10)" +
                    ") WITH (format = 'PARQUET')", sourceTable));

            assertUpdate(String.format("INSERT INTO %s VALUES " +
                    "(1, DECIMAL '12345.67', DECIMAL '12345678901234567890.1234567890'), " +
                    "(2, DECIMAL '999.99', DECIMAL '999999999999999999.9999999999'), " +
                    "(3, DECIMAL '-123.45', DECIMAL '-123456789012345.123456')", sourceTable), 3);

            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "short_decimal DECIMAL(15, 2), " +
                    "long_decimal DECIMAL(38, 10)" +
                    ") WITH (format = 'PARQUET')", targetTable));

            assertUpdate(String.format("INSERT INTO %s SELECT * FROM %s", targetTable, sourceTable), 3);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '3')");
            assertQuery(String.format("SELECT id, short_decimal, long_decimal FROM %s WHERE id = 1", targetTable),
                    "VALUES (1, DECIMAL '12345.67', DECIMAL '12345678901234567890.1234567890')");
            assertQuery(String.format("SELECT id, short_decimal, long_decimal FROM %s WHERE id = 2", targetTable),
                    "VALUES (2, DECIMAL '999.99', DECIMAL '999999999999999999.9999999999')");
            assertQuery(String.format("SELECT id, short_decimal FROM %s WHERE id = 3", targetTable),
                    "VALUES (3, DECIMAL '-123.45')");

            // Test direct INSERT with literal values that need casting
            assertUpdate(String.format("INSERT INTO %s VALUES " +
                    "(4, DECIMAL '1.23', DECIMAL '1.2345678901')", targetTable), 1);
            assertQuery(String.format("SELECT short_decimal, long_decimal FROM %s WHERE id = 4", targetTable),
                    "VALUES (DECIMAL '1.23', DECIMAL '1.2345678901')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", sourceTable));
        }
    }
}
