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

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;

public class TestCreateTable
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
    public void testCreateSimpleTable()
    {
        String tableName = "simple";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id BIGINT, data VARCHAR) WITH (format = 'PARQUET')", tableName));
            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '0')");
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'test')", tableName), 1);
            assertQuery(String.format("SELECT id, data FROM %s", tableName), "VALUES (BIGINT '1', 'test')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testAllPrimitiveTypes()
    {
        String tableName = "primitive_types";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "col_boolean BOOLEAN, " +
                    "col_integer INTEGER, " +
                    "col_bigint BIGINT, " +
                    "col_real REAL, " +
                    "col_double DOUBLE, " +
                    "col_decimal DECIMAL(10, 2), " +
                    "col_varchar VARCHAR, " +
                    "col_varbinary VARBINARY, " +
                    "col_date DATE, " +
                    "col_timestamp TIMESTAMP" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '0')");
            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "true, 123, 456789, REAL '1.23', 4.56, DECIMAL '78.90', 'text', X'ABCD', DATE '2024-01-01', TIMESTAMP '2024-01-01 12:00:00')", tableName), 1);
            assertQuery(String.format("SELECT col_boolean, col_integer, col_varchar FROM %s", tableName),
                    "VALUES (true, 123, 'text')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testComplexTypes()
    {
        String tableName = "complex_types";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "int_array ARRAY(INTEGER), " +
                    "string_map MAP(VARCHAR, INTEGER), " +
                    "person ROW(name VARCHAR, age INTEGER)" +
                    ") WITH (format = 'PARQUET')", tableName));

            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName), "VALUES (BIGINT '0')");
            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "1, ARRAY[1, 2, 3], MAP(ARRAY['a', 'b'], ARRAY[10, 20]), ROW('Alice', 30))", tableName), 1);
            assertQuery(String.format("SELECT id, int_array FROM %s", tableName),
                    "VALUES (1, ARRAY[1, 2, 3])");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testAsSelect()
    {
        String sourceTable = "source";
        String targetTable = "target";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id BIGINT, name VARCHAR, value DOUBLE) WITH (format = 'PARQUET')", sourceTable));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'first', 1.1), (2, 'second', 2.2), (3, 'third', 3.3)", sourceTable), 3);
            assertUpdate(String.format("CREATE TABLE %s WITH (format = 'PARQUET') AS SELECT * FROM %s", targetTable, sourceTable), 3);
            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '3')");
            assertQuery(String.format("SELECT id, name, value FROM %s ORDER BY id", targetTable),
                    "VALUES (BIGINT '1', 'first', DOUBLE '1.1'), (BIGINT '2', 'second', DOUBLE '2.2'), (BIGINT '3', 'third', DOUBLE '3.3')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", sourceTable));
        }
    }

    @Test
    public void testAsSelectWithFilter()
    {
        String sourceTable = "filter_source";
        String targetTable = "filter_target";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, category VARCHAR, amount DOUBLE) WITH (format = 'PARQUET')", sourceTable));
            assertUpdate(String.format("INSERT INTO %s VALUES " +
                    "(1, 'A', 100.0), (2, 'B', 200.0), (3, 'A', 150.0), (4, 'B', 250.0), (5, 'A', 175.0)", sourceTable), 5);
            assertUpdate(String.format("CREATE TABLE %s WITH (format = 'PARQUET') AS " +
                    "SELECT * FROM %s WHERE category = 'A'", targetTable, sourceTable), 3);
            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '3')");
            assertQuery(String.format("SELECT id, amount FROM %s ORDER BY id", targetTable),
                    "VALUES (1, DOUBLE '100.0'), (3, DOUBLE '150.0'), (5, DOUBLE '175.0')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", sourceTable));
        }
    }

    @Test
    public void testAsSelectWithAggregation()
    {
        String sourceTable = "agg_source";
        String targetTable = "agg_target";

        try {
            assertUpdate(String.format("CREATE TABLE %s (category VARCHAR, amount DOUBLE) WITH (format = 'PARQUET')", sourceTable));
            assertUpdate(String.format("INSERT INTO %s VALUES " +
                    "('A', 100.0), ('B', 200.0), ('A', 150.0), ('B', 250.0), ('A', 175.0)", sourceTable), 5);
            assertUpdate(String.format("CREATE TABLE %s WITH (format = 'PARQUET') AS " +
                    "SELECT category, SUM(amount) as total_amount FROM %s GROUP BY category", targetTable, sourceTable), 2);
            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '2')");
            assertQuery(String.format("SELECT * FROM %s ORDER BY category", targetTable));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", sourceTable));
        }
    }

    @Test
    public void testAsSelectWithColumnRename()
    {
        String sourceTable = "rename_source";
        String targetTable = "rename_target";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, old_name VARCHAR) WITH (format = 'PARQUET')", sourceTable));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'value1'), (2, 'value2')", sourceTable), 2);
            assertUpdate(String.format("CREATE TABLE %s WITH (format = 'PARQUET') AS " +
                    "SELECT id, old_name as new_name FROM %s", targetTable, sourceTable), 2);
            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '2')");
            assertQuery(String.format("SELECT id, new_name FROM %s ORDER BY id", targetTable),
                    "VALUES (1, 'value1'), (2, 'value2')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", sourceTable));
        }
    }

    @Test
    public void testAsSelectEmpty()
    {
        String sourceTable = "empty_source";
        String targetTable = "empty_target";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, data VARCHAR) WITH (format = 'PARQUET')", sourceTable));
            assertUpdate(String.format("CREATE TABLE %s WITH (format = 'PARQUET') AS SELECT * FROM %s", targetTable, sourceTable), 0);
            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '0')");
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'test')", targetTable), 1);
            assertQuery(String.format("SELECT id, data FROM %s", targetTable), "VALUES (1, 'test')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", sourceTable));
        }
    }
}
