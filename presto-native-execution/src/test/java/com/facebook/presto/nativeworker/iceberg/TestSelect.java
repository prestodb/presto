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

public class TestSelect
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
    public void testSelectAll()
    {
        String tableName = "select_all";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id BIGINT, data VARCHAR, value DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', 3.0)", tableName), 3);
            assertQuery(String.format("SELECT * FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', 'a', DOUBLE '1.0'), (BIGINT '2', 'b', DOUBLE '2.0'), (BIGINT '3', 'c', DOUBLE '3.0')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSelectProjection()
    {
        String tableName = "select_projection";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id BIGINT, data VARCHAR, value DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', 3.0)", tableName), 3);
            assertQuery(String.format("SELECT id FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1'), (BIGINT '2'), (BIGINT '3')");
            assertQuery(String.format("SELECT data, value FROM %s ORDER BY data", tableName),
                    "VALUES ('a', DOUBLE '1.0'), ('b', DOUBLE '2.0'), ('c', DOUBLE '3.0')");
            assertQuery(String.format("SELECT value, id, data FROM %s WHERE id = 2", tableName),
                    "VALUES (DOUBLE '2.0', BIGINT '2', 'b')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSelectWithFilter()
    {
        String tableName = "select_filter";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, category VARCHAR, amount DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES " +
                    "(1, 'A', 100.0), (2, 'B', 200.0), (3, 'A', 150.0), (4, 'B', 250.0), (5, 'A', 175.0)", tableName), 5);
            assertQuery(String.format("SELECT id, amount FROM %s WHERE category = 'A' ORDER BY id", tableName),
                    "VALUES (1, DOUBLE '100.0'), (3, DOUBLE '150.0'), (5, DOUBLE '175.0')");
            assertQuery(String.format("SELECT id FROM %s WHERE amount > 150.0 ORDER BY id", tableName),
                    "VALUES (2), (4), (5)");
            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE category = 'B'", tableName),
                    "VALUES (BIGINT '2')");
            assertQuery(String.format("SELECT id FROM %s WHERE id = 2", tableName),
                    "VALUES (2)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSelectWithAggregation()
    {
        String tableName = "select_aggregation";

        try {
            assertUpdate(String.format("CREATE TABLE %s (category VARCHAR, amount DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES " +
                    "('A', 100.0), ('B', 200.0), ('A', 150.0), ('B', 250.0), ('A', 175.0)", tableName), 5);
            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName),
                    "VALUES (BIGINT '5')");
            assertQuery(String.format("SELECT category, COUNT(*) as cnt FROM %s GROUP BY category ORDER BY category", tableName),
                    "VALUES ('A', BIGINT '3'), ('B', BIGINT '2')");
            assertQuery(String.format("SELECT SUM(amount) FROM %s", tableName),
                    "VALUES (DOUBLE '875.0')");
            assertQuery(String.format("SELECT category, SUM(amount) FROM %s GROUP BY category ORDER BY category", tableName));
            assertQuery(String.format("SELECT MAX(amount), MIN(amount) FROM %s", tableName),
                    "VALUES (DOUBLE '250.0', DOUBLE '100.0')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSelectWithOrderBy()
    {
        String tableName = "select_orderby";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, name VARCHAR, value DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES " +
                    "(3, 'charlie', 30.0), (1, 'alice', 10.0), (2, 'bob', 20.0)", tableName), 3);
            assertQuery(String.format("SELECT id, name FROM %s ORDER BY id", tableName),
                    "VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");
            assertQuery(String.format("SELECT id, name FROM %s ORDER BY name", tableName),
                    "VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");
            assertQuery(String.format("SELECT id, value FROM %s ORDER BY value DESC", tableName),
                    "VALUES (3, DOUBLE '30.0'), (2, DOUBLE '20.0'), (1, DOUBLE '10.0')");
            assertQuery(String.format("SELECT name FROM %s ORDER BY value ASC", tableName),
                    "VALUES ('alice'), ('bob'), ('charlie')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSelectWithNullValues()
    {
        String tableName = "select_nulls";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, data VARCHAR, value DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'a', 1.0), (2, NULL, 2.0), (3, 'c', NULL)", tableName), 3);
            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE data IS NULL", tableName),
                    "VALUES (BIGINT '1')");
            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE value IS NULL", tableName),
                    "VALUES (BIGINT '1')");
            assertQuery(String.format("SELECT id FROM %s WHERE data IS NOT NULL ORDER BY id", tableName),
                    "VALUES (1), (3)");
            assertQuery(String.format("SELECT id FROM %s WHERE value IS NOT NULL ORDER BY id", tableName),
                    "VALUES (1), (2)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSelectComplexTypes()
    {
        String tableName = "select_complex";

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "id INTEGER, " +
                    "int_array ARRAY(INTEGER), " +
                    "string_map MAP(VARCHAR, INTEGER), " +
                    "person ROW(name VARCHAR, age INTEGER)" +
                    ") WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "1, ARRAY[1, 2, 3], MAP(ARRAY['a', 'b'], ARRAY[10, 20]), ROW('Alice', 30))", tableName), 1);
            assertUpdate(String.format("INSERT INTO %s VALUES (" +
                    "2, ARRAY[4, 5], MAP(ARRAY['x'], ARRAY[100]), ROW('Bob', 25))", tableName), 1);
            assertQuery(String.format("SELECT id, int_array FROM %s WHERE id = 1", tableName),
                    "VALUES (1, ARRAY[1, 2, 3])");
            assertQuery(String.format("SELECT COUNT(*) FROM %s", tableName),
                    "VALUES (BIGINT '2')");
            assertQuery(String.format("SELECT id FROM %s ORDER BY id", tableName),
                    "VALUES (1), (2)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSelectWithLimit()
    {
        String tableName = "select_limit";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, data VARCHAR) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", tableName), 5);
            assertQuery(String.format("SELECT COUNT(*) FROM (SELECT * FROM %s LIMIT 3)", tableName),
                    "VALUES (BIGINT '3')");
            assertQuery(String.format("SELECT id FROM %s ORDER BY id LIMIT 2", tableName),
                    "VALUES (1), (2)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSelectWithMultipleFilters()
    {
        String tableName = "select_multiple_filters";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, category VARCHAR, amount DOUBLE, active BOOLEAN) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES " +
                    "(1, 'A', 100.0, true), (2, 'B', 200.0, false), (3, 'A', 150.0, true), " +
                    "(4, 'B', 250.0, true), (5, 'A', 175.0, false)", tableName), 5);
            assertQuery(String.format("SELECT id FROM %s WHERE category = 'A' AND active = true ORDER BY id", tableName),
                    "VALUES (1), (3)");
            assertQuery(String.format("SELECT id FROM %s WHERE amount > 150.0 AND active = true ORDER BY id", tableName),
                    "VALUES (4)");
            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE category = 'B' OR active = false", tableName),
                    "VALUES (BIGINT '3')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSelectWithJoin()
    {
        String table1 = "select_join_1";
        String table2 = "select_join_2";

        try {
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, name VARCHAR) WITH (format = 'PARQUET')", table1));
            assertUpdate(String.format("CREATE TABLE %s (id INTEGER, value DOUBLE) WITH (format = 'PARQUET')", table2));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')", table1), 3);
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 10.0), (2, 20.0), (4, 40.0)", table2), 3);
            assertQuery(String.format("SELECT t1.id, t1.name, t2.value FROM %s t1 INNER JOIN %s t2 ON t1.id = t2.id ORDER BY t1.id", table1, table2),
                    "VALUES (1, 'alice', DOUBLE '10.0'), (2, 'bob', DOUBLE '20.0')");
            assertQuery(String.format("SELECT COUNT(*) FROM %s t1 INNER JOIN %s t2 ON t1.id = t2.id", table1, table2),
                    "VALUES (BIGINT '2')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", table2));
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", table1));
        }
    }

    @Test
    public void testSelectDistinct()
    {
        String tableName = "select_distinct";

        try {
            assertUpdate(String.format("CREATE TABLE %s (category VARCHAR, value INTEGER) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES ('A', 1), ('B', 2), ('A', 3), ('B', 4), ('A', 5)", tableName), 5);
            assertQuery(String.format("SELECT DISTINCT category FROM %s ORDER BY category", tableName),
                    "VALUES ('A'), ('B')");
            assertQuery(String.format("SELECT COUNT(DISTINCT category) FROM %s", tableName),
                    "VALUES (BIGINT '2')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSelectWithHaving()
    {
        String tableName = "select_having";

        try {
            assertUpdate(String.format("CREATE TABLE %s (category VARCHAR, amount DOUBLE) WITH (format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES " +
                    "('A', 100.0), ('B', 200.0), ('A', 150.0), ('B', 250.0), ('C', 50.0)", tableName), 5);
            assertQuery(String.format("SELECT category FROM %s GROUP BY category HAVING SUM(amount) > 200.0 ORDER BY category", tableName),
                    "VALUES ('A'), ('B')");
            assertQuery(String.format("SELECT category, COUNT(*) FROM %s GROUP BY category HAVING COUNT(*) > 1 ORDER BY category", tableName),
                    "VALUES ('A', BIGINT '2'), ('B', BIGINT '2')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }
}
