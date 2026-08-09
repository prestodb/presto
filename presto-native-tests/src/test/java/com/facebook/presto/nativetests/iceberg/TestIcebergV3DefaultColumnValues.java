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
package com.facebook.presto.nativetests.iceberg;

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;

/**
 * Tests for Iceberg Format Version 3 Default Column Values (initial-default read support).
 */
public class TestIcebergV3DefaultColumnValues
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testDefaultForHistoricalRows()
    {
        String tableName = "orders_v3_default_basic";
        try {
            createTableWithRows(tableName, "(id BIGINT, amount DOUBLE)", "VALUES (1, 100.0), (2, 200.0)", 2);
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertQuery(String.format("SELECT country FROM %s ORDER BY id", tableName), "VALUES ('IN'), ('IN')");
            assertQuery(String.format("SELECT id, amount, country FROM %s ORDER BY id", tableName), "VALUES (BIGINT '1', DOUBLE '100.0', 'IN'), (BIGINT '2', DOUBLE '200.0', 'IN')");
        }
        finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testNewRowsWithoutExplicitValueReturnNull()
    {
        String tableName = "orders_v3_new_rows_null";
        try {
            createTableWithRows(tableName, "(id BIGINT, amount DOUBLE)", "VALUES (1, 100.0), (2, 200.0)", 2);
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertUpdate(String.format("INSERT INTO %s (id, amount) VALUES (3, 300.0)", tableName), 1);
            assertQuery(String.format("SELECT id, amount, country FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', DOUBLE '100.0', 'IN'), " + "(BIGINT '2', DOUBLE '200.0', 'IN'), " + "(BIGINT '3', DOUBLE '300.0', NULL)");
        }
        finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testMultipleDefaultColumnsAddedSequentially()
    {
        String tableName = "orders_v3_multi_default";
        try {
            createTableWithRows(tableName, "(id BIGINT, amount DOUBLE)", "VALUES (1, 100.0), (2, 200.0)", 2);
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertUpdate(String.format("INSERT INTO %s (id, amount) VALUES (3, 300.0)", tableName), 1);
            assertQuery(String.format("SELECT id, amount, country FROM %s ORDER BY id", tableName), "VALUES " +
                            "(BIGINT '1', DOUBLE '100.0', 'IN'), " + "(BIGINT '2', DOUBLE '200.0', 'IN'), " + "(BIGINT '3', DOUBLE '300.0', NULL)");
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country_new VARCHAR DEFAULT 'US'", tableName));
            assertQuery(String.format("SELECT id, amount, country, country_new FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', DOUBLE '100.0', 'IN', 'US'), " + "(BIGINT '2', DOUBLE '200.0', 'IN', 'US'), " + "(BIGINT '3', DOUBLE '300.0', NULL, 'US')");
        }
        finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testExplicitValueOverridesDefault()
    {
        String tableName = "orders_v3_explicit_override";
        try {
            createTableWithRows(tableName, "(id BIGINT, amount DOUBLE)", "VALUES (1, 100.0)", 1);
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (2, 200.0, 'US')", tableName), 1);
            assertQuery(String.format("SELECT id, amount, country FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', DOUBLE '100.0', 'IN'), " + "(BIGINT '2', DOUBLE '200.0', 'US')");
        }
        finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testAddColumnWithDefaultMultipleDataTypes()
    {
        String tableName = "orders_v3_multi_types";
        try {
            createTableWithRows(tableName, "(id BIGINT)", "VALUES (1), (2), (3)", 3);
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN priority INTEGER DEFAULT 5", tableName));
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN quantity BIGINT DEFAULT 100", tableName));
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN score DOUBLE DEFAULT 0.0E0", tableName));
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN is_active BOOLEAN DEFAULT true", tableName));
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertQuery(String.format("SELECT id, priority, quantity, score, is_active, country FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', INTEGER '5', BIGINT '100', DOUBLE '0.0', BOOLEAN 'true', 'IN'), " +
                    "(BIGINT '2', INTEGER '5', BIGINT '100', DOUBLE '0.0', BOOLEAN 'true', 'IN'), " +
                    "(BIGINT '3', INTEGER '5', BIGINT '100', DOUBLE '0.0', BOOLEAN 'true', 'IN')");
            assertUpdate(String.format("INSERT INTO %s (id) VALUES (4)", tableName), 1);
            assertQuery(String.format("SELECT id, priority, quantity, score, is_active, country FROM %s WHERE id = 4", tableName),
                    "VALUES (BIGINT '4', NULL, NULL, NULL, NULL, NULL)");
            assertUpdate(String.format("INSERT INTO %s VALUES (5, 10, 200, 99.5, false, 'US')", tableName), 1);
            assertQuery(String.format("SELECT id, priority, quantity, score, is_active, country FROM %s WHERE id = 5", tableName),
                    "VALUES (BIGINT '5', INTEGER '10', BIGINT '200', DOUBLE '99.5', BOOLEAN 'false', 'US')");
        }
        finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testSelectWithInitialDefaultAndFilters()
    {
        String tableName = "select_initial_default_filters";
        try {
            createTableWithRows(tableName, "(id INTEGER, name VARCHAR)", "VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", 3);
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            // This tests filter pushdown - without the fix, this would return 0 rows
            assertQuery(String.format("SELECT id, name, country FROM %s WHERE country = 'IN' ORDER BY id", tableName),
                    "VALUES (1, 'Alice', 'IN'), (2, 'Bob', 'IN'), (3, 'Charlie', 'IN')");
            // Test filter on column with initial-default (non-matching value)
            assertQueryReturnsEmptyResult(String.format("SELECT id, name, country FROM %s WHERE country = 'US'", tableName));
            // Test IS NOT NULL filter
            assertQuery(String.format("SELECT id, name, country FROM %s WHERE country IS NOT NULL ORDER BY id", tableName),
                    "VALUES (1, 'Alice', 'IN'), (2, 'Bob', 'IN'), (3, 'Charlie', 'IN')");
            // Test IS NULL filter
            assertQueryReturnsEmptyResult(String.format("SELECT id, name, country FROM %s WHERE country IS NULL", tableName));
            // Insert new data with explicit country value
            assertUpdate(String.format("INSERT INTO %s VALUES (4, 'David', 'US')", tableName), 1);
            // Test filter after new data inserted
            assertQuery(String.format("SELECT id, name, country FROM %s WHERE country = 'IN' ORDER BY id", tableName),
                    "VALUES (1, 'Alice', 'IN'), (2, 'Bob', 'IN'), (3, 'Charlie', 'IN')");
            assertQuery(String.format("SELECT id, name, country FROM %s WHERE country = 'US'", tableName), "VALUES (4, 'David', 'US')");
            // Test combined filter on file column and default column
            assertQuery(String.format("SELECT id, name, country FROM %s WHERE id > 1 AND country = 'IN' ORDER BY id", tableName), "VALUES (2, 'Bob', 'IN'), (3, 'Charlie', 'IN')");
            // Test combined filter with OR
            assertQuery(String.format("SELECT id FROM %s WHERE country = 'US' OR id = 1 ORDER BY id", tableName), "VALUES (1), (4)");
        }
        finally {
            dropTableIfExists(tableName);
        }
    }

    @Test
    public void testSelectWithNumericInitialDefaultAndFilters()
    {
        String tableName = "select_numeric_initial_default_filters";
        try {
            createTableWithRows(tableName, "(id INTEGER, name VARCHAR)", "VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", 3);
            // Add INTEGER column with initial-default value
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN age INTEGER DEFAULT 25", tableName));
            // Test 1: Filter pushdown on INTEGER initial-default - matching value
            assertQuery(
                    String.format("SELECT id, name, age FROM %s WHERE age = 25 ORDER BY id", tableName),
                    "VALUES (1, 'Alice', 25), (2, 'Bob', 25), (3, 'Charlie', 25)");
            // Test 2: Filter on INTEGER initial-default (non-matching value)
            assertQueryReturnsEmptyResult(String.format("SELECT id, name, age FROM %s WHERE age = 30", tableName));
            // Test 3: Range filter on INTEGER initial-default (greater than)
            assertQuery(String.format("SELECT id, name, age FROM %s WHERE age > 20 ORDER BY id", tableName), "VALUES (1, 'Alice', 25), (2, 'Bob', 25), (3, 'Charlie', 25)");
            // Test 4: Range filter on INTEGER initial-default (less than)
            assertQueryReturnsEmptyResult(String.format("SELECT id, name, age FROM %s WHERE age < 20", tableName));
            // Test 5: Range filter on INTEGER initial-default (between)
            assertQuery(String.format("SELECT id, name, age FROM %s WHERE age >= 25 AND age <= 30 ORDER BY id", tableName), "VALUES (1, 'Alice', 25), (2, 'Bob', 25), (3, 'Charlie', 25)");
            // Test 6: IS NOT NULL filter on INTEGER initial-default
            assertQuery(String.format("SELECT id, name, age FROM %s WHERE age IS NOT NULL ORDER BY id", tableName), "VALUES (1, 'Alice', 25), (2, 'Bob', 25), (3, 'Charlie', 25)");
            // Test 7: IS NULL filter on INTEGER initial-default
            assertQueryReturnsEmptyResult(String.format("SELECT id, name, age FROM %s WHERE age IS NULL", tableName));
            // Test 8: Insert new data with explicit age value
            assertUpdate(String.format("INSERT INTO %s VALUES (4, 'David', 30)", tableName), 1);
            // Test 9: Filter after new data inserted - matching default
            assertQuery(String.format("SELECT id, name, age FROM %s WHERE age = 25 ORDER BY id", tableName), "VALUES (1, 'Alice', 25), (2, 'Bob', 25), (3, 'Charlie', 25)");
            // Test 10: Filter after new data inserted - matching new value
            assertQuery(String.format("SELECT id, name, age FROM %s WHERE age = 30", tableName), "VALUES (4, 'David', 30)");
            // Test 11: Combined filter on file column and INTEGER default column
            assertQuery(String.format("SELECT id, name, age FROM %s WHERE id > 1 AND age = 25 ORDER BY id", tableName), "VALUES (2, 'Bob', 25), (3, 'Charlie', 25)");
            // Test 12: Combined filter with OR on INTEGER default
            assertQuery(String.format("SELECT id FROM %s WHERE age = 30 OR id = 1 ORDER BY id", tableName), "VALUES (1), (4)");
            // Add REAL column with initial-default value
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN score REAL DEFAULT 3.14", tableName));
            // Test 13: Filter on REAL initial-default (matching)
            assertQuery(String.format("SELECT id, name, score FROM %s WHERE score = 3.14 ORDER BY id", tableName), "VALUES (1, 'Alice', CAST(3.14 AS REAL)), (2, 'Bob', CAST(3.14 AS REAL)), (3, 'Charlie', CAST(3.14 AS REAL)), (4, 'David', CAST(3.14 AS REAL))");
            // Test 14: Filter on REAL initial-default (non-matching)
            assertQueryReturnsEmptyResult(String.format("SELECT id, name, score FROM %s WHERE score = 2.5", tableName));
            // Test 15: Range filter on REAL initial-default
            assertQuery(String.format("SELECT id, name, score FROM %s WHERE score > 3.0 AND score < 4.0 ORDER BY id", tableName), "VALUES (1, 'Alice', CAST(3.14 AS REAL)), (2, 'Bob', CAST(3.14 AS REAL)), (3, 'Charlie', CAST(3.14 AS REAL)), (4, 'David', CAST(3.14 AS REAL))");
            // Test 16: Insert new data with explicit REAL value
            assertUpdate(String.format("INSERT INTO %s VALUES (5, 'Eve', 35, 4.5)", tableName), 1);
            // Test 17: Combined filter on multiple numeric initial-defaults
            assertQuery(String.format("SELECT id, name, age, score FROM %s WHERE age = 25 AND score = 3.14 ORDER BY id", tableName), "VALUES (1, 'Alice', 25, CAST(3.14 AS REAL)), (2, 'Bob', 25, CAST(3.14 AS REAL)), (3, 'Charlie', 25, CAST(3.14 AS REAL))");
            // Test 18: Complex OR condition with numeric initial-defaults
            assertQuery(String.format("SELECT id FROM %s WHERE (age = 30 AND score = 3.14) OR (age = 35 AND score = 4.5) ORDER BY id", tableName), "VALUES (4), (5)");
            // Add BIGINT column with initial-default
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN salary BIGINT DEFAULT 50000", tableName));
            // Test 19: Filter on BIGINT initial-default
            assertQuery(String.format("SELECT id, name, salary FROM %s WHERE salary = 50000 ORDER BY id", tableName), "VALUES (1, 'Alice', 50000), (2, 'Bob', 50000), (3, 'Charlie', 50000), (4, 'David', 50000), (5, 'Eve', 50000)");
            // Test 20: Range filter on BIGINT initial-default
            assertQuery(String.format("SELECT id FROM %s WHERE salary >= 40000 AND salary <= 60000 ORDER BY id", tableName), "VALUES (1), (2), (3), (4), (5)");
            // Add SMALLINT column with initial-default
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN department_id SMALLINT DEFAULT CAST(10 AS SMALLINT)", tableName));
            // Test 21: Filter on SMALLINT initial-default
            assertQuery(String.format("SELECT id, name, department_id FROM %s WHERE department_id = 10 ORDER BY id", tableName), "VALUES (1, 'Alice', 10), (2, 'Bob', 10), (3, 'Charlie', 10), (4, 'David', 10), (5, 'Eve', 10)");
            // Add TINYINT column with initial-default
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN status TINYINT DEFAULT CAST(1 AS TINYINT)", tableName));
            // Test 22: Filter on TINYINT initial-default
            assertQuery(String.format("SELECT id, name, status FROM %s WHERE status = 1 ORDER BY id", tableName), "VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 1), (4, 'David', 1), (5, 'Eve', 1)");
            // Test 23: Combined filter on all numeric types
            assertQuery(String.format("SELECT id FROM %s WHERE age = 25 AND score = 3.14 AND salary = 50000 AND department_id = 10 AND status = 1 ORDER BY id", tableName), "VALUES (1), (2), (3)");
            // Add DOUBLE column with initial-default
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN rating DOUBLE DEFAULT 4.567", tableName));
            // Test 24: Filter on DOUBLE initial-default
            assertQuery(String.format("SELECT id, name, rating FROM %s WHERE rating = 4.567 ORDER BY id", tableName), "VALUES (1, 'Alice', 4.567), (2, 'Bob', 4.567), (3, 'Charlie', 4.567), (4, 'David', 4.567), (5, 'Eve', 4.567)");
            // Test 25: Range filter combining REAL and DOUBLE
            assertQuery(String.format("SELECT id FROM %s WHERE score > 3.0 AND rating > 4.0 ORDER BY id", tableName), "VALUES (1), (2), (3), (4), (5)");
        }
        finally {
            dropTableIfExists(tableName);
        }
    }

    private void createTableWithRows(String tableName, String tableDefinition, String values, long rowCount)
    {
        assertUpdate(String.format("CREATE TABLE %s %s WITH (\"format-version\" = '3', format = 'PARQUET')", tableName, tableDefinition));
        assertUpdate(String.format("INSERT INTO %s %s", tableName, values), rowCount);
    }

    private void dropTableIfExists(String tableName)
    {
        assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
    }
}
