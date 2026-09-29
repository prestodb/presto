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

import com.facebook.presto.Session;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;

/**
 * Tests for Iceberg Format Version 3 Default Column Values (initial-default read support and write-default write support).
 *
 * Every test method is parameterized with {@code pushdown_filter_enabled=true/false} so that
 * all scenarios are verified under both filter-pushdown configurations.
 */
public class TestIcebergV3DefaultColumnValues
        extends AbstractTestQueryFramework
{
    protected boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "false"));
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }
        return queryRunner;
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

    @DataProvider(name = "pushdownFilterEnabled")
    public static Object[][] pushdownFilterEnabledProvider()
    {
        return new Object[][] {
                {true},
                {false}
        };
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testDefaultForHistoricalRows(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "orders_v3_default_basic";
        try {
            createTableWithRows(session, tableName, "(id BIGINT, amount DOUBLE)", "VALUES (1, 100.0), (2, 200.0)", 2);
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertQuery(session, String.format("SELECT country FROM %s ORDER BY id", tableName), "VALUES ('IN'), ('IN')");
            assertQuery(session, format("SELECT country FROM %s WHERE country = 'IN'", tableName), "VALUES ('IN'), ('IN')");
            assertQuery(session, format("SELECT count(*) FROM %s WHERE country = 'IN'", tableName), "SELECT 2");
            assertQuery(session, String.format("SELECT id, amount, country FROM %s ORDER BY id", tableName), "VALUES (BIGINT '1', DOUBLE '100.0', 'IN'), (BIGINT '2', DOUBLE '200.0', 'IN')");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testNewRowsWithoutExplicitValueUseWriteDefault(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "orders_v3_new_rows_write_default";
        try {
            createTableWithRows(session, tableName, "(id BIGINT, amount DOUBLE)", "VALUES (1, 100.0), (2, 200.0)", 2);
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertUpdate(session, String.format("INSERT INTO %s (id, amount) VALUES (3, 300.0)", tableName), 1);
            assertQuery(session, String.format("SELECT id, amount, country FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', DOUBLE '100.0', 'IN'), " + "(BIGINT '2', DOUBLE '200.0', 'IN'), " + "(BIGINT '3', DOUBLE '300.0', 'IN')");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testMultipleDefaultColumnsAddedSequentially(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "orders_v3_multi_default";
        try {
            createTableWithRows(session, tableName, "(id BIGINT, amount DOUBLE)", "VALUES (1, 100.0), (2, 200.0)", 2);
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertUpdate(session, String.format("INSERT INTO %s (id, amount) VALUES (3, 300.0)", tableName), 1);
            assertQuery(session, String.format("SELECT id, amount, country FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', DOUBLE '100.0', 'IN'), " + "(BIGINT '2', DOUBLE '200.0', 'IN'), " + "(BIGINT '3', DOUBLE '300.0', 'IN')");
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN country_new VARCHAR DEFAULT 'US'", tableName));
            assertQuery(session, String.format("SELECT id, amount, country, country_new FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', DOUBLE '100.0', 'IN', 'US'), " + "(BIGINT '2', DOUBLE '200.0', 'IN', 'US'), " + "(BIGINT '3', DOUBLE '300.0', 'IN', 'US')");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testExplicitValueOverridesDefault(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "orders_v3_explicit_override";
        try {
            createTableWithRows(session, tableName, "(id BIGINT, amount DOUBLE)", "VALUES (1, 100.0)", 1);
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertUpdate(session, String.format("INSERT INTO %s VALUES (2, 200.0, 'US')", tableName), 1);
            assertQuery(session, String.format("SELECT id, amount, country FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', DOUBLE '100.0', 'IN'), " + "(BIGINT '2', DOUBLE '200.0', 'US')");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testAddColumnWithDefaultMultipleDataTypes(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "orders_v3_multi_types";
        try {
            createTableWithRows(session, tableName, "(id BIGINT)", "VALUES (1), (2), (3)", 3);
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN priority INTEGER DEFAULT 5", tableName));
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN quantity BIGINT DEFAULT 100", tableName));
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN score DOUBLE DEFAULT 0.0E0", tableName));
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN is_active BOOLEAN DEFAULT true", tableName));
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertQuery(session, String.format("SELECT id, priority, quantity, score, is_active, country FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', INTEGER '5', BIGINT '100', DOUBLE '0.0', BOOLEAN 'true', 'IN'), " +
                    "(BIGINT '2', INTEGER '5', BIGINT '100', DOUBLE '0.0', BOOLEAN 'true', 'IN'), " +
                    "(BIGINT '3', INTEGER '5', BIGINT '100', DOUBLE '0.0', BOOLEAN 'true', 'IN')");
            assertUpdate(session, String.format("INSERT INTO %s (id) VALUES (4)", tableName), 1);
            assertQuery(session, String.format("SELECT id, priority, quantity, score, is_active, country FROM %s WHERE id = 4", tableName),
                    "VALUES (BIGINT '4', INTEGER '5', BIGINT '100', DOUBLE '0.0', BOOLEAN 'true', 'IN')");
            assertUpdate(session, String.format("INSERT INTO %s VALUES (5, 10, 200, 99.5, false, 'US')", tableName), 1);
            assertQuery(session, String.format("SELECT id, priority, quantity, score, is_active, country FROM %s WHERE id = 5", tableName),
                    "VALUES (BIGINT '5', INTEGER '10', BIGINT '200', DOUBLE '99.5', BOOLEAN 'false', 'US')");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testSelectWithInitialDefaultAndFilters(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "select_initial_default_filters";
        try {
            createTableWithRows(session, tableName, "(id INTEGER, name VARCHAR)", "VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", 3);
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            // This tests filter pushdown - without the fix, this would return 0 rows when pushdown is enabled
            assertQuery(session, String.format("SELECT id, name, country FROM %s WHERE country = 'IN' ORDER BY id", tableName),
                    "VALUES (1, 'Alice', 'IN'), (2, 'Bob', 'IN'), (3, 'Charlie', 'IN')");
            // Test filter on column with initial-default (non-matching value)
            assertQueryReturnsEmptyResult(session, String.format("SELECT id, name, country FROM %s WHERE country = 'US'", tableName));
            // Test IS NOT NULL filter
            assertQuery(session, String.format("SELECT id, name, country FROM %s WHERE country IS NOT NULL ORDER BY id", tableName),
                    "VALUES (1, 'Alice', 'IN'), (2, 'Bob', 'IN'), (3, 'Charlie', 'IN')");
            // Test IS NULL filter
            assertQueryReturnsEmptyResult(session, String.format("SELECT id, name, country FROM %s WHERE country IS NULL", tableName));
            // Insert new data with explicit country value
            assertUpdate(session, String.format("INSERT INTO %s VALUES (4, 'David', 'US')", tableName), 1);
            // Test filter after new data inserted
            assertQuery(session, String.format("SELECT id, name, country FROM %s WHERE country = 'IN' ORDER BY id", tableName),
                    "VALUES (1, 'Alice', 'IN'), (2, 'Bob', 'IN'), (3, 'Charlie', 'IN')");
            assertQuery(session, String.format("SELECT id, name, country FROM %s WHERE country = 'US'", tableName), "VALUES (4, 'David', 'US')");
            // Test combined filter on file column and default column
            assertQuery(session, String.format("SELECT id, name, country FROM %s WHERE id > 1 AND country = 'IN' ORDER BY id", tableName), "VALUES (2, 'Bob', 'IN'), (3, 'Charlie', 'IN')");
            // Test combined filter with OR
            assertQuery(session, String.format("SELECT id FROM %s WHERE country = 'US' OR id = 1 ORDER BY id", tableName), "VALUES (1), (4)");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testSelectWithNumericInitialDefaultAndFilters(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "select_numeric_initial_default_filters";
        try {
            createTableWithRows(session, tableName, "(id INTEGER, name VARCHAR)", "VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')", 3);
            // Add INTEGER column with initial-default value
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN age INTEGER DEFAULT 25", tableName));
            // Test 1: Filter pushdown on INTEGER initial-default - matching value
            assertQuery(session,
                    String.format("SELECT id, name, age FROM %s WHERE age = 25 ORDER BY id", tableName),
                    "VALUES (1, 'Alice', 25), (2, 'Bob', 25), (3, 'Charlie', 25)");
            // Test 2: Filter on INTEGER initial-default (non-matching value)
            assertQueryReturnsEmptyResult(session, String.format("SELECT id, name, age FROM %s WHERE age = 30", tableName));
            // Test 3: Range filter on INTEGER initial-default (greater than)
            assertQuery(session, String.format("SELECT id, name, age FROM %s WHERE age > 20 ORDER BY id", tableName), "VALUES (1, 'Alice', 25), (2, 'Bob', 25), (3, 'Charlie', 25)");
            // Test 4: Range filter on INTEGER initial-default (less than)
            assertQueryReturnsEmptyResult(session, String.format("SELECT id, name, age FROM %s WHERE age < 20", tableName));
            // Test 5: Range filter on INTEGER initial-default (between)
            assertQuery(session, String.format("SELECT id, name, age FROM %s WHERE age >= 25 AND age <= 30 ORDER BY id", tableName), "VALUES (1, 'Alice', 25), (2, 'Bob', 25), (3, 'Charlie', 25)");
            // Test 6: IS NOT NULL filter on INTEGER initial-default
            assertQuery(session, String.format("SELECT id, name, age FROM %s WHERE age IS NOT NULL ORDER BY id", tableName), "VALUES (1, 'Alice', 25), (2, 'Bob', 25), (3, 'Charlie', 25)");
            // Test 7: IS NULL filter on INTEGER initial-default
            assertQueryReturnsEmptyResult(session, String.format("SELECT id, name, age FROM %s WHERE age IS NULL", tableName));
            // Test 8: Insert new data with explicit age value
            assertUpdate(session, String.format("INSERT INTO %s VALUES (4, 'David', 30)", tableName), 1);
            // Test 9: Filter after new data inserted - matching default
            assertQuery(session, String.format("SELECT id, name, age FROM %s WHERE age = 25 ORDER BY id", tableName), "VALUES (1, 'Alice', 25), (2, 'Bob', 25), (3, 'Charlie', 25)");
            // Test 10: Filter after new data inserted - matching new value
            assertQuery(session, String.format("SELECT id, name, age FROM %s WHERE age = 30", tableName), "VALUES (4, 'David', 30)");
            // Test 11: Combined filter on file column and INTEGER default column
            assertQuery(session, String.format("SELECT id, name, age FROM %s WHERE id > 1 AND age = 25 ORDER BY id", tableName), "VALUES (2, 'Bob', 25), (3, 'Charlie', 25)");
            // Test 12: Combined filter with OR on INTEGER default
            assertQuery(session, String.format("SELECT id FROM %s WHERE age = 30 OR id = 1 ORDER BY id", tableName), "VALUES (1), (4)");
            // Add REAL column with initial-default value
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN score REAL DEFAULT 3.14", tableName));
            // Test 13: Filter on REAL initial-default (matching)
            assertQuery(session, String.format("SELECT id, name, score FROM %s WHERE score = 3.14 ORDER BY id", tableName), "VALUES (1, 'Alice', CAST(3.14 AS REAL)), (2, 'Bob', CAST(3.14 AS REAL)), (3, 'Charlie', CAST(3.14 AS REAL)), (4, 'David', CAST(3.14 AS REAL))");
            // Test 14: Filter on REAL initial-default (non-matching)
            assertQueryReturnsEmptyResult(session, String.format("SELECT id, name, score FROM %s WHERE score = 2.5", tableName));
            // Test 15: Range filter on REAL initial-default
            assertQuery(session, String.format("SELECT id, name, score FROM %s WHERE score > 3.0 AND score < 4.0 ORDER BY id", tableName), "VALUES (1, 'Alice', CAST(3.14 AS REAL)), (2, 'Bob', CAST(3.14 AS REAL)), (3, 'Charlie', CAST(3.14 AS REAL)), (4, 'David', CAST(3.14 AS REAL))");
            // Test 16: Insert new data with explicit REAL value
            assertUpdate(session, String.format("INSERT INTO %s VALUES (5, 'Eve', 35, 4.5)", tableName), 1);
            // Test 17: Combined filter on multiple numeric initial-defaults
            assertQuery(session, String.format("SELECT id, name, age, score FROM %s WHERE age = 25 AND score = 3.14 ORDER BY id", tableName), "VALUES (1, 'Alice', 25, CAST(3.14 AS REAL)), (2, 'Bob', 25, CAST(3.14 AS REAL)), (3, 'Charlie', 25, CAST(3.14 AS REAL))");
            // Test 18: Complex OR condition with numeric initial-defaults
            assertQuery(session, String.format("SELECT id FROM %s WHERE (age = 30 AND score = 3.14) OR (age = 35 AND score = 4.5) ORDER BY id", tableName), "VALUES (4), (5)");
            // Add BIGINT column with initial-default
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN salary BIGINT DEFAULT 50000", tableName));
            // Test 19: Filter on BIGINT initial-default
            assertQuery(session, String.format("SELECT id, name, salary FROM %s WHERE salary = 50000 ORDER BY id", tableName), "VALUES (1, 'Alice', 50000), (2, 'Bob', 50000), (3, 'Charlie', 50000), (4, 'David', 50000), (5, 'Eve', 50000)");
            // Test 20: Range filter on BIGINT initial-default
            assertQuery(session, String.format("SELECT id FROM %s WHERE salary >= 40000 AND salary <= 60000 ORDER BY id", tableName), "VALUES (1), (2), (3), (4), (5)");
            // Add SMALLINT column with initial-default
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN department_id SMALLINT DEFAULT CAST(10 AS SMALLINT)", tableName));
            // Test 21: Filter on SMALLINT initial-default
            assertQuery(session, String.format("SELECT id, name, department_id FROM %s WHERE department_id = 10 ORDER BY id", tableName), "VALUES (1, 'Alice', 10), (2, 'Bob', 10), (3, 'Charlie', 10), (4, 'David', 10), (5, 'Eve', 10)");
            // Add TINYINT column with initial-default
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN status TINYINT DEFAULT CAST(1 AS TINYINT)", tableName));
            // Test 22: Filter on TINYINT initial-default
            assertQuery(session, String.format("SELECT id, name, status FROM %s WHERE status = 1 ORDER BY id", tableName), "VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 1), (4, 'David', 1), (5, 'Eve', 1)");
            // Test 23: Combined filter on all numeric types
            assertQuery(session, String.format("SELECT id FROM %s WHERE age = 25 AND score = 3.14 AND salary = 50000 AND department_id = 10 AND status = 1 ORDER BY id", tableName), "VALUES (1), (2), (3)");
            // Add DOUBLE column with initial-default
            assertUpdate(session, String.format("ALTER TABLE %s ADD COLUMN rating DOUBLE DEFAULT 4.567", tableName));
            // Test 24: Filter on DOUBLE initial-default
            assertQuery(session, String.format("SELECT id, name, rating FROM %s WHERE rating = 4.567 ORDER BY id", tableName), "VALUES (1, 'Alice', 4.567), (2, 'Bob', 4.567), (3, 'Charlie', 4.567), (4, 'David', 4.567), (5, 'Eve', 4.567)");
            // Test 25: Range filter combining REAL and DOUBLE
            assertQuery(session, String.format("SELECT id FROM %s WHERE score > 3.0 AND rating > 4.0 ORDER BY id", tableName), "VALUES (1), (2), (3), (4), (5)");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testDefaultDateColumnForHistoricalRows(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        // Iceberg stores DATE defaults as integer days since the Unix epoch.
        // The native worker must decode that integer and return the correct DATE
        // value for rows written before the column was added.
        String tableName = "orders_v3_default_date";
        try {
            // Write two rows before the date column exists.
            createTableWithRows(session, tableName, "(id BIGINT, name VARCHAR)", "VALUES (1, 'Alice'), (2, 'Bob')", 2);

            // Add a DATE column with an initial-default (2023-01-01 = day 19358 since epoch).
            assertUpdate(session, String.format(
                    "ALTER TABLE %s ADD COLUMN created_date DATE DEFAULT DATE '2023-01-01'", tableName));

            // Historical rows must surface the initial-default.
            assertQuery(
                    session,
                    String.format("SELECT id, created_date FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', DATE '2023-01-01'), (BIGINT '2', DATE '2023-01-01')");

            // New row written with an explicit date value must use that value.
            assertUpdate(session, String.format(
                    "INSERT INTO %s VALUES (3, 'Charlie', DATE '2024-06-15')", tableName), 1);
            assertQuery(
                    session,
                    String.format("SELECT id, created_date FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', DATE '2023-01-01'), "
                            + "(BIGINT '2', DATE '2023-01-01'), "
                            + "(BIGINT '3', DATE '2024-06-15')");

            // New row written without a date value must use the write-default.
            assertUpdate(session, String.format(
                    "INSERT INTO %s (id, name) VALUES (4, 'David')", tableName), 1);
            assertQuery(
                    session,
                    String.format("SELECT id, created_date FROM %s WHERE id = 4", tableName),
                    "VALUES (BIGINT '4', DATE '2023-01-01')");

            // SELECT * must return all columns for all rows, mixing the initial-default,
            // an explicit file value, and the write-default in the same result set.
            assertQuery(
                    session,
                    String.format("SELECT * FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', 'Alice', DATE '2023-01-01'), "
                            + "(BIGINT '2', 'Bob',   DATE '2023-01-01'), "
                            + "(BIGINT '3', 'Charlie', DATE '2024-06-15'), "
                            + "(BIGINT '4', 'David',   DATE '2023-01-01')");

            // Filter pushdown on the default date value must return historical rows
            // and the row written with the write-default.
            assertQuery(
                    session,
                    String.format("SELECT id FROM %s WHERE created_date = DATE '2023-01-01' ORDER BY id", tableName),
                    "VALUES (1), (2), (4)");

            // Filter on the explicit date must return only row 3.
            assertQuery(
                    session,
                    String.format("SELECT id FROM %s WHERE created_date = DATE '2024-06-15'", tableName),
                    "VALUES (3)");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testDefaultTimestampColumnForHistoricalRows(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        // Iceberg stores TIMESTAMP defaults as integer microseconds since the Unix epoch.
        // The native worker must decode that integer and return the correct TIMESTAMP
        // value for rows written before the column was added.
        String tableName = "orders_v3_default_timestamp";
        try {
            // Write two rows before the timestamp column exists.
            createTableWithRows(session, tableName, "(id BIGINT, name VARCHAR)", "VALUES (1, 'Alice'), (2, 'Bob')", 2);

            // Add a TIMESTAMP column with an initial-default
            // (2023-01-01 11:00:00 UTC = 1672570800000000 microseconds since epoch).
            assertUpdate(session, String.format(
                    "ALTER TABLE %s ADD COLUMN created_at TIMESTAMP DEFAULT TIMESTAMP '2023-01-01 11:00:00.000000'",
                    tableName));

            // Historical rows must surface the initial-default.
            assertQuery(
                    session,
                    String.format("SELECT id, created_at FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', TIMESTAMP '2023-01-01 11:00:00.000000'), "
                            + "(BIGINT '2', TIMESTAMP '2023-01-01 11:00:00.000000')");

            // New row written with an explicit timestamp must use that value.
            assertUpdate(session, String.format(
                    "INSERT INTO %s VALUES (3, 'Charlie', TIMESTAMP '2024-12-25 15:30:00.000000')", tableName), 1);
            assertQuery(
                    session,
                    String.format("SELECT id, created_at FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', TIMESTAMP '2023-01-01 11:00:00.000000'), "
                            + "(BIGINT '2', TIMESTAMP '2023-01-01 11:00:00.000000'), "
                            + "(BIGINT '3', TIMESTAMP '2024-12-25 15:30:00.000000')");

            // New row written without a timestamp value must use the write-default.
            assertUpdate(session, String.format(
                    "INSERT INTO %s (id, name) VALUES (4, 'David')", tableName), 1);
            assertQuery(
                    session,
                    String.format("SELECT id, created_at FROM %s WHERE id = 4", tableName),
                    "VALUES (BIGINT '4', TIMESTAMP '2023-01-01 11:00:00.000000')");

            // SELECT * must return all columns for all rows, mixing the initial-default,
            // an explicit file value, and the write-default in the same result set.
            assertQuery(
                    session,
                    String.format("SELECT * FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', 'Alice',   TIMESTAMP '2023-01-01 11:00:00.000000'), "
                            + "(BIGINT '2', 'Bob',     TIMESTAMP '2023-01-01 11:00:00.000000'), "
                            + "(BIGINT '3', 'Charlie', TIMESTAMP '2024-12-25 15:30:00.000000'), "
                            + "(BIGINT '4', 'David',   TIMESTAMP '2023-01-01 11:00:00.000000')");

            // Filter pushdown on the default timestamp must return historical rows
            // and the row written with the write-default.
            assertQuery(
                    session,
                    String.format(
                            "SELECT id FROM %s WHERE created_at = TIMESTAMP '2023-01-01 11:00:00.000000' ORDER BY id",
                            tableName),
                    "VALUES (1), (2), (4)");

            // Filter on the explicit timestamp must return only row 3.
            assertQuery(
                    session,
                    String.format(
                            "SELECT id FROM %s WHERE created_at = TIMESTAMP '2024-12-25 15:30:00.000000'",
                            tableName),
                    "VALUES (3)");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testInsertWithWriteDefault(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "orders_v3_write_default";
        try {
            createTableWithRows(session, tableName, "(id BIGINT, amount DOUBLE)", "VALUES (1, 100.0)", 1);
            assertUpdate(session, format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertUpdate(session, format("INSERT INTO %s (id, amount) VALUES (2, 200.0)", tableName), 1);
            assertUpdate(session, format("ALTER TABLE %s ALTER COLUMN country SET DEFAULT 'US'", tableName));
            assertUpdate(session, format("INSERT INTO %s (id, amount) VALUES (3, 300.0)", tableName), 1);
            // Row 1 has initial-default 'IN', row 2 has write-default 'IN', row 3 has write-default 'US'.
            assertQuery(session, format("SELECT id, amount, country FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', DOUBLE '100.0', 'IN'), " +
                    "(BIGINT '2', DOUBLE '200.0', 'IN'), " +
                    "(BIGINT '3', DOUBLE '300.0', 'US')");

            // Clearing the write-default must write NULL for omitted columns rather than falling back
            // to the initial-default 'IN', which still applies to row 1.
            assertUpdate(session, format("ALTER TABLE %s ALTER COLUMN country SET DEFAULT NULL", tableName));
            assertUpdate(session, format("INSERT INTO %s (id, amount) VALUES (4, 400.0)", tableName), 1);
            assertQuery(session, format("SELECT id, amount, country FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', DOUBLE '100.0', 'IN'), " +
                    "(BIGINT '2', DOUBLE '200.0', 'IN'), " +
                    "(BIGINT '3', DOUBLE '300.0', 'US'), " +
                    "(BIGINT '4', DOUBLE '400.0', NULL)");
            assertQuery(session, format("SELECT id FROM %s WHERE country IS NULL", tableName), "VALUES (BIGINT '4')");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testInsertWithExplicitNullOverridesWriteDefault(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "orders_v3_explicit_null";
        try {
            createTableWithRows(session, tableName, "(id BIGINT)", "VALUES (1)", 1);
            assertUpdate(session, format("ALTER TABLE %s ADD COLUMN status VARCHAR DEFAULT 'ACTIVE'", tableName));
            // An explicit NULL must be written as NULL, not replaced by the write-default.
            assertUpdate(session, format("INSERT INTO %s (id, status) VALUES (2, NULL)", tableName), 1);
            assertUpdate(session, format("INSERT INTO %s (id) VALUES (3)", tableName), 1);
            // Row 1 has initial-default 'ACTIVE', row 2 has NULL, row 3 has write-default 'ACTIVE'.
            assertQuery(session, format("SELECT id, status FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', 'ACTIVE'), (BIGINT '2', NULL), (BIGINT '3', 'ACTIVE')");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testInsertWithMultipleWriteDefaultColumns(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "orders_v3_multi_write_defaults";
        try {
            createTableWithRows(session, tableName, "(id BIGINT)", "VALUES (1)", 1);
            assertUpdate(session, format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'US'", tableName));
            assertUpdate(session, format("ALTER TABLE %s ADD COLUMN priority INTEGER DEFAULT 10", tableName));
            assertUpdate(session, format("ALTER TABLE %s ADD COLUMN is_enabled BOOLEAN DEFAULT true", tableName));
            assertUpdate(session, format("INSERT INTO %s (id) VALUES (2)", tableName), 1);
            // Only the omitted columns take their write-defaults.
            assertUpdate(session, format("INSERT INTO %s (id, country) VALUES (3, 'UK')", tableName), 1);
            assertQuery(session, format("SELECT id, country, priority, is_enabled FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', 'US', INTEGER '10', BOOLEAN 'true'), " +
                    "(BIGINT '2', 'US', INTEGER '10', BOOLEAN 'true'), " +
                    "(BIGINT '3', 'UK', INTEGER '10', BOOLEAN 'true')");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testInsertWithWriteDefaultDifferentDataTypes(boolean pushdownFilterEnabled)
    {
        // Covers types whose serialized default has a non-trivial string form, which the native
        // worker parses back into a constant. Other primitive types are covered by
        // testAddColumnWithDefaultMultipleDataTypes.
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "orders_v3_write_default_types";
        try {
            createTableWithRows(session, tableName, "(id BIGINT)", "VALUES (1)", 1);
            assertUpdate(session, format("ALTER TABLE %s ADD COLUMN price DECIMAL(10, 2) DEFAULT DECIMAL '12.34'", tableName));
            assertUpdate(session, format("ALTER TABLE %s ADD COLUMN ratio REAL DEFAULT REAL '1.5'", tableName));
            assertUpdate(session, format("INSERT INTO %s (id) VALUES (2)", tableName), 1);
            assertUpdate(session, format("INSERT INTO %s VALUES (3, DECIMAL '99.99', REAL '-2.25')", tableName), 1);
            assertQuery(session, format("SELECT id, price, ratio FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', DECIMAL '12.34', REAL '1.5'), " +
                    "(BIGINT '2', DECIMAL '12.34', REAL '1.5'), " +
                    "(BIGINT '3', DECIMAL '99.99', REAL '-2.25')");
            assertQuery(session, format("SELECT id FROM %s WHERE price = DECIMAL '12.34' ORDER BY id", tableName),
                    "VALUES (BIGINT '1'), (BIGINT '2')");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testInsertWithWriteDefaultOnPartitionedTable(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "orders_v3_partitioned_write_default";
        try {
            assertUpdate(session, format("CREATE TABLE %s (id BIGINT, ds DATE) " +
                    "WITH (\"format-version\" = '3', format = 'PARQUET', partitioning = ARRAY['ds'])", tableName));
            assertUpdate(session, format("INSERT INTO %s VALUES (1, DATE '2023-01-01')", tableName), 1);
            assertUpdate(session, format("ALTER TABLE %s ADD COLUMN region VARCHAR DEFAULT 'US'", tableName));
            assertUpdate(session, format("INSERT INTO %s (id, ds) VALUES (2, DATE '2023-01-02')", tableName), 1);
            assertUpdate(session, format("ALTER TABLE %s ALTER COLUMN region SET DEFAULT 'EU'", tableName));
            assertUpdate(session, format("INSERT INTO %s (id, ds) VALUES (3, DATE '2023-01-03')", tableName), 1);
            // Row 1 has initial-default 'US', row 2 has write-default 'US', row 3 has write-default 'EU'.
            assertQuery(session, format("SELECT id, ds, region FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', DATE '2023-01-01', 'US'), " +
                    "(BIGINT '2', DATE '2023-01-02', 'US'), " +
                    "(BIGINT '3', DATE '2023-01-03', 'EU')");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @DataProvider(name = "withPartitioning")
    public static Object[][] withPartitioningProvider()
    {
        Object[][] formatsAndPartitioning = {
                {"PARQUET", ""},
                {"PARQUET", " WITH (partitioning = 'identity')"},
                {"ORC", ""},
                {"ORC", " WITH (partitioning = 'identity')"}
        };
        Object[][] result = new Object[formatsAndPartitioning.length * 2][];
        int index = 0;
        for (Object[] formatAndPartitioning : formatsAndPartitioning) {
            for (boolean pushdownFilterEnabled : new boolean[] {true, false}) {
                result[index++] = new Object[] {pushdownFilterEnabled, formatAndPartitioning[0], formatAndPartitioning[1]};
            }
        }
        return result;
    }

    @Test(dataProvider = "withPartitioning")
    public void testInsertWithPartitionEvolution(boolean pushdownFilterEnabled, String fileFormat, String withPartitioning)
    {
        // When the default column is also an identity partition key, the write-default must
        // feed the partition value as well as the data file.
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = format("orders_v3_write_default_%s_%s_%s",
                fileFormat.toLowerCase(),
                withPartitioning.isEmpty() ? "unpartitioned" : "partitioned",
                pushdownFilterEnabled ? "pushdown" : "no_pushdown");
        try {
            assertUpdate(session, format("CREATE TABLE %s (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3', format = '%s')",
                    tableName, fileFormat));
            assertUpdate(session, format("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName), 2);
            assertUpdate(session, format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'US'%s", tableName, withPartitioning));
            assertUpdate(session, format("ALTER TABLE %s ALTER COLUMN country SET DEFAULT 'UK'", tableName));
            // Rows 1 and 2 have initial-default 'US', row 3 has write-default 'UK'.
            assertUpdate(session, format("INSERT INTO %s (id, name) VALUES (3, 'Carol')", tableName), 1);
            assertQuery(session, format("SELECT * FROM %s", tableName),
                    "VALUES (1, 'Alice', 'US'), (2, 'Bob', 'US'), (3, 'Carol', 'UK')");
            assertQuery(session, format("SELECT * FROM %s WHERE country = 'UK'", tableName), "VALUES (3, 'Carol', 'UK')");

            assertUpdate(session, format("INSERT INTO %s (id, name, country) VALUES (4, 'David', NULL), (5, 'Frank', 'FR')", tableName), 2);
            assertQuery(session, format("SELECT * FROM %s", tableName),
                    "VALUES (1, 'Alice', 'US'), (2, 'Bob', 'US'), (3, 'Carol', 'UK'), (4, 'David', NULL), (5, 'Frank', 'FR')");
            assertQuery(session, format("SELECT * FROM %s WHERE country = 'US'", tableName), "VALUES (1, 'Alice', 'US'), (2, 'Bob', 'US')");
            assertQuery(session, format("SELECT * FROM %s WHERE country <> 'US'", tableName), "VALUES (3, 'Carol', 'UK'), (5, 'Frank', 'FR')");
            assertQuery(session, format("SELECT * FROM %s WHERE country IS NULL", tableName), "VALUES (4, 'David', NULL)");
            assertQuery(session, format("SELECT * FROM %s WHERE country IN ('US', 'FR', 'CN')", tableName),
                    "VALUES (1, 'Alice', 'US'), (2, 'Bob', 'US'), (5, 'Frank', 'FR')");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testInsertSelectWithWriteDefault(boolean pushdownFilterEnabled)
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "orders_v3_insert_select_write_default";
        try {
            createTableWithRows(session, tableName, "(id BIGINT, amount DOUBLE)", "VALUES (1, 100.0), (2, 200.0)", 2);
            assertUpdate(session, format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertUpdate(session, format("ALTER TABLE %s ALTER COLUMN country SET DEFAULT 'US'", tableName));
            assertUpdate(session, format("INSERT INTO %1$s (id, amount) SELECT id + 10, amount * 2 FROM %1$s", tableName), 2);
            // Rows 1 and 2 have initial-default 'IN', rows 11 and 12 have write-default 'US'.
            assertQuery(session, format("SELECT id, amount, country FROM %s ORDER BY id", tableName),
                    "VALUES (BIGINT '1', DOUBLE '100.0', 'IN'), " +
                    "(BIGINT '2', DOUBLE '200.0', 'IN'), " +
                    "(BIGINT '11', DOUBLE '200.0', 'US'), " +
                    "(BIGINT '12', DOUBLE '400.0', 'US')");
        }
        finally {
            dropTableIfExists(session, tableName);
        }
    }

    private Session sessionWithPushdown(boolean pushdownFilterEnabled)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, Boolean.toString(pushdownFilterEnabled))
                .build();
    }

    private void dropTableIfExists(Session session, String tableName)
    {
        assertUpdate(session, format("DROP TABLE IF EXISTS %s", tableName));
    }

    private void createTableWithRows(Session session, String tableName, String tableDefinition, String values, long rowCount)
    {
        assertUpdate(session, format("CREATE TABLE %s %s WITH (\"format-version\" = '3', format = 'PARQUET')", tableName, tableDefinition));
        assertUpdate(session, format("INSERT INTO %s %s", tableName, values), rowCount);
    }
}
