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
import static java.lang.Thread.sleep;

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
    public void testAddColumnWithDefaultReturnsDefaultForHistoricalRows()
            throws InterruptedException
    {
        String tableName = "orders_v3_default_basic";
        try {
            assertUpdate(String.format("CREATE TABLE %s (id BIGINT, amount DOUBLE) WITH (\"format-version\" = '3', format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 100.0), (2, 200.0)", tableName), 2);
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertQuery(String.format("SELECT country FROM %s ORDER BY id", tableName), "VALUES ('IN'), ('IN')");
            sleep(100000);
            assertQuery(String.format("SELECT id, amount, country FROM %s ORDER BY id", tableName), "VALUES (BIGINT '1', DOUBLE '100.0', 'IN'), (BIGINT '2', DOUBLE '200.0', 'IN')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testNewRowsWithoutExplicitValueReturnNull()
    {
        String tableName = "orders_v3_new_rows_null";
        try {
            assertUpdate(String.format("CREATE TABLE %s (id BIGINT, amount DOUBLE) WITH (\"format-version\" = '3', format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 100.0), (2, 200.0)", tableName), 2);
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertUpdate(String.format("INSERT INTO %s (id, amount) VALUES (3, 300.0)", tableName), 1);
            assertQuery(String.format("SELECT id, amount, country FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', DOUBLE '100.0', 'IN'), " + "(BIGINT '2', DOUBLE '200.0', 'IN'), " + "(BIGINT '3', DOUBLE '300.0', NULL)");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testMultipleDefaultColumnsAddedSequentially()
    {
        String tableName = "orders_v3_multi_default";
        try {
            assertUpdate(String.format("CREATE TABLE %s (id BIGINT, amount DOUBLE) WITH (\"format-version\" = '3', format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 100.0), (2, 200.0)", tableName), 2);
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertUpdate(String.format("INSERT INTO %s (id, amount) VALUES (3, 300.0)", tableName), 1);
            assertQuery(String.format("SELECT id, amount, country FROM %s ORDER BY id", tableName), "VALUES " +
                            "(BIGINT '1', DOUBLE '100.0', 'IN'), " + "(BIGINT '2', DOUBLE '200.0', 'IN'), " + "(BIGINT '3', DOUBLE '300.0', NULL)");
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country_new VARCHAR DEFAULT 'US'", tableName));
            // All rows (including row 3 which was written before country_new) should return 'US' for country_new
            assertQuery(String.format("SELECT id, amount, country, country_new FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', DOUBLE '100.0', 'IN', 'US'), " + "(BIGINT '2', DOUBLE '200.0', 'IN', 'US'), " + "(BIGINT '3', DOUBLE '300.0', NULL, 'US')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testExplicitValueOverridesDefault()
    {
        String tableName = "orders_v3_explicit_override";
        try {
            assertUpdate(String.format("CREATE TABLE %s (id BIGINT, amount DOUBLE) WITH (\"format-version\" = '3', format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 100.0)", tableName), 1);
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            // Insert with explicit country value
            assertUpdate(String.format("INSERT INTO %s VALUES (2, 200.0, 'US')", tableName), 1);
            assertQuery(String.format("SELECT id, amount, country FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', DOUBLE '100.0', 'IN'), " + "(BIGINT '2', DOUBLE '200.0', 'US')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testFullSpecificationScenario()
    {
        String tableName = "orders_v3_native";
        try {
            assertUpdate(String.format("CREATE TABLE %s (id BIGINT, amount DOUBLE) WITH (\"format-version\" = '3', format = 'PARQUET')", tableName));
            assertUpdate(String.format("INSERT INTO %s VALUES (1, 100.0), (2, 200.0)", tableName), 2);
            assertQuery(String.format("SELECT * FROM %s ORDER BY id", tableName), "VALUES (BIGINT '1', DOUBLE '100.0'), (BIGINT '2', DOUBLE '200.0')");
            // Add column with default
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country VARCHAR DEFAULT 'IN'", tableName));
            assertUpdate(String.format("INSERT INTO %s (id, amount) VALUES (3, 300.0)", tableName), 1);
            // Rows 1,2 (historical) return 'IN'; row 3 (new, no explicit value) returns NULL
            assertQuery(String.format("SELECT id, amount, country FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', DOUBLE '100.0', 'IN'), " + "(BIGINT '2', DOUBLE '200.0', 'IN'), " + "(BIGINT '3', DOUBLE '300.0', NULL)");
            // Add second column with default
            assertUpdate(String.format("ALTER TABLE %s ADD COLUMN country_new VARCHAR DEFAULT 'US'", tableName));
            // All rows return 'US' for country_new
            assertQuery(String.format("SELECT id, amount, country, country_new FROM %s ORDER BY id", tableName), "VALUES " +
                    "(BIGINT '1', DOUBLE '100.0', 'IN', 'US'), " + "(BIGINT '2', DOUBLE '200.0', 'IN', 'US'), " + "(BIGINT '3', DOUBLE '300.0', NULL, 'US')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", tableName));
        }
    }
}
