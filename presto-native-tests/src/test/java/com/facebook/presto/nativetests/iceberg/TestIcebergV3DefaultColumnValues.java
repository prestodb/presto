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
