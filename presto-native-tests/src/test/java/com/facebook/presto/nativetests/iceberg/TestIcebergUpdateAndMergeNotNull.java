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

import com.facebook.presto.iceberg.CatalogType;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.sql.TestTable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;

public class TestIcebergUpdateAndMergeNotNull
        extends AbstractTestQueryFramework
{
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CatalogType.HADOOP)
                .setAddStorageFormatToPath(true)
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
                .setCatalogType(CatalogType.HADOOP)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testUpdateNullIntoNotNullColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "update_not_null", "CREATE TABLE {TABLE_NAME} (a BIGINT, b BIGINT NOT NULL, c BIGINT NOT NULL)")) {
            String tableName = table.getName();
            assertUpdate(format("INSERT INTO %s VALUES (1, 2, 3), (4, 5, 6)", tableName), 2);

            assertQueryFails(format("UPDATE %s SET b = NULL WHERE a = 1", tableName), notNullError(tableName, "b"));
            assertQueryFails(format("UPDATE %s SET c = IF(a = 4, NULL, c)", tableName), notNullError(tableName, "c"));
            assertUpdate(format("UPDATE %s SET b = NULL WHERE a = 999", tableName), 0);
            assertQuery(format("SELECT a, b, c FROM %s", tableName), "VALUES (1, 2, 3), (4, 5, 6)");
        }
    }

    @Test
    public void testMergeIntoNotNullColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "merge_not_null", "CREATE TABLE {TABLE_NAME} (a BIGINT, b BIGINT NOT NULL)")) {
            String tableName = table.getName();
            assertUpdate(format("INSERT INTO %s VALUES (1, 2)", tableName), 1);

            assertQueryFails(
                    format("MERGE INTO %s t USING (VALUES 1) s(a) ON t.a = s.a WHEN MATCHED THEN UPDATE SET b = NULL", tableName),
                    notNullError(tableName, "b"));
            assertQueryFails(
                    format("MERGE INTO %s t USING (VALUES 2) s(a) ON t.a = s.a WHEN NOT MATCHED THEN INSERT VALUES (s.a, NULL)", tableName),
                    notNullError(tableName, "b"));
            assertQueryFails(
                    format("MERGE INTO %s t USING (VALUES 2) s(a) ON t.a = s.a WHEN NOT MATCHED THEN INSERT (a) VALUES (s.a)", tableName),
                    notNullError(tableName, "b"));
            assertUpdate(format("MERGE INTO %s t USING (VALUES 999) s(a) ON t.a = s.a WHEN MATCHED THEN UPDATE SET b = NULL", tableName), 0);
            assertUpdate(format("MERGE INTO %s t USING (VALUES 3) s(a) ON t.a = s.a WHEN NOT MATCHED THEN INSERT VALUES (s.a, 4)", tableName), 1);
            assertQuery(format("SELECT a, b FROM %s", tableName), "VALUES (1, 2), (3, 4)");
        }
    }

    @Test
    public void testMergeMultipleSourceRowsMatch()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "merge_multiple_match", "CREATE TABLE {TABLE_NAME} (a BIGINT, b BIGINT)")) {
            String tableName = table.getName();
            assertUpdate(format("INSERT INTO %s VALUES (1, 2)", tableName), 1);

            assertQueryFails(
                    format("MERGE INTO %s t USING (VALUES 1, 1) s(a) ON t.a = s.a WHEN MATCHED THEN UPDATE SET b = 3", tableName),
                    ".*One or more rows in the target table matched multiple source rows\\..*");
            assertQuery(format("SELECT a, b FROM %s", tableName), "VALUES (1, 2)");
        }
    }

    private static String notNullError(String tableName, String column)
    {
        return ".*NULL value not allowed for NOT NULL column\\. Table: " + tableName + " Column: " + column + "\\b.*";
    }
}
