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
import com.facebook.presto.iceberg.CatalogType;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.sql.TestTable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.iceberg.NotNullConstraintErrors.nullValuesError;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;

public class TestIcebergAlterColumnNotNull
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
    public void testAlterColumnSetAndDropNotNull()
    {
        try (TestTable table = newTable("alter_column_set_and_drop_not_null", "(c1 BIGINT, c2 BIGINT, c3 BIGINT)")) {
            String tableName = table.getName();
            assertUpdate(format("INSERT INTO %s VALUES (1, 2, 3)", tableName), 1);

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c2 SET NOT NULL", tableName));
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c3 SET NOT NULL", tableName));
            assertColumnNullability(tableName, "VALUES ('c1', 'YES'), ('c2', 'NO'), ('c3', 'NO')");
            assertQueryFails(format("INSERT INTO %s VALUES (2, NULL, 3)", tableName), writerNullError("c2"));
            // MERGE checks NOT NULL with presto.default.fail, which the sidecar does not register.
            if (!sidecarEnabled) {
                assertQueryFails(
                        format("MERGE INTO %s t USING (VALUES 1) s(c1) ON t.c1 = s.c1 WHEN MATCHED THEN UPDATE SET c2 = NULL", tableName),
                        ".*NULL value not allowed for NOT NULL column\\. Table: .* Column: c2 .*");
                assertQueryFails(
                        format("MERGE INTO %s t USING (VALUES 2) s(c1) ON t.c1 = s.c1 WHEN NOT MATCHED THEN INSERT VALUES (s.c1, 2, NULL)", tableName),
                        ".*NULL value not allowed for NOT NULL column\\. Table: .* Column: c3 .*");
            }

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c2 DROP NOT NULL", tableName));
            assertColumnNullability(tableName, "VALUES ('c1', 'YES'), ('c2', 'YES'), ('c3', 'NO')");
            assertUpdate(format("INSERT INTO %s VALUES (2, NULL, 3)", tableName), 1);
            assertQueryFails(format("INSERT INTO %s VALUES (3, 2, NULL)", tableName), writerNullError("c3"));
            assertQuery(format("SELECT c1, c2, c3 FROM %s", tableName), "VALUES (1, 2, 3), (2, NULL, 3)");

            // DROP NOT NULL on a nullable column fails, as in Hive, and changes nothing.
            assertQueryFails(format("ALTER TABLE %s ALTER COLUMN c2 DROP NOT NULL", tableName), "Not Null constraint not found on column c2");
            assertColumnNullability(tableName, "VALUES ('c1', 'YES'), ('c2', 'YES'), ('c3', 'NO')");
        }
    }

    @Test
    public void testSetNotNullWithExistingNullData()
    {
        // The native writer records NULL counts, so SET NOT NULL rejects the column.
        try (TestTable table = newTable("alter_not_null_existing_nulls", "(c1 BIGINT, c2 BIGINT)")) {
            String tableName = table.getName();
            assertUpdate(format("INSERT INTO %s VALUES (1, NULL)", tableName), 1);

            assertQueryFails(format("ALTER TABLE %s ALTER COLUMN c2 SET NOT NULL", tableName), nullValuesError("c2", "main"));
            assertColumnNullability(tableName, "VALUES ('c1', 'YES'), ('c2', 'YES')");
            assertUpdate(format("INSERT INTO %s VALUES (2, NULL)", tableName), 1);
        }
    }

    @Test
    public void testUpdateNullIntoNotNullColumn()
    {
        try (TestTable table = newTable("alter_not_null_update", "(a BIGINT, b BIGINT NOT NULL, c BIGINT)")) {
            String tableName = table.getName();
            assertUpdate(format("INSERT INTO %s VALUES (1, 2, 3), (4, 5, 6)", tableName), 2);
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c SET NOT NULL", tableName));

            assertQueryFails(format("UPDATE %s SET b = NULL WHERE a = 1", tableName), updateNullError(tableName, "b"));
            assertQueryFails(format("UPDATE %s SET c = IF(a = 4, NULL, c)", tableName), updateNullError(tableName, "c"));
            assertQuery(format("SELECT a, b, c FROM %s", tableName), "VALUES (1, 2, 3), (4, 5, 6)");
        }
    }

    @Test
    public void testReadAcrossRequiredAndOptionalDataFiles()
    {
        // Files written while c2 is NOT NULL store it as required in Parquet; the rest store it as optional.
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", PUSHDOWN_FILTER_ENABLED, "true")
                .build();
        try (TestTable table = newTable("alter_not_null_mixed_files", "(c1 BIGINT, c2 BIGINT)")) {
            String tableName = table.getName();
            assertUpdate(format("INSERT INTO %s VALUES (1, 10)", tableName), 1);
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c2 SET NOT NULL", tableName));
            assertUpdate(format("INSERT INTO %s VALUES (2, 20)", tableName), 1);
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c2 DROP NOT NULL", tableName));
            assertUpdate(format("INSERT INTO %s VALUES (3, NULL)", tableName), 1);

            assertQuery(format("SELECT c1, c2 FROM %s", tableName), "VALUES (1, 10), (2, 20), (3, NULL)");
            assertQuery(format("SELECT c1 FROM %s WHERE c2 IS NULL", tableName), "VALUES 3");
            assertQuery(format("SELECT c1 FROM %s WHERE c2 IS NOT NULL", tableName), "VALUES 1, 2");
            assertQuery(session, format("SELECT c1, c2 FROM %s", tableName), "VALUES (1, 10), (2, 20), (3, NULL)");
            assertQuery(session, format("SELECT c1 FROM %s WHERE c2 IS NULL", tableName), "VALUES 3");
        }
    }

    @Test
    public void testAlterColumnNotNullAfterColumnReorder()
    {
        // The native writer checks a positional INSERT against the current column order.
        try (TestTable table = newTable("alter_column_not_null_reorder", "(a BIGINT, b BIGINT NOT NULL, c VARCHAR)")) {
            String tableName = table.getName();
            assertUpdate(format("INSERT INTO %s VALUES (1, 2, 'x')", tableName), 1);

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN b FIRST", tableName));
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c SET NOT NULL", tableName));
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c AFTER b", tableName));
            assertColumnNullability(tableName, "VALUES ('b', 'NO'), ('c', 'NO'), ('a', 'YES')");

            assertQueryFails(
                    format("INSERT INTO %s VALUES (NULL, 'y', 3)", tableName),
                    writerNullError("b"));
            assertQueryFails(
                    format("INSERT INTO %s VALUES (3, NULL, 4)", tableName),
                    writerNullError("c"));
            assertUpdate(format("INSERT INTO %s VALUES (3, 'y', NULL)", tableName), 1);

            // DROP NOT NULL finds the moved columns by name, including b, which was NOT NULL from CREATE TABLE.
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN b DROP NOT NULL", tableName));
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c DROP NOT NULL", tableName));
            assertColumnNullability(tableName, "VALUES ('b', 'YES'), ('c', 'YES'), ('a', 'YES')");
            assertUpdate(format("INSERT INTO %s VALUES (NULL, NULL, 5)", tableName), 1);

            assertQuery(format("SELECT a, b, c FROM %s", tableName), "VALUES (1, 2, 'x'), (NULL, 3, 'y'), (5, NULL, NULL)");
        }
    }

    @Test
    public void testAlterColumnNotNullOnPartitionColumn()
    {
        try (TestTable table = newTable("alter_column_not_null_partition", "(id BIGINT, region VARCHAR) WITH (partitioning = ARRAY['region'])")) {
            String tableName = table.getName();

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN region SET NOT NULL", tableName));
            assertColumnNullability(tableName, "VALUES ('id', 'YES'), ('region', 'NO')");

            assertUpdate(format("INSERT INTO %s VALUES (1, 'us-east')", tableName), 1);
            assertQueryFails(
                    format("INSERT INTO %s VALUES (2, NULL)", tableName),
                    writerNullError("region"));
        }
    }

    @Test
    public void testInsertOmittingNotNullColumnWithDefault()
    {
        try (TestTable table = newTable("insert_omitting_not_null_default", "(c1 BIGINT) WITH (\"format-version\" = '3')")) {
            String tableName = table.getName();
            assertUpdate(format("INSERT INTO %s VALUES 1", tableName), 1);
            assertUpdate(format("ALTER TABLE %s ADD COLUMN c2 BIGINT DEFAULT 5", tableName));
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c2 SET NOT NULL", tableName));

            assertUpdate(format("INSERT INTO %s (c1) VALUES (2)", tableName), 1);
            assertQueryFails(
                    format("INSERT INTO %s VALUES (3, NULL)", tableName),
                    writerNullError("c2"));
            assertQuery(format("SELECT c1, c2 FROM %s", tableName), "VALUES (1, 5), (2, 5)");
        }
    }

    private TestTable newTable(String namePrefix, String tableDefinition)
    {
        return new TestTable(getQueryRunner()::execute, namePrefix, "CREATE TABLE {TABLE_NAME} " + tableDefinition);
    }

    private void assertColumnNullability(String tableName, String expectedValues)
    {
        assertQueryOrdered(
                format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position",
                        getSession().getSchema().get(), tableName),
                expectedValues);
    }

    // The native writer's message starts with the Velox check that failed.
    private static String writerNullError(String column)
    {
        return ".*NULL value not allowed for NOT NULL column: " + column;
    }

    private static String updateNullError(String tableName, String column)
    {
        return ".*NULL value not allowed for NOT NULL column\\. Table: " + tableName + " Column: " + column + "\\b.*";
    }
}
