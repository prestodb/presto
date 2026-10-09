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
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.facebook.presto.tests.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;

public class TestIcebergAlterColumnNotNull
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CatalogType.HADOOP)
                .setAddStorageFormatToPath(true)
                .build();
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
        String tableName = "alter_column_set_and_drop_not_null_" + randomTableSuffix();
        String schema = getSession().getSchema().get();
        try {
            assertUpdate(format("CREATE TABLE %s (c1 BIGINT, c2 BIGINT, c3 VARCHAR, c4 BIGINT)", tableName));
            assertUpdate(format("INSERT INTO %s VALUES (1, 2, 'a', 4)", tableName), 1);

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c2 SET NOT NULL", tableName));
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c4 SET NOT NULL", tableName));
            assertQuery(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('c1', 'YES'), ('c2', 'NO'), ('c3', 'YES'), ('c4', 'NO')");

            assertQueryFails(
                    format("INSERT INTO %s VALUES (2, NULL, 'b', 5)", tableName),
                    "(?s).*NULL value not allowed for NOT NULL column: c2.*");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (2, 3, 'b', NULL)", tableName),
                    "(?s).*NULL value not allowed for NOT NULL column: c4.*");

            assertUpdate(format("INSERT INTO %s VALUES (2, 3, 'b', 5)", tableName), 1);
            assertUpdate(format("INSERT INTO %s VALUES (NULL, 6, NULL, 7)", tableName), 1);

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c2 DROP NOT NULL", tableName));
            assertQuery(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('c1', 'YES'), ('c2', 'YES'), ('c3', 'YES'), ('c4', 'NO')");

            assertUpdate(format("INSERT INTO %s VALUES (3, NULL, 'c', 8)", tableName), 1);
            assertQueryFails(
                    format("INSERT INTO %s VALUES (3, 9, 'c', NULL)", tableName),
                    "(?s).*NULL value not allowed for NOT NULL column: c4.*");

            assertQuery(format("SELECT count(*) FROM %s", tableName), "VALUES 4");

            // DROP NOT NULL on a column that is already nullable fails, as in the Hive connector, and leaves the schema unchanged.
            assertQueryFails(
                    format("ALTER TABLE %s ALTER COLUMN c2 DROP NOT NULL", tableName),
                    "(?s).*Not Null constraint not found on column c2.*");
            assertQuery(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('c1', 'YES'), ('c2', 'YES'), ('c3', 'YES'), ('c4', 'NO')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSetNotNullWithExistingNullData()
    {
        // Iceberg SET NOT NULL is metadata-only: existing rows with NULLs remain readable.
        String tableName = "alter_not_null_existing_nulls_" + randomTableSuffix();
        String schema = getSession().getSchema().get();
        try {
            assertUpdate(format("CREATE TABLE %s (c1 BIGINT, c2 BIGINT)", tableName));
            assertUpdate(format("INSERT INTO %s VALUES (1, NULL)", tableName), 1);

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c2 SET NOT NULL", tableName));

            assertQuery(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('c1', 'YES'), ('c2', 'NO')");

            assertQuery(format("SELECT c1, c2 FROM %s", tableName), "VALUES (1, NULL)");
            assertQuery(format("SELECT c1 FROM %s WHERE c2 IS NULL", tableName), "VALUES 1");

            assertQueryFails(
                    format("INSERT INTO %s VALUES (2, NULL)", tableName),
                    "(?s).*NULL value not allowed for NOT NULL column: c2.*");

            assertUpdate(format("INSERT INTO %s VALUES (2, 3)", tableName), 1);
            assertQuery(format("SELECT count(*) FROM %s", tableName), "VALUES 2");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSetNotNullWithExistingNullDataAndFilterPushdown()
    {
        // With filter pushdown, identity partition predicates are enforced only by partition pruning.
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", PUSHDOWN_FILTER_ENABLED, "true")
                .build();
        String tableName = "alter_not_null_existing_nulls_pushdown_" + randomTableSuffix();
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT, c2 BIGINT, region VARCHAR) WITH (partitioning = ARRAY['region'])", tableName));
            assertUpdate(format("INSERT INTO %s VALUES (1, NULL, NULL), (2, 5, 'us'), (3, NULL, 'us')", tableName), 3);
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c2 SET NOT NULL", tableName));
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN region SET NOT NULL", tableName));

            assertQuery(session, format("SELECT id FROM %s WHERE c2 IS NULL", tableName), "VALUES 1, 3");
            assertQuery(session, format("SELECT id FROM %s WHERE c2 IS NULL OR c2 = 5", tableName), "VALUES 1, 2, 3");
            assertQuery(session, format("SELECT id FROM %s WHERE region IS NULL", tableName), "VALUES 1");
            assertQuery(session, format("SELECT id FROM %s WHERE region IS NULL OR region = 'us'", tableName), "VALUES 1, 2, 3");
            assertQuery(session, format("SELECT id FROM %s WHERE region = 'us'", tableName), "VALUES 2, 3");
            assertQuery(session, format("SELECT id FROM %s WHERE c2 IS NOT NULL", tableName), "VALUES 2");
            assertQuery(session, format("SELECT id FROM %s WHERE region IS NOT NULL", tableName), "VALUES 2, 3");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testAlterColumnNotNullAfterColumnReorder()
    {
        // The native writer checks a positional INSERT against the column now in each position.
        String tableName = "alter_column_not_null_reorder_" + randomTableSuffix();
        String schema = getSession().getSchema().get();
        try {
            assertUpdate(format("CREATE TABLE %s (a BIGINT, b BIGINT NOT NULL, c VARCHAR)", tableName));
            assertUpdate(format("INSERT INTO %s VALUES (1, 2, 'x')", tableName), 1);

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN b FIRST", tableName));
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c SET NOT NULL", tableName));
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c AFTER b", tableName));
            assertQueryOrdered(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('b', 'NO'), ('c', 'NO'), ('a', 'YES')");

            assertQueryFails(
                    format("INSERT INTO %s VALUES (NULL, 'y', 3)", tableName),
                    "(?s).*NULL value not allowed for NOT NULL column: b.*");
            assertQueryFails(
                    format("INSERT INTO %s VALUES (3, NULL, 4)", tableName),
                    "(?s).*NULL value not allowed for NOT NULL column: c.*");
            assertUpdate(format("INSERT INTO %s VALUES (3, 'y', NULL)", tableName), 1);

            // DROP NOT NULL resolves the moved columns by name, including b, whose constraint was declared at create time.
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN b DROP NOT NULL", tableName));
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c DROP NOT NULL", tableName));
            assertQueryOrdered(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('b', 'YES'), ('c', 'YES'), ('a', 'YES')");
            assertUpdate(format("INSERT INTO %s VALUES (NULL, NULL, 5)", tableName), 1);

            assertQuery(format("SELECT a, b, c FROM %s", tableName), "VALUES (1, 2, 'x'), (NULL, 3, 'y'), (5, NULL, NULL)");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testSetNotNullOnStructColumn()
    {
        // SET NOT NULL applies to the top-level struct column, not its nested fields.
        String tableName = "alter_column_not_null_struct_" + randomTableSuffix();
        String schema = getSession().getSchema().get();
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT, payload ROW(x BIGINT, y VARCHAR))", tableName));

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN payload SET NOT NULL", tableName));
            assertQuery(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('id', 'YES'), ('payload', 'NO')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testDropNotNullOnColumnDefinedAtCreateTime()
    {
        String tableName = "alter_column_drop_not_null_create_time_" + randomTableSuffix();
        String schema = getSession().getSchema().get();
        try {
            assertUpdate(format("CREATE TABLE %s (c1 BIGINT, c2 BIGINT NOT NULL, c3 VARCHAR)", tableName));
            assertQuery(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('c1', 'YES'), ('c2', 'NO'), ('c3', 'YES')");

            assertQueryFails(
                    format("INSERT INTO %s VALUES (1, NULL, 'a')", tableName),
                    "(?s).*NULL value not allowed for NOT NULL column: c2.*");

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN c2 DROP NOT NULL", tableName));
            assertQuery(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('c1', 'YES'), ('c2', 'YES'), ('c3', 'YES')");

            assertUpdate(format("INSERT INTO %s VALUES (1, NULL, 'a')", tableName), 1);
            assertUpdate(format("INSERT INTO %s VALUES (2, 3, 'b')", tableName), 1);
            assertUpdate(format("INSERT INTO %s VALUES (3, NULL, NULL)", tableName), 1);

            assertQuery(format("SELECT c1, c2, c3 FROM %s ORDER BY c1", tableName),
                    "VALUES (1, NULL, 'a'), (2, 3, 'b'), (3, NULL, NULL)");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }

    @Test
    public void testAlterColumnNotNullOnPartitionColumn()
    {
        String tableName = "alter_column_not_null_partition_" + randomTableSuffix();
        String schema = getSession().getSchema().get();
        try {
            assertUpdate(format("CREATE TABLE %s (id BIGINT, region VARCHAR) WITH (partitioning = ARRAY['region'])", tableName));

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN region SET NOT NULL", tableName));
            assertQuery(
                    format("SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s' ORDER BY ordinal_position", tableName, schema),
                    "VALUES ('id', 'YES'), ('region', 'NO')");

            assertUpdate(format("INSERT INTO %s VALUES (1, 'us-east')", tableName), 1);
            assertQueryFails(
                    format("INSERT INTO %s VALUES (2, NULL)", tableName),
                    "(?s).*NULL value not allowed for NOT NULL column: region.*");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", tableName));
        }
    }
}
