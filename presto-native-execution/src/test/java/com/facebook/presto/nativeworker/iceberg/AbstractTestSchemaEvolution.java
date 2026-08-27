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

import com.facebook.presto.Session;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static java.lang.String.format;

/**
 * End-to-end schema-evolution tests for Iceberg tables read on native (Prestissimo)
 * workers. Each scenario evolves an Iceberg table's schema and asserts that the
 * native reader resolves columns by Iceberg field id (not by name or position).
 *
 * Concrete subclasses bind the storage format (PARQUET) so the same matrix
 * runs against every reader and proves parity.
 *
 * Two velox-level scenarios are intentionally absent because they are not
 * expressible as Presto Iceberg SQL DDL and are covered by the velox unit/e2e
 * tests instead:
 * - column reorder (no ALTER ... FIRST/AFTER; only SELECT projection order, which
 *   {@link #testRenameReorderDropAdd} exercises),
 * - nested struct field add/drop/reorder (the connector only evolves top-level
 *   columns).
 */
public abstract class AbstractTestSchemaEvolution
        extends AbstractTestQueryFramework
{
    protected abstract String storageFormat();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(storageFormat())
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(storageFormat())
                .setAddStorageFormatToPath(false)
                .build();
    }

    // Renaming a column must not change which data it reads back: the field id is
    // stable across the rename, so the values written under 'a' surface under 'a2'.
    @Test
    public void testRenameColumn()
    {
        String table = "schema_evolution_rename";
        try {
            assertUpdate(format("CREATE TABLE %s (a INTEGER, b VARCHAR) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, '1001'), (2, '1002')", table), 2);
            assertUpdate(format("ALTER TABLE %s RENAME COLUMN a TO a2", table));
            assertQuery(format("SELECT a2, b FROM %s ORDER BY a2", table), "VALUES (1, '1001'), (2, '1002')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // A newly added column reads back NULL for rows written before it existed, and
    // reads the written value for later rows. Dropping it leaves the others intact.
    @Test
    public void testAddAndDropColumn()
    {
        String table = "schema_evolution_add_drop";
        try {
            assertUpdate(format("CREATE TABLE %s (a INTEGER, b VARCHAR) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, '1001'), (2, '1002')", table), 2);

            assertUpdate(format("ALTER TABLE %s ADD COLUMN c VARCHAR", table));
            assertQuery(format("SELECT a, b, c FROM %s ORDER BY a", table), "VALUES (1, '1001', NULL), (2, '1002', NULL)");

            assertUpdate(format("INSERT INTO %s VALUES (3, '1003', 'new')", table), 1);
            assertQuery(format("SELECT a, b, c FROM %s ORDER BY a", table),
                    "VALUES (1, '1001', NULL), (2, '1002', NULL), (3, '1003', 'new')");

            assertUpdate(format("ALTER TABLE %s DROP COLUMN c", table));
            assertQuery(format("SELECT a, b FROM %s ORDER BY a", table), "VALUES (1, '1001'), (2, '1002'), (3, '1003')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // The core field-id guarantee: dropping a column and re-adding one with the
    // SAME name assigns a new field id, so the stale file column must NOT bind by
    // name. Rows written before the re-add read NULL; later rows read their value.
    // TODO: Re-enable once Velox implements ColumnMappingMode::kParquetFieldId
    // (ParquetReader.cpp throws VELOX_NYI). Until then the Parquet reader resolves
    // columns by name/position, so the re-added column binds to the stale file
    // column instead of null-filling by field id.
    @Test(enabled = false)
    public void testDropAndReAddSameName()
    {
        String table = "schema_evolution_drop_readd";
        try {
            assertUpdate(format("CREATE TABLE %s (a INTEGER, b VARCHAR) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, '1001'), (2, '1002')", table), 2);

            assertUpdate(format("ALTER TABLE %s DROP COLUMN a", table));
            assertUpdate(format("ALTER TABLE %s ADD COLUMN a INTEGER", table));
            // Old rows must read NULL for the re-added 'a', not the stale value 1/2.
            assertQuery(format("SELECT b, a FROM %s ORDER BY b", table), "VALUES ('1001', NULL), ('1002', NULL)");

            assertUpdate(format("INSERT INTO %s VALUES ('1003', 7)", table), 1);
            assertQuery(format("SELECT b, a FROM %s ORDER BY b", table),
                    "VALUES ('1001', NULL), ('1002', NULL), ('1003', 7)");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // Dropping a middle column then re-adding it leaves the trailing column's data
    // bound by field id, independent of physical position. This is the SQL-level
    // analog of the velox column-reorder test.
    // TODO: Re-enable once Velox implements ColumnMappingMode::kParquetFieldId
    // (ParquetReader.cpp throws VELOX_NYI); Parquet currently resolves by
    // name/position, breaking field-id resolution across a drop/re-add gap.
    @Test(enabled = false)
    public void testDropMiddleColumnReAdd()
    {
        String table = "schema_evolution_drop_middle";
        try {
            assertUpdate(format("CREATE TABLE %s (col0 INTEGER, col1 INTEGER, col2 INTEGER) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (0, 1, 2)", table), 1);

            assertUpdate(format("ALTER TABLE %s DROP COLUMN col1", table));
            assertUpdate(format("ALTER TABLE %s ADD COLUMN col1 INTEGER", table));
            // After re-add the physical order is (col0, col2, col1); INSERT follows
            // the logical column order col0, col2, col1.
            assertUpdate(format("INSERT INTO %s (col0, col2, col1) VALUES (3, 4, 5)", table), 1);

            // col2 keeps its value by field id across the drop/re-add of col1;
            // the old row reads NULL for the re-added col1.
            assertQuery(format("SELECT col0, col1, col2 FROM %s ORDER BY col0", table),
                    "VALUES (0, NULL, 2), (3, 5, 4)");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // Combined rename + drop + add, with reorder applied via SELECT projection
    // order. Mirrors the velox fieldIdRenameReorderDropAdd scenario.
    // TODO: Re-enable once Velox implements ColumnMappingMode::kParquetFieldId
    // (ParquetReader.cpp throws VELOX_NYI); the drop+add here needs field-id
    // resolution the Parquet reader does not yet support.
    @Test(enabled = false)
    public void testRenameReorderDropAdd()
    {
        String table = "schema_evolution_combined";
        try {
            assertUpdate(format("CREATE TABLE %s (a BIGINT, b INTEGER, c VARCHAR) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')", table), 3);

            assertUpdate(format("ALTER TABLE %s RENAME COLUMN c TO c2", table));
            assertUpdate(format("ALTER TABLE %s DROP COLUMN b", table));
            assertUpdate(format("ALTER TABLE %s ADD COLUMN d INTEGER", table));

            // Reorder is expressed through the projection order (c2, a, d). c2/a
            // resolve by field id to their original data; d is null-filled.
            assertQuery(format("SELECT c2, a, d FROM %s ORDER BY a", table),
                    "VALUES ('x', BIGINT '1', NULL), ('y', BIGINT '2', NULL), ('z', BIGINT '3', NULL)");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // Widening INTEGER to BIGINT must preserve existing values read from the file.
    @Test
    public void testTypePromotionIntegerToBigint()
    {
        String table = "schema_evolution_promote_bigint";
        try {
            assertUpdate(format("CREATE TABLE %s (a INTEGER, b VARCHAR) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, '1001'), (2, '1002')", table), 2);

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN a SET DATA TYPE BIGINT", table));
            assertQuery(format("SELECT a, b FROM %s ORDER BY a", table),
                    "VALUES (BIGINT '1', '1001'), (BIGINT '2', '1002')");

            assertUpdate(format("INSERT INTO %s VALUES (BIGINT '5000000000', '1003')", table), 1);
            assertQuery(format("SELECT a, b FROM %s ORDER BY a", table),
                    "VALUES (BIGINT '1', '1001'), (BIGINT '2', '1002'), (BIGINT '5000000000', '1003')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // Widening REAL to DOUBLE must preserve existing values read from the file.
    @Test
    public void testTypePromotionRealToDouble()
    {
        String table = "schema_evolution_promote_double";
        try {
            assertUpdate(format("CREATE TABLE %s (a REAL, b VARCHAR) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (REAL '1.5', 'x'), (REAL '2.5', 'y')", table), 2);

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN a SET DATA TYPE DOUBLE", table));
            assertQuery(format("SELECT a, b FROM %s ORDER BY b", table),
                    "VALUES (DOUBLE '1.5', 'x'), (DOUBLE '2.5', 'y')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // An unknown column is never stored in a data file and always reads back as null, including
    // rows written before the column was added (i.e. the file does not have the field at all).
    @Test
    public void testWriteUnknownColumn()
    {
        String table = "unknown_write";
        try {
            assertUpdate(format("CREATE TABLE %s (id INTEGER, u UNKNOWN) WITH (\"format-version\" = '3', format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, NULL)", table), 1);
            assertUpdate(format("INSERT INTO %s (id) VALUES 2", table), 1);
            assertQuery(format("SELECT * FROM %s ORDER BY id", table), "VALUES (1, NULL), (2, NULL)");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // Adding an unknown column to an existing table: rows written before the add read NULL for the
    // new column (the field is not in their file), and rows written after also read NULL.
    @Test
    public void testAddUnknownColumn()
    {
        String table = "unknown_add";
        try {
            assertUpdate(format("CREATE TABLE %s (id INTEGER) WITH (\"format-version\" = '3', format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES 1", table), 1);
            assertUpdate(format("ALTER TABLE %s ADD COLUMN u UNKNOWN", table));
            assertUpdate(format("INSERT INTO %s VALUES (2, NULL)", table), 1);
            assertQuery(format("SELECT * FROM %s ORDER BY id", table), "VALUES (1, NULL), (2, NULL)");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // Planning with pushdown_filter_enabled must succeed even when the table has an unknown column.
    // Filter pushdown converts the full table schema to Hive columns; an unsupported type would fail
    // at that step.
    @Test
    public void testReadUnknownColumnWithFilterPushdownPlans()
    {
        String table = "unknown_pushdown";
        Session pushdownEnabled = Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
        try {
            assertUpdate(format("CREATE TABLE %s (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3', format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", table), 2);
            assertUpdate(format("ALTER TABLE %s ADD COLUMN u UNKNOWN", table));
            assertQuerySucceeds(pushdownEnabled, format("EXPLAIN SELECT * FROM %s WHERE id = 1", table));
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // The UNKNOWN type was introduced in Iceberg format version 3; older tables must reject it.
    @Test
    public void testUnknownColumnRequiresV3()
    {
        String table = "unknown_requires_v3";
        try {
            assertQueryFails(
                    format("CREATE TABLE %s (id INTEGER, u UNKNOWN) WITH (\"format-version\" = '2', format = '%s')", table, storageFormat()),
                    "(?s).*Invalid type for u: unknown is not supported until v3.*");
            assertUpdate(format("CREATE TABLE %s (id INTEGER) WITH (\"format-version\" = '2', format = '%s')", table, storageFormat()));
            assertQueryFails(
                    format("ALTER TABLE %s ADD COLUMN u UNKNOWN", table),
                    "(?s).*Invalid type for u: unknown is not supported until v3.*");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // An unknown column only ever holds null so it cannot be declared NOT NULL.
    @Test
    public void testUnknownColumnCannotBeRequired()
    {
        String table = "unknown_not_null";
        try {
            assertQueryFails(
                    format("CREATE TABLE %s (id INTEGER, u UNKNOWN NOT NULL) WITH (\"format-version\" = '3', format = '%s')", table, storageFormat()),
                    ".*Cannot create required field with unknown type: u.*");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // Only null can be written to an unknown column; a non-null value is rejected at analysis time.
    @Test
    public void testWriteValueToUnknownColumnFails()
    {
        String table = "unknown_write_value_fails";
        try {
            assertUpdate(format("CREATE TABLE %s (id INTEGER, u UNKNOWN) WITH (\"format-version\" = '3', format = '%s')", table, storageFormat()));
            assertQueryFails(
                    format("INSERT INTO %s VALUES (1, 5)", table),
                    ".*'u' is of type unknown but expression is of type integer.*");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // CTAS preserves the unknown type for a column that is always null.
    @Test
    public void testCreateTableAsSelectWithUnknownColumn()
    {
        String table = "unknown_ctas";
        String copy = table + "_copy";
        try {
            assertUpdate(format("CREATE TABLE %s WITH (\"format-version\" = '3', format = '%s') AS SELECT 1 id, NULL u", table, storageFormat()), 1);
            assertQuery(format("SELECT * FROM %s", table), "VALUES (1, NULL)");
            assertUpdate(format("CREATE TABLE %s (id, u) WITH (\"format-version\" = '3', format = '%s') AS SELECT * FROM %s", copy, storageFormat(), table), 1);
            assertQuery(format("SELECT * FROM %s", copy), "VALUES (1, NULL)");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
            assertUpdate(format("DROP TABLE IF EXISTS %s", copy));
        }
    }

    // A sorted table with an unknown column reads back correctly; bucketing is rejected because
    // no hash is defined for the unknown type. (Identity-partition inserts are covered by the Java
    // test suite — Velox does not support identity transforms on UNKNOWN-typed partition columns.)
    @Test
    public void testUnknownColumnSortingAndBucketingRejected()
    {
        String sorted = "unknown_sorted";
        String bucketed = "unknown_bucketed";
        try {
            assertUpdate(format("CREATE TABLE %s (id INTEGER, u UNKNOWN) WITH (\"format-version\" = '3', format = '%s', sorted_by = ARRAY['u'])", sorted, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, NULL), (2, NULL)", sorted), 2);
            assertQuery(format("SELECT * FROM %s ORDER BY id", sorted), "VALUES (1, NULL), (2, NULL)");

            assertQueryFails(
                    format("CREATE TABLE %s (id INTEGER, u UNKNOWN) WITH (\"format-version\" = '3', format = '%s', partitioning = ARRAY['bucket(u, 2)'])", bucketed, storageFormat()),
                    ".*Invalid source type unknown for transform: bucket\\[2\\].*");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", sorted));
            assertUpdate(format("DROP TABLE IF EXISTS %s", bucketed));
        }
    }

    // Iceberg has not implemented promotion from unknown to any other type.
    @Test
    public void testPromoteUnknownColumnUnsupported()
    {
        String table = "unknown_promote";
        try {
            assertUpdate(format("CREATE TABLE %s (id INTEGER, u UNKNOWN) WITH (\"format-version\" = '3', format = '%s')", table, storageFormat()));
            assertQueryFails(
                    format("ALTER TABLE %s ALTER COLUMN u SET DATA TYPE INTEGER", table),
                    ".*Cannot change column type: u: unknown -> int.*");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // ORDER BY, GROUP BY, DISTINCT and aggregates on an unknown column all collapse to a single null
    // group because every value is null.
    @Test
    public void testUnknownColumnInSortsAndAggregations()
    {
        String table = "unknown_sort_agg";
        try {
            assertUpdate(format("CREATE TABLE %s (id INTEGER, u UNKNOWN) WITH (\"format-version\" = '3', format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, NULL), (2, NULL)", table), 2);
            assertQuery(format("SELECT id FROM %s ORDER BY u, id", table), "VALUES 1, 2");
            assertQuery(format("SELECT u, count(*) FROM %s GROUP BY 1", table), "VALUES (NULL, 2)");
            assertQuery(format("SELECT DISTINCT u FROM %s", table), "VALUES NULL");
            assertQuery(format("SELECT max(u) FROM %s", table), "VALUES NULL");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // Metadata tables and SHOW/ANALYZE must not fail when the table has an unknown column.
    @Test
    public void testUnknownColumnMetadataTablesAndStatistics()
    {
        String table = "unknown_metadata";
        try {
            assertUpdate(format("CREATE TABLE %s (id INTEGER, name VARCHAR, u UNKNOWN) WITH (\"format-version\" = '3', format = '%s', partitioning = ARRAY['id'])", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, 'Alice', NULL), (2, 'Bob', NULL)", table), 2);
            assertQuerySucceeds(format("SELECT * FROM \"%s$partitions\"", table));
            assertQuerySucceeds(format("SELECT * FROM \"%s$files\"", table));
            assertQuerySucceeds(format("SELECT * FROM \"%s$manifests\"", table));
            assertQuerySucceeds(format("SHOW STATS FOR %s", table));
            assertQuerySucceeds(format("ANALYZE %s", table));
            assertQuerySucceeds(format("SHOW COLUMNS FROM %s", table));
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // An unknown column can be renamed and dropped just like any other column.
    @Test
    public void testRenameAndDropUnknownColumn()
    {
        String table = "unknown_rename_drop";
        try {
            assertUpdate(format("CREATE TABLE %s (id INTEGER, u UNKNOWN) WITH (\"format-version\" = '3', format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, NULL)", table), 1);
            assertUpdate(format("ALTER TABLE %s RENAME COLUMN u TO u2", table));
            assertQuery(format("SELECT * FROM %s", table), "VALUES (1, NULL)");
            assertUpdate(format("ALTER TABLE %s DROP COLUMN u2", table));
            assertQuery(format("SELECT * FROM %s", table), "VALUES 1");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }
}
