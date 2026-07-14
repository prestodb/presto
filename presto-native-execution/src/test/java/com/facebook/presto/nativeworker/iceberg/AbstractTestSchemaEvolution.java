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

    // A NOT NULL constraint added via ALTER COLUMN is enforced by the engine's
    // generic TableWriter NOT NULL check (connector-agnostic: the same mechanism
    // Hive tables use), independent of Iceberg's own field-id schema evolution
    // exercised by the tests above. The constraint applies only to writes that
    // happen after it is added; it does not rewrite existing data files.
    @Test
    public void testAlterColumnSetNotNull()
    {
        String table = "schema_evolution_set_not_null";
        try {
            assertUpdate(format("CREATE TABLE %s (a INTEGER, b VARCHAR) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, '1001')", table), 1);

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN b SET NOT NULL", table));

            assertQueryFails(format("INSERT INTO %s VALUES (2, NULL)", table), "NULL value not allowed for NOT NULL column: b");
            assertUpdate(format("INSERT INTO %s VALUES (2, '1002')", table), 1);
            assertQuery(format("SELECT a, b FROM %s ORDER BY a", table), "VALUES (1, '1001'), (2, '1002')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // Dropping a NOT NULL constraint must allow subsequent writes with NULL for
    // that column, while leaving previously-written (non-null) data unaffected.
    @Test
    public void testAlterColumnDropNotNull()
    {
        String table = "schema_evolution_drop_not_null";
        try {
            assertUpdate(format("CREATE TABLE %s (a INTEGER, b VARCHAR NOT NULL) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, '1001')", table), 1);
            assertQueryFails(format("INSERT INTO %s VALUES (2, NULL)", table), "NULL value not allowed for NOT NULL column: b");

            assertUpdate(format("ALTER TABLE %s ALTER COLUMN b DROP NOT NULL", table));
            assertUpdate(format("INSERT INTO %s VALUES (2, NULL)", table), 1);
            assertQuery(format("SELECT a, b FROM %s ORDER BY a", table), "VALUES (1, '1001'), (2, NULL)");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }
}
