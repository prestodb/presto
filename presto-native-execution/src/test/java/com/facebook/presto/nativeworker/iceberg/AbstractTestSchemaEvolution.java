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

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergDistributedTestBase;
import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.ManifestFileCache;
import com.facebook.presto.iceberg.hive.IcebergFileHiveMetastore;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.cache.CacheBuilder;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static com.facebook.presto.hive.metastore.InMemoryCachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

/**
 * End-to-end schema-evolution tests for Iceberg tables read on native (Prestissimo)
 * workers. Each scenario evolves an Iceberg table's schema and asserts that the
 * native reader resolves columns by Iceberg field id (not by name or position).
 *
 * Concrete subclasses bind the storage format (PARQUET) so the same matrix
 * runs against every reader and proves parity.
 *
 * One velox-level scenario is intentionally absent because it is not
 * expressible as Presto Iceberg SQL DDL and is covered by the velox unit/e2e
 * tests instead:
 * - nested struct field add/drop/reorder (the connector only evolves top-level
 *   columns).
 *
 * Top-level column reordering via {@code ALTER TABLE … ADD COLUMN … FIRST|AFTER}
 * is tested by {@link #testAddColumnFirst} and {@link #testAddColumnAfter}.
 */
public abstract class AbstractTestSchemaEvolution
        extends AbstractTestQueryFramework
{
    private static final String TEST_SCHEMA = "tpch";

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

    // ADD COLUMN … FIRST positions the new column at ordinal 0. Old files do not
    // contain it, so the native reader must null-fill by field id, not by position.
    // New files written after the DDL carry the value at the new field id.
    @Test
    public void testAddColumnFirst()
    {
        String table = "schema_evolution_add_first";
        try {
            assertUpdate(format("CREATE TABLE %s (a INTEGER, b VARCHAR) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, 'x'), (2, 'y')", table), 2);

            assertUpdate(format("ALTER TABLE %s ADD COLUMN z INTEGER FIRST", table));

            // Old rows must read NULL for the new leading column.
            assertQuery(format("SELECT z, a, b FROM %s ORDER BY a", table),
                    "VALUES (NULL, 1, 'x'), (NULL, 2, 'y')");

            // New rows have a real value for z; pre-existing rows keep their field-id
            // bindings unchanged (a and b must not shift by one position).
            assertUpdate(format("INSERT INTO %s VALUES (99, 3, 'new')", table), 1);
            assertQuery(format("SELECT z, a, b FROM %s ORDER BY a", table),
                    "VALUES (NULL, 1, 'x'), (NULL, 2, 'y'), (99, 3, 'new')");
            assertQuery(format("SELECT b, z, a FROM %s ORDER BY a", table),
                    "VALUES ('x', NULL, 1), ('y', NULL, 2), ('new', 99, 3)");
            // SELECT * must return columns in schema order: z (FIRST), a, b.
            assertQuery(format("SELECT * FROM %s ORDER BY a", table),
                    "VALUES (NULL, 1, 'x'), (NULL, 2, 'y'), (99, 3, 'new')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // ADD COLUMN … AFTER <col> inserts the new column between two existing ones.
    // The native reader must correctly null-fill for old files and bind the value
    // for new files, in both cases using field id rather than physical position.
    @Test
    public void testAddColumnAfter()
    {
        String table = "schema_evolution_add_after";
        try {
            assertUpdate(format("CREATE TABLE %s (a INTEGER, c VARCHAR) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, 'x'), (2, 'y')", table), 2);

            // Insert column b between a and c.
            assertUpdate(format("ALTER TABLE %s ADD COLUMN b INTEGER AFTER a", table));

            // Old files do not have b; it must null-fill without disturbing c.
            assertQuery(format("SELECT a, b, c FROM %s ORDER BY a", table),
                    "VALUES (1, NULL, 'x'), (2, NULL, 'y')");

            // New rows carry a value for b; a and c keep their field-id bindings.
            assertUpdate(format("INSERT INTO %s VALUES (3, 10, 'z')", table), 1);
            assertQuery(format("SELECT a, b, c FROM %s ORDER BY a", table),
                    "VALUES (1, NULL, 'x'), (2, NULL, 'y'), (3, 10, 'z')");
            assertQuery(format("SELECT b, a, c FROM %s ORDER BY a", table),
                    "VALUES (NULL, 1, 'x'), (NULL, 2, 'y'), (10, 3, 'z')");
            // SELECT * must return columns in schema order: a, b (AFTER a), c.
            assertQuery(format("SELECT * FROM %s ORDER BY a", table),
                    "VALUES (1, NULL, 'x'), (2, NULL, 'y'), (3, 10, 'z')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // ALTER COLUMN … FIRST moves an existing column to ordinal 0. Existing files
    // still resolve the moved column by field id, not by position, so no data is
    // misread after the reorder.
    @Test
    public void testAlterColumnFirst()
    {
        String table = "schema_evolution_alter_first";
        try {
            assertUpdate(format("CREATE TABLE %s (a INTEGER, b INTEGER, c VARCHAR) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, 10, 'x'), (2, 20, 'y')", table), 2);

            // Move b to the front: schema becomes (b, a, c).
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN b FIRST", table));

            // Existing rows must be readable with the new column order.
            assertQuery(format("SELECT b, a, c FROM %s ORDER BY a", table),
                    "VALUES (10, 1, 'x'), (20, 2, 'y')");

            // New rows written after the reorder follow the new schema.
            assertUpdate(format("INSERT INTO %s VALUES (30, 3, 'z')", table), 1);
            assertQuery(format("SELECT b, a, c FROM %s ORDER BY a", table),
                    "VALUES (10, 1, 'x'), (20, 2, 'y'), (30, 3, 'z')");
            assertQuery(format("SELECT a, c, b FROM %s ORDER BY a", table),
                    "VALUES (1, 'x', 10), (2, 'y', 20), (3, 'z', 30)");
            // SELECT * must return columns in new schema order: b, a, c.
            assertQuery(format("SELECT * FROM %s ORDER BY a", table),
                    "VALUES (10, 1, 'x'), (20, 2, 'y'), (30, 3, 'z')");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // ALTER COLUMN … AFTER <col> moves an existing column to follow a named column.
    // Existing files still bind all columns by field id, so no values shift.
    @Test
    public void testAlterColumnAfter()
    {
        String table = "schema_evolution_alter_after";
        try {
            assertUpdate(format("CREATE TABLE %s (a INTEGER, b INTEGER, c VARCHAR) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, 10, 'x'), (2, 20, 'y')", table), 2);

            // Move a after c: schema becomes (b, c, a).
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN a AFTER c", table));

            // Existing rows must be readable with the new column order.
            assertQuery(format("SELECT b, c, a FROM %s ORDER BY a", table),
                    "VALUES (10, 'x', 1), (20, 'y', 2)");

            // New rows written after the reorder follow the new schema.
            assertUpdate(format("INSERT INTO %s VALUES (30, 'z', 3)", table), 1);
            assertQuery(format("SELECT b, c, a FROM %s ORDER BY a", table),
                    "VALUES (10, 'x', 1), (20, 'y', 2), (30, 'z', 3)");
            assertQuery(format("SELECT a, b, c FROM %s ORDER BY a", table),
                    "VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");
            // SELECT * must return columns in new schema order: b, c, a.
            assertQuery(format("SELECT * FROM %s ORDER BY a", table),
                    "VALUES (10, 'x', 1), (20, 'y', 2), (30, 'z', 3)");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // ADD a column at a specific position, write rows, then ALTER that same column to a
    // different position. Three file generations exist: rows written before the add
    // (null for the new column), rows written after the add but before the move, and
    // rows written after the move. All three must read back correctly by field id.
    @Test
    public void testAddThenAlterColumnPosition()
    {
        String table = "schema_evolution_add_then_alter";
        try {
            // Gen-1: schema (a, b, c)
            assertUpdate(format("CREATE TABLE %s (a INTEGER, b INTEGER, c VARCHAR) WITH (format = '%s')", table, storageFormat()));
            assertUpdate(format("INSERT INTO %s VALUES (1, 10, 'x')", table), 1);
            assertQuery(format("SELECT a, b, c FROM %s", table), "VALUES (1, 10, 'x')");
            assertQuery(format("SELECT c, a, b FROM %s", table), "VALUES ('x', 1, 10)");
            assertQuery(format("SELECT * FROM %s", table), "VALUES (1, 10, 'x')");

            // ADD COLUMN d AFTER b → schema (a, b, d, c)
            assertUpdate(format("ALTER TABLE %s ADD COLUMN d INTEGER AFTER b", table));

            // Gen-1 rows must null-fill d; schema order is now a, b, d, c.
            assertQuery(format("SELECT a, b, d, c FROM %s ORDER BY a", table),
                    "VALUES (1, 10, NULL, 'x')");
            assertQuery(format("SELECT c, d, a, b FROM %s ORDER BY a", table),
                    "VALUES ('x', NULL, 1, 10)");
            assertQuery(format("SELECT * FROM %s ORDER BY a", table),
                    "VALUES (1, 10, NULL, 'x')");

            // Gen-2: d has a value; gen-1 row still reads d=NULL.
            assertUpdate(format("INSERT INTO %s VALUES (2, 20, 99, 'y')", table), 1);
            assertQuery(format("SELECT a, b, d, c FROM %s ORDER BY a", table),
                    "VALUES (1, 10, NULL, 'x'), (2, 20, 99, 'y')");
            assertQuery(format("SELECT d, c, b, a FROM %s ORDER BY a", table),
                    "VALUES (NULL, 'x', 10, 1), (99, 'y', 20, 2)");
            assertQuery(format("SELECT * FROM %s ORDER BY a", table),
                    "VALUES (1, 10, NULL, 'x'), (2, 20, 99, 'y')");

            // ALTER COLUMN d FIRST → schema (d, a, b, c)
            assertUpdate(format("ALTER TABLE %s ALTER COLUMN d FIRST", table));

            // Both prior generations must re-read correctly under the new order.
            assertQuery(format("SELECT d, a, b, c FROM %s ORDER BY a", table),
                    "VALUES (NULL, 1, 10, 'x'), (99, 2, 20, 'y')");
            assertQuery(format("SELECT b, c, d, a FROM %s ORDER BY a", table),
                    "VALUES (10, 'x', NULL, 1), (20, 'y', 99, 2)");
            assertQuery(format("SELECT * FROM %s ORDER BY a", table),
                    "VALUES (NULL, 1, 10, 'x'), (99, 2, 20, 'y')");

            // Gen-3: follows new schema (d, a, b, c)
            assertUpdate(format("INSERT INTO %s VALUES (77, 3, 30, 'z')", table), 1);

            // All three generations resolve correctly by field id.
            assertQuery(format("SELECT d, a, b, c FROM %s ORDER BY a", table),
                    "VALUES (NULL, 1, 10, 'x'), (99, 2, 20, 'y'), (77, 3, 30, 'z')");
            assertQuery(format("SELECT a, b, c, d FROM %s ORDER BY a", table),
                    "VALUES (1, 10, 'x', NULL), (2, 20, 'y', 99), (3, 30, 'z', 77)");
            // SELECT * must return columns in current schema order: d, a, b, c.
            assertQuery(format("SELECT * FROM %s ORDER BY a", table),
                    "VALUES (NULL, 1, 10, 'x'), (99, 2, 20, 'y'), (77, 3, 30, 'z')");
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

    // A predicate on a column that only became a partition column later must
    // still be evaluated row-by-row against files written under the old spec.
    //
    // The native split (IcebergSplit) does not carry Iceberg's per-file
    // FileScanTask.residual(), so the concern is that a predicate Iceberg
    // simplified away for new-spec files could be lost for old-spec files.
    // Correctness instead rests on the table-level domain/remaining predicate,
    // which the native worker always receives and evaluates: the old-spec file
    // holds both rec_source values, so if the row-level predicate were dropped
    // for it, the 'B' row would leak into a rec_source = 'A' result.
    @Test
    public void testPartitionEvolutionKeepsRowLevelPredicate()
            throws Exception
    {
        String table = "schema_evolution_partition_evolution";
        try {
            assertUpdate(format(
                    "CREATE TABLE %s (id INTEGER, rec_source VARCHAR, int_load_ts TIMESTAMP) " +
                            "WITH (format = '%s', partitioning = ARRAY['day(int_load_ts)'])",
                    table, storageFormat()));

            // Old spec: a single file holding both rec_source values on the same
            // day, so rec_source is not constant within the file and cannot be
            // answered from partition metadata alone.
            assertUpdate(format("INSERT INTO %s VALUES " +
                    "(1, 'A', TIMESTAMP '2024-01-01 00:00:00'), " +
                    "(2, 'B', TIMESTAMP '2024-01-01 01:00:00')", table), 2);

            int oldSpecId = loadIcebergTable(table).spec().specId();

            // Promote rec_source to an identity partition field. There is no
            // Presto SQL DDL for adding an existing column to a partition spec
            // (setTableProperties only accepts updatable table properties), so
            // evolve through the Iceberg API.
            loadIcebergTable(table).updateSpec().addField("rec_source").commit();
            assertNotEquals(loadIcebergTable(table).spec().specId(), oldSpecId);

            // New spec: rec_source is a partition column, so these rows land in
            // per-value files where the predicate IS enforceable by pruning.
            assertUpdate(format("INSERT INTO %s VALUES " +
                    "(3, 'A', TIMESTAMP '2024-01-02 00:00:00'), " +
                    "(4, 'B', TIMESTAMP '2024-01-02 01:00:00')", table), 2);

            assertOldSpecFileHasNonTrivialResidual(table, oldSpecId);

            // id = 2 is the 'B' row inside the old-spec file. Its presence here
            // would mean the row-level predicate was lost during evolution.
            assertQuery(format("SELECT id FROM %s WHERE rec_source = 'A' ORDER BY id", table),
                    "VALUES 1, 3");
            assertQuery(format("SELECT id FROM %s WHERE rec_source = 'B' ORDER BY id", table),
                    "VALUES 2, 4");
            assertQuery(format("SELECT count(*) FROM %s", table), "VALUES 4");
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", table));
        }
    }

    // Makes the premise of testPartitionEvolutionKeepsRowLevelPredicate
    // explicit: Iceberg itself cannot fully answer rec_source = 'A' for the
    // old-spec file, so it leaves a non-trivial residual that something
    // downstream must still evaluate.
    private void assertOldSpecFileHasNonTrivialResidual(String table, int oldSpecId)
            throws IOException
    {
        boolean sawOldSpecFile = false;
        try (CloseableIterable<FileScanTask> tasks = loadIcebergTable(table).newScan()
                .filter(Expressions.equal("rec_source", "A"))
                .planFiles()) {
            for (FileScanTask task : tasks) {
                if (task.spec().specId() == oldSpecId) {
                    assertNotEquals(
                            task.residual().toString(),
                            Expressions.alwaysTrue().toString(),
                            "old-spec file should retain a residual for rec_source = 'A'");
                    sawOldSpecFile = true;
                }
            }
        }
        assertTrue(sawOldSpecFile, "scan did not plan any old-spec file");
    }

    // Loads the live Iceberg table behind the query runner. The fixture uses the
    // HIVE catalog backed by a file metastore, so go through
    // IcebergUtil.getHiveIcebergTable rather than constructing a HadoopCatalog.
    private Table loadIcebergTable(String tableName)
    {
        CatalogManager catalogManager = getDistributedQueryRunner().getCoordinator().getCatalogManager();
        ConnectorId connectorId = catalogManager.getCatalog(ICEBERG_CATALOG).get().getConnectorId();
        ConnectorSession connectorSession = getSession().toConnectorSession(connectorId);

        HdfsEnvironment hdfsEnvironment = IcebergDistributedTestBase.getHdfsEnvironment(
                new HiveClientConfig(),
                new MetastoreClientConfig(),
                new HiveS3Config());
        ExtendedHiveMetastore metastore = memoizeMetastore(
                new IcebergFileHiveMetastore(hdfsEnvironment, getCatalogDirectory().toString(), "test"),
                false,
                1000,
                0);

        return IcebergUtil.getHiveIcebergTable(
                metastore,
                hdfsEnvironment,
                new IcebergHiveTableOperationsConfig(),
                new ManifestFileCache(CacheBuilder.newBuilder().build(), false, 0, 1024),
                connectorSession,
                new IcebergCatalogName(ICEBERG_CATALOG),
                SchemaTableName.valueOf(TEST_SCHEMA + "." + tableName));
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        // Mirrors the query-runner fixture above, which sets
        // addStorageFormatToPath(false).
        return getIcebergDataDirectoryPath(
                dataDirectory,
                HIVE.name(),
                new IcebergConfig().getFileFormat(),
                false)
                .toFile();
    }
}
