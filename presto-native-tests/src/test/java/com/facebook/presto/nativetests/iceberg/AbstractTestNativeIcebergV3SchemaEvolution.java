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

import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import static java.lang.String.format;

/**
 * Schema evolution on V3 tables read by native workers. Columns must resolve by Iceberg field id,
 * so every scenario keeps data files written under older schemas readable.
 */
public abstract class AbstractTestNativeIcebergV3SchemaEvolution
        extends AbstractNativeIcebergV3Test
{
    protected AbstractTestNativeIcebergV3SchemaEvolution(String storageFormat)
    {
        super(storageFormat);
    }

    @Test
    public void testAddColumnOfEveryType()
    {
        String table = "evolve_add_types";
        createV3Table(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1)", 1);
        String[][] columns = {
                {"c_boolean", "boolean", "true"},
                {"c_bigint", "bigint", "BIGINT '10000000000'"},
                {"c_real", "real", "REAL '1.5'"},
                {"c_double", "double", "2.25"},
                {"c_decimal", "decimal(10, 2)", "DECIMAL '12.34'"},
                {"c_date", "date", "DATE '2024-01-01'"},
                {"c_timestamp", "timestamp", "TIMESTAMP '2024-01-01 12:34:56.789'"},
                {"c_varchar", "varchar", "'abc'"},
                {"c_varbinary", "varbinary", "X'0102'"},
                {"c_array", "array(integer)", "ARRAY[1, 2]"},
                {"c_map", "map(varchar, integer)", "MAP(ARRAY['k'], ARRAY[1])"},
                {"c_row", "row(x integer, y varchar)", "ROW(1, 'y')"}};
        StringBuilder values = new StringBuilder("2");
        for (String[] column : columns) {
            assertUpdate(format("ALTER TABLE %s ADD COLUMN %s %s", table, column[0], column[1]));
            values.append(", ").append(column[2]);
        }
        assertUpdate(format("INSERT INTO %s VALUES (%s)", table, values), 1);

        assertQuery("SELECT count(*) FROM " + table + " WHERE id = 1 AND c_boolean IS NULL AND c_bigint IS NULL AND c_varbinary IS NULL AND c_row IS NULL", "VALUES 1");
        assertQuery("SELECT id, c_boolean, c_bigint, c_real, c_double, c_decimal, c_date, c_timestamp, c_varchar, to_hex(c_varbinary), "
                + "json_format(CAST(c_array AS json)), json_format(CAST(c_map AS json)), c_row.x, c_row.y FROM " + table);
        dropTable(table);
    }

    @Test
    public void testFilterAndAggregateOnAddedColumn()
    {
        String table = "evolve_filter_added";
        createV3Table(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);
        assertUpdate("ALTER TABLE " + table + " ADD COLUMN score bigint");
        assertUpdate("INSERT INTO " + table + " VALUES (4, 'd', 40), (5, 'e', 50)", 2);

        assertQuery("SELECT id FROM " + table + " WHERE score IS NULL", "VALUES 1, 2, 3");
        assertQuery("SELECT id FROM " + table + " WHERE score > 40", "VALUES 5");
        assertQuery("SELECT count(score), sum(score), count(*) FROM " + table, "VALUES (2, 90, 5)");
        assertQuery("SELECT coalesce(score, -1), count(*) FROM " + table + " GROUP BY coalesce(score, -1)", "VALUES (-1, 3), (40, 1), (50, 1)");
        dropTable(table);
    }

    @Test
    public void testRenameColumnsInChain()
    {
        String table = "evolve_rename_chain";
        createV3Table(table, "a integer, b varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'x')", 1);
        assertUpdate("ALTER TABLE " + table + " RENAME COLUMN a TO a1");
        assertUpdate("INSERT INTO " + table + " VALUES (2, 'y')", 1);
        assertUpdate("ALTER TABLE " + table + " RENAME COLUMN a1 TO a2");
        assertUpdate("INSERT INTO " + table + " VALUES (3, 'z')", 1);
        assertQuery("SELECT a2, b FROM " + table, "VALUES (1, 'x'), (2, 'y'), (3, 'z')");
        dropTable(table);
    }

    @Test
    public void testSwapColumnNames()
    {
        // A name swap only changes the name -> field id mapping; each value stays with its field id.
        String table = "evolve_swap_names";
        createV3Table(table, "a integer, b integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 100)", 1);
        assertUpdate("ALTER TABLE " + table + " RENAME COLUMN a TO tmp");
        assertUpdate("ALTER TABLE " + table + " RENAME COLUMN b TO a");
        assertUpdate("ALTER TABLE " + table + " RENAME COLUMN tmp TO b");
        assertQuery("SELECT a, b FROM " + table, "VALUES (100, 1)");
        dropTable(table);
    }

    @Test
    public void testDropColumnThenFilterRemaining()
    {
        String table = "evolve_drop_filter";
        createV3Table(table, "id integer, dropped varchar, kept bigint");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'x', 10), (2, 'y', 20), (3, 'z', 30)", 3);
        assertUpdate("ALTER TABLE " + table + " DROP COLUMN dropped");
        assertQuery("SELECT id, kept FROM " + table + " WHERE kept >= 20", "VALUES (2, 20), (3, 30)");
        assertUpdate("INSERT INTO " + table + " VALUES (4, 40)", 1);
        assertQuery("SELECT * FROM " + table, "VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
        dropTable(table);
    }

    @Test
    public void testReorderColumns()
    {
        String table = "evolve_reorder";
        createV3Table(table, "a integer, b varchar, c double");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'x', 1.5)", 1);
        loadTable(table).updateSchema().moveFirst("c").commit();
        loadTable(table).updateSchema().moveAfter("a", "b").commit();
        assertUpdate("INSERT INTO " + table + " VALUES (2.5, 'y', 2)", 1);
        assertQuery("SELECT * FROM " + table, "VALUES (1.5, 'x', 1), (2.5, 'y', 2)");
        assertQuery("SELECT a, b, c FROM " + table, "VALUES (1, 'x', 1.5), (2, 'y', 2.5)");
        dropTable(table);
    }

    @Test
    public void testPromoteIntegerToBigint()
    {
        String table = "evolve_promote_bigint";
        createV3Table(table, "id integer, v integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 2147483647)", 1);
        assertUpdate("ALTER TABLE " + table + " ALTER COLUMN v SET DATA TYPE bigint");
        assertUpdate("INSERT INTO " + table + " VALUES (2, 5000000000)", 1);
        assertQuery("SELECT id, v FROM " + table, "VALUES (1, BIGINT '2147483647'), (2, BIGINT '5000000000')");
        assertQuery("SELECT sum(v) FROM " + table, "VALUES BIGINT '7147483647'");
        dropTable(table);
    }

    @Test
    public void testPromoteRealToDouble()
    {
        String table = "evolve_promote_double";
        createV3Table(table, "id integer, v real");
        assertUpdate("INSERT INTO " + table + " VALUES (1, REAL '1.5')", 1);
        assertUpdate("ALTER TABLE " + table + " ALTER COLUMN v SET DATA TYPE double");
        assertUpdate("INSERT INTO " + table + " VALUES (2, 2.25)", 1);
        assertQuery("SELECT id, v FROM " + table, "VALUES (1, 1.5), (2, 2.25)");
        dropTable(table);
    }

    @Test
    public void testWidenDecimalPrecision()
    {
        String table = "evolve_widen_decimal";
        createV3Table(table, "id integer, d decimal(9, 2)");
        assertUpdate("INSERT INTO " + table + " VALUES (1, DECIMAL '1234567.89')", 1);
        loadTable(table).updateSchema().updateColumn("d", Types.DecimalType.of(18, 2)).commit();
        assertUpdate("INSERT INTO " + table + " VALUES (2, DECIMAL '1234567890123456.78')", 1);
        assertQuery("SELECT id, d FROM " + table, "VALUES (1, DECIMAL '1234567.89'), (2, DECIMAL '1234567890123456.78')");
        dropTable(table);
    }

    @Test
    public void testPartitionOnAddedColumn()
    {
        String table = "evolve_partition_added";
        createV3Table(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (1), (2)", 2);
        assertUpdate("ALTER TABLE " + table + " ADD COLUMN region varchar");
        loadTable(table).updateSpec().addField("region").commit();
        assertUpdate("INSERT INTO " + table + " VALUES (3, 'us'), (4, 'eu'), (5, 'us')", 3);
        assertQuery("SELECT id FROM " + table + " WHERE region = 'us'", "VALUES 3, 5");
        assertQuery("SELECT id FROM " + table + " WHERE region IS NULL", "VALUES 1, 2");
        dropTable(table);
    }

    @Test
    public void testTimeTravelAcrossSchemaVersions()
    {
        // A snapshot is read with the schema of the current table.
        String table = "evolve_time_travel";
        createV3Table(table, "id integer, v varchar");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 'a')", 1);
        long beforeAdd = currentSnapshotId(table);
        assertUpdate("ALTER TABLE " + table + " ADD COLUMN extra bigint");
        assertUpdate("INSERT INTO " + table + " VALUES (2, 'b', 20)", 1);

        assertQuery("SELECT id, v FROM " + table + " FOR VERSION AS OF " + beforeAdd, "VALUES (1, 'a')");
        assertQuery("SELECT id, v, extra FROM " + table, "VALUES (1, 'a', NULL), (2, 'b', 20)");
        dropTable(table);
    }

    @Test
    public void testManySchemaVersions()
    {
        String table = "evolve_many_versions";
        createV3Table(table, "id integer");
        assertUpdate("INSERT INTO " + table + " VALUES (0)", 1);
        for (int i = 1; i <= 8; i++) {
            assertUpdate(format("ALTER TABLE %s ADD COLUMN c%s integer", table, i));
            StringBuilder values = new StringBuilder(String.valueOf(i));
            for (int j = 1; j <= i; j++) {
                values.append(", ").append(i * 10 + j);
            }
            assertUpdate(format("INSERT INTO %s (id, %s) VALUES (%s)", table, columnList(i), values), 1);
        }
        assertQuery("SELECT count(*), count(c1), count(c8), sum(c1), max(c8) FROM " + table, "VALUES (9, 8, 1, 368, 88)");
        assertQuery("SELECT * FROM " + table + " WHERE id = 4");
        dropTable(table);
    }

    private static String columnList(int count)
    {
        StringBuilder columns = new StringBuilder();
        for (int j = 1; j <= count; j++) {
            columns.append(j == 1 ? "" : ", ").append("c").append(j);
        }
        return columns.toString();
    }
}
