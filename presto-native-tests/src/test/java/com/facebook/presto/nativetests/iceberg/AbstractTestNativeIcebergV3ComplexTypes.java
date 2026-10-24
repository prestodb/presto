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
 * Iceberg list, map and struct columns (Presto array, map and row) on V3 tables: nesting,
 * NULLs, large collections, nested-field filters and struct evolution. Complex values are
 * compared as JSON text.
 */
public abstract class AbstractTestNativeIcebergV3ComplexTypes
        extends AbstractNativeIcebergV3Test
{
    private static final String NESTED_COLUMNS = "id integer, "
            + "items array(row(x integer, tags array(varchar))), "
            + "metrics map(varchar, array(bigint)), "
            + "info row(code bigint, detail row(k varchar, scores map(varchar, double)))";

    private static final String NESTED_ROWS = "VALUES "
            + "(1, ARRAY[ROW(1, ARRAY['a', 'b']), ROW(2, ARRAY[])], "
            + "MAP(ARRAY['m1', 'm2'], ARRAY[ARRAY[BIGINT '1', 2], ARRAY[BIGINT '3']]), "
            + "ROW(BIGINT '10', ROW('k1', MAP(ARRAY['s'], ARRAY[1.5])))), "
            + "(2, ARRAY[ROW(3, NULL), NULL], "
            + "MAP(ARRAY['m3'], ARRAY[CAST(NULL AS array(bigint))]), "
            + "ROW(BIGINT '20', ROW(NULL, CAST(NULL AS map(varchar, double))))), "
            + "(3, ARRAY[], MAP(), ROW(NULL, NULL)), "
            + "(4, NULL, NULL, NULL)";

    protected AbstractTestNativeIcebergV3ComplexTypes(String storageFormat)
    {
        super(storageFormat);
    }

    @Test
    public void testDeeplyNestedRoundTrip()
    {
        String table = "complex_round_trip";
        createV3Table(table, NESTED_COLUMNS);
        assertUpdate("INSERT INTO " + table + " " + NESTED_ROWS, 4);

        assertQuery(format("SELECT id, %s, %s, %s FROM %s", json("items"), json("metrics"), json("info"), table), "VALUES "
                + "(1, '[[1,[\"a\",\"b\"]],[2,[]]]', '{\"m1\":[1,2],\"m2\":[3]}', '[10,[\"k1\",{\"s\":1.5}]]'), "
                + "(2, '[[3,null],null]', '{\"m3\":null}', '[20,[null,null]]'), "
                + "(3, '[]', '{}', '[null,null]'), "
                + "(4, NULL, NULL, NULL)");
        assertDataFilesUseFormat(table);
        dropTable(table);
    }

    @Test
    public void testNullsAtEveryLevel()
    {
        String table = "complex_nulls";
        createV3Table(table, NESTED_COLUMNS);
        assertUpdate("INSERT INTO " + table + " " + NESTED_ROWS, 4);

        assertQuery("SELECT id, items IS NULL, metrics IS NULL, info IS NULL FROM " + table,
                "VALUES (1, false, false, false), (2, false, false, false), (3, false, false, false), (4, true, true, true)");
        assertQuery("SELECT id FROM " + table + " WHERE cardinality(items) = 0", "VALUES 3");
        assertQuery("SELECT id FROM " + table + " WHERE element_at(items, 2) IS NULL AND cardinality(items) = 2", "VALUES 2");
        assertQuery("SELECT id FROM " + table + " WHERE cardinality(items) > 0 AND element_at(items, 1).tags IS NULL", "VALUES 2");
        assertQuery("SELECT id FROM " + table + " WHERE element_at(metrics, 'm3') IS NULL AND contains(map_keys(metrics), 'm3')", "VALUES 2");
        assertQuery("SELECT id FROM " + table + " WHERE info IS NOT NULL AND info.code IS NULL", "VALUES 3");
        assertQuery("SELECT id FROM " + table + " WHERE info.detail IS NOT NULL AND info.detail.k IS NULL", "VALUES 2");
        assertQuery("SELECT id, info.detail.scores['s'] FROM " + table + " WHERE id = 1", "VALUES (1, 1.5)");
        dropTable(table);
    }

    @Test
    public void testIsNullFilterOnComplexColumns()
    {
        String table = "complex_is_null_filter";
        createV3Table(table, NESTED_COLUMNS);
        assertUpdate("INSERT INTO " + table + " " + NESTED_ROWS, 4);
        assertQuery("SELECT id FROM " + table + " WHERE items IS NULL", "VALUES 4");
        assertQuery("SELECT id FROM " + table + " WHERE metrics IS NULL", "VALUES 4");
        assertQuery("SELECT id FROM " + table + " WHERE info IS NULL", "VALUES 4");
        assertQuery("SELECT id FROM " + table + " WHERE items IS NOT NULL", "VALUES 1, 2, 3");
        dropTable(table);
    }

    @Test
    public void testLargeCollections()
    {
        String table = "complex_large";
        createV3Table(table, "id bigint, arr array(bigint), m map(bigint, varchar)");
        assertUpdate(format("INSERT INTO %s SELECT x, sequence(x, x + 99), map(sequence(1, 20), transform(sequence(1, 20), i -> CAST(i * x AS varchar))) FROM (%s)",
                table, rangeOneTo(2000)), 2000);

        assertQuery("SELECT sum(cardinality(arr)), sum(element_at(arr, 50)), sum(cardinality(m)) FROM " + table, "VALUES (200000, 2099000, 40000)");
        assertQuery("SELECT m[7] FROM " + table + " WHERE id = 1000", "VALUES '7000'");
        assertQuery(format("SELECT count(*) FROM %s CROSS JOIN UNNEST(arr) AS t(v)", table), "VALUES 200000");
        assertQuery("SELECT id FROM " + table + " WHERE contains(arr, 150) ORDER BY id LIMIT 3", "VALUES 51, 52, 53");
        dropTable(table);
    }

    @Test
    public void testNestedFieldFilters()
    {
        String table = "complex_filters";
        createV3Table(table, NESTED_COLUMNS);
        assertUpdate("INSERT INTO " + table + " " + NESTED_ROWS, 4);

        assertQuery("SELECT id FROM " + table + " WHERE info.code = 20", "VALUES 2");
        assertQuery("SELECT id FROM " + table + " WHERE info.detail.k = 'k1'", "VALUES 1");
        assertQuery("SELECT id FROM " + table + " WHERE info.code > 5", "VALUES 1, 2");
        assertQuery("SELECT id FROM " + table + " WHERE any_match(items, r -> r.x = 3)", "VALUES 2");
        assertQuery("SELECT id FROM " + table + " WHERE contains(element_at(items, 1).tags, 'b')", "VALUES 1");
        assertQuery("SELECT id FROM " + table + " WHERE cardinality(element_at(metrics, 'm1')) = 2", "VALUES 1");
        assertQuery("SELECT info.code, cardinality(items) FROM " + table + " WHERE id <= 3", "VALUES (10, 2), (20, 2), (NULL, 0)");
        dropTable(table);
    }

    @Test
    public void testAddAndRenameStructField()
    {
        // Old files must be read through the evolved struct by field id.
        String table = "complex_struct_evolution";
        createV3Table(table, "id integer, s row(a integer, b varchar)");
        assertUpdate("INSERT INTO " + table + " VALUES (1, ROW(10, 'x'))", 1);

        loadTable(table).updateSchema().addColumn("s", "c", Types.DoubleType.get()).commit();
        assertUpdate("INSERT INTO " + table + " VALUES (2, ROW(20, 'y', 2.5))", 1);
        assertQuery("SELECT id, s.a, s.b, s.c FROM " + table, "VALUES (1, 10, 'x', NULL), (2, 20, 'y', 2.5)");

        loadTable(table).updateSchema().renameColumn("s.b", "b2").commit();
        assertQuery("SELECT id, s.a, s.b2 FROM " + table, "VALUES (1, 10, 'x'), (2, 20, 'y')");
        dropTable(table);
    }

    @Test
    public void testDropStructField()
    {
        String table = "complex_struct_drop";
        createV3Table(table, "id integer, s row(a integer, b varchar)");
        assertUpdate("INSERT INTO " + table + " VALUES (1, ROW(10, 'x'))", 1);
        loadTable(table).updateSchema().addColumn("s", "c", Types.DoubleType.get()).commit();
        assertUpdate("INSERT INTO " + table + " VALUES (2, ROW(20, 'y', 2.5))", 1);
        loadTable(table).updateSchema().deleteColumn("s.a").commit();
        assertQuery("SELECT id, s.b, s.c FROM " + table, "VALUES (1, 'x', NULL), (2, 'y', 2.5)");
        dropTable(table);
    }

    @Test
    public void testPartitionedByNestedField()
    {
        // The native writer resolves partition columns against top-level columns only.
        String table = "complex_nested_partition";
        createV3Table(table, "id integer, s row(region varchar, n integer)");
        loadTable(table).updateSpec().addField("s.region").commit();
        assertQueryFails("INSERT INTO " + table + " VALUES (1, ROW('us', 1)), (2, ROW('eu', 2))", "(?s).*Partition column not found in table schema: s.region.*");
        dropTable(table);
    }

    private static String json(String column)
    {
        return format("json_format(CAST(%s AS json))", column);
    }
}
