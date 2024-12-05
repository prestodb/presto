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

package com.facebook.presto.tests;

import com.facebook.presto.Session;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.String.format;

public abstract class AbstractTestNanQueries
        extends AbstractTestQueryFramework
{
    public static final String DOUBLE_NANS_TABLE_NAME = "double_nans_table";
    public static final String DOUBLE_NAN_FIRST_COLUMN = "_double_nan_first";
    public static final String DOUBLE_NAN_MIDDLE_COLUMN = "_double_nan_middle";
    public static final String DOUBLE_NAN_LAST_COLUMN = "_double_nan_last";

    public static final String REAL_NANS_TABLE_NAME = "real_nans_table";
    public static final String REAL_NAN_FIRST_COLUMN = "_real_nan_first";
    public static final String REAL_NAN_MIDDLE_COLUMN = "_real_nan_middle";
    public static final String REAL_NAN_LAST_COLUMN = "_real_nan_last";

    public static final String DISTINCT_TABLE_NAME = "distinct_nans_table";
    public static final String DOUBLE_DISTINCT1_COLUMN = "_double_distinct1";
    public static final String DOUBLE_DISTINCT2_COLUMN = "_double_distinct2";
    public static final String REAL_DISTINCT1_COLUMN = "_real_distinct1";
    public static final String REAL_DISTINCT2_COLUMN = "_real_distinct2";
    public static final String EXTRA_DISTINCT_COLUMN = "_extra_column";

    public static final String ARRAY_TABLE_NAME = "array_nans_table";
    public static final String SIMPLE_DOUBLE_ARRAY_COLUMN = "simple_double_array";
    public static final String SIMPLE_REAL_ARRAY_COLUMN = "simple_real_array";

    public static final String ARRAY_TABLE_NAME_NO_NULL = "array_nans_table_no_null";
    public static final String SIMPLE_DOUBLE_ARRAY_COLUMN_NO_NULL = "simple_double_array_no_null";
    public static final String SIMPLE_REAL_ARRAY_COLUMN_NO_NULL = "simple_real_array_no_null";

    public static final String MAP_TABLE_NAME = "map_nans_table";
    public static final String DOUBLE_MAP_COLUMN = "double_map";
    public static final String REAL_MAP_COLUMN = "real_map";

    @BeforeClass
    public void setup()
    {
        @Language("SQL") String createDoubleTableQuery = "" +
                "CREATE TABLE " + DOUBLE_NANS_TABLE_NAME + " AS " +
                "SELECT * FROM (VALUES " +
                "(nan(), 0.0, 1.0)," +
                " (0.0, nan(), 2.0)," +
                "( infinity(), 3.0,  0.0)," +
                "( -4.0, 2.0, nan())) as t (_double_nan_first, _double_nan_middle, _double_nan_last)";
        assertUpdate(createDoubleTableQuery, 4);

        @Language("SQL") String createFloatTableQuery = "" +
                "CREATE TABLE " + REAL_NANS_TABLE_NAME + " AS " +
                "SELECT * FROM (VALUES " +
                "(CAST(nan() as REAL), CAST(0 AS REAL), CAST(1 AS REAL))," +
                " (CAST(0 as REAL), CAST(nan() AS REAL), CAST(2 AS REAL))," +
                "(CAST(infinity() AS REAL), CAST(3 AS REAL),  CAST(0 AS REAL))," +
                "( CAST(-4 AS REAL), CAST(2 AS REAL), CAST(nan() AS REAL))) as t (_real_nan_first, _real_nan_middle, _real_nan_last)";
        assertUpdate(createFloatTableQuery, 4);

        @Language("SQL") String createDistinctTableQuery = "" +
                "CREATE TABLE " + DISTINCT_TABLE_NAME + " AS " +
                "SELECT * FROM (VALUES " +
                "(nan(), DOUBLE '0.0', CAST(nan() as REAL), REAL '0.0', 'a'), " +
                " (DOUBLE '0.0', nan(), REAL '0.0', CAST(nan() AS REAL), 'b'), " +
                "(null, null, null, null, 'c'), " +
                "(nan(), nan(), CAST(nan() as REAL), CAST(nan() as REAL), 'd'), " +
                "(3.0, 3.0, REAL '3.0', REAL '3.0', 'e'), " +
                "(0.0, 0.0, REAL '0.0', REAL '0.0', 'f'), " +
                "(null, null, null, null, 'g'))" +
                "AS t (" + DOUBLE_DISTINCT1_COLUMN + ", " + DOUBLE_DISTINCT2_COLUMN + ", " + REAL_DISTINCT1_COLUMN + ", " + REAL_DISTINCT2_COLUMN + ", " + EXTRA_DISTINCT_COLUMN + ")";
        assertUpdate(createDistinctTableQuery, 7);

        @Language("SQL") String createArrayTableQuery = "" +
                "CREATE TABLE " + ARRAY_TABLE_NAME + " AS " +
                "SELECT * FROM (VALUES " +
                "(ARRAY[nan(), DOUBLE '0', DOUBLE '1', DOUBLE '-1'], ARRAY[cast(nan() AS REAL), REAL '0', REAL '1', REAL '-1']), " +
                "(ARRAY[ DOUBLE '0', nan(), DOUBLE '1', DOUBLE '-1'], ARRAY[REAL '0', CAST(nan() AS REAL),  REAL '1', REAL '-1']), " +
                "(ARRAY[ DOUBLE '0',  DOUBLE '1', DOUBLE '-1', nan()], ARRAY[REAL '0', REAL '1', REAL '-1',  CAST(nan() AS REAL)]), " +
                "(ARRAY[null, nan(), DOUBLE '200'], ARRAY[null, CAST(nan() AS REAL), REAL '200']), " +
                "(null, null), " +
                "(ARRAY[nan(), nan()], ARRAY[CAST(nan() AS REAL), CAST(nan() AS REAL)]), " +
                "(ARRAY[DOUBLE '0', DOUBLE '1', nan(), DOUBLE '-1', nan(), DOUBLE '1', DOUBLE '1', DOUBLE'0'], ARRAY [REAL '0', REAL '1', CAST(nan() AS REAL), REAL '-1', CAST(nan() AS REAL), REAL '1', REAL '1', REAL '0'])) " +
                "AS t (" + SIMPLE_DOUBLE_ARRAY_COLUMN + ", " + SIMPLE_REAL_ARRAY_COLUMN + ")";

        @Language("SQL") String createArrayTableNoNullQuery = "" +
                "CREATE TABLE " + ARRAY_TABLE_NAME_NO_NULL + " AS " +
                "SELECT * FROM (VALUES " +
                "(ARRAY[nan(), DOUBLE '0', DOUBLE '1', DOUBLE '-1'], ARRAY[cast(nan() AS REAL), REAL '0', REAL '1', REAL '-1']), " +
                "(ARRAY[ DOUBLE '0', nan(), DOUBLE '1', DOUBLE '-1'], ARRAY[REAL '0', CAST(nan() AS REAL),  REAL '1', REAL '-1']), " +
                "(ARRAY[ DOUBLE '0',  DOUBLE '1', DOUBLE '-1', nan()], ARRAY[REAL '0', REAL '1', REAL '-1',  CAST(nan() AS REAL)]), " +
                "(ARRAY[null, nan(), DOUBLE '200'], ARRAY[null, CAST(nan() AS REAL), REAL '200']), " +
                "(ARRAY[nan(), nan()], ARRAY[CAST(nan() AS REAL), CAST(nan() AS REAL)]), " +
                "(ARRAY[DOUBLE '0', DOUBLE '1', nan(), DOUBLE '-1', nan(), DOUBLE '1', DOUBLE '1', DOUBLE'0'], ARRAY [REAL '0', REAL '1', CAST(nan() AS REAL), REAL '-1', CAST(nan() AS REAL), REAL '1', REAL '1', REAL '0'])) " +
                "AS t (" + SIMPLE_DOUBLE_ARRAY_COLUMN_NO_NULL + ", " + SIMPLE_REAL_ARRAY_COLUMN_NO_NULL + ")";

        assertUpdate(createArrayTableQuery, 7);
        assertUpdate(createArrayTableNoNullQuery, 6);

        @Language("SQL") String createMapTableQuery = "" +
                "CREATE TABLE " + MAP_TABLE_NAME + " AS " +
                "SELECT * FROM (VALUES " +
                "(MAP(ARRAY[nan(), 1, 2], ARRAY[nan(), 100, 200]), MAP(ARRAY[CAST(nan() AS REAL), REAL '1', REAL '2'], ARRAY[CAST(nan() AS REAL), REAL '100', REAL '200']))," +
                "(MAP(ARRAY[2, nan(), 1], ARRAY[200, nan(), 100]), MAP(ARRAY[REAL '2', CAST(nan() AS REAL), REAL '1'], ARRAY[REAL '200', CAST(nan() AS REAL), REAL '100'])), " +
                "(MAP(ARRAY[2, 1, nan()], ARRAY[200, 100, nan()]), MAP(ARRAY[REAL '2', REAL '1', CAST(nan() AS REAL)], ARRAY[REAL '200', REAL '100', CAST(nan() AS REAL)]))) " +
                "AS t(" + DOUBLE_MAP_COLUMN + ", " + REAL_MAP_COLUMN + ")";
        assertUpdate(createMapTableQuery, 3);
    }

    @AfterClass
    public void tearDown()
    {
        assertUpdate("DROP TABLE " + DOUBLE_NANS_TABLE_NAME);
        assertUpdate("DROP TABLE " + REAL_NANS_TABLE_NAME);
        assertUpdate("DROP TABLE " + DISTINCT_TABLE_NAME);
        assertUpdate("DROP TABLE " + ARRAY_TABLE_NAME);
        assertUpdate("DROP TABLE " + MAP_TABLE_NAME);
    }

    @Test
    public void testDoubleLessThan()
    {
        assertQuery("SELECT nan() < 1.0", "SELECT false");
        assertQuery("SELECT infinity() < nan()", "SELECT true");
        assertQuery("SELECT nan() < infinity()", "SELECT false");
        assertQuery("SELECT nan() < nan()", "SELECT false");
        assertQuery(format("SELECT _double_nan_first < nan() from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (true), (true), (true))");
        assertQuery(format("SELECT nan() < _double_nan_first from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (false), (false), (false))");
    }

    @Test
    public void testRealLessThan()
    {
        assertQuery(format("SELECT _real_nan_first < CAST(nan() AS REAL) from %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (true), (true), (true))");
        assertQuery(format("SELECT CAST(nan() AS REAL) < _real_nan_first from %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (false), (false), (false))");
    }

    @Test
    public void testDoubleGreaterThan()
    {
        assertQuery("SELECT nan() > 1.0", "SELECT true");
        assertQuery("SELECT infinity() > nan()", "SELECT false");
        assertQuery("SELECT nan() > infinity()", "SELECT true");
        assertQuery("SELECT nan() > nan()", "SELECT false");
        assertQuery(format("SELECT _double_nan_first > nan() from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (false), (false), (false))");
        assertQuery(format("SELECT nan() > _double_nan_first from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (true), (true), (true))");
    }

    @Test
    public void testRealGreaterThan()
    {
        assertQuery(format("SELECT _real_nan_first > cast(nan() AS REAL) from %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (false), (false), (false))");
        assertQuery(format("SELECT CAST(nan() AS REAL)> _real_nan_first from %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (true), (true), (true))");
    }

    @Test
    public void testDoubleLessThanOrEqualTo()
    {
        assertQuery("SELECT nan() <= 1.0", "SELECT false");
        assertQuery("SELECT infinity() <= nan()", "SELECT true");
        assertQuery("SELECT nan() <= infinity()", "SELECT false");
        assertQuery("SELECT nan() <= nan()", "SELECT true");
        assertQuery(format("SELECT _double_nan_first <= nan() from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (true), (true), (true))");
        assertQuery(format("SELECT nan() <= _double_nan_first from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
    }

    @Test
    public void testRealLessThanOrEqualTo()
    {
        assertQuery(format("SELECT _real_nan_first <= CAST(nan() AS REAL) from %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (true), (true), (true))");
        assertQuery(format("SELECT CAST(nan() AS REAL) <= _real_nan_first from %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
    }

    @Test
    public void testDoubleGreaterThanOrEqualTo()
    {
        assertQuery("SELECT nan() >= 1.0", "SELECT true");
        assertQuery("SELECT infinity() >= nan()", "SELECT false");
        assertQuery("SELECT nan() >= infinity()", "SELECT true");
        assertQuery("SELECT nan() >= nan()", "SELECT true");
        assertQuery(format("SELECT _double_nan_first >= nan() from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
        assertQuery(format("SELECT nan() >= _double_nan_first from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (true), (true), (true))");
    }

    @Test
    public void testRealGreaterThanOrEqualTo()
    {
        assertQuery(format("SELECT _real_nan_first >= CAST(nan() AS REAL) from %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
        assertQuery(format("SELECT CAST(nan() AS REAL) >= _real_nan_first from %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (true), (true), (true))");
    }

    @Test
    public void testDoubleEquals()
    {
        assertQuery("SELECT nan() = nan()", "SELECT true");
        assertQuery("SELECT nan() = 3", "SELECT false");
        assertQuery(format("SELECT _double_nan_first = nan() from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
        assertQuery(format("SELECT nan() = _double_nan_first from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
    }

    @Test
    public void testRealEquals()
    {
        assertQuery(format("SELECT _real_nan_first = CAST(nan() AS REAL) from %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
        assertQuery(format("SELECT CAST(nan() AS REAL) = _real_nan_first from %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
    }

    @Test
    public void testDoubleNotEquals()
    {
        assertQuery("SELECT nan() <> nan()", "SELECT false");
        assertQuery("SELECT nan() <> 3", "SELECT true");
        assertQuery(format("SELECT _double_nan_first <> nan() from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (true), (true), (true))");
        assertQuery(format("SELECT nan() <> _double_nan_first from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (true), (true), (true))");
    }

    @Test
    public void testRealNotEquals()
    {
        assertQuery(format("SELECT _real_nan_first <> CAST(nan() AS REAL) from %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (true), (true), (true))");
        assertQuery(format("SELECT CAST(nan() AS REAL) <> _real_nan_first from %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (true), (true), (true))");
    }

    @Test
    public void testDoubleBetween()
    {
        assertQuery(format("SELECT nan() BETWEEN -infinity() AND _double_nan_first FROM %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
        assertQuery(format("SELECT _double_nan_first BETWEEN -infinity() AND nan() FROM %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES(true), (true), (true), (true))");
    }

    @Test
    public void testRealBetween()
    {
        assertQuery(format("SELECT CAST(nan() AS REAL) BETWEEN CAST(-infinity() AS REAL) AND _real_nan_first FROM %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
        assertQuery(format("SELECT _real_nan_first BETWEEN CAST(-infinity() AS REAL) AND cast(nan() AS REAL) FROM %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES(true), (true), (true), (true))");
    }

    @Test
    public void testDoubleIn()
    {
        assertQuery(format("SELECT nan() IN (1, 2, _double_nan_first) FROM %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
        assertQuery(format("SELECT _double_nan_first IN (nan(), 0, 6)FROM %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES(true), (true), (false), (false))");
    }

    @Test
    public void testRealIn()
    {
        assertQuery(format("SELECT CAST(nan() as REAL) IN (REAL '1', REAL '2', _real_nan_first) FROM %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
        assertQuery(format("SELECT _real_nan_first IN (CAST(nan() as REAL), REAL '0', REAL '6')FROM %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES(true), (true), (false), (false))");
    }

    @Test
    public void testDoubleNotIn()
    {
        assertQuery(format("SELECT nan() NOT IN (1, 2, _double_nan_first) FROM %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (true), (true), (true))");
        assertQuery(format("SELECT _double_nan_first NOT IN (nan(), 0, 6)FROM %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES(false), (false), (true), (true))");
    }

    @Test
    public void testRealNotIn()
    {
        assertQuery(format("SELECT CAST(nan() as REAL) NOT IN (REAL '1', REAL '2', _real_nan_first) FROM %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (true), (true), (true))");
        assertQuery(format("SELECT _real_nan_first NOT IN (CAST(nan() as REAL), 0, 6)FROM %s", REAL_NANS_TABLE_NAME), "SELECT * FROM (VALUES(false), (false), (true), (true))");
    }

    @Test
    public void testSelectDistinct()
    {
        assertQueryWithSameQueryRunner(format("SELECT DISTINCT _double_distinct1 FROM %s", DISTINCT_TABLE_NAME), "SELECT * FROM (VALUES(nan()), (0.0), (null), (3.0))");
        assertQueryWithSameQueryRunner(format("SELECT DISTINCT _real_distinct1 FROM %s", DISTINCT_TABLE_NAME), "SELECT * FROM (VALUES (CAST(nan() AS REAL)), (REAL '0.0'), (null), (REAL '3.0'))");
    }

    @Test
    public void testSelectDistinctAggregations()
    {
        Session markDistinct = Session.builder(getQueryRunner().getDefaultSession()).setSystemProperty("use_mark_distinct", "true").build();
        assertQuery(markDistinct, format("SELECT count(DISTINCT _double_distinct1), count(_double_distinct1), count(*) FROM %s", DISTINCT_TABLE_NAME), "SELECT 3, 5, 7");
        assertQuery(markDistinct, format("SELECT count(DISTINCT _real_distinct1), count(_real_distinct1), count(*) FROM %s", DISTINCT_TABLE_NAME), "SELECT 3, 5, 7");

        Session noMarkDistinct = Session.builder(getQueryRunner().getDefaultSession()).setSystemProperty("use_mark_distinct", "false").build();
        assertQuery(noMarkDistinct, format("SELECT count(DISTINCT _double_distinct1), count(_double_distinct1), count(*) FROM %s", DISTINCT_TABLE_NAME), "SELECT 3, 5, 7");
        assertQuery(noMarkDistinct, format("SELECT count(DISTINCT _real_distinct1), count(_real_distinct1), count(*) FROM %s", DISTINCT_TABLE_NAME), "SELECT 3, 5, 7");
    }

    @Test
    public void testGroupBy()
    {
        assertQueryWithSameQueryRunner(format("SELECT _double_distinct1, count(*) FROM %s GROUP BY _double_distinct1", DISTINCT_TABLE_NAME), "SELECT * FROM (VALUES (nan(), BIGINT '2'), (0.0, BIGINT '2'), (null, BIGINT '2'), (3.0, BIGINT '1'))");
        assertQueryWithSameQueryRunner(format("SELECT _real_distinct1, count(*) FROM %s GROUP BY _real_distinct1", DISTINCT_TABLE_NAME), "SELECT * FROM (VALUES (CAST(nan() as REAL), BIGINT '2'), (REAL '0.0', BIGINT '2'), (null, BIGINT '2'), (REAL '3.0', BIGINT '1'))");
    }

    @Test
    public void testMin()
    {
        assertQueryWithSameQueryRunner(format("SELECT min(%s), min(%s), min(%s) FROM %s", DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN, DOUBLE_NANS_TABLE_NAME), "SELECT DOUBLE '-4.0', DOUBLE '0.0', DOUBLE '0.0'");
        assertQueryWithSameQueryRunner(format("SELECT min(%s), min(%s), min(%s) FROM %s", REAL_NAN_FIRST_COLUMN, REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN, REAL_NANS_TABLE_NAME), "SELECT REAL '-4.0', REAL '0.0', REAL '0.0'");
    }

    @Test
    public void testDoubleArrayMinAgg()
    {
        assertQueryWithSameQueryRunner(format("SELECT min(%s) FROM %s WHERE none_match(%s, x -> x IS NULL)", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME, SIMPLE_DOUBLE_ARRAY_COLUMN), "SELECT ARRAY[0, 1, -1, nan()]");
    }

    @Test
    public void testRealArrayMinAgg()
    {
        assertQueryWithSameQueryRunner(format("SELECT min(%s) FROM %s WHERE none_match(%s, x -> x IS NULL)", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME, SIMPLE_REAL_ARRAY_COLUMN), "SELECT ARRAY[REAL'0', REAL '1', REAL '-1', CAST(nan() AS REAL)]");
    }

    @Test
    public void testDoubleArrayMaxAgg()
    {
        assertQueryWithSameQueryRunner(format("SELECT max(%s) FROM %s WHERE none_match(%s, x -> x IS NULL)", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME, SIMPLE_DOUBLE_ARRAY_COLUMN), "SELECT ARRAY[nan(), nan()]");
    }

    @Test
    public void testRealArrayMaxAgg()
    {
        assertQueryWithSameQueryRunner(format("SELECT max(%s) FROM %s WHERE none_match(%s, x -> x IS NULL)", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME, SIMPLE_REAL_ARRAY_COLUMN), "SELECT ARRAY[CAST(nan() AS REAL), CAST(nan() AS REAL)]");
    }

    @Test
    public void testMax()
    {
        assertQueryWithSameQueryRunner(format("SELECT max(%s), max(%s), max(%s) FROM %s", DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN, DOUBLE_NANS_TABLE_NAME), "SELECT nan(), nan(), nan()");
        assertQueryWithSameQueryRunner(format("SELECT max(%s), max(%s), max(%s) FROM %s", REAL_NAN_FIRST_COLUMN, REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN, REAL_NANS_TABLE_NAME), "SELECT CAST(nan() AS REAL), CAST(nan() AS REAL), CAST(nan() AS REAL)");
    }

    @Test
    public void testMinN()
    {
        assertQueryWithSameQueryRunner(format("SELECT min(%s, 2), min(%s, 2), min(%s, 2) FROM %s", DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN, DOUBLE_NANS_TABLE_NAME), "SELECT ARRAY [DOUBLE '-4.0', DOUBLE '0.0'], Array [DOUBLE '0.0', DOUBLE '2.0'], ARRAY[DOUBLE '0.0', DOUBLE '1.0']");
        assertQueryWithSameQueryRunner(format("SELECT min(%s, 2), min(%s, 2), min(%s, 2) FROM %s", REAL_NAN_FIRST_COLUMN, REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN, REAL_NANS_TABLE_NAME), "SELECT ARRAY [REAL '-4.0', REAL '0.0'], Array [REAL '0.0', REAL '2.0'], ARRAY[REAL '0.0', REAL '1.0']");
    }

    @Test
    public void testMaxN()
    {
        assertQueryWithSameQueryRunner(format("SELECT max(%s, 2), max(%s, 2), max(%s, 2) FROM %s", DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN, DOUBLE_NANS_TABLE_NAME), "SELECT ARRAY [nan(), infinity()], Array [nan(), 3.0], ARRAY[nan(), 2.0]");
        assertQueryWithSameQueryRunner(format("SELECT max(%s, 2), max(%s, 2), max(%s, 2) FROM %s", REAL_NAN_FIRST_COLUMN, REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN, REAL_NANS_TABLE_NAME), "SELECT ARRAY [CAST(nan() AS REAL), CAST(infinity() AS REAL)], Array [CAST(nan() AS REAL), REAL '3.0'], ARRAY[CAST(nan() AS REAL), REAL '2.0']");
    }

    @Test
    public void testMinBy()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT min_by(%s, %s), min_by(%s, %s), min_by(%s, %s) FROM %s",
                        DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NAN_MIDDLE_COLUMN,
                        DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN,
                        DOUBLE_NAN_LAST_COLUMN, DOUBLE_NAN_FIRST_COLUMN,
                        DOUBLE_NANS_TABLE_NAME),
                "SELECT nan(), DOUBLE '3.0', nan()");
        assertQueryWithSameQueryRunner(
                format("SELECT min_by(%s, %s), min_by(%s, %s), min_by(%s, %s) FROM %s",
                        REAL_NAN_FIRST_COLUMN, REAL_NAN_MIDDLE_COLUMN,
                        REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN,
                        REAL_NAN_LAST_COLUMN, REAL_NAN_FIRST_COLUMN,
                        REAL_NANS_TABLE_NAME),
                "SELECT CAST(nan() AS REAL), REAL'3.0', CAST(nan() AS REAL)");
    }

    @Test
    public void testMaxBy()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT max_by(%s, %s), max_by(%s, %s), max_by(%s, %s) FROM %s",
                        DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NAN_MIDDLE_COLUMN,
                        DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN,
                        DOUBLE_NAN_LAST_COLUMN, DOUBLE_NAN_FIRST_COLUMN,
                        DOUBLE_NANS_TABLE_NAME),
                "SELECT DOUBLE '0.0', DOUBLE '2.0', DOUBLE '1.0'");
        assertQueryWithSameQueryRunner(
                format("SELECT max_by(%s, %s), max_by(%s, %s), max_by(%s, %s) FROM %s",
                        REAL_NAN_FIRST_COLUMN, REAL_NAN_MIDDLE_COLUMN,
                        REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN,
                        REAL_NAN_LAST_COLUMN, REAL_NAN_FIRST_COLUMN,
                        REAL_NANS_TABLE_NAME),
                "SELECT REAL '0.0', REAL'2.0', REAL '1.0'");
    }

    @Test
    public void testMinByN()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT min_by(%s, %s, 2), min_by(%s, %s, 2), min_by(%s, %s, 2) FROM %s",
                        DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NAN_MIDDLE_COLUMN,
                        DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN,
                        DOUBLE_NAN_LAST_COLUMN, DOUBLE_NAN_FIRST_COLUMN,
                        DOUBLE_NANS_TABLE_NAME),
                "SELECT ARRAY[nan(), DOUBLE '-4.0'], ARRAY[DOUBLE '3.0', DOUBLE '0.0'], ARRAY[nan(), DOUBLE '2.0']");
        assertQueryWithSameQueryRunner(
                format("SELECT min_by(%s, %s, 2), min_by(%s, %s, 2), min_by(%s, %s, 2) FROM %s",
                        REAL_NAN_FIRST_COLUMN, REAL_NAN_MIDDLE_COLUMN,
                        REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN,
                        REAL_NAN_LAST_COLUMN, REAL_NAN_FIRST_COLUMN,
                        REAL_NANS_TABLE_NAME),
                "SELECT ARRAY[CAST(nan() AS REAL), REAL '-4.0'], ARRAY[REAL '3.0', REAL '0.0'], ARRAY[CAST(nan() AS REAL), REAL '2.0']");
    }

    @Test
    public void testMaxByN()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT max_by(%s, %s, 2), max_by(%s, %s, 2), max_by(%s, %s, 2) FROM %s",
                        DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NAN_MIDDLE_COLUMN,
                        DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN,
                        DOUBLE_NAN_LAST_COLUMN, DOUBLE_NAN_FIRST_COLUMN,
                        DOUBLE_NANS_TABLE_NAME),
                "SELECT ARRAY [DOUBLE '0.0', infinity()], ARRAY[DOUBLE '2.0', nan()], ARRAY[DOUBLE '1.0', DOUBLE '0.0']");
        assertQueryWithSameQueryRunner(
                format("SELECT max_by(%s, %s, 2), max_by(%s, %s, 2), max_by(%s, %s, 2) FROM %s",
                        REAL_NAN_FIRST_COLUMN, REAL_NAN_MIDDLE_COLUMN,
                        REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN,
                        REAL_NAN_LAST_COLUMN, REAL_NAN_FIRST_COLUMN,
                        REAL_NANS_TABLE_NAME),
                "SELECT ARRAY [REAL '0.0', CAST(infinity() AS REAL)], ARRAY[REAL '2.0', CAST(nan() AS REAL)], ARRAY[REAL '1.0', REAL '0.0']");
    }

    @Test
    public void testGreatest()
    {
        assertQueryWithSameQueryRunner("SELECT GREATEST(1.5E0, nan())", "SELECT nan()");
        assertQueryWithSameQueryRunner(
                format("SELECT greatest(%s, %s, %s) FROM %s", DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN, DOUBLE_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES (nan()), (nan()), (infinity()), (nan()))");
        assertQueryWithSameQueryRunner(
                format("SELECT greatest(%s, %s, %s) FROM %s", REAL_NAN_FIRST_COLUMN, REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN, REAL_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES (CAST(nan() AS REAL)), (CAST(nan() AS REAL)), CAST(infinity() AS REAL), (CAST(nan() AS REAL)))");
    }

    @Test
    public void testLeast()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT least(%s, %s, %s) FROM %s", DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN, DOUBLE_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES (DOUBLE '0.0'), (DOUBLE '0.0'), (DOUBLE '0.0'), (DOUBLE '-4.0'))");
        assertQueryWithSameQueryRunner(
                format("SELECT least(%s, %s, %s) FROM %s", REAL_NAN_FIRST_COLUMN, REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN, REAL_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES (REAL '0.0'), (REAL '0.0'), (REAL'0.0'), REAL'-4.0')");
    }

    @Test
    public void testDoubleSetAgg()
    {
        assertQueryWithSameQueryRunner(format("SELECT set_agg(%s), set_agg(%s) FROM %s", DOUBLE_DISTINCT1_COLUMN, DOUBLE_DISTINCT2_COLUMN, DISTINCT_TABLE_NAME), "SELECT ARRAY[nan(), 0.0, null, 3.0], ARRAY[0, nan(), null, 3.0]");
    }

    @Test
    public void testRealSetAgg()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT set_agg(%s), set_agg(%s) FROM %s", REAL_DISTINCT1_COLUMN, REAL_DISTINCT2_COLUMN, DISTINCT_TABLE_NAME),
                "SELECT ARRAY[cast(nan() as REAL), 0.0, null, REAL '3.0'], ARRAY[REAL '0.0', cast(nan() as REAL), null, REAL '3.0']");
    }

    @Test
    public void testDoubleSetUnion()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(set_union(%s)) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT ARRAY[-1, 0, 1, 200, nan(), null]");
    }

    @Test
    public void testRealSetUnion()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(set_union(%s)) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT ARRAY[REAL '-1', REAL '0', REAL '1', REAL '200', CAST(nan() AS REAL), null]");
    }

    @Test
    public void testDoubleHistogram()
    {
        assertQueryWithSameQueryRunner(format("SELECT histogram(%s), histogram(%s) FROM %s", DOUBLE_DISTINCT1_COLUMN, DOUBLE_DISTINCT2_COLUMN, DISTINCT_TABLE_NAME),
                "SELECT MAP(ARRAY[nan(), 0.0, 3.0], ARRAY[BIGINT '2', BIGINT '2', BIGINT '1']), MAP(ARRAY[0.0, nan(),  3.0], ARRAY[BIGINT '2', BIGINT '2', BIGINT '1'])");
    }

    @Test
    public void testRealHistogram()
    {
        assertQueryWithSameQueryRunner(format("SELECT histogram(%s), histogram(%s) FROM %s", REAL_DISTINCT1_COLUMN, REAL_DISTINCT2_COLUMN, DISTINCT_TABLE_NAME),
                "SELECT MAP(ARRAY[CAST(nan() AS REAL), REAL '0.0', 3.0], ARRAY[BIGINT '2', BIGINT '2', BIGINT '1']), MAP(ARRAY[REAL '0.0', CAST(nan() AS REAL), REAL '3.0'], ARRAY[BIGINT '2', BIGINT '2', BIGINT '1'])");
    }

    @Test
    public void testDoubleMapAgg()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(map_keys(map_agg(%1$s, %3$s))), array_sort(map_keys(map_agg(%2$s, %3$s)))  FROM %4$s WHERE %1$s IS NOT NULL AND %2$s IS NOT NULL",
                        DOUBLE_DISTINCT1_COLUMN, DOUBLE_DISTINCT2_COLUMN, EXTRA_DISTINCT_COLUMN, DISTINCT_TABLE_NAME),
                "SELECT ARRAY[0.0, 3.0, nan()], ARRAY[0.0, 3.0, nan()]");
    }

    @Test
    public void testRealMapAgg()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(map_keys(map_agg(%1$s, %3$s))), array_sort(map_keys(map_agg(%2$s, %3$s)))  FROM %4$s WHERE %1$s IS NOT NULL AND %2$s IS NOT NULL",
                        REAL_DISTINCT1_COLUMN, REAL_DISTINCT2_COLUMN, EXTRA_DISTINCT_COLUMN, DISTINCT_TABLE_NAME),
                "SELECT ARRAY[REAL '0.0', REAL '3.0', CAST(nan() AS REAL)], ARRAY[REAL '0.0', REAL '3.0', CAST(nan() AS REAL)]");
    }

    @Test
    public void testDoubleMapUnion()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(map_keys(map_union(%s))) FROM %s", DOUBLE_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT ARRAY[1, 2, nan()]");
    }

    @Test
    public void testRealMapUnion()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(map_keys(map_union(%s))) FROM %s", REAL_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT ARRAY[REAL '1', REAL '2', CAST(nan() AS REAL)]");
    }

    @Test
    public void testDoubleMapUnionSum()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT map_union_sum(%s) FROM %s", DOUBLE_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT MAP(ARRAY[1, 2, nan()], ARRAY[300, 600, nan()])");
    }

    @Test
    public void testRealMapUnionSum()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT map_union_sum(%s) FROM %s", REAL_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT MAP(ARRAY[REAL '1', REAL '2', CAST(nan() AS REAL)], ARRAY[REAL '300', REAL '600', CAST(nan() AS REAL)])");
    }

    @Test
    public void testDoubleMultimapAgg()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT multimap_agg(%1$s, %3$s), multimap_agg(%2$s, %3$s)  FROM %4$s WHERE %1$s IS NOT NULL AND %2$s IS NOT NULL",
                        DOUBLE_DISTINCT1_COLUMN, DOUBLE_DISTINCT2_COLUMN, EXTRA_DISTINCT_COLUMN, DISTINCT_TABLE_NAME),
                "SELECT MAP(ARRAY[nan(), 0.0, 3.0], ARRAY[ARRAY['a', 'd'], ARRAY['b', 'f'], ARRAY['e']]), MAP(ARRAY[0.0, nan(), 3.0], ARRAY[ARRAY['a', 'f'], ARRAY['b', 'd'], ARRAY['e']])");
    }

    @Test
    public void testRealMultimapAgg()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT multimap_agg(%1$s, %3$s), multimap_agg(%2$s, %3$s)  FROM %4$s WHERE %1$s IS NOT NULL AND %2$s IS NOT NULL",
                        REAL_DISTINCT1_COLUMN, REAL_DISTINCT2_COLUMN, EXTRA_DISTINCT_COLUMN, DISTINCT_TABLE_NAME),
                "SELECT " +
                        "MAP(ARRAY[CAST(nan() AS REAL), REAL '0.0', REAL '3.0'], ARRAY[ARRAY['a', 'd'], ARRAY['b', 'f'], ARRAY['e']]), " +
                        "MAP(ARRAY[REAL '0.0', CAST(nan() AS REAL), REAL '3.0'], ARRAY[ARRAY['a', 'f'], ARRAY['b', 'd'], ARRAY['e']])");
    }

    @Test
    public void testDoubleAllMatch()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT all_match(%s, x -> x = nan()) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (false), (false), (false), (null), (false), (true), (false))");
    }

    @Test
    public void testRealAllMatch()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT all_match(%s, x -> x = nan()) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (false), (false), (false), (null), (false), (true), (false))");
    }

    @Test
    public void testDoubleAnyMatch()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT any_match(%s, x -> x = nan()) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (true), (true), (true), (null), (true), (true), (true))");
    }

    @Test
    public void testRealAnyMatch()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT any_match(%s, x -> x = nan()) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (true), (true), (true), (null), (true), (true), (true))");
    }

    @Test
    public void testDoubleArrayDistinct()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_distinct(%s)) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[-1, 0, 1, nan()]), (ARRAY[-1, 0, 1, nan()]), (ARRAY[-1, 0, 1, nan()]), (ARRAY[200, nan(), null]), (null), (ARRAY[nan()]), (ARRAY[-1, 0, 1, nan()]))");
    }

    @Test
    public void testRealArrayDistinct()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_distinct(%s)) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '200', CAST(nan() AS REAL), null]), (null), (ARRAY[CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]))");
    }

    @Test
    public void testDoubleArrayDuplicates()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_duplicates(%s)) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[]), (ARRAY[]), (ARRAY[]), (ARRAY[]), (null), (ARRAY[nan()]), (ARRAY[0, 1, nan()]))");
    }

    @Test
    public void testRealArrayDuplicates()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_duplicates(%s)) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[]), (ARRAY[]), (ARRAY[]), (ARRAY[]), (null), (ARRAY[CAST(nan() AS REAL)]), (ARRAY[REAL '0', REAL '1', CAST(nan() AS REAL)]))");
    }

    @Test
    public void testDoubleArrayExcept()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_except(%s, ARRAY[nan()])) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1']), (ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1']), (ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1']), (ARRAY[DOUBLE '200', null]), (null), (ARRAY[]), (ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1']))");
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_except(ARRAY[nan()], %s)) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[]), (ARRAY[]), (ARRAY[]), (ARRAY[]), (null), (ARRAY[]), (ARRAY[]))");
    }

    @Test
    public void testRealArrayExcept()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_except(%s, ARRAY[CAST(nan() AS REAL)])) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM " +
                        "(VALUES (ARRAY[REAL '-1', REAL '0', REAL '1']), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1']), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1']), (ARRAY[REAL '200', null]), " +
                        "(null), " +
                        "(ARRAY[]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1']))");
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_except(ARRAY[cast(nan() AS REAL)], %s)) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[]), (ARRAY[]), (ARRAY[]), (ARRAY[]), (null), (ARRAY[]), (ARRAY[]))");
    }

    @Test
    public void testDoubleArrayFrequency()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_frequency(filter(%s, x -> x IS NOT NULL)) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(map(ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1', nan()], ARRAY[1, 1, 1, 1])), " +
                        "(map(ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1', nan()], ARRAY[1, 1, 1, 1])), " +
                        "(map(ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1', nan()], ARRAY[1, 1, 1, 1])), " +
                        "(map(ARRAY[DOUBLE '200', nan()], ARRAY[1, 1])), " +
                        "(null), " +
                        "(map(ARRAY[nan()], ARRAY[2])), " +
                        "(map(ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1', nan()], ARRAY[1, 2, 3, 2])))");
    }

    @Test
    public void testRealArrayFrequency()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_frequency(filter(%s, x -> x IS NOT NULL)) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(map(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)], ARRAY[1, 1, 1, 1])), " +
                        "(map(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)], ARRAY[1, 1, 1, 1])), " +
                        "(map(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)], ARRAY[1, 1, 1, 1])), " +
                        "(map(ARRAY[REAL '200', CAST(nan() AS REAL)], ARRAY[1, 1])), " +
                        "(null), " +
                        "(map(ARRAY[CAST(nan() AS REAL)], ARRAY[2])), " +
                        "(map(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)], ARRAY[1, 2, 3, 2])))");
    }

    @Test
    public void testDoubleArrayHasDuplicates()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_has_duplicates(%s) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (false), (false), (false), (false), (null), (true), (true))");
    }

    @Test
    public void testRealArrayHasDuplicates()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_has_duplicates(%s) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (false), (false), (false), (false), (null), (true), (true))");
    }

    @Test
    public void testDoubleArrayIntersect1()
    {
        // Testing the two argument function signature
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_intersect(%s, ARRAY[nan()])) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[nan()]), (ARRAY[nan()]), (ARRAY[nan()]), (ARRAY[nan()]), (null), (ARRAY[nan()]), (ARRAY[nan()]))");
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_intersect(ARRAY[nan()], %s)) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[nan()]), (ARRAY[nan()]), (ARRAY[nan()]), (ARRAY[nan()]), (null), (ARRAY[nan()]), (ARRAY[nan()]))");
    }

    @Test
    public void testRealArrayIntersect()
    {
        // Testing the two argument function signature
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_intersect(%s, ARRAY[nan()])) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[nan()]), (ARRAY[nan()]), (ARRAY[nan()]), (ARRAY[nan()]), (null), (ARRAY[nan()]), (ARRAY[nan()]))");
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_intersect(ARRAY[nan()], %s)) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[nan()]), (ARRAY[nan()]), (ARRAY[nan()]), (ARRAY[nan()]), (null), (ARRAY[nan()]), (ARRAY[nan()]))");
    }

    @Test
    public void testDoubleArrayIntersect2()
    {
        // Test the array of arrays function signature
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_intersect(array_agg(%s))) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT NULL");
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_intersect(array_agg(%s))) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN_NO_NULL, ARRAY_TABLE_NAME_NO_NULL),
                "SELECT * FROM (VALUES (ARRAY[nan()]))");
    }

    @Test
    public void testRealArrayIntersect2()
    {
        // Test the array of arrays function signature
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_intersect(array_agg(%s))) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT NULL");
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_intersect(array_agg(%s))) FROM %s", SIMPLE_REAL_ARRAY_COLUMN_NO_NULL, ARRAY_TABLE_NAME_NO_NULL),
                "SELECT * FROM (VALUES (ARRAY[CAST(nan() AS REAL)]))");
    }

    @Test
    public void testDoubleArrayLeastFrequent()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_least_frequent(%s) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[-1]), (ARRAY[-1]), (ARRAY[-1]), (ARRAY[200]), (null), (ARRAY[nan()]), (ARRAY[-1]))");
    }

    @Test
    public void testRealArrayLeastFrequent()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_least_frequent(%s) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[REAL '-1']), (ARRAY[REAL '-1']), (ARRAY[REAL '-1']), (ARRAY[REAL '200']), (null), (ARRAY[CAST(nan() AS REAL)]), (ARRAY[REAL '-1']))");
    }

    @Test
    public void testDoubleArrayLeastFrequentN()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_least_frequent(%s, 3) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[-1, 0 , 1]), (ARRAY[-1, 0, 1]), (ARRAY[-1, 0, 1]), (ARRAY[200, nan()]), (null), (ARRAY[nan()]), (ARRAY[-1, 0, nan()]))");
    }

    @Test
    public void testRealArrayLeastFrequentN()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_least_frequent(%s, 3) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(ARRAY[REAL '-1', REAL '0' , REAL '1']), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1']), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1']), " +
                        "(ARRAY[REAL '200', CAST(nan() AS REAL)]), " +
                        "(null), " +
                        "(ARRAY[CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', CAST(nan() AS REAL)]))");
    }

    @Test
    public void testDoubleArrayMax()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_max(%s) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (nan()), (nan()), (nan()), (null), (null), (nan()), (nan()))");
    }

    @Test
    public void testRealArrayMax()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_max(%s) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (CAST(nan() AS REAL)), (CAST(nan() AS REAL)), (CAST(nan() AS REAL)), (null), (null), (CAST(nan() AS REAL)), (CAST(nan() AS REAL)))");
    }

    @Test
    public void testDoubleArrayMaxBy()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_max_by(%s, x -> x + 1) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (nan()), (nan()), (nan()), (null), (null), (nan()), (nan()))");
    }

    @Test
    public void testRealArrayMaxBy()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_max_by(%s, x -> x +1) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (CAST(nan() AS REAL)), (CAST(nan() AS REAL)), (CAST(nan() AS REAL)), (null), (null), (CAST(nan() AS REAL)), (CAST(nan() AS REAL)))");
    }

    @Test
    public void testDoubleArrayMin()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_min(%s) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (-1), (-1), (-1), (null), (null), (nan()), (-1))");
    }

    @Test
    public void testRealArrayMin()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_min(%s) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (REAL '-1'), (REAL '-1'), (REAL '-1'), (null), (null), (CAST(nan() AS REAL)), (REAL '-1'))");
    }

    @Test
    public void testDoubleArrayMinBy()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_min_by(%s, x -> x + 1) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (-1), (-1), (-1), (null), (null), (nan()), (-1))");
    }

    @Test
    public void testRealArrayMinBy()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_min_by(%s, x -> x + 1) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (REAL '-1'), (REAL '-1'), (REAL '-1'), (null), (null), (CAST(nan() AS REAL)), (REAL '-1'))");
    }

    @Test
    public void testDoubleArrayPosition()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_position(%s, nan()) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (BIGINT '1'), (BIGINT '2'), (BIGINT '4'), (BIGINT '2'), (null), (BIGINT '1'), (BIGINT '3'))");
    }

    @Test
    public void testRealArrayPosition()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_position(%s, nan()) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (BIGINT '1'), (BIGINT '2'), (BIGINT '4'), (BIGINT '2'), (null), (BIGINT '1'), (BIGINT '3'))");
    }

    @Test
    public void testDoubleArrayPositionI()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_position(%s, nan(), 2) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (BIGINT '0'), (BIGINT '0'), (BIGINT '0'), (BIGINT '0'), (null), (BIGINT '2'), (BIGINT '5'))");
    }

    @Test
    public void testRealArrayPositionI()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_position(%s, nan(), 2) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (BIGINT '0'), (BIGINT '0'), (BIGINT '0'), (BIGINT '0'), (null), (BIGINT '2'), (BIGINT '5'))");
    }

    @Test
    public void testDoubleArrayRemove()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_remove(%s, nan()) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(ARRAY[DOUBLE '0', DOUBLE '1', DOUBLE '-1']), " +
                        "(ARRAY[DOUBLE '0', DOUBLE '1', DOUBLE '-1']), " +
                        "(ARRAY[DOUBLE '0', DOUBLE '1', DOUBLE '-1']), " +
                        "(ARRAY[null, DOUBLE '200']), " +
                        "(null), " +
                        "(ARRAY[]), " +
                        "(ARRAY[DOUBLE '0', DOUBLE '1', DOUBLE '-1', DOUBLE '1', DOUBLE '1', DOUBLE '0']))");
    }

    @Test
    public void testRealArrayRemove()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_remove(%s, CAST(nan() AS REAL)) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(ARRAY[REAL '0', REAL '1', REAL '-1']), " +
                        "(ARRAY[REAL '0', REAL '1', REAL '-1']), " +
                        "(ARRAY[REAL '0', REAL '1', REAL '-1']), " +
                        "(ARRAY[null, REAL '200']), " +
                        "(null), " +
                        "(ARRAY[]), " +
                        "(ARRAY[REAL '0', REAL '1', REAL '-1', REAL '1', REAL '1', REAL '0']))");
    }

    @Test
    public void testDoubleArraySort()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(%s) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[-1, 0, 1, nan()]), (ARRAY[-1, 0, 1, nan()]), (ARRAY[-1, 0, 1, nan()]), (ARRAY[200, nan(), null]), (null), (ARRAY[nan(), nan()]), (ARRAY[-1, 0, 0, 1, 1, 1, nan(), nan()]))");
    }

    @Test
    public void testRealArraySort()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(%s) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '200', CAST(nan() AS REAL), null]), " +
                        "(null), " +
                        "(ARRAY[CAST(nan() AS REAL), CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '0', REAL '1', REAL '1', REAL '1', CAST(nan() AS REAL), CAST(nan() AS REAL)]))");
    }

    @Test
    public void testDoubleArraySortLambda()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(%s, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1))) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[-1, 0, 1, nan()]), (ARRAY[-1, 0, 1, nan()]), (ARRAY[-1, 0, 1, nan()]), (ARRAY[200, nan(), null]), (null), (ARRAY[nan(), nan()]), (ARRAY[-1, 0, 0, 1, 1, 1, nan(), nan()]))");
    }

    @Test
    public void testRealArraySortLambda()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(%s, (x, y) -> IF(x > y, 1, IF(x = y, 0, -1))) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '200', CAST(nan() AS REAL), null]), " +
                        "(null), (ARRAY[CAST(nan() AS REAL), CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '0', REAL'1', REAL '1', REAL '1', CAST(nan() AS REAL), CAST(nan() AS REAL)]))");
    }

    @Test
    public void testDoubleArraySortDesc()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort_desc(%s) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[nan(), 1, 0, -1]), (ARRAY[nan(), 1, 0, -1]), (ARRAY[nan(), 1, 0, -1]), (ARRAY[nan(), 200, null]), (null), (ARRAY[nan(), nan()]), (ARRAY[nan(), nan(), 1, 1, 1, 0, 0, -1]))");
    }

    @Test
    public void testRealArraySortDesc()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort_desc(%s) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(ARRAY[CAST(nan() AS REAL), REAL '1', REAL '0', REAL '-1']), " +
                        "(ARRAY[CAST(nan() AS REAL), REAL '1', REAL '0', REAL '-1']), " +
                        "(ARRAY[CAST(nan() AS REAL), REAL '1', REAL '0', REAL '-1']), " +
                        "(ARRAY[CAST(nan() AS REAL), REAL '200', null])," +
                        " (null)," +
                        " (ARRAY[CAST(nan() AS REAL), CAST(nan() AS REAL)]), " +
                        "(ARRAY[CAST(nan() AS REAL), CAST(nan() AS REAL), REAL '1', REAL '1', REAL '1', REAL '0', REAL '0', REAL '-1']))");
    }

    @Test
    public void testDoubleArrayTopN()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_top_n(%s, 2) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[nan(), 1]), (ARRAY[nan(), 1]), (ARRAY[nan(), 1]), (ARRAY[nan(), 200]), (null), (ARRAY[nan(), nan()]), (ARRAY[nan(), nan()]))");
    }

    @Test
    public void testRealArrayTopN()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_top_n(%s, 2) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(ARRAY[CAST(nan() AS REAL), REAL '1']), " +
                        "(ARRAY[CAST(nan() AS REAL), REAL '1']), " +
                        "(ARRAY[CAST(nan() AS REAL), REAL '1']), " +
                        "(ARRAY[CAST(nan() AS REAL), REAL '200'])," +
                        " (null)," +
                        " (ARRAY[CAST(nan() AS REAL), CAST(nan() AS REAL)]), " +
                        "(ARRAY[CAST(nan() AS REAL), CAST(nan() AS REAL)]))");
    }

    @Test
    public void testDoubleArraysOverlap()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT arrays_overlap(%s, ARRAY[nan()]) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (true), (true), (true), (true), (null), (true), (true))");
        assertQueryWithSameQueryRunner(
                format("SELECT arrays_overlap(ARRAY[nan()], %s) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (true), (true), (true), (true), (null), (true), (true))");
    }

    @Test
    public void testRealArraysOverlap()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT arrays_overlap(%s, ARRAY[nan()]) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (true), (true), (true), (true), (null), (true), (true))");
        assertQueryWithSameQueryRunner(
                format("SELECT arrays_overlap(ARRAY[nan()], %s) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (true), (true), (true), (true), (null), (true), (true))");
    }

    @Test
    public void testDoubleArrayUnion()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_union(%s, ARRAY[nan()])) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1', nan()]), " +
                        "(ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1', nan()]), " +
                        "(ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1', nan()]), " +
                        "(ARRAY[DOUBLE '200', nan(), null]), " +
                        "(null), " +
                        "(ARRAY[nan()]), " +
                        "(ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1', nan()]))");
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_union(ARRAY[nan()], %s)) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1', nan()]), " +
                        "(ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1', nan()]), " +
                        "(ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1', nan()]), " +
                        "(ARRAY[DOUBLE '200', nan(), null]), " +
                        "(null), " +
                        "(ARRAY[nan()]), " +
                        "(ARRAY[DOUBLE '-1', DOUBLE '0', DOUBLE '1', nan()]))");
    }

    @Test
    public void testRealArrayUnion()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_union(%s, ARRAY[CAST(nan() AS REAL)])) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '200', CAST(nan() AS REAL), null]), " +
                        "(null), " +
                        "(ARRAY[CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]))");
        assertQueryWithSameQueryRunner(
                format("SELECT array_sort(array_union(ARRAY[CAST(nan() AS REAL)], %s)) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '200', CAST(nan() AS REAL), null]), " +
                        "(null), " +
                        "(ARRAY[CAST(nan() AS REAL)]), " +
                        "(ARRAY[REAL '-1', REAL '0', REAL '1', CAST(nan() AS REAL)]))");
    }

    @Test
    public void testDoubleContains()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT contains(%s, nan()) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (true), (true), (true), (true), (null), (true), (true))");
    }

    @Test
    public void testRealContains()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT contains(%s, CAST(nan() AS REAL)) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (true), (true), (true), (true), (null), (true), (true))");
    }

    @Test
    public void testDoubleNoneMatch()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT none_match(%s, x -> x = nan()) FROM %s", SIMPLE_DOUBLE_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (false), (false), (false), (null), (false), (false), (false))");
    }

    @Test
    public void testRealNoneMatch()
    {
        assertQueryWithSameQueryRunner(
                format("SELECT none_match(%s, x -> x = nan()) FROM %s", SIMPLE_REAL_ARRAY_COLUMN, ARRAY_TABLE_NAME),
                "SELECT * FROM (VALUES (false), (false), (false), (null), (false), (false), (false))");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Duplicate map keys \\(NaN\\) are not allowed")
    public void testDoubleMapDuplicateKeys()
    {
        computeActual("select MAP(array[1, nan(), nan()], array['a', 'b','c'])");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Duplicate map keys \\(NaN\\) are not allowed")
    public void testRealMapDuplicateKeys()
    {
        computeActual("select MAP(array[REAL '1', CAST(nan() AS REAL), CAST(nan() AS REAL)], array['a', 'b','c'])");
    }

    @Test
    public void testDoubleMapAccessor()
    {
        assertQueryWithSameQueryRunner(format("SELECT %s[nan()] FROM %s", DOUBLE_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (nan()), (nan()), (nan()))");
    }

    @Test
    public void testRealMapAccessor()
    {
        assertQueryWithSameQueryRunner(format("SELECT %s[CAST(nan() AS REAL)] FROM %s", REAL_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (CAST(nan() AS REAL)), CAST(nan() AS REAL), CAST(nan() AS REAL))");
    }

    @Test
    public void testDoubleElementAt()
    {
        assertQueryWithSameQueryRunner(format("SELECT element_at(%s, nan()) FROM %s", DOUBLE_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (nan()), (nan()), (nan()))");
    }

    @Test
    public void testRealElementAt()
    {
        assertQueryWithSameQueryRunner(format("SELECT element_at(%s, nan()) FROM %s", REAL_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (CAST(nan() AS REAL)), CAST(nan() AS REAL), CAST(nan() AS REAL))");
    }

    @Test
    public void testDoubleMapSubset()
    {
        assertQueryWithSameQueryRunner(format("SELECT map_subset(%s, ARRAY[nan()]) FROM %s", DOUBLE_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (MAP(ARRAY[nan()], ARRAY[nan()])), (MAP(ARRAY[nan()], ARRAY[nan()])), (MAP(ARRAY[nan()], ARRAY[nan()])))");
    }

    @Test
    public void testRealMapSubset()
    {
        assertQueryWithSameQueryRunner(format("SELECT map_subset(%s, ARRAY[CAST(nan() AS REAL)]) FROM %s", REAL_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (MAP(ARRAY[CAST(nan() AS REAL)], ARRAY[CAST(nan() AS REAL)])), (MAP(ARRAY[CAST(nan() AS REAL)], ARRAY[CAST(nan() AS REAL)])), (MAP(ARRAY[CAST(nan() AS REAL)], ARRAY[CAST(nan() AS REAL)])))");
    }

    @Test
    public void testDoubleMapKeyExists()
    {
        assertQueryWithSameQueryRunner(format("SELECT map_key_exists(%s, nan()) FROM %s", DOUBLE_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (true), (true), (true))");
    }

    @Test
    public void testRealMapKeyExists()
    {
        assertQueryWithSameQueryRunner(format("SELECT map_key_exists(%s, CAST(nan() AS REAL)) FROM %s", REAL_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (true), (true), (true))");
    }

    @Test
    public void testDoubleMapToNKeys()
    {
        assertQueryWithSameQueryRunner(format("SELECT map_top_n_keys(%s, 2) FROM %s", DOUBLE_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[nan(), 2]), (ARRAY[nan(), 2]), (ARRAY[nan(), 2]))");
    }

    @Test
    public void testRealMapToNKey()
    {
        assertQueryWithSameQueryRunner(format("SELECT map_top_n_keys(%s, 2) FROM %s", REAL_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[CAST(nan() AS REAL), REAL '2']), (ARRAY[CAST(nan() AS REAL), REAL '2']), (ARRAY[CAST(nan() AS REAL), REAL '2']))");
    }

    @Test
    public void testDoubleMapKeysByTopNValues()
    {
        assertQueryWithSameQueryRunner(format("SELECT map_keys_by_top_n_values(%s, 2) FROM %s", DOUBLE_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[nan(), 2]), (ARRAY[nan(), 2]), (ARRAY[nan(), 2]))");
    }

    @Test
    public void testRealMapKeysByTopNValues()
    {
        assertQueryWithSameQueryRunner(format("SELECT map_keys_by_top_n_values(%s, 2) FROM %s", REAL_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[CAST(nan() AS REAL), REAL '2']), (ARRAY[CAST(nan() AS REAL), REAL '2']), (ARRAY[CAST(nan() AS REAL), REAL '2']))");
    }

    @Test
    public void testDoubleMapToN()
    {
        assertQueryWithSameQueryRunner(format("SELECT map_top_n(%s, 2) FROM %s", DOUBLE_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (MAP(ARRAY[nan(), 2], ARRAY[nan(), 200])), (MAP(ARRAY[nan(), 2], ARRAY[nan(), 200])), (MAP(ARRAY[nan(), 2], ARRAY[nan(), 200])))");
    }

    @Test
    public void testRealMapToN()
    {
        assertQueryWithSameQueryRunner(format("SELECT map_top_n(%s, 2) FROM %s", REAL_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES " +
                        "(MAP(ARRAY[CAST(nan() AS REAL), REAL '2'], ARRAY[CAST(nan() AS REAL), REAL '200'])), " +
                        "(MAP(ARRAY[CAST(nan() AS REAL), REAL '2'], ARRAY[CAST(nan() AS REAL), REAL '200'])), " +
                        "(MAP(ARRAY[CAST(nan() AS REAL), REAL '2'], ARRAY[CAST(nan() AS REAL), REAL '200'])))");
    }

    @Test
    public void testRealMapToNKeys()
    {
        assertQueryWithSameQueryRunner(format("SELECT map_top_n_keys(%s, 2) FROM %s", REAL_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[CAST(nan() AS REAL), REAL '2']), (ARRAY[CAST(nan() AS REAL), REAL '2']), (ARRAY[CAST(nan() AS REAL), REAL '2']))");
    }

    @Test
    public void testDoubleMapToNValues()
    {
        assertQueryWithSameQueryRunner(format("SELECT map_top_n_values(%s, 2) FROM %s", DOUBLE_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[nan(), 200]), (ARRAY[nan(), 200]), (ARRAY[nan(), 200]))");
    }

    @Test
    public void testRealMapToNValues()
    {
        assertQueryWithSameQueryRunner(format("SELECT map_top_n_values(%s, 2) FROM %s", REAL_MAP_COLUMN, MAP_TABLE_NAME),
                "SELECT * FROM (VALUES (ARRAY[CAST(nan() AS REAL), REAL '200']), (ARRAY[CAST(nan() AS REAL), REAL '200']), (ARRAY[CAST(nan() AS REAL), REAL '200']))");
    }

    @Test
    public void testDoubleOrderBy()
    {
        assertQueryOrderedWithSameQueryRunner(format("SELECT %s FROM %s ORDER BY 1", DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(-4), (0), (infinity()), (nan()))");
        assertQueryOrderedWithSameQueryRunner(format("SELECT %s FROM %s ORDER BY 1 DESC", DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(nan()), (infinity()), (0), (-4))");
    }

    @Test
    public void testRealOrderBy()
    {
        assertQueryOrderedWithSameQueryRunner(format("SELECT %s FROM %s ORDER BY 1", REAL_NAN_FIRST_COLUMN, REAL_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(REAL '-4'), (REAL '0'), (CAST(infinity() AS REAL)), (CAST(nan() AS REAL)))");
        assertQueryOrderedWithSameQueryRunner(format("SELECT %s FROM %s ORDER BY 1 DESC", REAL_NAN_FIRST_COLUMN, REAL_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(CAST(nan() AS REAL)), (CAST(infinity() AS REAL)), (REAL '0'), (REAL '-4'))");
    }

    @Test
    public void testDoubleOrderByLimit()
    {
        assertQueryOrderedWithSameQueryRunner(format("SELECT %s FROM %s ORDER BY 1 LIMIT 2", DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(DOUBLE '-4.0'), (DOUBLE '0.0'))");
        assertQueryOrderedWithSameQueryRunner(format("SELECT %s FROM %s ORDER BY 1 DESC LIMIT 2", DOUBLE_NAN_FIRST_COLUMN, DOUBLE_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(nan()), (infinity()))");
    }

    @Test
    public void testRealOrderByLimit()
    {
        assertQueryOrderedWithSameQueryRunner(format("SELECT %s FROM %s ORDER BY 1 LIMIT 2", REAL_NAN_FIRST_COLUMN, REAL_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(REAL '-4'), (REAL '0'))");
        assertQueryOrderedWithSameQueryRunner(format("SELECT %s FROM %s ORDER BY 1 DESC LIMIT 2", REAL_NAN_FIRST_COLUMN, REAL_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(CAST(nan() AS REAL)), (CAST(infinity() AS REAL)))");
    }

    @Test
    public void testDoubleInnerJoin()
    {
        assertQueryWithSameQueryRunner(format("SELECT %1$s FROM (SELECT %1$s from %3$s) JOIN (SELECT %2$s FROM %3$s) on %1$s = %2$s", DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN, DOUBLE_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(DOUBLE '0'), (DOUBLE '2'), (nan()))");
    }

    @Test
    public void testRealInnerJoin()
    {
        assertQueryWithSameQueryRunner(format("SELECT %1$s FROM (SELECT %1$s from %3$s) JOIN (SELECT %2$s FROM %3$s) on %1$s = %2$s", REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN, REAL_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(REAL '0'), (REAL '2'), (CAST(nan() AS REAL)))");
    }

    @Test
    public void testDoubleLeftJoin()
    {
        assertQueryWithSameQueryRunner(format("SELECT %1$s, %2$s FROM (SELECT %1$s from %3$s) LEFT JOIN (SELECT %2$s FROM %3$s) on %1$s = %2$s", DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN, DOUBLE_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(DOUBLE '0', DOUBLE '0'), (DOUBLE '2', DOUBLE '2'), (nan(), nan()), (DOUBLE '3', null))");
    }

    @Test
    public void testRealLeftJoin()
    {
        assertQueryWithSameQueryRunner(format("SELECT %1$s, %2$s FROM (SELECT %1$s from %3$s) LEFT JOIN (SELECT %2$s FROM %3$s) on %1$s = %2$s", REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN, REAL_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(REAL '0', REAL '0'), (REAL '2', REAL '2'), (CAST(nan() AS REAL), CAST(nan() AS REAL)), (REAL '3', null))");
    }

    @Test
    public void testDoubleRightJoin()
    {
        assertQueryWithSameQueryRunner(format("SELECT %1$s, %2$s FROM (SELECT %1$s from %3$s) RIGHT JOIN (SELECT %2$s FROM %3$s) on %1$s = %2$s", DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN, DOUBLE_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(DOUBLE '0', DOUBLE '0'), (DOUBLE '2', DOUBLE '2'), (nan(), nan()), (null, DOUBLE '1'))");
    }

    @Test
    public void testRealRightJoin()
    {
        assertQueryWithSameQueryRunner(format("SELECT %1$s, %2$s FROM (SELECT %1$s from %3$s) RIGHT JOIN (SELECT %2$s FROM %3$s) on %1$s = %2$s", REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN, REAL_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(REAL '0', REAL '0'), (REAL '2', REAL '2'), (CAST(nan() AS REAL), CAST(nan() AS REAL)), (null, REAL '1'))");
    }

    @Test
    public void testDoubleFullJoin()
    {
        assertQueryWithSameQueryRunner(format("SELECT %1$s, %2$s FROM (SELECT %1$s from %3$s) FULL OUTER JOIN (SELECT %2$s FROM %3$s) on %1$s = %2$s", DOUBLE_NAN_MIDDLE_COLUMN, DOUBLE_NAN_LAST_COLUMN, DOUBLE_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(DOUBLE '0', DOUBLE '0'), (DOUBLE '2', DOUBLE '2'), (nan(), nan()), (DOUBLE '3', null), (null, DOUBLE '1'))");
    }

    @Test
    public void testRealFullJoin()
    {
        assertQueryWithSameQueryRunner(format("SELECT %1$s, %2$s FROM (SELECT %1$s from %3$s) FULL OUTER JOIN (SELECT %2$s FROM %3$s) on %1$s = %2$s", REAL_NAN_MIDDLE_COLUMN, REAL_NAN_LAST_COLUMN, REAL_NANS_TABLE_NAME),
                "SELECT * FROM (VALUES(REAL '0', REAL '0'), (REAL '2', REAL '2'), (CAST(nan() AS REAL), CAST(nan() AS REAL)), (REAL '3', null), (null, REAL '1'))");
    }
}
