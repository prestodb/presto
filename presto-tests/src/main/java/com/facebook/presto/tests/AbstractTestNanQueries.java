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
    }

    @AfterClass
    public void tearDown()
    {
        assertUpdate("DROP TABLE " + DOUBLE_NANS_TABLE_NAME);
        assertUpdate("DROP TABLE " + REAL_NANS_TABLE_NAME);
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
    public void testDoubleEquals()
    {
        assertQuery("SELECT nan() = nan()", "SELECT true");
        assertQuery("SELECT nan() = 3", "SELECT false");
        assertQuery(format("SELECT _double_nan_first = nan() from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
        assertQuery(format("SELECT nan() = _double_nan_first from %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
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
    public void testDoubleBetween()
    {
        assertQuery(format("SELECT nan() BETWEEN -infinity() AND _double_nan_first FROM %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
        assertQuery(format("SELECT _double_nan_first BETWEEN -infinity() AND nan() FROM %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES(true), (true), (true), (true))");
    }

    @Test
    public void testDoubleIn()
    {
        assertQuery(format("SELECT nan() IN (1, 2, _double_nan_first) FROM %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (true), (false), (false), (false))");
        assertQuery(format("SELECT _double_nan_first IN (nan(), 0, 6)FROM %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES(true), (true), (false), (false))");
    }

    @Test
    public void testDoubleNotIn()
    {
        assertQuery(format("SELECT nan() NOT IN (1, 2, _double_nan_first) FROM %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES (false), (true), (true), (true))");
        assertQuery(format("SELECT _double_nan_first NOT IN (nan(), 0, 6)FROM %s", DOUBLE_NANS_TABLE_NAME), "SELECT * FROM (VALUES(false), (false), (true), (true))");
    }
