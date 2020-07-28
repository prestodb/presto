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
import com.facebook.presto.common.type.LongEnumType.LongEnumMap;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.VarcharEnumType.VarcharEnumMap;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.type.LongEnumParametricType;
import com.facebook.presto.type.VarcharEnumParametricType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestEnums
        extends AbstractTestQueryFramework
{
    private static final Long BIG_VALUE = Integer.MAX_VALUE + 10L; // 2147483657

    private static final LongEnumParametricType MOOD_ENUM = new LongEnumParametricType("Mood", new LongEnumMap(ImmutableMap.of(
            "HAPPY", 0L,
            "SAD", 1L,
            "MELLOW", BIG_VALUE,
            "curious", -2L)));
    private static final VarcharEnumParametricType COUNTRY_ENUM = new VarcharEnumParametricType("Country", new VarcharEnumMap(ImmutableMap.of(
            "US", "United States",
            "BAHAMAS", "The Bahamas",
            "FRANCE", "France",
            "CHINA", "中国",
            "भारत", "India")));
    private static final VarcharEnumParametricType TEST_ENUM = new VarcharEnumParametricType("TestEnum", new VarcharEnumMap(ImmutableMap.of(
            "TEST", "\"}\"",
            "TEST2", "",
            "TEST3", " ",
            "TEST4", ")))\"\"")));

    static class TestEnumPlugin
            implements Plugin
    {
        @Override
        public Iterable<ParametricType> getParametricTypes()
        {
            return ImmutableList.of(MOOD_ENUM, COUNTRY_ENUM, TEST_ENUM);
        }
    }

    protected TestEnums()
    {
        super(TestEnums::createQueryRunner);
    }

    private static QueryRunner createQueryRunner()
    {
        try {
            Session session = testSessionBuilder().build();
            QueryRunner queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();
            queryRunner.installPlugin(new TestEnumPlugin());
            return queryRunner;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assertQueryResultUnordered(@Language("SQL") String query, List<List<Object>> expectedRows)
    {
        MaterializedResult rows = computeActual(query);
        assertEquals(
                ImmutableSet.copyOf(rows.getMaterializedRows()),
                expectedRows.stream().map(row -> new MaterializedRow(1, row)).collect(Collectors.toSet()));
    }

    private void assertSingleValue(@Language("SQL") String expression, Object expectedResult)
    {
        assertQueryResultUnordered("SELECT " + expression, singletonList(singletonList(expectedResult)));
    }

    @Test
    public void testEnumLiterals()
    {
        assertQueryResultUnordered(
                "SELECT Mood.HAPPY, mood.happY, \"mood\".SAD, \"mood\".\"mellow\"",
                singletonList(ImmutableList.of(0L, 0L, 1L, BIG_VALUE)));

        assertQueryResultUnordered(
                "SELECT Country.us, country.\"CHINA\", Country.\"भारत\"",
                singletonList(ImmutableList.of("United States", "中国", "India")));

        assertQueryResultUnordered(
                "SELECT testEnum.TEST, testEnum.TEST2, testEnum.TEST3, array[testEnum.TEST4]",
                singletonList(ImmutableList.of("\"}\"", "", " ", ImmutableList.of(")))\"\""))));

        assertQueryFails("SELECT mood.hello", ".*No key 'HELLO' in enum 'Mood'");
    }

    @Test
    public void testEnumCasts()
    {
        assertSingleValue("CAST(CAST(1 AS TINYINT) AS Mood)", 1L);
        assertSingleValue("CAST('The Bahamas' AS COUNTRY)", "The Bahamas");
        assertSingleValue("CAST(row(1, 1) as row(x BIGINT, y Mood))", ImmutableList.of(1L, 1L));
        assertSingleValue("CAST(mood.MELLOW AS BIGINT)", BIG_VALUE);
        assertSingleValue(
                "cast(map(array[country.FRANCE], array[array[mood.HAPPY]]) as JSON)",
                "{\"France\":[0]}");
        assertSingleValue(
                "map_filter(MAP(ARRAY[country.FRANCE, country.US], ARRAY[mood.HAPPY, mood.SAD]), (k,v) -> CAST(v AS BIGINT) > 0)",
                ImmutableMap.of("United States", 1L));
        assertSingleValue(
                "cast(JSON '{\"France\": [0]}' as MAP<Country,ARRAY<Mood>>)",
                ImmutableMap.of("France", singletonList(0L)));
        assertQueryFails("select cast(7 as mood)", ".*No value '7' in enum 'Mood'");
    }

    @Test
    public void testVarcharEnumComparisonOperators()
    {
        assertSingleValue("country.US = CAST('United States' AS country)", true);
        assertSingleValue("country.FRANCE = country.BAHAMAS", false);

        assertSingleValue("country.FRANCE != country.US", true);
        assertSingleValue("array[country.FRANCE, country.BAHAMAS] != array[country.US, country.BAHAMAS]", true);

        assertSingleValue("country.CHINA IN (country.US, null, country.BAHAMAS, country.China)", true);
        assertSingleValue("country.BAHAMAS IN (country.US, country.FRANCE)", false);

        assertSingleValue("country.BAHAMAS < country.US", true);
        assertSingleValue("country.BAHAMAS < country.BAHAMAS", false);

        assertSingleValue("country.\"भारत\" <= country.\"भारत\"", true);
        assertSingleValue("country.\"भारत\" <= country.FRANCE", false);

        assertSingleValue("country.\"भारत\" >= country.FRANCE", true);
        assertSingleValue("country.BAHAMAS >= country.US", false);

        assertSingleValue("country.\"भारत\" > country.FRANCE", true);
        assertSingleValue("country.CHINA > country.CHINA", false);

        assertSingleValue("country.\"भारत\" between country.FRANCE and country.BAHAMAS", true);
        assertSingleValue("country.US between country.FRANCE and country.\"भारत\"", false);

        assertQueryFails("select country.US = mood.HAPPY", ".* '=' cannot be applied to Country.*, Mood.*");
        assertQueryFails("select country.US IN (country.CHINA, mood.SAD)", ".* All IN list values must be the same type.*");
        assertQueryFails("select country.US IN (mood.HAPPY, mood.SAD)", ".* IN value and list items must be the same type: Country");
        assertQueryFails("select country.US > 2", ".* '>' cannot be applied to Country.*, integer");
    }

    @Test
    public void testLongEnumComparisonOperators()
    {
        assertSingleValue("mood.HAPPY = CAST(0 AS mood)", true);
        assertSingleValue("mood.HAPPY = mood.SAD", false);

        assertSingleValue("mood.SAD != mood.MELLOW", true);
        assertSingleValue("array[mood.HAPPY, mood.SAD] != array[mood.SAD, mood.HAPPY]", true);

        assertSingleValue("mood.SAD IN (mood.HAPPY, null, mood.SAD)", true);
        assertSingleValue("mood.HAPPY IN (mood.SAD, mood.MELLOW)", false);

        assertSingleValue("mood.CURIOUS < mood.MELLOW", true);
        assertSingleValue("mood.SAD < mood.HAPPY", false);

        assertSingleValue("mood.HAPPY <= mood.HAPPY", true);
        assertSingleValue("mood.HAPPY <= mood.CURIOUS", false);

        assertSingleValue("mood.MELLOW >= mood.SAD", true);
        assertSingleValue("mood.HAPPY >= mood.SAD", false);

        assertSingleValue("mood.SAD > mood.HAPPY", true);
        assertSingleValue("mood.HAPPY > mood.HAPPY", false);

        assertSingleValue("mood.HAPPY between mood.CURIOUS and mood.SAD ", true);
        assertSingleValue("mood.MELLOW between mood.SAD and mood.HAPPY", false);

        assertQueryFails("select mood.HAPPY = 3", ".* '=' cannot be applied to Mood.*, integer");
    }

    @Test
    public void testEnumHashOperators()
    {
        assertQueryResultUnordered(
                "SELECT DISTINCT x " +
                        "FROM (VALUES mood.happy, mood.sad, mood.sad, mood.happy) t(x)",
                ImmutableList.of(
                        ImmutableList.of(0L),
                        ImmutableList.of(1L)));

        assertQueryResultUnordered(
                "SELECT DISTINCT x " +
                        "FROM (VALUES country.FRANCE, country.FRANCE, country.\"भारत\") t(x)",
                ImmutableList.of(
                        ImmutableList.of("France"),
                        ImmutableList.of("India")));

        assertQueryResultUnordered(
                "SELECT APPROX_DISTINCT(x), APPROX_DISTINCT(y)" +
                        "FROM (VALUES (country.FRANCE, mood.HAPPY), " +
                        "             (country.FRANCE, mood.SAD)," +
                        "             (country.US, mood.HAPPY)) t(x, y)",
                ImmutableList.of(
                        ImmutableList.of(2L, 2L)));
    }

    @Test
    public void testEnumAggregation()
    {
        assertQueryResultUnordered(
                "  SELECT a, ARRAY_AGG(DISTINCT b) " +
                        "FROM (VALUES (mood.happy, country.us), " +
                        "             (mood.happy, country.china)," +
                        "             (mood.happy, country.CHINA)," +
                        "             (mood.sad, country.us)) t(a, b)" +
                        "GROUP BY a",
                ImmutableList.of(
                        ImmutableList.of(0L, ImmutableList.of("United States", "中国")),
                        ImmutableList.of(1L, ImmutableList.of("United States"))));
    }

    @Test
    public void testEnumJoin()
    {
        assertQueryResultUnordered(
                "  SELECT t1.a, t2.b " +
                        "FROM (VALUES mood.happy, mood.sad, mood.mellow) t1(a) " +
                        "JOIN (VALUES (mood.sad, 'hello'), (mood.happy, 'world')) t2(a, b) " +
                        "ON t1.a = t2.a",
                ImmutableList.of(
                        ImmutableList.of(1L, "hello"),
                        ImmutableList.of(0L, "world")));
    }

    @Test
    public void testEnumWindow()
    {
        assertQueryResultUnordered(
                "  SELECT first_value(b) OVER (PARTITION BY a ORDER BY a) AS rnk " +
                        "FROM (VALUES (mood.happy, 1), (mood.happy, 3), (mood.sad, 5)) t(a, b)",
                ImmutableList.of(singletonList(1), singletonList(1), singletonList(5)));
    }
}
