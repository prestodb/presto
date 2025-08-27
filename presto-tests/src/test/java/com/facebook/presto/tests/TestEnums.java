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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.BigintEnumType.LongEnumMap;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.common.type.VarcharEnumType.VarcharEnumMap;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.StandardTypes.BIGINT_ENUM;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR_ENUM;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestEnums
        extends AbstractTestQueryFramework
{
    protected static final Long BIG_VALUE = Integer.MAX_VALUE + 10L; // 2147483657

    protected static final UserDefinedType MOOD_ENUM = new UserDefinedType(QualifiedObjectName.valueOf("test.enum.mood"), new TypeSignature(
            BIGINT_ENUM,
            TypeSignatureParameter.of(new LongEnumMap("test.enum.mood", ImmutableMap.of(
                    "HAPPY", 0L,
                "SAD", 1L,
                "MELLOW", BIG_VALUE,
                "curious", -2L)))));
    protected static final UserDefinedType COUNTRY_ENUM = new UserDefinedType(QualifiedObjectName.valueOf("test.enum.country"), new TypeSignature(
            VARCHAR_ENUM,
            TypeSignatureParameter.of(new VarcharEnumMap("test.enum.country", ImmutableMap.of(
            "US", "United States",
            "BAHAMAS", "The Bahamas",
            "FRANCE", "France",
            "CHINA", "中国",
            "भारत", "India")))));
    protected static final UserDefinedType TEST_ENUM = new UserDefinedType(QualifiedObjectName.valueOf("test.enum.testenum"), new TypeSignature(
            VARCHAR_ENUM,
            TypeSignatureParameter.of(new VarcharEnumMap("test.enum.testenum", ImmutableMap.of(
                    "TEST", "\"}\"",
                    "TEST2", "",
                    "TEST3", " ",
                    "TEST4", ")))\"\"",
                    "TEST5", "France")))));
    protected static final UserDefinedType TEST_BIGINT_ENUM = new UserDefinedType(QualifiedObjectName.valueOf("test.enum.testbigintenum"), new TypeSignature(
            BIGINT_ENUM,
            TypeSignatureParameter.of(new LongEnumMap("test.enum.testbigintenum", ImmutableMap.of(
            "TEST", 6L,
            "TEST2", 8L)))));
    protected static final UserDefinedType MARKET_SEGMENT_ENUM = new UserDefinedType(QualifiedObjectName.valueOf("test.enum.market_segment"), new TypeSignature(
            VARCHAR_ENUM,
            TypeSignatureParameter.of(new VarcharEnumMap("test.enum.market_segment", ImmutableMap.of(
                    "MKT_BUILDING", "BUILDING",
                    "MKT_FURNITURE", "FURNITURE")))));

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        try {
            Session session = testSessionBuilder().build();
            DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();
            queryRunner.enableTestFunctionNamespaces(ImmutableList.of("test"), ImmutableMap.of());
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(MOOD_ENUM);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(COUNTRY_ENUM);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(TEST_ENUM);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(TEST_BIGINT_ENUM);
            queryRunner.getMetadata().getFunctionAndTypeManager().addUserDefinedType(MARKET_SEGMENT_ENUM);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void assertQueryResultUnordered(@Language("SQL") String query, List<List<Object>> expectedRows)
    {
        MaterializedResult rows = computeActual(query);
        assertEquals(
                ImmutableSet.copyOf(rows.getMaterializedRows()),
                expectedRows.stream().map(row -> new MaterializedRow(1, row)).collect(Collectors.toSet()));
    }

    protected void assertSingleValue(@Language("SQL") String expression, Object expectedResult)
    {
        assertQueryResultUnordered("SELECT " + expression, singletonList(singletonList(expectedResult)));
    }

    @Test
    public void testEnumLiterals()
    {
        assertQueryResultUnordered(
                "SELECT test.enum.mood.HAPPY, test.enum.mood.happY, \"test.enum.mood\".SAD, \"test.enum.mood\".\"mellow\"",
                singletonList(ImmutableList.of(0L, 0L, 1L, BIG_VALUE)));

        assertQueryResultUnordered(
                "SELECT test.enum.country.us, test.enum.country.\"CHINA\", test.enum.country.\"भारत\"",
                singletonList(ImmutableList.of("United States", "中国", "India")));

        assertQueryResultUnordered(
                "SELECT test.enum.testEnum.TEST, test.enum.testEnum.TEST2, test.enum.testEnum.TEST3, array[test.enum.testEnum.TEST4]",
                singletonList(ImmutableList.of("\"}\"", "", " ", ImmutableList.of(")))\"\""))));

        assertQueryResultUnordered(
                "SELECT MAP(ARRAY[test.enum.mood.HAPPY], ARRAY[1])",
                singletonList(ImmutableList.of(ImmutableMap.of(0L, 1))));

        assertQueryFails("SELECT test.enum.mood.hello", ".*'test.enum.mood.hello' cannot be resolved");
    }

    @Test
    public void testEnumCasts()
    {
        assertSingleValue("CAST(CAST(1 AS TINYINT) AS test.enum.mood)", 1L);
        assertSingleValue("CAST('The Bahamas' AS test.enum.country)", "The Bahamas");
        assertSingleValue("CAST(row(1, 1) as row(x BIGINT, y test.enum.mood))", ImmutableList.of(1L, 1L));
        assertSingleValue("CAST(test.enum.mood.MELLOW AS BIGINT)", BIG_VALUE);
        assertSingleValue(
                "cast(map(array[test.enum.country.FRANCE], array[array[test.enum.mood.HAPPY]]) as JSON)",
                "{\"France\":[0]}");
        assertSingleValue(
                "cast(JSON '{\"France\": [0]}' as MAP<test.enum.country,ARRAY<test.enum.mood>>)",
                ImmutableMap.of("France", singletonList(0L)));
    }

    @Test
    public void testVarcharEnumComparisonOperators()
    {
        assertSingleValue("test.enum.country.US = CAST('United States' AS test.enum.country)", true);
        assertSingleValue("test.enum.country.FRANCE = test.enum.country.BAHAMAS", false);

        assertSingleValue("test.enum.country.FRANCE != test.enum.country.US", true);
        assertSingleValue("array[test.enum.country.FRANCE, test.enum.country.BAHAMAS] != array[test.enum.country.US, test.enum.country.BAHAMAS]", true);

        assertSingleValue("test.enum.country.CHINA IN (test.enum.country.US, null, test.enum.country.BAHAMAS, test.enum.country.China)", true);
        assertSingleValue("test.enum.country.BAHAMAS IN (test.enum.country.US, test.enum.country.FRANCE)", false);

        assertSingleValue("test.enum.country.BAHAMAS < test.enum.country.US", true);
        assertSingleValue("test.enum.country.BAHAMAS < test.enum.country.BAHAMAS", false);

        assertSingleValue("test.enum.country.\"भारत\" <= test.enum.country.\"भारत\"", true);
        assertSingleValue("test.enum.country.\"भारत\" <= test.enum.country.FRANCE", false);

        assertSingleValue("test.enum.country.\"भारत\" >= test.enum.country.FRANCE", true);
        assertSingleValue("test.enum.country.BAHAMAS >= test.enum.country.US", false);

        assertSingleValue("test.enum.country.\"भारत\" > test.enum.country.FRANCE", true);
        assertSingleValue("test.enum.country.CHINA > test.enum.country.CHINA", false);

        assertSingleValue("test.enum.country.\"भारत\" between test.enum.country.FRANCE and test.enum.country.BAHAMAS", true);
        assertSingleValue("test.enum.country.US between test.enum.country.FRANCE and test.enum.country.\"भारत\"", false);
    }

    @Test
    public void testLongEnumComparisonOperators()
    {
        assertSingleValue("test.enum.mood.HAPPY = CAST(0 AS test.enum.mood)", true);
        assertSingleValue("test.enum.mood.HAPPY = test.enum.mood.SAD", false);
        assertSingleValue("array[test.enum.mood.HAPPY, test.enum.mood.SAD] = array[test.enum.mood.HAPPY, test.enum.mood.SAD]", true);

        assertSingleValue("test.enum.mood.SAD != test.enum.mood.MELLOW", true);
        assertSingleValue("array[test.enum.mood.HAPPY, test.enum.mood.SAD] != array[test.enum.mood.SAD, test.enum.mood.HAPPY]", true);

        assertSingleValue("test.enum.mood.SAD IN (test.enum.mood.HAPPY, null, test.enum.mood.SAD)", true);
        assertSingleValue("test.enum.mood.HAPPY IN (test.enum.mood.SAD, test.enum.mood.MELLOW)", false);

        assertSingleValue("test.enum.mood.CURIOUS < test.enum.mood.MELLOW", true);
        assertSingleValue("test.enum.mood.SAD < test.enum.mood.HAPPY", false);

        assertSingleValue("test.enum.mood.HAPPY <= test.enum.mood.HAPPY", true);
        assertSingleValue("test.enum.mood.HAPPY <= test.enum.mood.CURIOUS", false);

        assertSingleValue("test.enum.mood.MELLOW >= test.enum.mood.SAD", true);
        assertSingleValue("test.enum.mood.HAPPY >= test.enum.mood.SAD", false);

        assertSingleValue("test.enum.mood.SAD > test.enum.mood.HAPPY", true);
        assertSingleValue("test.enum.mood.HAPPY > test.enum.mood.HAPPY", false);

        assertSingleValue("test.enum.mood.HAPPY between test.enum.mood.CURIOUS and test.enum.mood.SAD ", true);
        assertSingleValue("test.enum.mood.MELLOW between test.enum.mood.SAD and test.enum.mood.HAPPY", false);
    }

    @Test
    public void testEnumFailureCases()
    {
        // Invalid cast
        assertQueryFails("select cast(7 as test.enum.mood)", ".*No value '7' in enum 'BigintEnum'");
        assertQueryFails("SELECT cast(test.enum.country.FRANCE as test.enum.testenum)", ".*Cannot cast test.enum.country.* to test.enum.testenum.*");

        // Invalid comparison between different enum types or between enum and base types
        assertQueryFails("select test.enum.country.US = test.enum.mood.HAPPY", ".* '=' cannot be applied to test.enum.country:VarcharEnum\\(test.enum.country.*\\), test.enum.mood:BigintEnum\\(test.enum.mood.*\\)");
        assertQueryFails("select test.enum.country.US > 2", ".* '>' cannot be applied to test.enum.country:VarcharEnum\\(test.enum.country.*\\), integer");
        assertQueryFails("select test.enum.mood.HAPPY = 3", ".* '=' cannot be applied to test.enum.mood:BigintEnum\\(test.enum.mood.*, integer");
        assertQueryFails("select test.enum.country.US IN (test.enum.country.CHINA, test.enum.mood.SAD)", ".* All IN list values must be the same type.*");
        assertQueryFails("select test.enum.country.US IN (test.enum.mood.HAPPY, test.enum.mood.SAD)", ".* IN value and list items must be the same type: test.enum.country");
    }

    @Test
    public void testEnumHashOperators()
    {
        assertQueryResultUnordered(
                "SELECT DISTINCT x " +
                        "FROM (VALUES test.enum.mood.happy, test.enum.mood.sad, test.enum.mood.sad, test.enum.mood.happy) t(x)",
                ImmutableList.of(
                        ImmutableList.of(0L),
                        ImmutableList.of(1L)));

        assertQueryResultUnordered(
                "SELECT DISTINCT x " +
                        "FROM (VALUES test.enum.country.FRANCE, test.enum.country.FRANCE, test.enum.country.\"भारत\") t(x)",
                ImmutableList.of(
                        ImmutableList.of("France"),
                        ImmutableList.of("India")));

        assertQueryResultUnordered(
                "SELECT APPROX_DISTINCT(x), APPROX_DISTINCT(y)" +
                        "FROM (VALUES (test.enum.country.FRANCE, test.enum.mood.HAPPY), " +
                        "             (test.enum.country.FRANCE, test.enum.mood.SAD)," +
                        "             (test.enum.country.US, test.enum.mood.HAPPY)) t(x, y)",
                ImmutableList.of(
                        ImmutableList.of(2L, 2L)));
    }

    @Test
    public void testEnumAggregation()
    {
        assertQueryResultUnordered(
                "  SELECT a, ARRAY_AGG(DISTINCT b) " +
                        "FROM (VALUES (test.enum.mood.happy, test.enum.country.us), " +
                        "             (test.enum.mood.happy, test.enum.country.china)," +
                        "             (test.enum.mood.happy, test.enum.country.CHINA)," +
                        "             (test.enum.mood.sad, test.enum.country.us)) t(a, b)" +
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
                        "FROM (VALUES test.enum.mood.happy, test.enum.mood.sad, test.enum.mood.mellow) t1(a) " +
                        "JOIN (VALUES (test.enum.mood.sad, 'hello'), (test.enum.mood.happy, 'world')) t2(a, b) " +
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
                        "FROM (VALUES (test.enum.mood.happy, 1), (test.enum.mood.happy, 3), (test.enum.mood.sad, 5)) t(a, b)",
                ImmutableList.of(singletonList(1), singletonList(1), singletonList(5)));
    }

    @Test
    public void testCastFunctionCaching()
    {
        assertSingleValue("CAST(' ' as test.enum.TestEnum)", " ");
        assertSingleValue("CAST(8 as test.enum.TestBigintEnum)", 8L);
    }

    @Test
    public void testEnumKey()
    {
        assertSingleValue("enum_key(test.enum.mood.curious)", "CURIOUS");
        assertSingleValue("enum_key(test.enum.country.CHINA)", "CHINA");

        assertSingleValue("enum_key(cast(1 as test.enum.mood))", "SAD");
        assertSingleValue("enum_key(cast('中国' as test.enum.country))", "CHINA");

        assertSingleValue("enum_key(try_cast(7 as test.enum.mood))", null);
        assertSingleValue("enum_key(try_cast('invalid_value' as test.enum.country))", null);
    }

    @Test
    public void testEnumKeyDistributed()
    {
        assertQueryResultUnordered(
                "SELECT DISTINCT enum_key(try_cast(nationkey as test.enum.mood)) from tpch.sf100.customer where nationkey = cast(test.enum.mood.SAD as bigint)",
                ImmutableList.of(singletonList("SAD")));

        assertQueryResultUnordered(
                "SELECT DISTINCT enum_key(try_cast(mktsegment as test.enum.market_segment)) from tpch.sf100.customer where mktsegment IN ('BUILDING', 'FURNITURE')",
                ImmutableList.of(singletonList("MKT_BUILDING"), singletonList("MKT_FURNITURE")));
    }
}
