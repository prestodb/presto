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
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.testing.Arguments;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.type.SqlIntervalDayTime;
import com.facebook.presto.type.SqlIntervalYearMonth;
import com.facebook.presto.util.DateTimeZoneIndex;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import io.airlift.tpch.TpchTable;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.LEGACY_ORDER_BY;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static com.facebook.presto.operator.scalar.InvokeFunction.INVOKE_FUNCTION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_AGGREGATE_OR_GROUP_BY;
import static com.facebook.presto.sql.tree.ExplainType.Type.DISTRIBUTED;
import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.facebook.presto.testing.Arguments.toArgumentsArrays;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.tests.QueryTemplate.parameter;
import static com.facebook.presto.tests.QueryTemplate.queryTemplate;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestQueries
        extends AbstractTestQueryFramework
{
    // We can just use the default type registry, since we don't use any parametric types
    protected static final List<SqlFunction> CUSTOM_FUNCTIONS = new FunctionListBuilder()
            .aggregate(CustomSum.class)
            .window(CustomRank.class)
            .scalars(CustomAdd.class)
            .scalars(CreateHll.class)
            .functions(APPLY_FUNCTION, INVOKE_FUNCTION)
            .getFunctions();

    public static final List<PropertyMetadata<?>> TEST_SYSTEM_PROPERTIES = ImmutableList.of(
            PropertyMetadata.stringSessionProperty(
                    "test_string",
                    "test string property",
                    "test default",
                    false),
            PropertyMetadata.longSessionProperty(
                    "test_long",
                    "test long property",
                    42L,
                    false));
    public static final List<PropertyMetadata<?>> TEST_CATALOG_PROPERTIES = ImmutableList.of(
            PropertyMetadata.stringSessionProperty(
                    "connector_string",
                    "connector string property",
                    "connector default",
                    false),
            PropertyMetadata.longSessionProperty(
                    "connector_long",
                    "connector long property",
                    33L,
                    false),
            PropertyMetadata.booleanSessionProperty(
                    "connector_boolean",
                    "connector boolean property",
                    true,
                    false),
            PropertyMetadata.doubleSessionProperty(
                    "connector_double",
                    "connector double property",
                    99.0,
                    false));

    protected AbstractTestQueries(QueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    @Test
    public void testParsingError()
    {
        assertQueryFails("SELECT foo FROM", "line 1:16: no viable alternative at input.*");
    }

    @Test
    public void selectLargeInterval()
    {
        MaterializedResult result = computeActual("SELECT INTERVAL '30' DAY");
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), new SqlIntervalDayTime(30, 0, 0, 0, 0));

        result = computeActual("SELECT INTERVAL '" + Short.MAX_VALUE + "' YEAR");
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), new SqlIntervalYearMonth(Short.MAX_VALUE, 0));
    }

    @Test
    public void selectNull()
    {
        assertQuery("SELECT NULL", "SELECT NULL FROM (SELECT * FROM ORDERS LIMIT 1)");
    }

    @Test
    public void testLimitIntMax()
    {
        assertQuery("SELECT orderkey from orders LIMIT " + Integer.MAX_VALUE);
        assertQuery("SELECT orderkey from orders ORDER BY orderkey LIMIT " + Integer.MAX_VALUE);
    }

    @Test
    public void testNonDeterministic()
    {
        MaterializedResult materializedResult = computeActual("SELECT rand() FROM orders LIMIT 10");
        long distinctCount = materializedResult.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .distinct()
                .count();
        assertTrue(distinctCount >= 8, "rand() must produce different rows");

        materializedResult = computeActual("SELECT apply(1, x -> x + rand()) FROM orders LIMIT 10");
        distinctCount = materializedResult.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .distinct()
                .count();
        assertTrue(distinctCount >= 8, "rand() must produce different rows");
    }

    @Test
    public void testLambdaCapture()
            throws Exception
    {
        // Test for lambda expression without capture can be found in TestLambdaExpression

        assertQuery("SELECT apply(c1, x -> x + c2) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(c1, c2)", "VALUES 3, 7, 11");
        assertQuery("SELECT apply(c1 + 10, x -> apply(x + 100, y -> c1)) FROM (VALUES 1) t(c1)", "VALUES 1");

        // reference lambda variable of the not-immediately-enclosing lambda
        assertQuery("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 1000) t(x)", "VALUES 1");
        assertQuery("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 'abc') t(x)", "VALUES 1");
        assertQuery("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 1000) t(x)", "VALUES 1");
        assertQuery("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 'abc') t(x)", "VALUES 1");

        // in join post-filter
        assertQuery("SELECT * from (VALUES true) t(x) left join (VALUES 1001) t2(y) ON (apply(false, z -> apply(false, y -> x)))", "SELECT true, 1001");
    }

    @Test
    public void testLambdaInAggregationContext()
    {
        assertQuery("SELECT apply(sum(x), i -> i * i) FROM (VALUES 1, 2, 3, 4, 5) t(x)", "SELECT 225");
        assertQuery("SELECT apply(x, i -> i - 1), sum(y) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) group by x", "VALUES (0, 30), (1, 50)");
        assertQuery("SELECT x, apply(sum(y), i -> i * 10) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) group by x", "VALUES (1, 300), (2, 500)");
        assertQuery("SELECT apply(8, x -> x + 1) FROM (VALUES (1, 2)) t(x,y) GROUP BY y", "SELECT 9");

        assertQuery("SELECT apply(CAST(ROW(1) AS ROW(someField BIGINT)), x -> x.someField) FROM (VALUES (1,2)) t(x,y) GROUP BY y", "SELECT 1");
        assertQuery("SELECT apply(sum(x), x -> x * x) FROM (VALUES 1, 2, 3, 4, 5) t(x)", "SELECT 225");
        // nested lambda expression uses the same variable name
        assertQuery("SELECT apply(sum(x), x -> apply(x, x -> x * x)) FROM (VALUES 1, 2, 3, 4, 5) t(x)", "SELECT 225");
    }

    @Test
    public void testLambdaInSubqueryContext()
    {
        assertQuery("SELECT apply(x, i -> i * i) FROM (SELECT 10 x)", "SELECT 100");
        assertQuery("SELECT apply((SELECT 10), i -> i * i)", "SELECT 100");

        // with capture
        assertQuery("SELECT apply(x, i -> i * x) FROM (SELECT 10 x)", "SELECT 100");
        assertQuery("SELECT apply(x, y -> y * x) FROM (SELECT 10 x, 3 y)", "SELECT 100");
        assertQuery("SELECT apply(x, z -> y * x) FROM (SELECT 10 x, 3 y)", "SELECT 30");
    }

    @Test
    public void testTryLambdaRepeated()
    {
        assertQuery("SELECT x + x FROM (SELECT apply(a, i -> i * i) x FROM (VALUES 3) t(a))", "SELECT 18");
        assertQuery("SELECT apply(a, i -> i * i) + apply(a, i -> i * i) FROM (VALUES 3) t(a)", "SELECT 18");
        assertQuery("SELECT apply(a, i -> i * i), apply(a, i -> i * i) FROM (VALUES 3) t(a)", "SELECT 9, 9");
        assertQuery("SELECT try(10 / a) + try(10 / a) FROM (VALUES 5) t(a)", "SELECT 4");
        assertQuery("SELECT try(10 / a), try(10 / a) FROM (VALUES 5) t(a)", "SELECT 2, 2");
    }

    @Test
    public void testNonDeterministicFilter()
    {
        MaterializedResult materializedResult = computeActual("SELECT u FROM ( SELECT if(rand() > 0.5, 0, 1) AS u ) WHERE u <> u");
        assertEquals(materializedResult.getRowCount(), 0);

        materializedResult = computeActual("SELECT u, v FROM ( SELECT if(rand() > 0.5, 0, 1) AS u, 4*4 as v ) WHERE u <> u and v > 10");
        assertEquals(materializedResult.getRowCount(), 0);

        materializedResult = computeActual("SELECT u, v, w FROM ( SELECT if(rand() > 0.5, 0, 1) AS u, 4*4 as v, 'abc' as w ) WHERE v > 10");
        assertEquals(materializedResult.getRowCount(), 1);
    }

    @Test
    public void testNonDeterministicProjection()
    {
        MaterializedResult materializedResult = computeActual("select r, r + 1 from (select rand(100) r from orders) limit 10");
        assertEquals(materializedResult.getRowCount(), 10);
        for (MaterializedRow materializedRow : materializedResult) {
            assertEquals(materializedRow.getFieldCount(), 2);
            assertEquals(((Number) materializedRow.getField(0)).intValue() + 1, materializedRow.getField(1));
        }
    }

    @Test
    public void testMapSubscript()
    {
        assertQuery("select map(array[1], array['aa'])[1]", "select 'aa'");
        assertQuery("select map(array['a'], array['aa'])['a']", "select 'aa'");
        assertQuery("select map(array[array[1,1]], array['a'])[array[1,1]]", "select 'a'");
        assertQuery("select map(array[(1,2)], array['a'])[(1,2)]", "select 'a'");
    }

    @Test
    public void testVarbinary()
    {
        assertQuery("SELECT LENGTH(x) FROM (SELECT from_base64('gw==') as x)", "SELECT 1");
        assertQuery("SELECT LENGTH(from_base64('gw=='))", "SELECT 1");
    }

    @Test
    public void testRowFieldAccessor()
    {
        //Dereference only
        assertQuery("SELECT a.col0 FROM (VALUES ROW (CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)))) AS t (a)", "SELECT 1");
        assertQuery("SELECT a.col0 FROM (VALUES ROW (CAST(ROW(1.0, 2.0) AS ROW(col0 integer, col1 integer)))) AS t (a)", "SELECT 1.0");
        assertQuery("SELECT a.col0 FROM (VALUES ROW (CAST(ROW(TRUE, FALSE) AS ROW(col0 boolean, col1 boolean)))) AS t (a)", "SELECT TRUE");
        assertQuery("SELECT a.col1 FROM (VALUES ROW (CAST(ROW(1.0, 'kittens') AS ROW(col0 varchar, col1 varchar)))) AS t (a)", "SELECT 'kittens'");
        assertQuery("SELECT a.col2.col1 FROM (VALUES ROW(CAST(ROW(1.0, ARRAY[2], row(3, 4.0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a)", "SELECT 4.0");

        // mixture of row field reference and table field reference
        assertQuery("SELECT cast(row(1, t.x) as row(col0 bigint, col1 bigint)).col1 FROM (VALUES 1, 2, 3) t(x)", "SELECT * FROM (VALUES 1, 2, 3)");
        assertQuery("SELECT Y.col1 FROM (SELECT cast(row(1, t.x) as row(col0 bigint, col1 bigint)) AS Y FROM (VALUES 1, 2, 3) t(x)) test_t", "SELECT * FROM (VALUES 1, 2, 3)");

        // Subscript + Dereference
        assertQuery("SELECT a.col1[2] FROM (VALUES ROW(CAST(ROW(1.0, ARRAY[22, 33, 44, 55], row(3, 4.0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a)", "SELECT 33");
        assertQuery("SELECT a.col1[2].col0, a.col1[2].col1 FROM (VALUES ROW(cast(row(1.0, ARRAY[row(31, 4.1), row(32, 4.2)], row(3, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double))))) t(a)", "SELECT 32, 4.2");

        assertQuery("SELECT cast(row(11, 12) as row(col0 bigint, col1 bigint)).col0", "SELECT 11");
    }

    @Test
    public void testRowFieldAccessorInAggregate()
    {
        assertQuery("SELECT a.col0, SUM(a.col1[2]), SUM(a.col2.col0), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[2, 13, 4], row(11, 4.1))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.0, ARRAY[2, 23, 4], row(12, 14.0))  AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.0, ARRAY[22, 33, 44], row(13, 5.0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a) " +
                        "GROUP BY a.col0",
                "SELECT * FROM VALUES (1.0, 46, 24, 9.1), (2.0, 23, 12, 14.0)");

        assertQuery("SELECT a.col2.col0, SUM(a.col0), SUM(a.col1[2]), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[2, 13, 4], row(11, 4.1))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.0, ARRAY[2, 23, 4], row(11, 14.0))  AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(7.0, ARRAY[22, 33, 44], row(13, 5.0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a) " +
                        "GROUP BY a.col2.col0",
                "SELECT * FROM VALUES (11, 3.0, 36, 18.1), (13, 7.0, 33, 5.0)");

        assertQuery("SELECT a.col1[1].col0, SUM(a.col0), SUM(a.col1[1].col1), SUM(a.col1[2].col0), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[row(31, 4.5), row(12, 4.2)], row(3, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.1, ARRAY[row(41, 3.1), row(32, 4.2)], row(6, 6.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(31, 4.2), row(22, 4.2)], row(5, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double))))) t(a) " +
                        "GROUP BY a.col1[1].col0",
                "SELECT * FROM VALUES (31, 3.2, 8.7, 34, 8.0), (41, 3.1, 3.1, 32, 6.0)");

        assertQuery("SELECT a.col1[1].col0, SUM(a.col0), SUM(a.col1[1].col1), SUM(a.col1[2].col0), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(31, 4.2), row(22, 4.2)], row(5, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.0, ARRAY[row(31, 4.5), row(12, 4.2)], row(3, 4.1)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.1, ARRAY[row(41, 3.1), row(32, 4.2)], row(6, 6.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.3, ARRAY[row(41, 3.1), row(32, 4.2)], row(6, 6.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))) " +
                        ") t(a) " +
                        "GROUP BY a.col1[1]",
                "SELECT * FROM VALUES (31, 2.2, 4.2, 22, 4.0), (31, 1.0, 4.5, 12, 4.1), (41, 6.4, 6.2, 64, 12.0)");

        assertQuery("SELECT a.col1[2], SUM(a.col0), SUM(a.col1[1]), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[2, 13, 4], row(11, 4.1))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.0, ARRAY[2, 13, 4], row(12, 14.0))  AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(7.0, ARRAY[22, 33, 44], row(13, 5.0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a) " +
                        "GROUP BY a.col1[2]",
                "SELECT * FROM VALUES (13, 3.0, 4, 18.1), (33, 7.0, 22, 5.0)");

        assertQuery("SELECT a.col2.col0, SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(31, 4.2), row(22, 4.2)], row(5, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.0, ARRAY[row(31, 4.5), row(12, 4.2)], row(3, 4.1)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.1, ARRAY[row(41, 3.1), row(32, 4.2)], row(6, 6.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.3, ARRAY[row(41, 3.1), row(32, 4.2)], row(6, 6.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))) " +
                        ") t(a) " +
                        "GROUP BY a.col2",
                "SELECT * FROM VALUES (5, 4.0), (3, 4.1), (6, 12.0)");

        assertQuery("SELECT a.col2.col0, a.col0, SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[2, 13, 4], row(11, 4.1))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.0, ARRAY[2, 23, 4], row(11, 14.0))  AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.5, ARRAY[2, 13, 4], row(11, 4.1))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.5, ARRAY[2, 13, 4], row(11, 4.1))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(7.0, ARRAY[22, 33, 44], row(13, 5.0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a) " +
                        "WHERE a.col1[2] < 30 " +
                        "GROUP BY 1, 2 ORDER BY 1",
                "SELECT * FROM VALUES (11, 1.0, 4.1), (11, 1.5, 8.2), (11, 2.0, 14.0)");

        assertQuery("SELECT a[1].col0, COUNT(1) FROM " +
                        "(VALUES " +
                        "(ROW(CAST(ARRAY[row(31, 4.2), row(22, 4.2)] AS ARRAY(ROW(col0 integer, col1 double))))), " +
                        "(ROW(CAST(ARRAY[row(31, 4.5), row(12, 4.2)] AS ARRAY(ROW(col0 integer, col1 double))))), " +
                        "(ROW(CAST(ARRAY[row(41, 3.1), row(32, 4.2)] AS ARRAY(ROW(col0 integer, col1 double))))), " +
                        "(ROW(CAST(ARRAY[row(31, 3.1), row(32, 4.2)] AS ARRAY(ROW(col0 integer, col1 double))))) " +
                        ") t(a) " +
                        "GROUP BY 1 " +
                        "ORDER BY 2 DESC",
                "SELECT * FROM VALUES (31, 3), (41, 1)");
    }

    @Test
    public void testRowFieldAccessorInWindowFunction()
    {
        assertQuery("SELECT a.col0, " +
                        "SUM(a.col1[1].col1) OVER(PARTITION BY a.col2.col0), " +
                        "SUM(a.col2.col1) OVER(PARTITION BY a.col2.col0) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[row(31, 14.5), row(12, 4.2)], row(3, 4.0))  AS ROW(col0 double, col1 array(ROW(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(41, 13.1), row(32, 4.2)], row(6, 6.0))  AS ROW(col0 double, col1 array(ROW(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(41, 17.1), row(45, 4.2)], row(7, 16.0)) AS ROW(col0 double, col1 array(ROW(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(41, 13.1), row(32, 4.2)], row(6, 6.0))  AS ROW(col0 double, col1 array(ROW(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.1, ARRAY[row(41, 13.1), row(32, 4.2)], row(6, 6.0))  AS ROW(col0 double, col1 array(ROW(col0 integer, col1 double)), col2 row(col0 integer, col1 double))))) t(a) ",
                "SELECT * FROM VALUES (1.0, 14.5, 4.0), (2.2, 39.3, 18.0), (2.2, 39.3, 18.0), (2.2, 17.1, 16.0), (3.1, 39.3, 18.0)");

        assertQuery("SELECT a.col1[1].col0, " +
                        "SUM(a.col0) OVER(PARTITION BY a.col1[1].col0), " +
                        "SUM(a.col1[1].col1) OVER(PARTITION BY a.col1[1].col0), " +
                        "SUM(a.col2.col1) OVER(PARTITION BY a.col1[1].col0) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[row(31, 14.5), row(12, 4.2)], row(3, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.1, ARRAY[row(41, 13.1), row(32, 4.2)], row(6, 6.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(31, 14.2), row(22, 5.2)], row(5, 4.0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double))))) t(a) " +
                        "WHERE a.col1[2].col1 > a.col2.col0",
                "SELECT * FROM VALUES (31, 3.2, 28.7, 8.0), (31, 3.2, 28.7, 8.0)");
    }

    @Test
    public void testRowFieldAccessorInJoin()
    {
        assertQuery("" +
                        "SELECT t.a.col1, custkey, orderkey FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1, 11) AS ROW(col0 integer, col1 integer))), " +
                        "ROW(CAST(ROW(2, 22) AS ROW(col0 integer, col1 integer))), " +
                        "ROW(CAST(ROW(3, 33) AS ROW(col0 integer, col1 integer)))) t(a) " +
                        "INNER JOIN orders " +
                        "ON t.a.col0 = orders.orderkey",
                "SELECT * FROM VALUES (11, 370, 1), (22, 781, 2), (33, 1234, 3)");
    }

    @Test
    public void testRowCast()
    {
        assertQuery("SELECT cast(row(1, 2) as row(aa bigint, bb boolean)).aa", "SELECT 1");
        assertQuery("SELECT cast(row(1, 2) as row(aa bigint, bb boolean)).bb", "SELECT true");
        assertQuery("SELECT cast(row(1, 2) as row(aa bigint, bb varchar)).bb", "SELECT '2'");
        assertQuery("SELECT cast(row(true, array[0, 2]) as row(aa boolean, bb array(boolean))).bb[1]", "SELECT false");
        assertQuery("SELECT cast(row(0.1, array[0, 2], row(1, 0.5)) as row(aa bigint, bb array(boolean), cc row(dd varchar, ee varchar))).cc.ee", "SELECT '0.5'");
        assertQuery("SELECT cast(array[row(0.1, array[0, 2], row(1, 0.5))] as array<row(aa bigint, bb array(boolean), cc row(dd varchar, ee varchar))>)[1].cc.ee", "SELECT '0.5'");
    }

    @Test
    public void testDereferenceInSubquery()
    {
        assertQuery("" +
                        "SELECT x " +
                        "FROM (" +
                        "   SELECT a.x" +
                        "   FROM (VALUES 1, 2, 3) a(x)" +
                        ") " +
                        "GROUP BY x",
                "SELECT * FROM VALUES 1, 2, 3");

        assertQuery("" +
                        "SELECT t2.*, max(t1.b) as max_b " +
                        "FROM (VALUES (1, 'a'),  (2, 'b'), (1, 'c'), (3, 'd')) t1(a, b) " +
                        "INNER JOIN " +
                        "(VALUES 1, 2, 3, 4) t2(a) " +
                        "ON t1.a = t2.a " +
                        "GROUP BY t2.a",
                "SELECT * FROM VALUES (1, 'c'), (2, 'b'), (3, 'd')");

        assertQuery("" +
                        "SELECT t2.*, max(t1.b1) as max_b1 " +
                        "FROM (VALUES (1, 'a'),  (2, 'b'), (1, 'c'), (3, 'd')) t1(a1, b1) " +
                        "INNER JOIN " +
                        "(VALUES (1, 11, 111), (2, 22, 222), (3, 33, 333), (4, 44, 444)) t2(a2, b2, c2) " +
                        "ON t1.a1 = t2.a2 " +
                        "GROUP BY t2.a2, t2.b2, t2.c2",
                "SELECT * FROM VALUES (1, 11, 111, 'c'), (2, 22, 222, 'b'), (3, 33, 333, 'd')");

        assertQuery("" +
                "SELECT custkey, orders2 " +
                "FROM (" +
                "   SELECT x.custkey, SUM(x.orders) + 1 orders2 " +
                "   FROM ( " +
                "      SELECT x.custkey, COUNT(x.orderkey) orders " +
                "      FROM ORDERS x " +
                "      WHERE x.custkey < 100 " +
                "      GROUP BY x.custkey " +
                "   ) x " +
                "   GROUP BY x.custkey" +
                ") " +
                "ORDER BY custkey");
    }

    @Test
    public void testDereferenceInFunctionCall()
    {
        assertQuery("" +
                "SELECT COUNT(DISTINCT custkey) " +
                "FROM ( " +
                "  SELECT x.custkey " +
                "  FROM ORDERS x " +
                "  WHERE custkey < 100 " +
                ") t");
    }

    @Test
    public void testDereferenceInComparison()
    {
        assertQuery("" +
                "SELECT orders.custkey, orders.orderkey " +
                "FROM ORDERS " +
                "WHERE orders.custkey > orders.orderkey AND orders.custkey < 200.3");
    }

    @Test
    public void testMissingRowFieldInGroupBy()
    {
        assertQueryFails(
                "SELECT a.col0, count(*) FROM (VALUES ROW(cast(ROW(1, 1) as ROW(col0 integer, col1 integer)))) t(a)",
                "line 1:8: '\"a\".\"col0\"' must be an aggregate expression or appear in GROUP BY clause");
    }

    @Test
    public void testWhereWithRowField()
    {
        assertQuery("SELECT a.col0 FROM (VALUES ROW(CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)))) AS t (a) WHERE a.col0 > 0", "SELECT 1");
        assertQuery("SELECT SUM(a.col0) FROM (VALUES ROW(CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)))) AS t (a) WHERE a.col0 <= 0", "SELECT null");

        assertQuery("SELECT a.col0 FROM (VALUES ROW(CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)))) AS t (a) WHERE a.col0 < a.col1", "SELECT 1");
        assertQuery("SELECT SUM(a.col0) FROM (VALUES ROW(CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)))) AS t (a) WHERE a.col0 < a.col1", "SELECT 1");
        assertQuery("SELECT SUM(a.col0) FROM (VALUES ROW(CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)))) AS t (a) WHERE a.col0 > a.col1", "SELECT null");
    }

    @Test
    public void testUnnest()
    {
        assertQuery("SELECT 1 FROM (VALUES (ARRAY[1])) AS t (a) CROSS JOIN UNNEST(a)", "SELECT 1");
        assertQuery("SELECT x[1] FROM UNNEST(ARRAY[ARRAY[1, 2, 3]]) t(x)", "SELECT 1");
        assertQuery("SELECT x[1][2] FROM UNNEST(ARRAY[ARRAY[ARRAY[1, 2, 3]]]) t(x)", "SELECT 2");
        assertQuery("SELECT x[2] FROM UNNEST(ARRAY[MAP(ARRAY[1,2], ARRAY['hello', 'hi'])]) t(x)", "SELECT 'hi'");
        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3])", "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery("SELECT a FROM UNNEST(ARRAY[1, 2, 3]) t(a)", "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery("SELECT a, b FROM UNNEST(ARRAY[1, 2], ARRAY[3, 4]) t(a, b)", "SELECT * FROM VALUES (1, 3), (2, 4)");
        assertQuery("SELECT a, b FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b)", "SELECT * FROM VALUES (1, 4), (2, 5), (3, NULL)");
        assertQuery("SELECT a FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b)", "SELECT * FROM VALUES 1, 2, 3");
        assertQuery("SELECT b FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b)", "SELECT * FROM VALUES 4, 5, NULL");
        assertQuery("SELECT count(*) FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5])", "SELECT 3");
        assertQuery("SELECT a FROM UNNEST(ARRAY['kittens', 'puppies']) t(a)", "SELECT * FROM VALUES ('kittens'), ('puppies')");
        assertQuery("" +
                        "SELECT c " +
                        "FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b) " +
                        "CROSS JOIN (values (8), (9)) t2(c)",
                "SELECT * FROM VALUES 8, 8, 8, 9, 9, 9");
        assertQuery("" +
                        "SELECT a.custkey, t.e " +
                        "FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a " +
                        "CROSS JOIN UNNEST(my_array) t(e)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (1), (2), (3))");
        assertQuery("" +
                        "SELECT a.custkey, t.e " +
                        "FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a, " +
                        "UNNEST(my_array) t(e)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (1), (2), (3))");
        assertQuery("SELECT * FROM UNNEST(ARRAY[0, 1]) CROSS JOIN UNNEST(ARRAY[0, 1]) CROSS JOIN UNNEST(ARRAY[0, 1])",
                "SELECT * FROM VALUES (0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0), (1, 0, 1), (1, 1, 0), (1, 1, 1)");
        assertQuery("SELECT * FROM UNNEST(ARRAY[0, 1]), UNNEST(ARRAY[0, 1]), UNNEST(ARRAY[0, 1])",
                "SELECT * FROM VALUES (0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0), (1, 0, 1), (1, 1, 0), (1, 1, 1)");
        assertQuery("SELECT a, b FROM UNNEST(MAP(ARRAY[1,2], ARRAY['cat', 'dog'])) t(a, b)", "SELECT * FROM VALUES (1, 'cat'), (2, 'dog')");
        assertQuery("SELECT a, b FROM UNNEST(MAP(ARRAY[1,2], ARRAY['cat', NULL])) t(a, b)", "SELECT * FROM VALUES (1, 'cat'), (2, NULL)");

        assertQuery("SELECT 1 FROM (VALUES (ARRAY[1])) AS t (a) CROSS JOIN UNNEST(a) WITH ORDINALITY", "SELECT 1");
        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY", "SELECT * FROM VALUES (1, 1), (2, 2), (3, 3)");
        assertQuery("SELECT b FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY t(a, b)", "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery("SELECT a, b, c FROM UNNEST(ARRAY[10, 20, 30], ARRAY[4, 5]) WITH ORDINALITY t(a, b, c)", "SELECT * FROM VALUES (10, 4, 1), (20, 5, 2), (30, NULL, 3)");
        assertQuery("SELECT a, b FROM UNNEST(ARRAY['kittens', 'puppies']) WITH ORDINALITY t(a, b)", "SELECT * FROM VALUES ('kittens', 1), ('puppies', 2)");
        assertQuery("" +
                        "SELECT c " +
                        "FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) WITH ORDINALITY t(a, b, c) " +
                        "CROSS JOIN (values (8), (9)) t2(d)",
                "SELECT * FROM VALUES 1, 1, 2, 2, 3, 3");
        assertQuery("" +
                        "SELECT a.custkey, t.e, t.f " +
                        "FROM (SELECT custkey, ARRAY[10, 20, 30] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a " +
                        "CROSS JOIN UNNEST(my_array) WITH ORDINALITY t(e, f)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (10, 1), (20, 2), (30, 3))");
        assertQuery("" +
                        "SELECT a.custkey, t.e, t.f " +
                        "FROM (SELECT custkey, ARRAY[10, 20, 30] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a, " +
                        "UNNEST(my_array) WITH ORDINALITY t(e, f)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (10, 1), (20, 2), (30, 3))");

        assertQuery("SELECT * FROM orders, UNNEST(ARRAY[1])", "SELECT orders.*, 1 FROM orders");
    }

    @Test
    public void testArrays()
    {
        assertQuery("SELECT a[1] FROM (SELECT ARRAY[orderkey] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey FROM orders");
        assertQuery("SELECT a[1 + cast(round(rand()) AS BIGINT)] FROM (SELECT ARRAY[orderkey, orderkey] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey FROM orders");
        assertQuery("SELECT a[1] + 1 FROM (SELECT ARRAY[orderkey] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey + 1 FROM orders");
        assertQuery("SELECT a[1] FROM (SELECT ARRAY[orderkey + 1] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey + 1 FROM orders");
        assertQuery("SELECT a[1][1] FROM (SELECT ARRAY[ARRAY[orderkey + 1]] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey + 1 FROM orders");
        assertQuery("SELECT CARDINALITY(a) FROM (SELECT ARRAY[orderkey, orderkey + 1] AS a FROM orders ORDER BY orderkey) t", "SELECT 2 FROM orders");
    }

    @Test
    public void testRows()
    {
        // Using JSON_FORMAT(CAST(_ AS JSON)) because H2 does not support ROW type
        assertQuery("SELECT JSON_FORMAT(CAST(ROW(1 + 2, CONCAT('a', 'b')) AS JSON))", "SELECT '[3,\"ab\"]'");
        assertQuery("SELECT JSON_FORMAT(CAST(ROW(a + b) AS JSON)) FROM (VALUES (1, 2)) AS t(a, b)", "SELECT '[3]'");
        assertQuery("SELECT JSON_FORMAT(CAST(ROW(1, ROW(9, a, ARRAY[], NULL), ROW(1, 2)) AS JSON)) FROM (VALUES ('a')) t(a)", "SELECT '[1,[9,\"a\",[],null],[1,2]]'");
        assertQuery("SELECT JSON_FORMAT(CAST(ROW(ROW(ROW(ROW(ROW(a, b), c), d), e), f) AS JSON)) FROM (VALUES (ROW(0, 1), 2, '3', NULL, ARRAY[5], ARRAY[])) t(a, b, c, d, e, f)",
                "SELECT '[[[[[[0,1],2],\"3\"],null],[5]],[]]'");
        assertQuery("SELECT JSON_FORMAT(CAST(ARRAY_AGG(ROW(a, b)) AS JSON)) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(a, b)", "SELECT '[[1,2],[3,4],[5,6]]'");
        assertQuery("SELECT CONTAINS(ARRAY_AGG(ROW(a, b)), ROW(1, 2)) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(a, b)", "SELECT TRUE");
        assertQuery("SELECT JSON_FORMAT(CAST(ARRAY_AGG(ROW(c, d)) AS JSON)) FROM (VALUES (ARRAY[1, 3, 5], ARRAY[2, 4, 6])) AS t(a, b) CROSS JOIN UNNEST(a, b) AS u(c, d)",
                "SELECT '[[1,2],[3,4],[5,6]]'");
        assertQuery("SELECT JSON_FORMAT(CAST(ROW(x, y, z) AS JSON)) FROM (VALUES ROW(1, NULL, '3')) t(x,y,z)", "SELECT '[1,null,\"3\"]'");
        assertQuery("SELECT JSON_FORMAT(CAST(ROW(x, y, z) AS JSON)) FROM (VALUES ROW(1, CAST(NULL AS INTEGER), '3')) t(x,y,z)", "SELECT '[1,null,\"3\"]'");
    }

    @Test
    public void testMaps()
    {
        assertQuery("SELECT m[max_key] FROM (SELECT map_agg(orderkey, orderkey) m, max(orderkey) max_key FROM orders)", "SELECT max(orderkey) FROM orders");
    }

    @Test
    public void testValues()
    {
        assertQuery("VALUES 1, 2, 3, 4");
        assertQuery("VALUES 1, 3, 2, 4 ORDER BY 1", "SELECT * FROM (VALUES 1, 3, 2, 4) ORDER BY 1");
        assertQuery("VALUES (1.1, 2, 'foo'), (sin(3.3), 2+2, 'bar')");
        assertQuery("VALUES (1.1, 2), (sin(3.3), 2+2) ORDER BY 1", "VALUES (sin(3.3), 2+2), (1.1, 2)");
        assertQuery("VALUES (1.1, 2), (sin(3.3), 2+2) LIMIT 1", "VALUES (1.1, 2)");
        assertQuery("SELECT * FROM (VALUES (1.1, 2), (sin(3.3), 2+2))");
        assertQuery(
                "SELECT * FROM (VALUES (1.1, 2), (sin(3.3), 2+2)) x (a, b) LEFT JOIN (VALUES (1.1, 2), (1.1, 2+2)) y (a, b) USING (a)",
                "VALUES (1.1, 2, 1.1, 4), (1.1, 2, 1.1, 2), (sin(3.3), 4, NULL, NULL)");
        assertQuery("SELECT 1.1 in (VALUES (1.1), (2.2))", "VALUES (TRUE)");

        assertQuery("" +
                        "WITH a AS (VALUES (1.1, 2), (sin(3.3), 2+2)) " +
                        "SELECT * FROM a",
                "VALUES (1.1, 2), (sin(3.3), 2+2)");

        // implicity coersions
        assertQuery("VALUES 1, 2.2, 3, 4.4");
        assertQuery("VALUES (1, 2), (3.3, 4.4)");
        assertQuery("VALUES true, 1.0 in (1, 2, 3)");
    }

    @Test
    public void testSpecialFloatingPointValues()
    {
        MaterializedResult actual = computeActual("SELECT nan(), infinity(), -infinity()");
        MaterializedRow row = getOnlyElement(actual.getMaterializedRows());
        assertEquals(row.getField(0), Double.NaN);
        assertEquals(row.getField(1), Double.POSITIVE_INFINITY);
        assertEquals(row.getField(2), Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testMaxMinStringWithNulls()
    {
        assertQuery("SELECT custkey, MAX(NULLIF(orderstatus, 'O')), MIN(NULLIF(orderstatus, 'O')) FROM orders GROUP BY custkey");
    }

    @Test
    public void testApproxPercentile()
    {
        MaterializedResult raw = computeActual("SELECT orderstatus, orderkey, totalprice FROM ORDERS");

        Multimap<String, Long> orderKeyByStatus = ArrayListMultimap.create();
        Multimap<String, Double> totalPriceByStatus = ArrayListMultimap.create();
        for (MaterializedRow row : raw.getMaterializedRows()) {
            orderKeyByStatus.put((String) row.getField(0), (Long) row.getField(1));
            totalPriceByStatus.put((String) row.getField(0), (Double) row.getField(2));
        }

        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, " +
                "   approx_percentile(orderkey, 0.5), " +
                "   approx_percentile(totalprice, 0.5)," +
                "   approx_percentile(orderkey, 2, 0.5)," +
                "   approx_percentile(totalprice, 2, 0.5)\n" +
                "FROM ORDERS\n" +
                "GROUP BY orderstatus");

        for (MaterializedRow row : actual.getMaterializedRows()) {
            String status = (String) row.getField(0);
            Long orderKey = (Long) row.getField(1);
            Double totalPrice = (Double) row.getField(2);
            Long orderKeyWeighted = (Long) row.getField(3);
            Double totalPriceWeighted = (Double) row.getField(4);

            List<Long> orderKeys = Ordering.natural().sortedCopy(orderKeyByStatus.get(status));
            List<Double> totalPrices = Ordering.natural().sortedCopy(totalPriceByStatus.get(status));

            // verify real rank of returned value is within 1% of requested rank
            assertTrue(orderKey >= orderKeys.get((int) (0.49 * orderKeys.size())));
            assertTrue(orderKey <= orderKeys.get((int) (0.51 * orderKeys.size())));

            assertTrue(orderKeyWeighted >= orderKeys.get((int) (0.49 * orderKeys.size())));
            assertTrue(orderKeyWeighted <= orderKeys.get((int) (0.51 * orderKeys.size())));

            assertTrue(totalPrice >= totalPrices.get((int) (0.49 * totalPrices.size())));
            assertTrue(totalPrice <= totalPrices.get((int) (0.51 * totalPrices.size())));

            assertTrue(totalPriceWeighted >= totalPrices.get((int) (0.49 * totalPrices.size())));
            assertTrue(totalPriceWeighted <= totalPrices.get((int) (0.51 * totalPrices.size())));
        }
    }

    @Test
    public void testComplexQuery()
    {
        MaterializedResult actual = computeActual("SELECT sum(orderkey), row_number() OVER (ORDER BY orderkey)\n" +
                "FROM orders\n" +
                "WHERE orderkey <= 10\n" +
                "GROUP BY orderkey\n" +
                "HAVING sum(orderkey) >= 3\n" +
                "ORDER BY orderkey DESC\n" +
                "LIMIT 3");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(7L, 5L)
                .row(6L, 4L)
                .row(5L, 3L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testWhereNull()
    {
        // This query is has this strange shape to force the compiler to leave a true on the stack
        // with the null flag set so if the filter method is not handling nulls correctly, this
        // query will fail
        assertQuery("SELECT custkey FROM orders WHERE custkey = custkey AND cast(nullif(custkey, custkey) as boolean) AND cast(nullif(custkey, custkey) as boolean)");
    }

    @Test
    public void testSumOfNulls()
    {
        assertQuery("SELECT orderstatus, sum(CAST(NULL AS BIGINT)) FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testAggregationWithSomeArgumentCasts()
    {
        assertQuery("SELECT APPROX_PERCENTILE(0.1, x), AVG(x), MIN(x) FROM (values 1, 1, 1) t(x)", "SELECT 0.1, 1.0, 1");
    }

    @Test
    public void testAggregationWithHaving()
    {
        assertQuery("SELECT a, count(1) FROM (VALUES 1, 2, 3, 2) t(a) GROUP BY a HAVING count(1) > 1", "SELECT 2, 2");
    }

    @Test
    public void testApproximateCountDistinct()
    {
        assertQuery("SELECT approx_distinct(custkey) FROM orders", "SELECT 996");
        assertQuery("SELECT approx_distinct(custkey, 0.023) FROM orders", "SELECT 996");
        assertQuery("SELECT approx_distinct(CAST(custkey AS DOUBLE)) FROM orders", "SELECT 1031");
        assertQuery("SELECT approx_distinct(CAST(custkey AS DOUBLE), 0.023) FROM orders", "SELECT 1031");
        assertQuery("SELECT approx_distinct(CAST(custkey AS VARCHAR)) FROM orders", "SELECT 1011");
        assertQuery("SELECT approx_distinct(CAST(custkey AS VARCHAR), 0.023) FROM orders", "SELECT 1011");
        assertQuery("SELECT approx_distinct(to_utf8(CAST(custkey AS VARCHAR))) FROM orders", "SELECT 1011");
        assertQuery("SELECT approx_distinct(to_utf8(CAST(custkey AS VARCHAR)), 0.023) FROM orders", "SELECT 1011");
    }

    @Test
    public void testApproximateCountDistinctGroupBy()
    {
        MaterializedResult actual = computeActual("SELECT orderstatus, approx_distinct(custkey) FROM orders GROUP BY orderstatus");
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 995L)
                .row("F", 993L)
                .row("P", 303L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproximateCountDistinctGroupByWithStandardError()
    {
        MaterializedResult actual = computeActual("SELECT orderstatus, approx_distinct(custkey, 0.023) FROM orders GROUP BY orderstatus");
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 995L)
                .row("F", 993L)
                .row("P", 303L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testCountBoolean()
    {
        assertQuery("SELECT COUNT(true) FROM orders");
    }

    @Test
    public void testJoinWithMultiFieldGroupBy()
    {
        assertQuery("SELECT orderstatus FROM lineitem JOIN (SELECT DISTINCT orderkey, orderstatus FROM ORDERS) T on lineitem.orderkey = T.orderkey");
    }

    @Test
    public void testGroupByRepeatedField()
    {
        assertQuery("SELECT sum(custkey) FROM orders GROUP BY orderstatus, orderstatus");
    }

    @Test
    public void testGroupByRepeatedField2()
    {
        assertQuery("SELECT count(*) FROM (select orderstatus a, orderstatus b FROM orders) GROUP BY a, b");
    }

    @Test
    public void testGroupByMultipleFieldsWithPredicateOnAggregationArgument()
    {
        assertQuery("SELECT custkey, orderstatus, MAX(orderkey) FROM ORDERS WHERE orderkey = 1 GROUP BY custkey, orderstatus");
    }

    @Test
    public void testReorderOutputsOfGroupByAggregation()
    {
        assertQuery(
                "SELECT orderstatus, a, custkey, b FROM (SELECT custkey, orderstatus, -COUNT(*) a, MAX(orderkey) b FROM ORDERS WHERE orderkey = 1 GROUP BY custkey, orderstatus) T");
    }

    @Test
    public void testGroupAggregationOverNestedGroupByAggregation()
    {
        assertQuery("SELECT sum(custkey), max(orderstatus), min(c) FROM (SELECT orderstatus, custkey, COUNT(*) c FROM ORDERS GROUP BY orderstatus, custkey) T");
    }

    @Test
    public void test15WayGroupBy()
    {
        // Among other things, this test verifies we are not getting for overflow in the distributed HashPagePartitionFunction
        assertQuery("" +
                "SELECT " +
                "    orderkey + 1, orderkey + 2, orderkey + 3, orderkey + 4, orderkey + 5, " +
                "    orderkey + 6, orderkey + 7, orderkey + 8, orderkey + 9, orderkey + 10, " +
                "    count(*) " +
                "FROM orders " +
                "GROUP BY " +
                "    orderkey + 1, orderkey + 2, orderkey + 3, orderkey + 4, orderkey + 5, " +
                "    orderkey + 6, orderkey + 7, orderkey + 8, orderkey + 9, orderkey + 10");
    }

    @Test
    public void testDistinctMultipleFields()
    {
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM ORDERS");
    }

    @Test
    public void testDistinctJoin()
    {
        assertQuery("SELECT COUNT(DISTINCT CAST(b.quantity AS BIGINT)), a.orderstatus " +
                "FROM orders a " +
                "JOIN lineitem b " +
                "ON a.orderkey = b.orderkey " +
                "GROUP BY a.orderstatus");
    }

    @Test
    public void testArithmeticNegation()
    {
        assertQuery("SELECT -custkey FROM orders");
    }

    @Test
    public void testDistinct()
    {
        assertQuery("SELECT DISTINCT custkey FROM orders");
    }

    @Test
    public void testDistinctGroupBy()
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count, orderdate");
    }

    @Test
    public void testSingleDistinctOptimizer()
    {
        assertQuery("SELECT custkey, orderstatus, COUNT(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");
        assertQuery("SELECT custkey, orderstatus, COUNT(DISTINCT orderkey), SUM(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");
        assertQuery("" +
                "SELECT custkey, COUNT(DISTINCT orderstatus) FROM (" +
                "   SELECT orders.custkey AS custkey, orders.orderstatus AS orderstatus " +
                "   FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey " +
                "   GROUP BY orders.custkey, orders.orderstatus" +
                ") " +
                "GROUP BY custkey");
        assertQuery("SELECT custkey, COUNT(DISTINCT orderkey), COUNT(DISTINCT orderstatus) FROM orders GROUP BY custkey");

        assertQuery("SELECT SUM(DISTINCT x) FROM (SELECT custkey, COUNT(DISTINCT orderstatus) x FROM orders GROUP BY custkey) t");
    }

    @Test
    public void testExtractDistinctAggregationOptimizer()
    {
        assertQuery("SELECT max(orderstatus), COUNT(orderkey), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT custkey, orderstatus, avg(shippriority), SUM(DISTINCT orderkey) FROM orders GROUP BY custkey, orderstatus");

        assertQuery("SELECT s, MAX(custkey), SUM(a) FROM (" +
                "    SELECT custkey, avg(shippriority) as a, SUM(DISTINCT orderkey) as s FROM orders GROUP BY custkey, orderstatus" +
                ") " +
                "GROUP BY s");

        assertQuery("SELECT max(orderstatus), COUNT(distinct orderkey), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT max(orderstatus), COUNT(distinct shippriority), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT COUNT(tan(shippriority)), sum(DISTINCT orderkey) FROM orders");

        assertQuery("SELECT count(DISTINCT a), max(b) FROM (VALUES (row(1, 2), 3)) t(a, b)", "VALUES (1, 3)");

        // Test overlap between GroupBy columns and aggregation columns
        assertQuery("SELECT shippriority, MAX(orderstatus), SUM(DISTINCT shippriority) FROM orders GROUP BY shippriority");

        assertQuery("SELECT shippriority, COUNT(shippriority), SUM(DISTINCT orderkey) FROM orders GROUP BY shippriority");

        assertQuery("SELECT shippriority, COUNT(shippriority), SUM(DISTINCT shippriority) FROM orders GROUP BY shippriority");

        assertQuery("SELECT clerk, shippriority, MAX(orderstatus), SUM(DISTINCT shippriority) FROM orders GROUP BY clerk, shippriority");

        assertQuery("SELECT clerk, shippriority, COUNT(shippriority), SUM(DISTINCT orderkey) FROM orders GROUP BY clerk, shippriority");

        assertQuery("SELECT clerk, shippriority, COUNT(shippriority), SUM(DISTINCT shippriority) FROM orders GROUP BY clerk, shippriority");
    }

    @Test
    public void testDistinctHaving()
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) AS count " +
                "FROM orders " +
                "GROUP BY orderdate " +
                "HAVING COUNT(DISTINCT clerk) > 1");
    }

    @Test
    public void testAggregationFilter()
    {
        assertQuery("SELECT sum(x) FILTER (WHERE y > 4) FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)", "SELECT 4");
        assertQuery("SELECT sum(x) FILTER (WHERE x > 1), sum(y) FILTER (WHERE y > 4) FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)", "SELECT 8, 5");
        assertQuery("SELECT sum(x) FILTER (WHERE x > 1), sum(x) FROM (VALUES (1), (2), (2), (4)) t (x)", "SELECT 8, 9");
        assertQuery("SELECT count(*) FILTER (WHERE x > 1), sum(x) FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)", "SELECT 3, 9");
        assertQuery("SELECT count(*) FILTER (WHERE x > 1), count(DISTINCT y) FROM (VALUES (1, 10), (2, 10), (3, 10), (4, 20)) t (x, y)", "SELECT 3, 2");

        assertQuery("" +
                        "SELECT sum(b) FILTER (WHERE true) " +
                        "FROM (SELECT count(*) FILTER (WHERE true) AS b)",
                "SELECT 1");

        assertQuery("SELECT count(1) FILTER (WHERE orderstatus = 'O') FROM orders", "SELECT count(*) FROM orders WHERE orderstatus = 'O'");

        // TODO: enable when DISTINCT is allowed with filtered aggregations
        // assertQuery("SELECT count(distinct x) FILTER (where y = 1) FROM (VALUES (2, 1), (1, 2), (1,1)) t(x, y)", "SELECT 2");
        // assertQuery("SELECT sum(DISTINCT x) FILTER (WHERE x > 1) AS x FROM (VALUES (1), (2), (2), (4)) t (x)", "SELECT 6");
        // assertQuery("SELECT sum(DISTINCT x) FILTER (WHERE y > 3), sum(DISTINCT y) FILTER (WHERE x > 1) FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)", "SELECT 6, 9");
        // assertQuery("SELECT sum(x) FILTER (WHERE x > 1) AS x, sum(DISTINCT x) FROM (VALUES (1), (2), (2), (4)) t (x)", "SELECT 8, 9");
    }

    @Test
    public void testDistinctWindow()
    {
        MaterializedResult actual = computeActual(
                "SELECT RANK() OVER (PARTITION BY orderdate ORDER BY COUNT(DISTINCT clerk)) rnk " +
                        "FROM orders " +
                        "GROUP BY orderdate, custkey " +
                        "ORDER BY rnk " +
                        "LIMIT 1");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT).row(1L).build();
        assertEquals(actual, expected);
    }

    @Test
    public void testDistinctWhere()
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) FROM orders WHERE LENGTH(clerk) > 5");
    }

    @Test
    public void testMultipleDifferentDistinct()
    {
        assertQuery("SELECT COUNT(DISTINCT orderstatus), SUM(DISTINCT custkey) FROM orders");
    }

    @Test
    public void testMultipleDistinct()
    {
        assertQuery(
                "SELECT COUNT(DISTINCT custkey), SUM(DISTINCT custkey) FROM orders",
                "SELECT COUNT(*), SUM(custkey) FROM (SELECT DISTINCT custkey FROM orders) t");
    }

    @Test
    public void testComplexDistinct()
    {
        assertQuery(
                "SELECT COUNT(DISTINCT custkey), " +
                        "SUM(DISTINCT custkey), " +
                        "SUM(DISTINCT custkey + 1.0), " +
                        "AVG(DISTINCT custkey), " +
                        "VARIANCE(DISTINCT custkey) FROM orders",
                "SELECT COUNT(*), " +
                        "SUM(custkey), " +
                        "SUM(custkey + 1.0), " +
                        "AVG(custkey), " +
                        "VARIANCE(custkey) FROM (SELECT DISTINCT custkey FROM orders) t");
    }

    @Test
    public void testDistinctLimit()
    {
        assertQuery("" +
                "SELECT DISTINCT orderstatus, custkey " +
                "FROM (SELECT orderstatus, custkey FROM orders ORDER BY orderkey LIMIT 10) " +
                "LIMIT 10");
        assertQuery("SELECT COUNT(*) FROM (SELECT DISTINCT orderstatus, custkey FROM orders LIMIT 10)");
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM orders WHERE custkey = 1268 LIMIT 2");

        assertQuery("" +
                        "SELECT DISTINCT x " +
                        "FROM (VALUES 1) t(x) JOIN (VALUES 10, 20) u(a) ON t.x < u.a " +
                        "LIMIT 100",
                "SELECT 1");
    }

    @Test
    public void testCountDistinct()
    {
        assertQuery("SELECT COUNT(DISTINCT custkey + 1) FROM orders", "SELECT COUNT(*) FROM (SELECT DISTINCT custkey + 1 FROM orders) t");
    }

    @Test
    public void testDistinctWithOrderBy()
    {
        assertQueryOrdered("SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 10");
    }

    @Test
    public void testDistinctWithOrderByNotInSelect()
    {
        assertQueryFails(
                "SELECT DISTINCT custkey FROM orders ORDER BY orderkey LIMIT 10",
                "line 1:1: For SELECT DISTINCT, ORDER BY expressions must appear in select list");
    }

    @Test
    public void testOrderByLimit()
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey DESC LIMIT 10");
    }

    @Test
    public void testOrderByExpressionWithLimit()
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey + 1 DESC LIMIT 10");
    }

    @Test
    public void testOrderByWithOutputColumnReference()
    {
        assertQueryOrdered("SELECT a*2 AS b FROM (VALUES -1, 0, 2) t(a) ORDER BY b*-1", "VALUES 4, 0, -2");
        assertQueryOrdered("SELECT a*2 AS b FROM (VALUES -1, 0, 2) t(a) ORDER BY b", "VALUES -2, 0, 4");
        assertQueryOrdered("SELECT a*-2 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a*-1", "VALUES 2, 0, -4");
        assertQueryOrdered("SELECT a*-2 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY t.a*-1", "VALUES -4, 0, 2");
        assertQueryOrdered("SELECT a*-2 FROM (VALUES -1, 0, 2) t(a) ORDER BY a*-1", "VALUES -4, 0, 2");
        assertQueryOrdered("SELECT a*-2 FROM (VALUES -1, 0, 2) t(a) ORDER BY t.a*-1", "VALUES -4, 0, 2");
        assertQueryOrdered("SELECT a, a* -1 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY t.a", "VALUES (-1, 1), (0, 0), (2, -2)");
        assertQueryOrdered("SELECT a, a* -2 AS b FROM (VALUES -1, 0, 2) t(a) ORDER BY a + b", "VALUES (2, -4), (0, 0), (-1, 2)");
        assertQueryOrdered("SELECT a as b, a* -2 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a + b", "VALUES (2, -4), (0, 0), (-1, 2)");
        assertQueryOrdered("SELECT a* -2 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a + t.a", "VALUES -4, 0, 2");

        // coercions
        assertQueryOrdered("SELECT 1 x ORDER BY degrees(x)", "VALUES 1");
        assertQueryOrdered("SELECT a + 1 as b FROM (VALUES 1, 2) t(a) ORDER BY -1.0 * b", "VALUES 3, 2");
        assertQueryOrdered("SELECT a as b FROM (VALUES 1, 2) t(a) ORDER BY -1.0 * b", "VALUES 2, 1");
        assertQueryOrdered("SELECT a as a FROM (VALUES 1, 2) t(a) ORDER BY -1.0 * a", "VALUES 2, 1");
        assertQueryOrdered("SELECT 1 x ORDER BY degrees(x)", "VALUES 1");

        // groups
        assertQueryOrdered("select max(a+b), min(a+b) as a FROM (values (1,2),(3,2),(1,5)) t(a,b) GROUP BY a ORDER BY max(t.a+t.b)", "VALUES (5, 5), (6, 3)");
        assertQueryOrdered("select max(a+b), min(a+b) as a FROM (values (1,2),(3,2),(1,5)) t(a,b) GROUP BY a ORDER BY max(t.a+t.b)*-0.1", "VALUES (6, 3), (5, 5)");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY max(b*1.0)", "VALUES 2, 1");
        assertQueryOrdered("SELECT max(a) AS b FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY b", "VALUES 1, 2");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY b*1.0", "VALUES 2, 1");
        assertQueryOrdered("SELECT max(a)*100 AS c FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY max(b) + c", "VALUES 100, 200");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY b", "VALUES 2, 1");
        assertQueryOrdered("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY t.b ORDER BY t.b*1.0", "VALUES 2, 1");
        assertQueryOrdered("SELECT -(a+b) as a, -(a+b) as b, a+b from (values (41, 42), (-41, -42)) t(a,b) group by a+b order by a+b", "VALUES (-83, -83, 83), (83, 83, -83)");
        assertQueryOrdered("SELECT c.a FROM (SELECT CAST(ROW(-a.a) as ROW(a BIGINT)) a FROM (VALUES (2), (1)) a(a) GROUP BY a.a ORDER BY a.a) t(c)", "VALUES -2, -1");
        assertQueryOrdered("select -a as a FROM (values (1,2),(3,2)) t(a,b) GROUP BY GROUPING SETS ((a), (a, b)) ORDER BY -a", "VALUES -1, -1, -3, -3");
        assertQueryOrdered("select a as foo FROM (values (1,2),(3,2)) t(a,b) GROUP BY GROUPING SETS ((a), (a, b)) HAVING b IS NOT NULL ORDER BY -a", "VALUES 3, 1");
        assertQueryOrdered("select max(a) FROM (values (1,2),(3,2)) t(a,b) ORDER BY max(-a)", "VALUES 3");
        assertQueryFails("SELECT max(a) AS a FROM (values (1,2)) t(a,b) GROUP BY b ORDER BY max(a+b)", ".*Invalid reference to output projection attribute from ORDER BY aggregation");

        // lambdas
        assertQueryOrdered("SELECT x as y FROM (values (1,2), (2,3)) t(x, y) GROUP BY x ORDER BY apply(x, x -> -x) + 2*x", "VALUES 1, 2");
        assertQueryOrdered("SELECT -y AS x FROM (values (1,2), (2,3)) t(x, y) GROUP BY y ORDER BY apply(x, x -> -x)", "VALUES -2, -3");
        assertQueryOrdered("SELECT -y AS x FROM (values (1,2), (2,3)) t(x, y) GROUP BY y ORDER BY sum(apply(-y, x -> x * 1.0))", "VALUES -3, -2");

        // distinct
        assertQueryOrdered("SELECT DISTINCT -a as b FROM (VALUES 1, 2) t(a) ORDER BY b", "VALUES -2, -1");
        assertQueryOrdered("SELECT DISTINCT -a as b FROM (VALUES 1, 2) t(a) ORDER BY 1", "VALUES -2, -1");
        assertQueryOrdered("SELECT DISTINCT max(a) as b FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY b", "VALUES 1, 2");
        assertQueryFails("SELECT DISTINCT -a as b FROM (VALUES (1, 2), (3, 4)) t(a, c) ORDER BY c", ".*For SELECT DISTINCT, ORDER BY expressions must appear in select list");
        assertQueryFails("SELECT DISTINCT -a as b FROM (VALUES (1, 2), (3, 4)) t(a, c) ORDER BY 2", ".*ORDER BY position 2 is not in select list");

        // window
        assertQueryOrdered("SELECT a FROM (VALUES 1, 2) t(a) ORDER BY -row_number() OVER ()", "VALUES 2, 1");
        assertQueryOrdered("SELECT -a AS a, first_value(-a) OVER (ORDER BY a ROWS 0 PRECEDING) AS b FROM (VALUES 1, 2) t(a) ORDER BY first_value(a) OVER (ORDER BY a ROWS 0 PRECEDING)", "VALUES (-2, -2), (-1, -1)");
        assertQueryOrdered("SELECT -a AS a FROM (VALUES 1, 2) t(a) ORDER BY first_value(a+t.a*2) OVER (ORDER BY a ROWS 0 PRECEDING)", "VALUES -1, -2");

        assertQueryFails("SELECT a, a* -1 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a", ".*'a' is ambiguous");
    }

    @Test
    public void testLegacyOrderByWithOutputColumnReference()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(LEGACY_ORDER_BY, "true")
                .build();

        assertQueryOrdered(session, "SELECT -a AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a", "VALUES -2, 0, 1");
        assertQueryOrdered(session, "SELECT -a AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY 1.0+a", "VALUES 1, 0, -2");
        assertQueryOrdered(session, "SELECT -a AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY 1", "VALUES -2, 0, 1");
        assertQueryFails(session, "SELECT -a AS b FROM (VALUES -1, 0, 2) t(a) ORDER BY 1.0+b", ".*Column 'b' cannot be resolved");

        // groups
        assertQueryOrdered(session, "select max(a+b), min(a+b) as a FROM (values (1,2),(3,2),(1,5)) t(a,b) GROUP BY a ORDER BY max(a+b)", "VALUES (5, 5), (6, 3)");
        assertQueryOrdered(session, "select max(a+b), min(a+b) as a FROM (values (1,2),(3,2),(1,5)) t(a,b) GROUP BY a ORDER BY 1", "VALUES (5, 5), (6, 3)");
        assertQueryOrdered(session, "SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY max(b)", "VALUES 2, 1");
        assertQueryOrdered(session, "SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY b", "VALUES 2, 1");
        assertQueryOrdered(session, "SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY t.b ORDER BY t.b", "VALUES 2, 1");

        // distinct
        assertQueryOrdered(session, "SELECT DISTINCT -a as b FROM (VALUES 1, 2) t(a) ORDER BY b", "VALUES -2, -1");
        assertQueryOrdered(session, "SELECT DISTINCT -a as b FROM (VALUES 1, 2) t(a) ORDER BY 1", "VALUES -2, -1");
        assertQueryFails(session, "SELECT DISTINCT -a as b FROM (VALUES (1, 2), (3, 4)) t(a, c) ORDER BY c", ".*For SELECT DISTINCT, ORDER BY expressions must appear in select list");
        assertQueryFails(session, "SELECT DISTINCT -a as b FROM (VALUES (1, 2), (3, 4)) t(a, c) ORDER BY 2", ".*ORDER BY position 2 is not in select list");

        // window
        assertQueryOrdered(session, "SELECT a FROM (VALUES 1, 2) t(a) ORDER BY -row_number() OVER ()", "VALUES 2, 1");
        assertQueryOrdered(session, "SELECT -a AS a FROM (VALUES 1, 2) t(a) ORDER BY first_value(a) OVER (ORDER BY a ROWS 0 PRECEDING)", "VALUES -1, -2");
        assertQueryOrdered(session, "SELECT -a AS a FROM (VALUES 1, 2) t(a) ORDER BY first_value(a+t.a*2) OVER (ORDER BY a ROWS 0 PRECEDING)", "VALUES -1, -2");

        assertQueryFails(session, "SELECT a as a, a* -1 AS a FROM (VALUES -1, 0, 2) t(a) ORDER BY a", ".*'a' in ORDER BY is ambiguous");
    }

    @Test
    public void testOrderByWithAggregation()
            throws Exception
    {
        assertQuery("" +
                        "SELECT x, sum(cast(x AS double))\n" +
                        "FROM (VALUES '1.0') t(x)\n" +
                        "GROUP BY x\n" +
                        "ORDER BY sum(cast(t.x AS double))",
                "VALUES ('1.0', 1.0)");
    }

    @Test
    public void testGroupByOrderByLimit()
    {
        assertQueryOrdered("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey ORDER BY SUM(totalprice) DESC LIMIT 10");
    }

    @Test
    public void testLimitZero()
    {
        assertQuery("SELECT custkey, totalprice FROM orders LIMIT 0");
    }

    @Test
    public void testLimitAll()
    {
        assertQuery("SELECT custkey, totalprice FROM orders LIMIT ALL", "SELECT custkey, totalprice FROM orders");
    }

    @Test
    public void testOrderByLimitZero()
    {
        assertQuery("SELECT custkey, totalprice FROM orders ORDER BY orderkey LIMIT 0");
    }

    @Test
    public void testOrderByLimitAll()
    {
        assertQuery("SELECT custkey, totalprice FROM orders ORDER BY orderkey LIMIT ALL", "SELECT custkey, totalprice FROM orders ORDER BY orderkey");
    }

    @Test
    public void testRepeatedAggregations()
    {
        assertQuery("SELECT SUM(orderkey), SUM(orderkey) FROM ORDERS");
    }

    @Test
    public void testRepeatedOutputs()
    {
        assertQuery("SELECT orderkey a, orderkey b FROM ORDERS WHERE orderstatus = 'F'");
    }

    @Test
    public void testRepeatedOutputs2()
    {
        // this test exposed a bug that wasn't caught by other tests that resulted in the execution engine
        // trying to read orderkey as the second field, causing a type mismatch
        assertQuery("SELECT orderdate, orderdate, orderkey FROM orders");
    }

    @Test
    public void testLimit()
    {
        MaterializedResult actual = computeActual("SELECT orderkey FROM ORDERS LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testAggregationWithLimit()
    {
        MaterializedResult actual = computeActual("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey LIMIT 10");
        MaterializedResult all = computeExpected("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testLimitInInlineView()
    {
        MaterializedResult actual = computeActual("SELECT orderkey FROM (SELECT orderkey FROM ORDERS LIMIT 100) T LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testCountAll()
    {
        assertQuery("SELECT COUNT(*) FROM ORDERS");
        assertQuery("SELECT COUNT(42) FROM ORDERS", "SELECT COUNT(*) FROM ORDERS");
        assertQuery("SELECT COUNT(42 + 42) FROM ORDERS", "SELECT COUNT(*) FROM ORDERS");
        assertQuery("SELECT COUNT(null) FROM ORDERS", "SELECT 0");
    }

    @Test
    public void testCountColumn()
    {
        assertQuery("SELECT COUNT(orderkey) FROM ORDERS");
        assertQuery("SELECT COUNT(orderstatus) FROM ORDERS");
        assertQuery("SELECT COUNT(orderdate) FROM ORDERS");
        assertQuery("SELECT COUNT(1) FROM ORDERS");

        assertQuery("SELECT COUNT(NULLIF(orderstatus, 'F')) FROM ORDERS");
        assertQuery("SELECT COUNT(CAST(NULL AS BIGINT)) FROM ORDERS"); // todo: make COUNT(null) work
    }

    @Test
    public void testWildcard()
    {
        assertQuery("SELECT * FROM ORDERS");
    }

    @Test
    public void testMultipleWildcards()
    {
        assertQuery("SELECT *, 123, * FROM ORDERS");
    }

    @Test
    public void testMixedWildcards()
    {
        assertQuery("SELECT *, orders.*, orderkey FROM orders");
    }

    @Test
    public void testQualifiedWildcardFromAlias()
    {
        assertQuery("SELECT T.* FROM ORDERS T");
    }

    @Test
    public void testQualifiedWildcardFromInlineView()
    {
        assertQuery("SELECT T.* FROM (SELECT orderkey + custkey FROM ORDERS) T");
    }

    @Test
    public void testQualifiedWildcard()
    {
        assertQuery("SELECT ORDERS.* FROM ORDERS");
    }

    @Test
    public void testAverageAll()
    {
        assertQuery("SELECT AVG(totalprice) FROM ORDERS");
    }

    @Test
    public void testVariance()
    {
        // int64
        assertQuery("SELECT VAR_SAMP(custkey) FROM ORDERS");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT VAR_SAMP(totalprice) FROM ORDERS");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testVariancePop()
    {
        // int64
        assertQuery("SELECT VAR_POP(custkey) FROM ORDERS");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT VAR_POP(totalprice) FROM ORDERS");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testStdDev()
    {
        // int64
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM ORDERS");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM ORDERS");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testStdDevPop()
    {
        // int64
        assertQuery("SELECT STDDEV_POP(custkey) FROM ORDERS");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM ORDERS ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM ORDERS LIMIT 0) T");

        // double
        assertQuery("SELECT STDDEV_POP(totalprice) FROM ORDERS");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM ORDERS ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM ORDERS LIMIT 0) T");
    }

    @Test
    public void testCountAllWithPredicate()
    {
        assertQuery("SELECT COUNT(*) FROM ORDERS WHERE orderstatus = 'F'");
    }

    @Test
    public void testGroupByArray()
    {
        assertQuery("SELECT col[1], count FROM (SELECT ARRAY[custkey] col, COUNT(*) count FROM ORDERS GROUP BY 1 ORDER BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey ORDER BY custkey");
    }

    @Test
    public void testGroupByMap()
    {
        assertQuery("SELECT col[1], count FROM (SELECT MAP(ARRAY[1], ARRAY[custkey]) col, COUNT(*) count FROM ORDERS GROUP BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testGroupByComplexMap()
    {
        assertQuery("SELECT MAP_KEYS(x)[1] FROM (VALUES MAP(ARRAY['a'], ARRAY[ARRAY[1]]), MAP(ARRAY['b'], ARRAY[ARRAY[2]])) t(x) GROUP BY x", "SELECT * FROM (VALUES 'a', 'b')");
    }

    @Test
    public void testGroupByRow()
    {
        assertQuery("SELECT col.col1, count FROM (SELECT cast(row(custkey, custkey) as row(col0 bigint, col1 bigint)) col, COUNT(*) count FROM ORDERS GROUP BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testJoinCoercion()
    {
        assertQuery("SELECT COUNT(*) FROM orders t join (SELECT * FROM orders LIMIT 1) t2 ON sin(t2.custkey) = 0");
    }

    @Test
    public void testJoinCoercionOnEqualityComparison()
    {
        assertQuery("SELECT o.clerk, avg(o.shippriority), COUNT(l.linenumber) FROM orders o LEFT OUTER JOIN lineitem l ON o.orderkey=l.orderkey AND o.shippriority=1 GROUP BY o.clerk");
    }

    @Test
    public void testGroupByNoAggregations()
    {
        assertQuery("SELECT custkey FROM ORDERS GROUP BY custkey");
    }

    @Test
    public void testGroupByCount()
    {
        assertQuery(
                "SELECT orderstatus, COUNT(*) FROM ORDERS GROUP BY orderstatus",
                "SELECT orderstatus, CAST(COUNT(*) AS INTEGER) FROM orders GROUP BY orderstatus"
        );
    }

    @Test
    public void testGroupByMultipleFields()
    {
        assertQuery("SELECT custkey, orderstatus, COUNT(*) FROM ORDERS GROUP BY custkey, orderstatus");
    }

    @Test
    public void testGroupByWithAlias()
    {
        assertQuery(
                "SELECT orderdate x, COUNT(*) FROM orders GROUP BY orderdate",
                "SELECT orderdate x, CAST(COUNT(*) AS INTEGER) FROM orders GROUP BY orderdate"
        );
    }

    @Test
    public void testGroupBySum()
    {
        assertQuery("SELECT suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testGroupByRequireIntegerCoercion()
    {
        assertQuery("SELECT partkey, COUNT(DISTINCT shipdate), SUM(linenumber) FROM lineitem GROUP BY partkey");
    }

    @Test
    public void testGroupByEmptyGroupingSet()
    {
        assertQuery("SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY ()",
                "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupByWithWildcard()
    {
        assertQuery("SELECT * FROM (SELECT orderkey FROM orders) t GROUP BY orderkey");
    }

    @Test
    public void testSingleGroupingSet()
    {
        assertQuery(
                "SELECT linenumber, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "GROUP BY GROUPING SETS (linenumber)",
                "SELECT linenumber, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "GROUP BY linenumber");
    }

    @Test
    public void testGroupingSets()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testGroupingSetsNoInput()
    {
        assertQuery(
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY linenumber, suppkey " +
                        "UNION " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY suppkey");
    }

    @Test
    public void testGroupingSetsWithGlobalAggregationNoInput()
    {
        assertQuery(
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey), ())",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY linenumber, suppkey " +
                        "UNION " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY suppkey " +
                        "UNION " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0");
    }

    @Test
    public void testGroupingSetsWithSingleDistinct()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testGroupingSetsWithMultipleDistinct()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) FROM lineitem GROUP BY suppkey");
    }

    @Test
    public void testGroupingSetsWithMultipleDistinctNoInput()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY GROUPING SETS ((linenumber, suppkey), (suppkey))",
                "SELECT linenumber, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY linenumber, suppkey " +
                        "UNION " +
                        "SELECT NULL, suppkey, SUM(DISTINCT CAST(quantity AS BIGINT)), COUNT(DISTINCT linestatus) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY suppkey");
    }

    @Test
    public void testGroupingSetsGrandTotalSet()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), ())",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsRepeatedSetsAll()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((), (linenumber, suppkey), (), (linenumber, suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem UNION ALL " +
                        "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsRepeatedSetsAllNoInput()
    {
        assertQuery(
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY GROUPING SETS ((), (linenumber, suppkey), (), (linenumber, suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY linenumber, suppkey " +
                        "UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "UNION ALL " +
                        "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0 " +
                        "GROUP BY linenumber, suppkey " +
                        "UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) " +
                        "FROM lineitem " +
                        "WHERE quantity < 0");
    }

    @Test
    public void testGroupingSetsRepeatedSetsDistinct()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY DISTINCT GROUPING SETS ((), (linenumber, suppkey), (), (linenumber, suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsGrandTotalSetFirst()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((), (linenumber), (linenumber, suppkey))",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT linenumber, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsOnlyGrandTotalSet()
    {
        assertQuery("SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS (())",
                "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsMultipleGrandTotalSets()
    {
        assertQuery("SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((), ())",
                "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem UNION ALL " +
                        "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsMultipleGrandTotalSetsNoInput()
    {
        assertQuery("SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0 GROUP BY GROUPING SETS ((), ())",
                "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0 UNION ALL " +
                        "SELECT SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0");
    }

    @Test
    public void testGroupingSetsAliasedGroupingColumns()
    {
        assertQuery("SELECT lna, lnb, SUM(quantity) " +
                        "FROM (SELECT linenumber lna, linenumber lnb, CAST(quantity AS BIGINT) quantity FROM lineitem) " +
                        "GROUP BY GROUPING SETS ((lna, lnb), (lna), (lnb), ())",
                "SELECT linenumber, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT linenumber, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetMixedExpressionAndColumn()
    {
        assertQuery("SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate), ROLLUP(suppkey)",
                "SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate), suppkey UNION ALL " +
                        "SELECT NULL, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate)");
    }

    @Test
    public void testGroupingSetMixedExpressionAndOrdinal()
    {
        assertQuery("SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY 2, ROLLUP(suppkey)",
                "SELECT suppkey, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate), suppkey UNION ALL " +
                        "SELECT NULL, month(shipdate), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY month(shipdate)");
    }

    @Test
    public void testGroupingSetSubsetAndPartitioning()
    {
        assertQuery("SELECT COUNT_IF(x IS NULL) FROM (" +
                        "SELECT x, y, COUNT(z) FROM (SELECT CAST(lineitem.orderkey AS BIGINT) x, lineitem.linestatus y, SUM(lineitem.quantity) z FROM lineitem " +
                        "JOIN orders ON lineitem.orderkey = orders.orderkey GROUP BY 1, 2) GROUP BY GROUPING SETS ((x, y), ()))",
                "SELECT 1");
    }

    @Test
    public void testGroupingSetPredicatePushdown()
    {
        assertQuery("SELECT * FROM (" +
                        "SELECT COALESCE(orderpriority, 'ALL'), COALESCE(shippriority, -1) sp FROM (" +
                        "SELECT orderpriority, shippriority, COUNT(1) FROM orders GROUP BY GROUPING SETS ((orderpriority), (shippriority)))) WHERE sp=-1",
                "SELECT orderpriority, -1 FROM orders GROUP BY orderpriority");
    }

    @Test
    public void testGroupingSetsAggregateOnGroupedColumn()
    {
        assertQuery("SELECT orderpriority, COUNT(orderpriority) FROM orders GROUP BY ROLLUP (orderpriority)",
                "SELECT orderpriority, COUNT(orderpriority) FROM orders GROUP BY orderpriority UNION " +
                        "SELECT NULL, COUNT(orderpriority) FROM orders");
    }

    @Test
    public void testGroupingSetsMultipleAggregatesOnGroupedColumn()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(suppkey), COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), ())",
                "SELECT linenumber, suppkey, SUM(suppkey), COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, NULL, SUM(suppkey), COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsMultipleAggregatesOnUngroupedColumn()
    {
        assertQuery("SELECT linenumber, suppkey, COUNT(CAST(quantity AS BIGINT)), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), ())",
                "SELECT linenumber, suppkey, COUNT(CAST(quantity AS BIGINT)), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, NULL, COUNT(CAST(quantity AS BIGINT)), SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsMultipleAggregatesWithGroupedColumns()
    {
        assertQuery("SELECT linenumber, suppkey, COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY GROUPING SETS ((linenumber, suppkey), ())",
                "SELECT linenumber, suppkey, COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION " +
                        "SELECT NULL, NULL, COUNT(linenumber), SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testGroupingSetsWithSingleDistinctAndUnion()
    {
        assertQuery("SELECT suppkey, COUNT(DISTINCT linenumber) FROM " +
                        "(SELECT * FROM lineitem WHERE linenumber%2 = 0 UNION ALL SELECT * FROM lineitem WHERE linenumber%2 = 1) " +
                        "GROUP BY GROUPING SETS ((suppkey), ())",
                "SELECT suppkey, COUNT(DISTINCT linenumber) FROM lineitem GROUP BY suppkey UNION ALL " +
                        "SELECT NULL, COUNT(DISTINCT linenumber) FROM lineitem");
    }

    @Test
    public void testGroupingSetsWithSingleDistinctAndUnionGroupedArguments()
    {
        assertQuery("SELECT linenumber, COUNT(DISTINCT linenumber) FROM " +
                        "(SELECT * FROM lineitem WHERE linenumber%2 = 0 UNION ALL SELECT * FROM lineitem WHERE linenumber%2 = 1) " +
                        "GROUP BY GROUPING SETS ((linenumber), ())",
                "SELECT DISTINCT linenumber, 1 FROM lineitem UNION ALL " +
                        "SELECT NULL, COUNT(DISTINCT linenumber) FROM lineitem");
    }

    @Test
    public void testGroupingSetsWithMultipleDistinctAndUnion()
    {
        assertQuery("SELECT linenumber, COUNT(DISTINCT linenumber), SUM(DISTINCT suppkey) FROM " +
                        "(SELECT * FROM lineitem WHERE linenumber%2 = 0 UNION ALL SELECT * FROM lineitem WHERE linenumber%2 = 1) " +
                        "GROUP BY GROUPING SETS ((linenumber), ())",
                "SELECT linenumber, 1, SUM(DISTINCT suppkey) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, COUNT(DISTINCT linenumber), SUM(DISTINCT suppkey) FROM lineitem");
    }

    @Test
    public void testRollup()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY ROLLUP (linenumber, suppkey)",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT linenumber, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testCube()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY CUBE (linenumber, suppkey)",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT linenumber, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem");
    }

    @Test
    public void testCubeNoInput()
    {
        assertQuery("SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0 GROUP BY CUBE (linenumber, suppkey)",
                "SELECT linenumber, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0 GROUP BY linenumber, suppkey UNION ALL " +
                        "SELECT linenumber, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0 GROUP BY linenumber UNION ALL " +
                        "SELECT NULL, suppkey, SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0 GROUP BY suppkey UNION ALL " +
                        "SELECT NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem WHERE quantity < 0");
    }

    @Test
    public void testGroupingCombinationsAll()
    {
        assertQuery("SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, ROLLUP (suppkey, linenumber), CUBE (linenumber)",
                "SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, suppkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, suppkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, NULL, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, suppkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, suppkey, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, suppkey UNION ALL " +
                        "SELECT orderkey, partkey, NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey");
    }

    @Test
    public void testGroupingCombinationsDistinct()
    {
        assertQuery("SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY DISTINCT orderkey, partkey, ROLLUP (suppkey, linenumber), CUBE (linenumber)",
                "SELECT orderkey, partkey, suppkey, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, suppkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, NULL, linenumber, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, linenumber UNION ALL " +
                        "SELECT orderkey, partkey, suppkey, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey, suppkey UNION ALL " +
                        "SELECT orderkey, partkey, NULL, NULL, SUM(CAST(quantity AS BIGINT)) FROM lineitem GROUP BY orderkey, partkey");
    }

    @Test
    public void testRollupOverUnion()
    {
        assertQuery("" +
                        "SELECT orderstatus, sum(orderkey)\n" +
                        "FROM (SELECT orderkey, orderstatus\n" +
                        "      FROM orders\n" +
                        "      UNION ALL\n" +
                        "      SELECT orderkey, orderstatus\n" +
                        "      FROM orders) x\n" +
                        "GROUP BY ROLLUP (orderstatus)",
                "VALUES ('P', 21470000),\n" +
                        "('O', 439774330),\n" +
                        "('F', 438500670),\n" +
                        "(NULL, 899745000)");
    }

    @Test
    public void testIntersect()
    {
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT select regionkey FROM nation WHERE nationkey > 21");
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT DISTINCT SELECT regionkey FROM nation WHERE nationkey > 21",
                "VALUES 1, 3");
        assertQuery(
                "WITH wnation AS (SELECT nationkey, regionkey FROM nation) " +
                        "SELECT regionkey FROM wnation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM wnation WHERE nationkey > 21", "VALUES 1, 3");
        assertQuery(
                "SELECT num FROM (SELECT 1 as num FROM nation WHERE nationkey=10 " +
                        "INTERSECT SELECT 1 FROM nation WHERE nationkey=20) T");
        assertQuery(
                "SELECT nationkey, nationkey / 2 FROM (SELECT nationkey FROM nation WHERE nationkey < 10 " +
                        "INTERSECT SELECT nationkey FROM nation WHERE nationkey > 4) T WHERE nationkey % 2 = 0");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION SELECT 4");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "UNION SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "INTERSECT SELECT 1");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION ALL SELECT 3");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION ALL SELECT 3");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) " +
                        "INTERSECT SELECT * FROM (VALUES 1.0, 2)",
                "VALUES 1.0, 2.0");
        assertQuery("SELECT NULL, NULL INTERSECT SELECT NULL, NULL FROM nation");

        MaterializedResult emptyResult = computeActual("SELECT 100 INTERSECT (SELECT regionkey FROM nation WHERE nationkey <10)");
        assertEquals(emptyResult.getMaterializedRows().size(), 0);
    }

    @Test
    public void testIntersectWithAggregation()
    {
        assertQuery("SELECT COUNT(*) FROM nation INTERSECT SELECT COUNT(regionkey) FROM nation HAVING SUM(regionkey) IS NOT NULL");
        assertQuery("SELECT SUM(nationkey), COUNT(name) FROM (SELECT nationkey,name FROM nation INTERSECT SELECT regionkey, name FROM nation) n");
        assertQuery("SELECT COUNT(*) * 2 FROM nation INTERSECT (SELECT SUM(nationkey) FROM nation GROUP BY regionkey ORDER BY 1 LIMIT 2)");
        assertQuery("SELECT COUNT(a) FROM (SELECT nationkey AS a FROM (SELECT nationkey FROM nation INTERSECT SELECT regionkey FROM nation) n1 INTERSECT SELECT regionkey FROM nation) n2");
        assertQuery("SELECT COUNT(*), SUM(2), regionkey FROM (SELECT nationkey, regionkey FROM nation INTERSECT SELECT regionkey, regionkey FROM nation) n GROUP BY regionkey");
        assertQuery("SELECT COUNT(*) FROM (SELECT nationkey FROM nation INTERSECT SELECT 2) n1 INTERSECT SELECT regionkey FROM nation");
    }

    @Test
    public void testIntersectAllFails()
    {
        assertQueryFails("SELECT * FROM (VALUES 1, 2, 3, 4) INTERSECT ALL SELECT * FROM (VALUES 3, 4)", "line 1:35: INTERSECT ALL not yet implemented");
    }

    @Test
    public void testExcept()
    {
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "EXCEPT select regionkey FROM nation WHERE nationkey > 21");
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "EXCEPT DISTINCT SELECT regionkey FROM nation WHERE nationkey > 21",
                "VALUES 0, 4");
        assertQuery(
                "WITH wnation AS (SELECT nationkey, regionkey FROM nation) " +
                        "SELECT regionkey FROM wnation WHERE nationkey < 7 " +
                        "EXCEPT SELECT regionkey FROM wnation WHERE nationkey > 21",
                "VALUES 0, 4");
        assertQuery(
                "SELECT num FROM (SELECT 1 as num FROM nation WHERE nationkey=10 " +
                        "EXCEPT SELECT 2 FROM nation WHERE nationkey=20) T");
        assertQuery(
                "SELECT nationkey, nationkey / 2 FROM (SELECT nationkey FROM nation WHERE nationkey < 10 " +
                        "EXCEPT SELECT nationkey FROM nation WHERE nationkey > 4) T WHERE nationkey % 2 = 0");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "EXCEPT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION SELECT 3");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "UNION SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "EXCEPT SELECT 1");
        assertQuery(
                "SELECT regionkey FROM (SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "EXCEPT SELECT regionkey FROM nation WHERE nationkey > 21) " +
                        "UNION ALL SELECT 4");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) " +
                        "EXCEPT SELECT * FROM (VALUES 3.0, 2)");
        assertQuery("SELECT NULL, NULL EXCEPT SELECT NULL, NULL FROM nation");

        assertQuery(
                "(SELECT * FROM (VALUES 1) EXCEPT SELECT * FROM (VALUES 0))" +
                        "EXCEPT (SELECT * FROM (VALUES 1) EXCEPT SELECT * FROM (VALUES 1))");

        MaterializedResult emptyResult = computeActual("SELECT 0 EXCEPT (SELECT regionkey FROM nation WHERE nationkey <10)");
        assertEquals(emptyResult.getMaterializedRows().size(), 0);
    }

    @Test
    public void testExceptWithAggregation()
    {
        assertQuery("SELECT COUNT(*) FROM nation EXCEPT SELECT COUNT(regionkey) FROM nation where regionkey < 3 HAVING SUM(regionkey) IS NOT NULL");
        assertQuery("SELECT SUM(nationkey), COUNT(name) FROM (SELECT nationkey, name FROM nation where nationkey < 6 EXCEPT SELECT regionkey, name FROM nation) n");
        assertQuery("(SELECT SUM(nationkey) FROM nation GROUP BY regionkey ORDER BY 1 LIMIT 2) EXCEPT SELECT COUNT(*) * 2 FROM nation");
        assertQuery("SELECT COUNT(a) FROM (SELECT nationkey AS a FROM (SELECT nationkey FROM nation EXCEPT SELECT regionkey FROM nation) n1 EXCEPT SELECT regionkey FROM nation) n2");
        assertQuery("SELECT COUNT(*), SUM(2), regionkey FROM (SELECT nationkey, regionkey FROM nation EXCEPT SELECT regionkey, regionkey FROM nation) n GROUP BY regionkey HAVING regionkey < 3");
        assertQuery("SELECT COUNT(*) FROM (SELECT nationkey FROM nation EXCEPT SELECT 10) n1 EXCEPT SELECT regionkey FROM nation");
    }

    @Test
    public void testExceptAllFails()
    {
        assertQueryFails("SELECT * FROM (VALUES 1, 2, 3, 4) EXCEPT ALL SELECT * FROM (VALUES 3, 4)", "line 1:35: EXCEPT ALL not yet implemented");
    }

    @Test
    public void testCountAllWithComparison()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < discount");
    }

    @Test
    public void testSelectWithComparison()
    {
        assertQuery("SELECT orderkey FROM lineitem WHERE tax < discount");
    }

    @Test
    public void testCountWithNotPredicate()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE NOT tax < discount");
    }

    @Test
    public void testCountWithNullPredicate()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE NULL");
    }

    @Test
    public void testCountWithIsNullPredicate()
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') IS NULL",
                "SELECT COUNT(*) FROM orders WHERE orderstatus = 'F' "
        );
    }

    @Test
    public void testCountWithIsNotNullPredicate()
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') IS NOT NULL",
                "SELECT COUNT(*) FROM orders WHERE orderstatus <> 'F' "
        );
    }

    @Test
    public void testCountWithNullIfPredicate()
    {
        assertQuery("SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') = orderstatus ");
    }

    @Test
    public void testCountWithCoalescePredicate()
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE COALESCE(NULLIF(orderstatus, 'F'), 'bar') = 'bar'",
                "SELECT COUNT(*) FROM orders WHERE orderstatus = 'F'"
        );
    }

    @Test
    public void testCountWithAndPredicate()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < discount AND tax > 0.01 AND discount < 0.05");
    }

    @Test
    public void testCountWithOrPredicate()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < 0.01 OR discount > 0.05");
    }

    @Test
    public void testCountWithInlineView()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT orderkey FROM lineitem) x");
    }

    @Test
    public void testNestedCount()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT orderkey, COUNT(*) FROM lineitem GROUP BY orderkey) x");
    }

    @Test
    public void testAggregationWithProjection()
    {
        assertQuery("SELECT sum(totalprice * 2) - sum(totalprice) FROM orders");
    }

    @Test
    public void testAggregationWithProjection2()
    {
        assertQuery("SELECT sum(totalprice * 2) + sum(totalprice * 2) FROM orders");
    }

    @Test
    public void testGroupByOnSupersetOfPartitioning()
    {
        assertQuery("SELECT orderdate, c, count(*) FROM (SELECT orderdate, count(*) c FROM orders GROUP BY orderdate) GROUP BY orderdate, c");
    }

    @Test
    public void testInlineView()
    {
        assertQuery("SELECT orderkey, custkey FROM (SELECT orderkey, custkey FROM ORDERS) U");
    }

    @Test
    public void testAliasedInInlineView()
    {
        assertQuery("SELECT x, y FROM (SELECT orderkey x, custkey y FROM ORDERS) U");
    }

    @Test
    public void testInlineViewWithProjections()
    {
        assertQuery("SELECT x + 1, y FROM (SELECT orderkey * 10 x, custkey y FROM ORDERS) u");
    }

    @Test
    public void testGroupByWithoutAggregation()
    {
        assertQuery("SELECT orderstatus FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testNestedGroupByWithSameKey()
    {
        assertQuery("SELECT custkey, sum(t) FROM (SELECT custkey, count(*) t FROM orders GROUP BY custkey) GROUP BY custkey");
    }

    @Test
    public void testGroupByWithNulls()
    {
        assertQuery("SELECT key, COUNT(*) FROM (" +
                "SELECT CASE " +
                "  WHEN orderkey % 3 = 0 THEN NULL " +
                "  WHEN orderkey % 5 = 0 THEN 0 " +
                "  ELSE orderkey " +
                "  END as key " +
                "FROM lineitem) " +
                "GROUP BY key");
    }

    @Test
    public void testHistogram()
    {
        assertQuery("SELECT lines, COUNT(*) FROM (SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey) U GROUP BY lines");
    }

    @Test
    public void testSimpleJoin()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey");
        assertQuery("" +
                "SELECT COUNT(*) FROM " +
                "(SELECT orderkey FROM lineitem WHERE orderkey < 1000) a " +
                "JOIN " +
                "(SELECT orderkey FROM orders WHERE orderkey < 2000) b " +
                "ON NOT (a.orderkey <= b.orderkey)");
    }

    @Test
    public void testJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = 2");
    }

    @Test
    public void testJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.orderkey = 2");
    }

    @Test
    public void testSimpleJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2");
    }

    @Test
    public void testJoinDoubleClauseWithLeftOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testJoinDoubleClauseWithRightOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey");
    }

    @Test
    public void testJoinWithAlias()
    {
        assertQuery("SELECT * FROM (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) x");
    }

    @Test
    public void testJoinWithConstantExpression()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND 123 = 123");
    }

    @Test
    public void testJoinWithConstantFalseExpressionWithCoercion()
    {
        // Covers #7520

        // Cannot use assertQuery because H2 behaves differently than Presto in CHAR(x) = CHAR(y) comparison, when x != y
        // assertQuery("select (cast ('a' as char(1)) = cast ('a' as char(2)))") would fail
        MaterializedResult actual = computeActual("select count(*) > 0 from nation join region on (cast('a' as char(1)) = cast('a' as char(2)))");

        MaterializedResult expected = resultBuilder(getSession(), BOOLEAN)
                .row(false)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testJoinWithConstantTrueExpressionWithCoercion()
    {
        // Covers #7520
        assertQuery("select count(*) > 0 from nation join region on (cast(1.2 as real) = cast(1.2 as decimal(2,1)))");
    }

    @Test
    public void testJoinWithCanonicalizedConstantFalseExpressionWithCoercion()
    {
        // Covers #7520

        // Cannot use assertQuery because H2 behaves differently than Presto in CHAR(x) = CHAR(y) comparison, when x != y
        // assertQuery("select (cast ('a' as char(1)) = cast ('a' as char(2)))") would fail
        MaterializedResult actual = computeActual("select count(*) > 0 from nation join region ON CAST((CASE WHEN (TRUE IS NOT NULL) THEN 'a' ELSE 'a' END) as char(1)) = CAST('a' as char(2))");

        MaterializedResult expected = resultBuilder(getSession(), BOOLEAN)
                .row(false)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testJoinWithCanonicalizedConstantTrueExpressionWithCoercion()
    {
        // Covers #7520
        assertQuery("select count(*) > 0 from nation join region ON CAST((CASE WHEN (TRUE IS NOT NULL) THEN '1.2' ELSE '1.2' END) as real) = CAST(1.2 as decimal(2,1))");
    }

    @Test
    public void testJoinWithConstantPredicatePushDown()
    {
        assertQuery("" +
                "SELECT\n" +
                "  a.orderstatus\n" +
                "  , a.clerk\n" +
                "FROM (\n" +
                "  SELECT DISTINCT orderstatus, clerk FROM orders\n" +
                ") a\n" +
                "INNER JOIN (\n" +
                "  SELECT DISTINCT orderstatus, clerk FROM orders\n" +
                ") b\n" +
                "ON\n" +
                "  a.orderstatus = b.orderstatus\n" +
                "  and a.clerk = b.clerk\n" +
                "where a.orderstatus = 'F'\n");
    }

    @Test
    public void testJoinWithInferredFalseJoinClause()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM orders\n" +
                "JOIN lineitem\n" +
                "ON CAST(orders.orderkey AS VARCHAR) = CAST(lineitem.orderkey AS VARCHAR)\n" +
                "WHERE orders.orderkey = 1 AND lineitem.orderkey = 2\n");
    }

    @Test
    public void testJoinUsing()
    {
        assertQuery(
                "SELECT COUNT(*) FROM lineitem join orders using (orderkey)",
                "SELECT COUNT(*) FROM lineitem join orders on lineitem.orderkey = orders.orderkey"
        );
    }

    @Test
    public void testJoinCriteriaCoercion()
    {
        assertQuery(
                "SELECT * FROM (VALUES (1.0, 2.0)) x (a, b) JOIN (VALUES (1, 3)) y (a, b) USING(a)",
                "VALUES (1.0, 2.0, 1, 3)"
        );
        assertQuery(
                "SELECT * FROM (VALUES (1, 2)) x (a, b) JOIN (VALUES (CAST (1 AS SMALLINT), CAST(3 AS SMALLINT))) y (a, b) USING(a)",
                "VALUES (1, 2, 1, 3)"
        );
        assertQuery(
                "SELECT * FROM (VALUES (1.0, 2.0)) x (a, b) JOIN (VALUES (1, 3)) y (a, b) ON x.a = y.a",
                "VALUES (1.0, 2.0, 1, 3)"
        );
        assertQuery(
                "SELECT * FROM (VALUES (1, 2)) x (a, b) JOIN (VALUES (SMALLINT '1', SMALLINT '3')) y (a, b) ON x.a = y.a",
                "VALUES (1, 2, 1, 3)"
        );

        // short decimal, long decimal
        assertQuery(
                format("SELECT * FROM " +
                        "   (VALUES (CAST(1 AS DECIMAL(%1$d,0)), 2)) x (a, b) , " +
                        "   (VALUES (CAST(0 AS DECIMAL(%1$d,0)), SMALLINT '3')) y (a, b) " +
                        " WHERE x.a = y.a + 1", Decimals.MAX_SHORT_PRECISION),
                "VALUES (1, 2, 0, 3)");
        assertQuery(
                format("SELECT * FROM " +
                        "   (VALUES (CAST(1 AS DECIMAL(%1$d,0)), 2)) x (a, b) " +
                        "   INNER JOIN " +
                        "   (VALUES (CAST(0 AS DECIMAL(%1$d,0)), SMALLINT '3')) y (a, b) " +
                        "   ON x.a = y.a + 1", Decimals.MAX_SHORT_PRECISION),
                "VALUES (1, 2, 0, 3)");
        assertQuery(
                format("SELECT * FROM " +
                        "   (VALUES (CAST(1 AS DECIMAL(%1$d,0)), 2)) x (a, b) " +
                        "   LEFT JOIN (VALUES (CAST(0 AS DECIMAL(%1$d,0)), SMALLINT '3')) y (a, b) " +
                        "   ON x.a = y.a + 1", Decimals.MAX_SHORT_PRECISION),
                "VALUES (1, 2, 0, 3)");
        assertQuery(
                format("SELECT * FROM " +
                        "   (VALUES CAST(1 as decimal(%d,0))) t1 (a), " +
                        "   (VALUES CAST(1 as decimal(%d,0))) t2 (b) " +
                        "   WHERE a = b", Decimals.MAX_SHORT_PRECISION, Decimals.MAX_SHORT_PRECISION + 1),
                "VALUES (1, 1)");
    }

    @Test
    public void testJoinWithReversedComparison()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.orderkey = lineitem.orderkey");
    }

    @Test
    public void testJoinWithComplexExpressions()
    {
        assertQuery("SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = CAST(orders.orderkey AS BIGINT)");
    }

    @Test
    public void testJoinWithComplexExpressions2()
    {
        assertQuery(
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = CASE WHEN orders.custkey = 1 and orders.orderstatus = 'F' THEN orders.orderkey ELSE NULL END");
    }

    @Test
    public void testJoinWithComplexExpressions3()
    {
        assertQuery(
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey + 1 = orders.orderkey + 1",
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey "
                // H2 takes a million years because it can't join efficiently on a non-indexed field/expression
        );
    }

    @Test
    public void testJoinWithNormalization()
    {
        assertQuery("select COUNT(*) from nation a join nation b on not ((a.nationkey + b.nationkey) <> b.nationkey)");
        assertQuery("select COUNT(*) from nation a join nation b on not (a.nationkey <> b.nationkey)");
        assertQuery("select COUNT(*) from nation a join nation b on not (a.nationkey = b.nationkey)");
        assertQuery("select COUNT(*) from nation a join nation b on not (not cast(a.nationkey as boolean))");
        assertQuery("select COUNT(*) from nation a join nation b on not not not (a.nationkey = b.nationkey)");
    }

    @Test
    public void testSelfJoin()
    {
        assertQuery("SELECT COUNT(*) FROM orders a JOIN orders b on a.orderkey = b.orderkey");
    }

    @Test
    public void testWildcardFromJoin()
    {
        assertQuery(
                "SELECT * FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b using (orderkey)",
                "SELECT * FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b on a.orderkey = b.orderkey"
        );
    }

    @Test
    public void testQualifiedWildcardFromJoin()
    {
        assertQuery(
                "SELECT a.*, b.* FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b using (orderkey)",
                "SELECT a.*, b.* FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b on a.orderkey = b.orderkey"
        );
    }

    @Test
    public void testJoinAggregations()
    {
        assertQuery(
                "SELECT x + y FROM (" +
                        "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN (" +
                        "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b ON a.orderdate = b.orderdate");
    }

    @Test
    public void testNonEqualityJoin()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity + length(orders.comment) > 7");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NOT lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON NOT NOT lineitem.orderkey = orders.orderkey AND NOT NOT lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NOT NOT NOT lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity <= 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity != 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate > orders.orderdate");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderdate < lineitem.shipdate");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment LIKE '%forges%'");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment LIKE lineitem.comment");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.comment LIKE '%forges%'");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.comment LIKE orders.comment");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment NOT LIKE '%forges%'");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.comment NOT LIKE lineitem.comment");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NOT (orders.comment LIKE '%forges%')");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NOT (orders.comment LIKE lineitem.comment)");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity + length(orders.comment) > 7");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND NULL");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2, 11), (2, 10)");
        assertQuery(
                "SELECT COUNT(*) FROM (VALUES 1, 2) t1(a) JOIN (VALUES 10, 11) t2(b) ON a > 2",
                "VALUES (0)");
    }

    @Test
    public void testNonEqualityLeftJoin()
    {
        assertQuery("SELECT COUNT(*) FROM " +
                "      (SELECT * FROM lineitem ORDER BY orderkey,linenumber LIMIT 5) l " +
                "         LEFT OUTER JOIN " +
                "      (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o " +
                "         ON " +
                "      o.custkey != 1000 WHERE o.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000.0 WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > orders.totalprice WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > lineitem.quantity WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE orders.orderkey IS NULL");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > d",
                "VALUES (1, 2, 1, 1), (1, 1, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b < d",
                "VALUES (1, 1, 1, 2), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 2",
                "VALUES (1, 1, NULL,  NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 2",
                "VALUES (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND c = d",
                "VALUES (1, 1, 1, 1), (1, 2, 1, 1)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND c < d",
                "VALUES (1, 1, 1, 2), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON c = d",
                "VALUES (1, 1, 1, 1), (1, 2, 1, 1)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON c < d",
                "VALUES (1, 1, 1, 2), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON 1 = 1",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (1, NULL), (2, 11), (2, 10)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (1, 11), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON a > b",
                "VALUES (1, NULL), (2, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON a < b",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");

        assertQuery(
                "SELECT * FROM (VALUES 1) t1(a) LEFT OUTER JOIN (VALUES (1,2,2), (1,2,3), (1, 2, NULL)) t2(x,y,z) ON a=x AND y = z",
                "VALUES (1, 1, 2, 2)");
    }

    @Test
    public void testNonEqalityJoinWithScalarRequiringSessionParameter()
    {
        assertQuery("SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND from_unixtime(b) > current_timestamp",
                "VALUES (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
    }

    @Test
    public void testNonEqualityJoinWithTryInFilter()
    {
        assertQuery("SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) " +
                        "             ON a=c AND TRY(1 / (b-a) != 1000)",
                "VALUES (1, 1, NULL, NULL), (1, 2, 1, 1), (1, 2, 1, 2)");

        // use of scalar requiring session parameter within try
        assertQuery("SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) LEFT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) " +
                        "             ON a=c AND TRY(1 / (b-a) != 1000 OR from_unixtime(b) > current_timestamp)",
                "VALUES (1, 1, NULL, NULL), (1, 2, 1, 1), (1, 2, 1, 2)");
    }

    @Test
    public void testLeftJoinWithEmptyInnerTable()
    {
        // Use orderkey = rand() to create an empty relation
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON a.orderkey = b.orderkey");
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON a.orderkey > b.orderkey");
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON 1 = 1");
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON b.orderkey > 1");
        assertQuery("SELECT * FROM lineitem a LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) b ON b.orderkey > b.totalprice");
    }

    @Test
    public void testRightJoinWithEmptyInnerTable()
    {
        // Use orderkey = rand() to create an empty relation
        assertQuery("SELECT * FROM orders b RIGHT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON a.orderkey = b.orderkey");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON a.orderkey > b.orderkey");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON 1 = 1");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON b.orderkey > 1");
        assertQuery("SELECT * FROM orders b LEFT JOIN (SELECT * FROM orders WHERE orderkey = rand()) a ON b.orderkey > b.totalprice");
    }

    @Test
    public void testNonEqualityRightJoin()
    {
        assertQuery("SELECT COUNT(*) FROM " +
                "      (SELECT * FROM lineitem ORDER BY orderkey,linenumber LIMIT 5) l " +
                "         RIGHT OUTER JOIN " +
                "      (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o " +
                "         ON " +
                "      l.quantity != 5 WHERE l.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5.0 WHERE lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > lineitem.suppkey WHERE lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity*1000 > orders.totalprice WHERE lineitem.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.totalprice > 1000 WHERE lineitem.orderkey IS NULL");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > d",
                "VALUES (1, 2, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b < d",
                "VALUES (1, 1, 1, 2), (NULL, NULL, 1, 1)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND c = d",
                "VALUES (1, 2, 1, 1), (1, 1, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND c < d",
                "VALUES (NULL, NULL, 1, 1), (1, 2, 1, 2), (1, 1, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON c = d",
                "VALUES (1, 1, 1, 1), (1, 2, 1, 1), (NULL, NULL, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) RIGHT OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON c < d",
                "VALUES (NULL, NULL, 1, 1), (1, 1, 1, 2), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON 1 = 1",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2, 11), (2, 10)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (NULL, 10), (1, 11), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON a > b",
                "VALUES (NULL, 10), (NULL, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON a < b",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");
    }

    @Test
    public void testJoinUsingSymbolsFromJustOneSideOfJoin()
    {
        assertQuery(
                "SELECT b FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (10), (11), (11)");
        assertQuery(
                "SELECT a FROM (VALUES 1, 2) t1(a) RIGHT OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2), (2)");
        assertQuery(
                "SELECT b FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (11), (11)");
        assertQuery(
                "SELECT a FROM (VALUES 1, 2) t1(a) LEFT OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (1), (2), (2)");
        assertQuery(
                "SELECT a FROM (VALUES 1, 2) t1(a) JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2), (2)");
        assertQuery(
                "SELECT b FROM (VALUES 1, 2) t1(a) JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (11), (11)");
    }

    @Test
    public void testJoinsWithTrueJoinCondition()
    {
        // inner join
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (0, 10), (0, 11), (1, 10), (1, 11)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");

        // left join
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) LEFT JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (0, 10), (0, 11), (1, 10), (1, 11)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) LEFT JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) LEFT JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "VALUES (0, NULL), (1, NULL)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) LEFT JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");

        // right join
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) RIGHT JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (0, 10), (0, 11), (1, 10), (1, 11)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) RIGHT JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (NULL, 10), (NULL, 11)");
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) RIGHT JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) RIGHT JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");

        // full join
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) FULL JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (0, 10), (0, 11), (1, 10), (1, 11)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) FULL JOIN (VALUES 10, 11) t2(b) ON TRUE",
                "VALUES (NULL, 10), (NULL, 11)");
        assertQuery("SELECT * FROM (VALUES 0, 1) t1(a) FULL JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "VALUES (0, NULL), (1, NULL)");
        assertQuery("SELECT * FROM (SELECT 1 WHERE FALSE) t1(a) FULL JOIN (SELECT 1 WHERE FALSE) t2(b) ON TRUE",
                "SELECT 1 WHERE FALSE");
    }

    @Test
    public void testNonEqualityFullJoin()
    {
        assertQuery(
                "SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE lineitem.orderkey IS NULL OR orders.orderkey IS NULL",
                "SELECT COUNT(*) FROM " +
                        "(SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 " +
                        "    UNION ALL " +
                        "SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 " +
                        "    WHERE lineitem.orderkey IS NULL) " +
                        " WHERE o1 IS NULL OR o2 IS NULL"
        );
        assertQuery(
                "SELECT COUNT(*) FROM lineitem FULL OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 WHERE lineitem.orderkey IS NULL OR orders.orderkey IS NULL",
                "SELECT COUNT(*) FROM " +
                        "(SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 " +
                        "    UNION ALL " +
                        "SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > 1000 " +
                        "    WHERE lineitem.orderkey IS NULL) " +
                        " WHERE o1 IS NULL OR o2 IS NULL"
        );
        assertQuery(
                "SELECT COUNT(*) FROM lineitem FULL OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > lineitem.quantity WHERE lineitem.orderkey IS NULL OR orders.orderkey IS NULL",
                "SELECT COUNT(*) FROM " +
                        "(SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > lineitem.quantity " +
                        "    UNION ALL " +
                        "SELECT lineitem.orderkey AS o1, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey > lineitem.quantity " +
                        "    WHERE lineitem.orderkey IS NULL) " +
                        " WHERE o1 IS NULL OR o2 IS NULL"
        );
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > d",
                "VALUES (1, 2, 1, 1), (NULL, NULL, 1, 2), (1, 1, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b < d",
                "VALUES (1, 1, 1, 2), (NULL, NULL, 1, 1), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2), (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 2",
                "VALUES (NULL, NULL, 1, 1), (NULL, NULL, 1, 2), (1, 1, NULL, NULL), (1, 2, NULL, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND b > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (1,2)) t1(a,b) FULL OUTER JOIN (VALUES (1,1), (1,2)) t2(c,d) ON a=c AND d > 0",
                "VALUES (1, 1, 1, 1), (1, 1, 1, 2), (1, 2, 1, 1), (1, 2, 1, 2)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON a > 1",
                "VALUES (2, 11), (2, 10), (1, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON b > 10",
                "VALUES (NULL, 10), (1, 11), (2, 11)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON a > b",
                "VALUES (NULL, 10), (NULL, 11), (1, NULL), (2, NULL)");
        assertQuery(
                "SELECT * FROM (VALUES 1, 2) t1(a) FULL OUTER JOIN (VALUES 10, 11) t2(b) ON a < b",
                "VALUES (1, 10), (1, 11), (2, 10), (2, 11)");
    }

    @Test
    public void testJoinOnMultipleFields()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate = orders.orderdate");
    }

    @Test
    public void testJoinUsingMultipleFields()
    {
        assertQuery(
                "SELECT COUNT(*) FROM lineitem JOIN (SELECT orderkey, orderdate shipdate FROM ORDERS) T USING (orderkey, shipdate)",
                "SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate = orders.orderdate"
        );
    }

    @Test
    public void testColocatedJoinWithLocalUnion()
    {
        assertQuery(
                "select count(*) from ((select * from orders) union all (select * from orders)) join orders using (orderkey)",
                "select 2 * count(*) from orders");
    }

    @Test
    public void testJoinWithNonJoinExpression()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey = 1");
    }

    @Test
    public void testJoinWithNullValues()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 512 = 0\n" +
                ") AS lineitem \n" +
                "JOIN (\n" +
                "  SELECT CASE WHEN orderkey % 2 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 512 = 0\n" +
                ") AS orders\n" +
                "ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testJoinWithMultipleInSubqueryClauses()
    {
        QueryTemplate.Parameter type = parameter("type").of("");
        QueryTemplate.Parameter condition = parameter("condition").of("true");
        QueryTemplate queryTemplate = queryTemplate(
                "SELECT * FROM (VALUES 1,2,3,4) t(x) %type% JOIN (VALUES 1,2,3,5) t2(y) ON %condition%",
                type,
                condition);

        QueryTemplate.Parameter twoDuplicatedInSubqueriesCondition = condition.of(
                "(x in (VALUES 1,2,3)) = (y in (VALUES 1,2,3)) AND (x in (VALUES 1,2,4)) = (y in (VALUES 1,2,4))");
        assertQuery(
                queryTemplate.replace(twoDuplicatedInSubqueriesCondition),
                "VALUES (1,1), (1,2), (2,2), (2,1), (3,3)");
        assertQuery(
                queryTemplate.replace(condition.of("(x in (VALUES 1,2)) = (y in (VALUES 1,2)) AND (x in (VALUES 1)) = (y in (VALUES 3))")),
                "VALUES (2,2), (2,1), (3,5), (4,5)");
        assertQuery(
                queryTemplate.replace(condition.of("(x in (VALUES 1,2)) = (y in (VALUES 1,2)) AND (x in (VALUES 1)) != (y in (VALUES 3))")),
                "VALUES (1,2), (1,1), (3, 3), (4,3)");
        assertQuery(
                queryTemplate.replace(condition.of("(x in (VALUES 1)) = (y in (VALUES 1)) AND (x in (SELECT 2)) != (y in (SELECT 2))")),
                "VALUES (2,3), (2,5), (3, 2), (4,2)");

        QueryTemplate.Parameter left = type.of("left");
        QueryTemplate.Parameter right = type.of("right");
        QueryTemplate.Parameter full = type.of("full");
        for (QueryTemplate.Parameter joinType : ImmutableList.of(left, right, full)) {
            for (String joinCondition : ImmutableList.of("x IN (VALUES 1)", "y in (VALUES 1)")) {
                assertQueryFails(
                        queryTemplate.replace(joinType, condition.of(joinCondition)),
                        ".*IN with subquery predicate in join condition is not supported");
            }
        }

        assertQuery(
                queryTemplate.replace(left, twoDuplicatedInSubqueriesCondition),
                "VALUES (1,1), (1,2), (2,2), (2,1), (3,3), (4, null)");
        assertQuery(
                queryTemplate.replace(right, twoDuplicatedInSubqueriesCondition),
                "VALUES (1,1), (1,2), (2,2), (2,1), (3,3), (null, 5)");
        assertQuery(
                queryTemplate.replace(full, twoDuplicatedInSubqueriesCondition),
                "VALUES (1,1), (1,2), (2,2), (2,1), (3,3), (4, null), (null, 5)");
    }

    @Test
    public void testJoinWithInSubqueryToBeExecutedAsPostJoinFilter()
    {
        QueryTemplate.Parameter type = parameter("type").of("");
        QueryTemplate.Parameter condition = parameter("condition").of("true");
        QueryTemplate queryTemplate = queryTemplate(
                "SELECT * FROM (VALUES 1,2,3,4) t(x) %type% JOIN (VALUES 1,2,3,5) t2(y) ON %condition%",
                type,
                condition);

        assertQuery(
                queryTemplate.replace(condition.of("(x+y in (VALUES 4))")),
                "VALUES (1,3), (2,2), (3,1)");
        assertQuery(
                queryTemplate.replace(condition.of("(x+y in (VALUES 4)) AND (x*y in (VALUES 4,5))")),
                "VALUES (2,2)");
        assertQuery(
                queryTemplate.replace(condition.of("(x+y in (VALUES 4,5)) AND (x*y IN (VALUES 4,5))")),
                "VALUES (4,1), (2,2)");
        assertQuery(
                queryTemplate.replace(condition.of("(x+y in (VALUES 4,5)) AND (x in (VALUES 4,5)) != (y in (VALUES 4,5))")),
                "VALUES (4,1)");

        for (QueryTemplate.Parameter joinType : type.of("left", "right", "full")) {
            assertQueryFails(
                    queryTemplate.replace(
                            joinType,
                            condition.of("(x+y in (VALUES 4,5)) AND (x in (VALUES 4,5)) != (y in (VALUES 4,5))")),
                    ".*IN with subquery predicate in join condition is not supported");
        }
    }

    @Test
    public void testOuterJoinWithComplexCorrelatedSubquery()
    {
        QueryTemplate.Parameter type = parameter("type");
        QueryTemplate.Parameter condition = parameter("condition");
        QueryTemplate queryTemplate = queryTemplate(
                "SELECT * FROM (VALUES 1,2,3,4) t(x) %type% JOIN (VALUES 1,2,3,5) t2(y) ON %condition%",
                type,
                condition);

        queryTemplate.replaceAll(
                (query) -> assertQueryFails(query, "line .*: .* is not supported"),
                ImmutableList.of(type.of("left"), type.of("right"), type.of("full")),
                ImmutableList.of(
                        condition.of("EXISTS(SELECT 1 WHERE x = y)"),
                        condition.of("(SELECT x = y)"),
                        condition.of("true IN (SELECT x = y)")));
    }

    @Test
    public void testJoinWithMultipleScalarSubqueryClauses()
    {
        QueryTemplate.Parameter type = parameter("type").of("");
        QueryTemplate.Parameter condition = parameter("condition");
        QueryTemplate queryTemplate = queryTemplate(
                "SELECT * FROM (VALUES 1,2,3,4) t(x) %type% JOIN (VALUES 1,2,3,5) t2(y) ON %condition%",
                type,
                condition);

        QueryTemplate.Parameter multipleScalarJoinCondition =
                condition.of("(x = (VALUES 1)) AND (y = (VALUES 2)) AND (x in (VALUES 2)) = (y in (VALUES 1))");
        assertQuery(queryTemplate.replace(multipleScalarJoinCondition), "VALUES (1,2)");
        assertQuery(
                queryTemplate.replace(condition.of("(x = (VALUES 2)) = (y > (VALUES 0)) AND (x > (VALUES 1)) = (y < (VALUES 3))")),
                "VALUES (2,2), (2,1)");
        assertQuery(
                queryTemplate.replace(condition.of("(x = (VALUES 1)) = (y = (VALUES 1)) AND (x = (SELECT 2)) != (y = (SELECT 3))")),
                "VALUES (2,5), (2,2), (3,3), (4,3)");

        assertQuery(
                queryTemplate.replace(type.of("left"), multipleScalarJoinCondition),
                "VALUES (1,2), (2,null), (3, null), (4, null)");
        assertQuery(
                queryTemplate.replace(type.of("right"), multipleScalarJoinCondition),
                "VALUES (1,2), (null,1), (null, 3), (null, 5)");
        assertQuery(
                queryTemplate.replace(type.of("full"), multipleScalarJoinCondition),
                "VALUES (1,2), (2,null), (3, null), (4, null), (null,1), (null, 3), (null, 5)");
    }

    @Test
    public void testJoinWithScalarSubqueryToBeExecutedAsPostJoinFilter()
    {
        QueryTemplate.Parameter type = parameter("type").of("");
        QueryTemplate.Parameter condition = parameter("condition");
        QueryTemplate queryTemplate = queryTemplate(
                "SELECT * FROM (VALUES 1,2,3,4) t(x) %type% JOIN (VALUES 1,2,3,5) t2(y) ON %condition%",
                type,
                condition);

        QueryTemplate.Parameter xPlusYEqualsSubqueryJoinCondition = condition.of("(x+y = (SELECT 4))");
        assertQuery(
                queryTemplate.replace(xPlusYEqualsSubqueryJoinCondition),
                "VALUES (1,3), (2,2), (3,1)");
        assertQuery(queryTemplate.replace(condition.of("(x+y = (VALUES 4)) AND (x*y = (VALUES 4))")), "VALUES (2,2)");

        // all combination of duplicated subquery
        assertQuery(
                queryTemplate.replace(condition.of("x+y > (VALUES 3) AND (x = (VALUES 3)) != (y = (VALUES 3))")),
                "VALUES (3,1), (3,2), (1,3), (2,3), (4,3), (3,5)");
        assertQuery(
                queryTemplate.replace(condition.of("x+y >= (VALUES 5) AND (x = (VALUES 3)) != (y = (VALUES 3))")),
                "VALUES (3,2), (2,3), (4,3), (3,5)");
        assertQuery(
                queryTemplate.replace(condition.of("x+y >= (VALUES 3) AND (x = (VALUES 5)) != (y = (VALUES 3))")),
                "VALUES (1,3), (2,3), (3,3), (4,3)");
        assertQuery(
                queryTemplate.replace(condition.of("x+y >= (VALUES 3) AND (x = (VALUES 3)) != (y = (VALUES 5))")),
                "VALUES (3,1), (3,2), (3,3), (1,5), (2,5), (4,5)");
        assertQuery(
                queryTemplate.replace(condition.of("x+y >= (VALUES 4) AND (x = (VALUES 3)) != (y = (VALUES 5))")),
                "VALUES (3,1), (3,2), (3,3), (1,5), (2,5), (4,5)");

        // non inner joins
        assertQuery(
                queryTemplate.replace(type.of("left"), xPlusYEqualsSubqueryJoinCondition),
                "VALUES (1,3), (2,2), (3,1), (4, null)");
        assertQuery(
                queryTemplate.replace(type.of("right"), xPlusYEqualsSubqueryJoinCondition),
                "VALUES (1,3), (2,2), (3,1), (null, 5)");
        assertQuery(
                queryTemplate.replace(type.of("full"), xPlusYEqualsSubqueryJoinCondition),
                "VALUES (1,3), (2,2), (3,1), (4, null), (null, 5)");
    }

    @Test
    public void testJoinWithScalarSubqueryInOnClause()
    {
        assertQuery(
                "SELECT count() FROM nation a" +
                        " INNER JOIN nation b ON a.name = (SELECT max(name) FROM nation)" +
                        " INNER JOIN nation c ON c.name = split_part(b.name,'<',2)",
                "SELECT 0");
    }

    @Test
    public void testJoinWithScalarSubqueryToBeExecutedAsPostJoinFilterWithEmptyInnerTable()
    {
        String noOutputQuery = "SELECT 1 WHERE false";
        QueryTemplate.Parameter type = parameter("type").of("");
        QueryTemplate.Parameter condition = parameter("condition");
        QueryTemplate queryTemplate = queryTemplate(
                "SELECT * FROM (" + noOutputQuery + ") t(x) %type% JOIN (VALUES 1) t2(y) ON %condition%",
                type);

        QueryTemplate.Parameter xPlusYEqualsSubqueryJoinCondition = condition.of("(x+y = (SELECT 4))");
        assertQuery(queryTemplate.replace(xPlusYEqualsSubqueryJoinCondition), noOutputQuery);
        assertQuery(queryTemplate.replace(condition.of("(x+y = (VALUES 4)) AND (x*y = (VALUES 4))")), noOutputQuery);

        // non inner joins
        assertQuery(queryTemplate.replace(xPlusYEqualsSubqueryJoinCondition, type.of("left")), noOutputQuery);
        assertQuery(queryTemplate.replace(xPlusYEqualsSubqueryJoinCondition, type.of("right")), "VALUES (null,1)");
        assertQuery(queryTemplate.replace(xPlusYEqualsSubqueryJoinCondition, type.of("full")), "VALUES (null,1)");
    }

    @Test
    public void testInUncorrelatedSubquery()
    {
        assertQuery(
                "SELECT CASE WHEN false THEN 1 IN (VALUES 2) END",
                "SELECT NULL");
        assertQuery(
                "SELECT x FROM (VALUES 2) t(x) where MAP(ARRAY[8589934592], ARRAY[x]) IN (VALUES MAP(ARRAY[8589934592],ARRAY[2]))",
                "SELECT 2");
        assertQuery(
                "SELECT a IN (VALUES 2), a FROM (VALUES (2)) t(a)",
                "SELECT TRUE, 2");
    }

    @Test
    public void testJoinWithExpressionsThatMayReturnNull()
    {
        assertQuery("" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "    SELECT a, nullif(a, 1)\n" +
                        "    FROM (VALUES 1) w(a)\n" +
                        ") t(a,b)\n" +
                        "JOIN (VALUES 1) u(x) ON t.a = u.x",
                "SELECT 1, NULL, 1");

        assertQuery("" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "    SELECT a, contains(array[2, null], a)\n" +
                        "    FROM (VALUES 1) w(a)\n" +
                        ") t(a,b)\n" +
                        "JOIN (VALUES 1) u(x) ON t.a = u.x\n",
                "SELECT 1, NULL, 1");

        assertQuery("" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "    SELECT a, array[null][a]\n" +
                        "    FROM (VALUES 1) w(a)\n" +
                        ") t(a,b)\n" +
                        "JOIN (VALUES 1) u(x) ON t.a = u.x",
                "SELECT 1, NULL, 1");

        assertQuery("" +
                        "SELECT *\n" +
                        "FROM (\n" +
                        "    SELECT a, try(a / 0)\n" +
                        "    FROM (VALUES 1) w(a)\n" +
                        ") t(a,b)\n" +
                        "JOIN (VALUES 1) u(x) ON t.a = u.x",
                "SELECT 1, NULL, 1");
    }

    @Test
    public void testLeftFilteredJoin()
    {
        // Test predicate move around
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testRightFilteredJoin()
    {
        // Test predicate move around
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM lineitem JOIN (SELECT *  FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testJoinWithFullyPushedDownJoinClause()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.custkey = 1 AND lineitem.orderkey = 1");
    }

    @Test
    public void testJoinPredicateMoveAround()
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8 AND lineitem.linenumber % 2 = 0\n" +
                "WHERE orders.custkey % 8 < 7 AND orders.custkey % 8 = lineitem.orderkey % 8 AND lineitem.suppkey % 7 > orders.custkey % 7");
    }

    @Test
    public void testSimpleFullJoin()
    {
        assertQuery("SELECT a, b FROM (VALUES (1), (2)) t (a) FULL OUTER JOIN (VALUES (1), (3)) u (b) ON a = b",
                "SELECT * FROM (VALUES (1, 1), (2, NULL), (NULL, 3))");
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey " +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey " +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
        assertQuery("SELECT COUNT(*) FROM lineitem FULL OUTER JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey " +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey " +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");

        // The above outer join queries will produce the same result even if they are inner join.
        // The below query uses "orderkey = custkey" as join condition.
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.custkey",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.custkey " +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.custkey " +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testFullJoinNormalizedToLeft()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey WHERE lineitem.orderkey IS NOT NULL",
                "SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey WHERE lineitem.orderkey IS NOT NULL");

        // The above outer join queries will produce the same result even if they are inner join.
        // The below query uses "orderkey = custkey" as join condition.
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.custkey WHERE lineitem.orderkey IS NOT NULL",
                "SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.custkey WHERE lineitem.orderkey IS NOT NULL");
    }

    @Test
    public void testFullJoinNormalizedToRight()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey WHERE orders.orderkey IS NOT NULL",
                "SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey  WHERE orders.orderkey IS NOT NULL");

        // The above outer join queries will produce the same result even if they are inner join.
        // The below query uses "orderkey = custkey" as join condition.
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.custkey WHERE orders.custkey IS NOT NULL",
                "SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.custkey  WHERE orders.custkey IS NOT NULL");
    }

    @Test
    public void testFullJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem FULL JOIN orders ON lineitem.orderkey = 1024",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = 1024 " +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = 1024 " +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testFullJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem FULL JOIN orders ON orders.orderkey = 1024",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT OUTER JOIN orders ON orders.orderkey = 1024 " +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT OUTER JOIN orders ON orders.orderkey = 1024 " +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testSimpleFullJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2" +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2" +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testSimpleFullJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem FULL JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2",
                "SELECT COUNT(*) FROM (" +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2" +
                        "UNION ALL " +
                        "SELECT lineitem.orderkey, orders.orderkey AS o2 FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2" +
                        "WHERE lineitem.orderkey IS NULL" +
                        ")");
    }

    @Test
    public void testOuterJoinWithNullsOnProbe()
    {
        assertQuery(
                "SELECT DISTINCT a.orderkey FROM " +
                        "(SELECT CASE WHEN orderkey > 10 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "RIGHT OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey");

        assertQuery(
                "SELECT DISTINCT a.orderkey FROM " +
                        "(SELECT CASE WHEN orderkey > 2 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "FULL OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey",
                "SELECT DISTINCT orderkey FROM (" +
                        "SELECT a.orderkey FROM " +
                        "(SELECT CASE WHEN orderkey > 2 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "RIGHT OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey " +
                        "UNION ALL " +
                        "SELECT a.orderkey FROM" +
                        "(SELECT CASE WHEN orderkey > 2 THEN orderkey END orderkey FROM orders WHERE orderkey < 100) a " +
                        "LEFT OUTER JOIN " +
                        "(SELECT * FROM orders WHERE orderkey < 100) b ON a.orderkey = b.orderkey " +
                        "WHERE a.orderkey IS NULL)");
    }

    @Test
    public void testOuterJoinWithCommonExpression()
    {
        MaterializedResult actual = computeActual("SELECT count(1), count(one) " +
                "FROM (values (1, 'a'), (2, 'a')) as l(k, a) " +
                "LEFT JOIN (select k, 1 one from (values 1) as r(k)) r " +
                "ON l.k = r.k GROUP BY a");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(2L, 1L) // (total rows, # of non null values)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testSimpleLeftJoin()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey");

        // With null base (null row value) in dereference expression
        assertQuery(
                "SELECT x.val FROM " +
                        "(SELECT CAST(ROW(v) as ROW(val integer)) FROM (VALUES 1, 2, 3) t(v)) ta (x) " +
                        "LEFT OUTER JOIN " +
                        "(SELECT CAST(ROW(v) as ROW(val integer)) FROM (VALUES 1, 2, 3) t(v)) tb (y) " +
                        "ON x.val=y.val " +
                        "WHERE y.val=1",
                "SELECT 1");
    }

    @Test
    public void testLeftJoinNormalizedToInner()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey WHERE orders.orderkey IS NOT NULL");
    }

    @Test
    public void testLeftJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testLeftJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN orders ON orders.orderkey = 1024");
    }

    @Test
    public void testSimpleLeftJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleLeftJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2");
    }

    @Test
    public void testDoubleFilteredLeftJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredLeftJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testLeftJoinDoubleClauseWithLeftOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testLeftJoinDoubleClauseWithRightOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey");
    }

    @Test
    public void testBuildFilteredLeftJoin()
    {
        assertQuery("SELECT * FROM lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testProbeFilteredLeftJoin()
    {
        assertQuery("SELECT * FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a LEFT JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testLeftJoinPredicateMoveAround()
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "LEFT JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8\n" +
                "WHERE (orders.custkey % 8 < 7 OR orders.custkey % 8 IS NULL) AND orders.custkey % 8 = lineitem.orderkey % 8");
    }

    @Test
    public void testLeftJoinEqualityInference()
    {
        // Test that we can infer orders.orderkey % 4 = orders.custkey % 3 on the inner side
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 4 = 0 AND suppkey % 2 = partkey % 2 AND linenumber % 3 = orderkey % 3) lineitem\n" +
                "LEFT JOIN (SELECT * FROM orders WHERE orderkey % 4 = 0) orders\n" +
                "ON lineitem.linenumber % 3 = orders.orderkey % 4 AND lineitem.orderkey % 3 = orders.custkey % 3\n" +
                "WHERE lineitem.suppkey % 2 = lineitem.linenumber % 3");
    }

    @Test
    public void testLeftJoinWithNullValues()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 512 = 0\n" +
                ") AS lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT CASE WHEN orderkey % 2 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 512 = 0\n" +
                ") AS orders\n" +
                "ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testSimpleRightJoin()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey");

        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.custkey");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testRightJoinNormalizedToInner()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey WHERE lineitem.orderkey IS NOT NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.custkey WHERE lineitem.orderkey IS NOT NULL");
    }

    @Test
    public void testRightJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testRightJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredRightJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredRightJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testSimpleRightJoinWithLeftConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleRightJoinWithRightConstantEquality()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2");
    }

    @Test
    public void testRightJoinDoubleClauseWithLeftOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testRightJoinDoubleClauseWithRightOverlap()
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey");
    }

    @Test
    public void testBuildFilteredRightJoin()
    {
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a RIGHT JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testProbeFilteredRightJoin()
    {
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM lineitem RIGHT JOIN (SELECT *  FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testRightJoinPredicateMoveAround()
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "RIGHT JOIN (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8\n" +
                "WHERE (orders.custkey % 8 < 7 OR orders.custkey % 8 IS NULL) AND orders.custkey % 8 = lineitem.orderkey % 8");
    }

    @Test
    public void testRightJoinEqualityInference()
    {
        // Test that we can infer orders.orderkey % 4 = orders.custkey % 3 on the inner side
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM orders WHERE orderkey % 4 = 0) orders\n" +
                "RIGHT JOIN (SELECT * FROM lineitem WHERE orderkey % 4 = 0 AND suppkey % 2 = partkey % 2 AND linenumber % 3 = orderkey % 3) lineitem\n" +
                "ON lineitem.linenumber % 3 = orders.orderkey % 4 AND lineitem.orderkey % 3 = orders.custkey % 3\n" +
                "WHERE lineitem.suppkey % 2 = lineitem.linenumber % 3");
    }

    @Test
    public void testRightJoinWithNullValues()
    {
        assertQuery("" +
                "SELECT lineitem.orderkey, orders.orderkey\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE partkey % 512 = 0\n" +
                ") AS lineitem \n" +
                "RIGHT JOIN (\n" +
                "  SELECT CASE WHEN orderkey % 2 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders\n" +
                "  WHERE custkey % 512 = 0\n" +
                ") AS orders\n" +
                "ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testJoinWithDuplicateRelations()
    {
        assertQuery("SELECT * FROM orders JOIN orders USING (orderkey)", "SELECT * FROM orders o1 JOIN orders o2 ON o1.orderkey = o2.orderkey");
        assertQuery("SELECT * FROM lineitem x JOIN orders x USING (orderkey)", "SELECT * FROM lineitem l JOIN orders o ON l.orderkey = o.orderkey");
    }

    @Test
    public void testJoinWithStatefulFilterFunction()
    {
        assertQuery("SELECT *\n" +
                        "FROM (VALUES 1, 2) a(id)\n" +
                        "FULL JOIN (VALUES 2, 3) b(id)\n" +
                        "ON (array_intersect(array[a.id], array[b.id]) = array[a.id])",
                "VALUES (1, null), (2, 2), (null, 3)");
    }

    @Test
    public void testAggregationOverRigthJoinOverSingleStreamProbe()
    {
        // this should return one row since value is always 'value'
        // this test verifies that the two streams produced by the right join
        // are handled gathered for the aggergation operator
        assertQueryOrdered("" +
                        "SELECT\n" +
                        "  value\n" +
                        "FROM\n" +
                        "(\n" +
                        "    SELECT\n" +
                        "        key\n" +
                        "    FROM\n" +
                        "        (VALUES 'match') as a(key)\n" +
                        "        LEFT JOIN (SELECT * FROM (VALUES (0)) limit 0) AS x(ignored)\n" +
                        "        ON TRUE\n" +
                        "    GROUP BY 1\n" +
                        ") a\n" +
                        "RIGHT JOIN\n" +
                        "(\n" +
                        "    VALUES\n" +
                        "    ('match', 'value'),\n" +
                        "    ('no-match', 'value')\n" +
                        ") AS b(key, value)\n" +
                        "ON a.key = b.key\n" +
                        "GROUP BY 1\n",
                "VALUES 'value'");
    }

    @Test
    public void testOrderBy()
    {
        assertQueryOrdered("SELECT orderstatus FROM orders ORDER BY orderstatus");
    }

    @Test
    public void testOrderBy2()
    {
        assertQueryOrdered("SELECT orderstatus FROM orders ORDER BY orderkey DESC");
    }

    @Test
    public void testOrderByMultipleFields()
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM orders ORDER BY custkey DESC, orderstatus");
    }

    @Test
    public void testOrderByWithNulls()
    {
        // nulls first
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS FIRST, custkey ASC");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) DESC NULLS FIRST, custkey ASC");

        // nulls last
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS LAST, custkey ASC");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) DESC NULLS LAST, custkey ASC");

        // assure that default is nulls last
        assertQueryOrdered(
                "SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC, custkey ASC",
                "SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS LAST, custkey ASC");
    }

    @Test
    public void testOrderByAlias()
    {
        assertQueryOrdered("SELECT orderstatus x FROM orders ORDER BY x ASC");
    }

    @Test
    public void testOrderByAliasWithSameNameAsUnselectedColumn()
    {
        assertQueryOrdered("SELECT orderstatus orderdate FROM orders ORDER BY orderdate ASC");
    }

    @Test
    public void testOrderByOrdinal()
    {
        assertQueryOrdered("SELECT orderstatus, orderdate FROM orders ORDER BY 2, 1");
    }

    @Test
    public void testOrderByOrdinalWithWildcard()
    {
        assertQueryOrdered("SELECT * FROM orders ORDER BY 1");
    }

    @Test
    public void testGroupByOrdinal()
    {
        assertQuery(
                "SELECT orderstatus, sum(totalprice) FROM orders GROUP BY 1",
                "SELECT orderstatus, sum(totalprice) FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testGroupBySearchedCase()
    {
        assertQuery("SELECT CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END");

        assertQuery(
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY 1",
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' ELSE 'b' END");
    }

    @Test
    public void testGroupBySearchedCaseNoElse()
    {
        // whole CASE in group by clause
        assertQuery("SELECT CASE WHEN orderstatus = 'O' THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' END");

        assertQuery(
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY 1",
                "SELECT CASE WHEN orderstatus = 'O' THEN 'a' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE WHEN orderstatus = 'O' THEN 'a' END");

        assertQuery("SELECT CASE WHEN true THEN orderstatus END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");
    }

    @Test
    public void testGroupByCase()
    {
        // whole CASE in group by clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END");

        assertQuery(
                "SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY 1",
                "SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END");

        // operand in group by clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // condition in group by clause
        assertQuery("SELECT CASE 'O' WHEN orderstatus THEN 'a' ELSE 'b' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // 'then' in group by clause
        assertQuery("SELECT CASE 1 WHEN 1 THEN orderstatus ELSE 'x' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // 'else' in group by clause
        assertQuery("SELECT CASE 1 WHEN 1 THEN 'x' ELSE orderstatus END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");
    }

    @Test
    public void testGroupByCaseNoElse()
    {
        // whole CASE in group by clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY CASE orderstatus WHEN 'O' THEN 'a' END");

        // operand in group by clause
        assertQuery("SELECT CASE orderstatus WHEN 'O' THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // condition in group by clause
        assertQuery("SELECT CASE 'O' WHEN orderstatus THEN 'a' END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        // 'then' in group by clause
        assertQuery("SELECT CASE 1 WHEN 1 THEN orderstatus END, count(*)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");
    }

    @Test
    public void testGroupByCast()
    {
        // whole CAST in group by expression
        assertQuery("SELECT CAST(orderkey AS VARCHAR), count(*) FROM orders GROUP BY CAST(orderkey AS VARCHAR)");

        assertQuery(
                "SELECT CAST(orderkey AS VARCHAR), count(*) FROM orders GROUP BY 1",
                "SELECT CAST(orderkey AS VARCHAR), count(*) FROM orders GROUP BY CAST(orderkey AS VARCHAR)");

        // argument in group by expression
        assertQuery("SELECT CAST(orderkey AS VARCHAR), count(*) FROM orders GROUP BY orderkey");
    }

    @Test
    public void testGroupByCoalesce()
    {
        // whole COALESCE in group by
        assertQuery("SELECT COALESCE(orderkey, custkey), count(*) FROM orders GROUP BY COALESCE(orderkey, custkey)");

        assertQuery(
                "SELECT COALESCE(orderkey, custkey), count(*) FROM orders GROUP BY 1",
                "SELECT COALESCE(orderkey, custkey), count(*) FROM orders GROUP BY COALESCE(orderkey, custkey)"
        );

        // operands in group by
        assertQuery("SELECT COALESCE(orderkey, 1), count(*) FROM orders GROUP BY orderkey");

        // operands in group by
        assertQuery("SELECT COALESCE(1, orderkey), count(*) FROM orders GROUP BY orderkey");
    }

    @Test
    public void testGroupByNullIf()
    {
        // whole NULLIF in group by
        assertQuery("SELECT NULLIF(orderkey, custkey), count(*) FROM orders GROUP BY NULLIF(orderkey, custkey)");

        assertQuery(
                "SELECT NULLIF(orderkey, custkey), count(*) FROM orders GROUP BY 1",
                "SELECT NULLIF(orderkey, custkey), count(*) FROM orders GROUP BY NULLIF(orderkey, custkey)");

        // first operand in group by
        assertQuery("SELECT NULLIF(orderkey, 1), count(*) FROM orders GROUP BY orderkey");

        // second operand in group by
        assertQuery("SELECT NULLIF(1, orderkey), count(*) FROM orders GROUP BY orderkey");
    }

    @Test
    public void testGroupByExtract()
    {
        // whole expression in group by
        assertQuery("SELECT EXTRACT(YEAR FROM now()), count(*) FROM orders GROUP BY EXTRACT(YEAR FROM now())");

        assertQuery(
                "SELECT EXTRACT(YEAR FROM now()), count(*) FROM orders GROUP BY 1",
                "SELECT EXTRACT(YEAR FROM now()), count(*) FROM orders GROUP BY EXTRACT(YEAR FROM now())");

        // argument in group by
        assertQuery("SELECT EXTRACT(YEAR FROM now()), count(*) FROM orders GROUP BY now()");
    }

    @Test
    public void testGroupByNullConstant()
    {
        assertQuery("" +
                "SELECT count(*)\n" +
                "FROM (\n" +
                "  SELECT cast(null as VARCHAR) constant, orderdate\n" +
                "  FROM orders\n" +
                ") a\n" +
                "group by constant, orderdate\n");
    }

    @Test
    public void testChecksum()
    {
        assertQuery("SELECT to_hex(checksum(0))", "select '0000000000000000'");
    }

    @Test
    public void testMaxBy()
    {
        assertQuery("SELECT MAX_BY(orderkey, totalprice) FROM orders", "SELECT orderkey FROM orders ORDER BY totalprice DESC LIMIT 1");
    }

    @Test
    public void testMaxByN()
    {
        assertQuery("SELECT y FROM (SELECT MAX_BY(orderkey, totalprice, 2) mx FROM orders) CROSS JOIN UNNEST(mx) u(y)",
                "SELECT orderkey FROM orders ORDER BY totalprice DESC LIMIT 2");
    }

    @Test
    public void testMinBy()
    {
        assertQuery("SELECT MIN_BY(orderkey, totalprice) FROM orders", "SELECT orderkey FROM orders ORDER BY totalprice ASC LIMIT 1");
        assertQuery("SELECT MIN_BY(a, ROW(b, c)) FROM (VALUES (1, 2, 3), (2, 2, 1)) AS t(a, b, c)", "SELECT 2");
    }

    @Test
    public void testMinByN()
    {
        assertQuery("SELECT y FROM (SELECT MIN_BY(orderkey, totalprice, 2) mx FROM orders) CROSS JOIN UNNEST(mx) u(y)",
                "SELECT orderkey FROM orders ORDER BY totalprice ASC LIMIT 2");
    }

    @Test
    public void testGroupByBetween()
    {
        // whole expression in group by
        assertQuery("SELECT orderkey BETWEEN 1 AND 100 FROM orders GROUP BY orderkey BETWEEN 1 AND 100 ");

        // expression in group by
        assertQuery("SELECT CAST(orderkey BETWEEN 1 AND 100 AS BIGINT) FROM orders GROUP BY orderkey");

        // min in group by
        assertQuery("SELECT CAST(50 BETWEEN orderkey AND 100 AS BIGINT) FROM orders GROUP BY orderkey");

        // max in group by
        assertQuery("SELECT CAST(50 BETWEEN 1 AND orderkey AS BIGINT) FROM orders GROUP BY orderkey");
    }

    @Test
    public void testAggregationImplicitCoercion()
    {
        assertQuery("SELECT 1.0 / COUNT(*) FROM orders");
        assertQuery("SELECT custkey, 1.0 / COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testWindowImplicitCoercion()
    {
        MaterializedResult actual = computeActual("SELECT orderkey, 1.0 / row_number() OVER (ORDER BY orderkey) FROM orders LIMIT 2");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, DOUBLE)
                .row(1L, 1.0)
                .row(2L, 0.5)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testWindowsSameOrdering()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT " +
                "sum(quantity) OVER(PARTITION BY suppkey ORDER BY orderkey)," +
                "min(tax) OVER(PARTITION BY suppkey ORDER BY shipdate)" +
                "FROM lineitem " +
                "ORDER BY 1 " +
                "LIMIT 10");

        MaterializedResult expected = resultBuilder(getSession(), DOUBLE, DOUBLE)
                .row(1.0, 0.0)
                .row(2.0, 0.0)
                .row(2.0, 0.0)
                .row(3.0, 0.0)
                .row(3.0, 0.0)
                .row(4.0, 0.0)
                .row(4.0, 0.0)
                .row(5.0, 0.0)
                .row(5.0, 0.0)
                .row(5.0, 0.0)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testWindowsPrefixPartitioning()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT " +
                "max(tax) OVER(PARTITION BY suppkey, tax ORDER BY receiptdate)," +
                "sum(quantity) OVER(PARTITION BY suppkey ORDER BY orderkey)" +
                "FROM lineitem " +
                "ORDER BY 2, 1 " +
                "LIMIT 10");

        MaterializedResult expected = resultBuilder(getSession(), DOUBLE, DOUBLE)
                .row(0.06, 1.0)
                .row(0.02, 2.0)
                .row(0.06, 2.0)
                .row(0.02, 3.0)
                .row(0.08, 3.0)
                .row(0.03, 4.0)
                .row(0.03, 4.0)
                .row(0.02, 5.0)
                .row(0.03, 5.0)
                .row(0.07, 5.0)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testWindowsDifferentPartitions()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT " +
                "sum(quantity) OVER(PARTITION BY suppkey ORDER BY orderkey)," +
                "count(discount) OVER(PARTITION BY partkey ORDER BY receiptdate)," +
                "min(tax) OVER(PARTITION BY suppkey, tax ORDER BY receiptdate)" +
                "FROM lineitem " +
                "ORDER BY 1, 2 " +
                "LIMIT 10");

        MaterializedResult expected = resultBuilder(getSession(), DOUBLE, BIGINT, DOUBLE)
                .row(1.0, 10L, 0.06)
                .row(2.0, 4L, 0.06)
                .row(2.0, 16L, 0.02)
                .row(3.0, 3L, 0.08)
                .row(3.0, 38L, 0.02)
                .row(4.0, 10L, 0.03)
                .row(4.0, 10L, 0.03)
                .row(5.0, 9L, 0.03)
                .row(5.0, 13L, 0.07)
                .row(5.0, 15L, 0.02)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testWindowsConstantExpression()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT " +
                "sum(size) OVER(PARTITION BY type ORDER BY brand)," +
                "lag(partkey, 1) OVER(PARTITION BY type ORDER BY name)" +
                "FROM part " +
                "ORDER BY 1, 2 " +
                "LIMIT 10");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1L, 315L)
                .row(1L, 881L)
                .row(1L, 1009L)
                .row(3L, 1087L)
                .row(3L, 1187L)
                .row(3L, 1529L)
                .row(4L, 969L)
                .row(5L, 151L)
                .row(5L, 505L)
                .row(5L, 872L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testDependentWindows()
            throws Exception
    {
        // For such query as below generated plan has two adjacent window nodes where second depends on output of first.

        String sql = "WITH " +
                "t1 AS (" +
                "SELECT extendedprice FROM lineitem ORDER BY orderkey, partkey LIMIT 2)," +
                "t2 AS (" +
                "SELECT extendedprice, sum(extendedprice) OVER() AS x FROM t1)," +
                "t3 AS (" +
                "SELECT max(x) OVER() FROM t2) " +
                "SELECT * FROM t3";

        assertQuery(sql, "VALUES 59645.36, 59645.36");
    }

    @Test
    public void testHaving()
    {
        assertQuery("SELECT orderstatus, sum(totalprice) FROM orders GROUP BY orderstatus HAVING orderstatus = 'O'");
    }

    @Test
    public void testHaving2()
    {
        assertQuery("SELECT custkey, sum(orderkey) FROM orders GROUP BY custkey HAVING sum(orderkey) > 400000");
    }

    @Test
    public void testHaving3()
    {
        assertQuery("SELECT custkey, sum(totalprice) * 2 FROM orders GROUP BY custkey");
        assertQuery("SELECT custkey, avg(totalprice + 5) FROM orders GROUP BY custkey");
        assertQuery("SELECT custkey, sum(totalprice) * 2 FROM orders GROUP BY custkey HAVING avg(totalprice + 5) > 10");
    }

    @Test
    public void testHavingWithoutGroupBy()
    {
        assertQuery("SELECT sum(orderkey) FROM orders HAVING sum(orderkey) > 400000");
    }

    @Test
    public void testGroupByAsJoinProbe()
    {
        // we join on customer key instead of order key because
        // orders is effectively distributed on order key due the
        // generated data being sorted
        assertQuery("SELECT " +
                "  b.orderkey, " +
                "  b.custkey, " +
                "  a.custkey " +
                "FROM ( " +
                "  SELECT custkey" +
                "  FROM orders " +
                "  GROUP BY custkey" +
                ") a " +
                "JOIN orders b " +
                "  ON a.custkey = b.custkey ");
    }

    @Test
    public void testJoinEffectivePredicateWithNoRanges()
    {
        assertQuery("" +
                "SELECT * FROM orders a " +
                "   JOIN (SELECT * FROM orders WHERE orderkey IS NULL) b " +
                "   ON a.orderkey = b.orderkey");
    }

    @Test
    public void testColumnAliases()
    {
        assertQuery(
                "SELECT x, T.y, z + 1 FROM (SELECT custkey, orderstatus, totalprice FROM orders) T (x, y, z)",
                "SELECT custkey, orderstatus, totalprice + 1 FROM orders");
    }

    @Test
    public void testSameInputToAggregates()
    {
        assertQuery("SELECT max(a), max(b) FROM (SELECT custkey a, custkey b FROM orders) x");
    }

    @Test
    public void testWindowFunctionWithImplicitCoercion()
    {
        assertQuery("SELECT *, 1.0 * sum(x) OVER () FROM (VALUES 1) t(x)", "SELECT 1, 1.0");
    }

    @SuppressWarnings("PointlessArithmeticExpression")
    @Test
    public void testWindowFunctionsExpressions()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus\n" +
                ", row_number() OVER (ORDER BY orderkey * 2) *\n" +
                "  row_number() OVER (ORDER BY orderkey DESC) + 100\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) x\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, VARCHAR, BIGINT)
                .row(1L, "O", (1L * 10) + 100)
                .row(2L, "O", (2L * 9) + 100)
                .row(3L, "F", (3L * 8) + 100)
                .row(4L, "O", (4L * 7) + 100)
                .row(5L, "F", (5L * 6) + 100)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testWindowFunctionsFromAggregate()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "  SELECT orderstatus, clerk, sales\n" +
                "  , rank() OVER (PARTITION BY x.orderstatus ORDER BY sales DESC) rnk\n" +
                "  FROM (\n" +
                "    SELECT orderstatus, clerk, sum(totalprice) sales\n" +
                "    FROM orders\n" +
                "    GROUP BY orderstatus, clerk\n" +
                "   ) x\n" +
                ") x\n" +
                "WHERE rnk <= 2\n" +
                "ORDER BY orderstatus, rnk");

        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, DOUBLE, BIGINT)
                .row("F", "Clerk#000000090", 2784836.61, 1L)
                .row("F", "Clerk#000000084", 2674447.15, 2L)
                .row("O", "Clerk#000000500", 2569878.29, 1L)
                .row("O", "Clerk#000000050", 2500162.92, 2L)
                .row("P", "Clerk#000000071", 841820.99, 1L)
                .row("P", "Clerk#000001000", 643679.49, 2L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testOrderByWindowFunction()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, row_number() OVER (ORDER BY orderkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY 2 DESC\n" +
                "LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(34L, 10L)
                .row(33L, 9L)
                .row(32L, 8L)
                .row(7L, 7L)
                .row(6L, 6L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testSameWindowFunctionsTwoCoerces()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT 12.0 * row_number() OVER ()/row_number() OVER(),\n" +
                "row_number() OVER()\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY 2 DESC\n" +
                "LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), DOUBLE, BIGINT)
                .row(12.0, 10L)
                .row(12.0, 9L)
                .row(12.0, 8L)
                .row(12.0, 7L)
                .row(12.0, 6L)
                .build();

        assertEquals(actual, expected);

        actual = computeActual("" +
                "SELECT (MAX(x.a) OVER () - x.a) * 100.0 / MAX(x.a) OVER ()\n" +
                "FROM (VALUES 1, 2, 3, 4) x(a)");

        expected = resultBuilder(getSession(), DOUBLE)
                .row(75.0)
                .row(50.0)
                .row(25.0)
                .row(0.0)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testRowNumberNoOptimization()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE NOT rn <= 10");
        MaterializedResult all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());
        assertEquals(actual.getMaterializedRows().size(), all.getMaterializedRows().size() - 10);
        assertContains(all, actual);

        actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn - 5 <= 10");
        all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());
        assertEquals(actual.getMaterializedRows().size(), 15);
        assertContains(all, actual);
    }

    @Test
    public void testRowNumberLimit()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT row_number() OVER (PARTITION BY orderstatus) rn, orderstatus\n" +
                "FROM orders\n" +
                "LIMIT 10");
        assertEquals(actual.getMaterializedRows().size(), 10);

        actual = computeActual("" +
                "SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn\n" +
                "FROM orders\n" +
                "LIMIT 10");
        assertEquals(actual.getMaterializedRows().size(), 10);

        actual = computeActual("" +
                "SELECT row_number() OVER () rn, orderstatus\n" +
                "FROM orders\n" +
                "LIMIT 10");
        assertEquals(actual.getMaterializedRows().size(), 10);

        actual = computeActual("" +
                "SELECT row_number() OVER (ORDER BY orderkey) rn\n" +
                "FROM orders\n" +
                "LIMIT 10");
        assertEquals(actual.getMaterializedRows().size(), 10);
    }

    @Test
    public void testRowNumberMultipleFilters()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (" +
                "   SELECT a, row_number() OVER (PARTITION BY a ORDER BY a) rn\n" +
                "   FROM (VALUES (1), (1), (1), (2), (2), (3)) t (a)) t " +
                "WHERE rn < 3 AND rn % 2 = 0 AND a = 2 LIMIT 2");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(2, 2L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testRowNumberFilterAndLimit()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (" +
                "SELECT a, row_number() OVER (PARTITION BY a ORDER BY a) rn\n" +
                "FROM (VALUES (1), (2), (1), (2)) t (a)) t WHERE rn < 2 LIMIT 2");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1, 1L)
                .row(2, 1L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        actual = computeActual("" +
                "SELECT * FROM (" +
                "SELECT a, row_number() OVER (PARTITION BY a) rn\n" +
                "FROM (VALUES (1), (2), (1), (2), (1)) t (a)) t WHERE rn < 3 LIMIT 2");

        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1, 1L)
                .row(1, 2L)
                .row(2, 1L)
                .row(2, 2L)
                .build();
        assertEquals(actual.getMaterializedRows().size(), 2);
        assertContains(expected, actual);
    }

    @Test
    public void testRowNumberUnpartitionedFilter()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 5 AND orderstatus != 'Z'");
        MaterializedResult all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());
        assertEquals(actual.getMaterializedRows().size(), 5);
        assertContains(all, actual);

        actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn < 5");
        all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 4);
        assertContains(all, actual);

        actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") LIMIT 5");
        all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 5);
        assertContains(all, actual);
    }

    @Test
    public void testRowNumberPartitionedFilter()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 5");
        MaterializedResult all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());

        // there are 3 distinct orderstatus, so expect 15 rows.
        assertEquals(actual.getMaterializedRows().size(), 15);
        assertContains(all, actual);

        // Test for unreferenced outputs
        actual = computeActual("" +
                "SELECT orderkey FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus) rn, orderkey\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 5");
        all = computeExpected("SELECT orderkey FROM ORDERS", actual.getTypes());

        // there are 3 distinct orderstatus, so expect 15 rows.
        assertEquals(actual.getMaterializedRows().size(), 15);
        assertContains(all, actual);
    }

    @Test
    public void testRowNumberJoin()
    {
        MaterializedResult actual = computeActual("SELECT a, rn\n" +
                "FROM (\n" +
                "    SELECT a, row_number() OVER (ORDER BY a) rn\n" +
                "    FROM (VALUES (1), (2)) t (a)\n" +
                ") a\n" +
                "JOIN (VALUES (2)) b (b) ON a.a = b.b\n" +
                "LIMIT 1");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(2, 2L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        actual = computeActual("SELECT a, rn\n" +
                "FROM (\n" +
                "    SELECT a, row_number() OVER (PARTITION BY a ORDER BY a) rn\n" +
                "    FROM (VALUES (1), (2), (1), (2)) t (a)\n" +
                ") a\n" +
                "JOIN (VALUES (2)) b (b) ON a.a = b.b\n" +
                "LIMIT 2");

        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(2, 1L)
                .row(2, 2L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testRowNumberUnpartitionedFilterLimit()
    {
        assertQuery("" +
                "SELECT row_number() OVER ()\n" +
                "FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey\n" +
                "WHERE orders.orderkey = 10000\n" +
                "LIMIT 20");
    }

    @Test
    public void testRowNumberPropertyDerivation()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus, SUM(rn) OVER (PARTITION BY orderstatus) c\n" +
                "FROM (\n" +
                "   SELECT orderkey, orderstatus, row_number() OVER (PARTITION BY orderstatus) rn\n" +
                "   FROM (\n" +
                "       SELECT * FROM orders ORDER BY orderkey LIMIT 10\n" +
                "   )\n" +
                ")");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT, VARCHAR, BIGINT)
                .row(1L, "O", 21L)
                .row(2L, "O", 21L)
                .row(3L, "F", 10L)
                .row(4L, "O", 21L)
                .row(5L, "F", 10L)
                .row(6L, "F", 10L)
                .row(7L, "O", 21L)
                .row(32L, "O", 21L)
                .row(33L, "F", 10L)
                .row(34L, "O", 21L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testWindowPropertyDerivation()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, orderkey,\n" +
                "SUM(s) OVER (PARTITION BY orderstatus),\n" +
                "SUM(s) OVER (PARTITION BY orderstatus, orderkey),\n" +
                "SUM(s) OVER (PARTITION BY orderstatus ORDER BY orderkey),\n" +
                "SUM(s) OVER (ORDER BY orderstatus, orderkey)\n" +
                "FROM (\n" +
                "   SELECT orderkey, orderstatus, SUM(orderkey) OVER (ORDER BY orderstatus, orderkey) s\n" +
                "   FROM (\n" +
                "       SELECT * FROM orders ORDER BY orderkey LIMIT 10\n" +
                "   )\n" +
                ")");
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT)
                .row("F", 3L, 72L, 3L, 3L, 3L)
                .row("F", 5L, 72L, 8L, 11L, 11L)
                .row("F", 6L, 72L, 14L, 25L, 25L)
                .row("F", 33L, 72L, 47L, 72L, 72L)
                .row("O", 1L, 433L, 48L, 48L, 120L)
                .row("O", 2L, 433L, 50L, 98L, 170L)
                .row("O", 4L, 433L, 54L, 152L, 224L)
                .row("O", 7L, 433L, 61L, 213L, 285L)
                .row("O", 32L, 433L, 93L, 306L, 378L)
                .row("O", 34L, 433L, 127L, 433L, 505L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testTopNUnpartitionedWindow()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 5");
        String sql = "SELECT row_number() OVER (), orderkey, orderstatus FROM orders ORDER BY orderkey LIMIT 5";
        MaterializedResult expected = computeExpected(sql, actual.getTypes());
        assertEquals(actual, expected);
    }

    @Test
    public void testTopNUnpartitionedLargeWindow()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 10000");
        String sql = "SELECT row_number() OVER (), orderkey, orderstatus FROM orders ORDER BY orderkey LIMIT 10000";
        MaterializedResult expected = computeExpected(sql, actual.getTypes());
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testTopNPartitionedWindow()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 2");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR)
                .row(1L, 1L, "O")
                .row(2L, 2L, "O")
                .row(1L, 3L, "F")
                .row(2L, 5L, "F")
                .row(1L, 65L, "P")
                .row(2L, 197L, "P")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        // Test for unreferenced outputs
        actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 2");
        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(1L, 3L)
                .row(2L, 5L)
                .row(1L, 65L)
                .row(2L, 197L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 2");
        expected = resultBuilder(getSession(), BIGINT, VARCHAR)
                .row(1L, "O")
                .row(2L, "O")
                .row(1L, "F")
                .row(2L, "F")
                .row(1L, "P")
                .row(2L, "P")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testTopNUnpartitionedWindowWithEqualityFilter()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn = 2");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR)
                .row(2L, 2L, "O")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testTopNUnpartitionedWindowWithCompositeFilter()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn = 1 OR rn IN (3, 4) OR rn BETWEEN 6 AND 7");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR)
                .row(1L, 1L, "O")
                .row(3L, 3L, "F")
                .row(4L, 4L, "O")
                .row(6L, 6L, "F")
                .row(7L, 7L, "O")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testTopNPartitionedWindowWithEqualityFilter()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn = 2");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR)
                .row(2L, 2L, "O")
                .row(2L, 5L, "F")
                .row(2L, 197L, "P")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        // Test for unreferenced outputs
        actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey\n" +
                "   FROM orders\n" +
                ") WHERE rn = 2");
        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(2L, 2L)
                .row(2L, 5L)
                .row(2L, 197L)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn = 2");
        expected = resultBuilder(getSession(), BIGINT, VARCHAR)
                .row(2L, "O")
                .row(2L, "F")
                .row(2L, "P")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testWindowFunctionWithGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT *, rank() OVER (PARTITION BY x)\n" +
                "FROM (SELECT 'foo' x)\n" +
                "GROUP BY 1");

        MaterializedResult expected = resultBuilder(getSession(), createVarcharType(3), BIGINT)
                .row("foo", 1L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testPartialPrePartitionedWindowFunction()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, COUNT(*) OVER (PARTITION BY orderkey, custkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(2L, 1L)
                .row(3L, 1L)
                .row(4L, 1L)
                .row(5L, 1L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testFullPrePartitionedWindowFunction()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, COUNT(*) OVER (PARTITION BY orderkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(2L, 1L)
                .row(3L, 1L)
                .row(4L, 1L)
                .row(5L, 1L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testPartialPreSortedWindowFunction()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, COUNT(*) OVER (ORDER BY orderkey, custkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(3L, 3L)
                .row(4L, 4L)
                .row(5L, 5L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testFullPreSortedWindowFunction()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, COUNT(*) OVER (ORDER BY orderkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(3L, 3L)
                .row(4L, 4L)
                .row(5L, 5L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testFullyPartitionedAndPartiallySortedWindowFunction()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, custkey, orderPriority, COUNT(*) OVER (PARTITION BY orderkey ORDER BY custkey, orderPriority)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey, custkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR, BIGINT)
                .row(1L, 370L, "5-LOW", 1L)
                .row(2L, 781L, "1-URGENT", 1L)
                .row(3L, 1234L, "5-LOW", 1L)
                .row(4L, 1369L, "5-LOW", 1L)
                .row(5L, 445L, "5-LOW", 1L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testFullyPartitionedAndFullySortedWindowFunction()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, custkey, COUNT(*) OVER (PARTITION BY orderkey ORDER BY custkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey, custkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, BIGINT)
                .row(1L, 370L, 1L)
                .row(2L, 781L, 1L)
                .row(3L, 1234L, 1L)
                .row(4L, 1369L, 1L)
                .row(5L, 445L, 1L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testOrderByWindowFunctionWithNulls()
    {
        MaterializedResult actual;
        MaterializedResult expected;

        // Nulls first
        actual = computeActual("" +
                "SELECT orderkey, row_number() OVER (ORDER BY nullif(orderkey, 3) NULLS FIRST)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY 2 ASC\n" +
                "LIMIT 5");

        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(3L, 1L)
                .row(1L, 2L)
                .row(2L, 3L)
                .row(4L, 4L)
                .row(5L, 5L)
                .build();

        assertEquals(actual, expected);

        // Nulls last
        actual = computeActual("" +
                "SELECT orderkey, row_number() OVER (ORDER BY nullif(orderkey, 3) NULLS LAST)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY 2 DESC\n" +
                "LIMIT 5");

        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(3L, 10L)
                .row(34L, 9L)
                .row(33L, 8L)
                .row(32L, 7L)
                .row(7L, 6L)
                .build();

        assertEquals(actual, expected);

        // and nulls last should be the default
        actual = computeActual("" +
                "SELECT orderkey, row_number() OVER (ORDER BY nullif(orderkey, 3))\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY 2 DESC\n" +
                "LIMIT 5");

        assertEquals(actual, expected);
    }

    @Test
    public void testValueWindowFunctions()
    {
        MaterializedResult actual = computeActual("SELECT * FROM (\n" +
                "  SELECT orderkey, orderstatus\n" +
                "    , first_value(orderkey + 1000) OVER (PARTITION BY orderstatus ORDER BY orderkey) fvalue\n" +
                "    , nth_value(orderkey + 1000, 2) OVER (PARTITION BY orderstatus ORDER BY orderkey\n" +
                "        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) nvalue\n" +
                "    FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) x\n" +
                "  ) x\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, VARCHAR, BIGINT, BIGINT)
                .row(1L, "O", 1001L, 1002L)
                .row(2L, "O", 1001L, 1002L)
                .row(3L, "F", 1003L, 1005L)
                .row(4L, "O", 1001L, 1002L)
                .row(5L, "F", 1003L, 1005L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testWindowFrames()
    {
        MaterializedResult actual = computeActual("SELECT * FROM (\n" +
                "  SELECT orderkey, orderstatus\n" +
                "    , sum(orderkey + 1000) OVER (PARTITION BY orderstatus ORDER BY orderkey\n" +
                "        ROWS BETWEEN mod(custkey, 2) PRECEDING AND custkey / 500 FOLLOWING)\n" +
                "    FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) x\n" +
                "  ) x\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, VARCHAR, BIGINT)
                .row(1L, "O", 1001L)
                .row(2L, "O", 3007L)
                .row(3L, "F", 3014L)
                .row(4L, "O", 4045L)
                .row(5L, "F", 2008L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testWindowNoChannels()
    {
        MaterializedResult actual = computeActual("SELECT rank() OVER ()\n" +
                "FROM (SELECT * FROM orders LIMIT 10)\n" +
                "LIMIT 3");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1L)
                .row(1L)
                .row(1L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testScalarFunction()
    {
        assertQuery("SELECT SUBSTR('Quadratically', 5, 6) FROM orders LIMIT 1");
    }

    @Test
    public void testCast()
    {
        assertQuery("SELECT CAST('1' AS BIGINT) FROM orders");
        assertQuery("SELECT CAST(totalprice AS BIGINT) FROM orders");
        assertQuery("SELECT CAST(orderkey AS DOUBLE) FROM orders");
        assertQuery("SELECT CAST(orderkey AS VARCHAR) FROM orders");
        assertQuery("SELECT CAST(orderkey AS BOOLEAN) FROM orders");

        assertQuery("SELECT try_cast('1' AS BIGINT) FROM orders", "SELECT CAST('1' AS BIGINT) FROM orders");
        assertQuery("SELECT try_cast(totalprice AS BIGINT) FROM orders", "SELECT CAST(totalprice AS BIGINT) FROM orders");
        assertQuery("SELECT try_cast(orderkey AS DOUBLE) FROM orders", "SELECT CAST(orderkey AS DOUBLE) FROM orders");
        assertQuery("SELECT try_cast(orderkey AS VARCHAR) FROM orders", "SELECT CAST(orderkey AS VARCHAR) FROM orders");
        assertQuery("SELECT try_cast(orderkey AS BOOLEAN) FROM orders", "SELECT CAST(orderkey AS BOOLEAN) FROM orders");

        assertQuery("SELECT try_cast('foo' AS BIGINT) FROM orders", "SELECT CAST(null AS BIGINT) FROM orders");
        assertQuery("SELECT try_cast(clerk AS BIGINT) FROM orders", "SELECT CAST(null AS BIGINT) FROM orders");
        assertQuery("SELECT try_cast(orderkey * orderkey AS VARCHAR) FROM orders", "SELECT CAST(orderkey * orderkey AS VARCHAR) FROM orders");
        assertQuery("SELECT try_cast(try_cast(orderkey AS VARCHAR) AS BIGINT) FROM orders", "SELECT orderkey FROM orders");
        assertQuery("SELECT try_cast(clerk AS VARCHAR) || try_cast(clerk AS VARCHAR) FROM orders", "SELECT clerk || clerk FROM orders");

        assertQuery("SELECT coalesce(try_cast('foo' AS BIGINT), 456) FROM orders", "SELECT 456 FROM orders");
        assertQuery("SELECT coalesce(try_cast(clerk AS BIGINT), 456) FROM orders", "SELECT 456 FROM orders");

        assertQuery("SELECT CAST(x AS BIGINT) FROM (VALUES 1, 2, 3, NULL) t (x)", "VALUES 1, 2, 3, NULL");
        assertQuery("SELECT try_cast(x AS BIGINT) FROM (VALUES 1, 2, 3, NULL) t (x)", "VALUES 1, 2, 3, NULL");
    }

    @Test
    public void testInvalidCast()
    {
        assertQueryFails(
                "SELECT CAST(1 AS DATE) FROM orders",
                "line 1:8: Cannot cast integer to date");
    }

    @Test
    public void testInvalidCastInMultilineQuery()
    {
        assertQueryFails(
                "SELECT CAST(totalprice AS BIGINT),\n" +
                        "CAST(2015 AS DATE),\n" +
                        "CAST(orderkey AS DOUBLE) FROM orders",
                "line 2:1: Cannot cast integer to date");
    }

    @Test
    public void testTryInvalidCast()
    {
        assertQuery("SELECT TRY(CAST('a' AS BIGINT))",
                "SELECT NULL");
    }

    @Test
    public void testConcatOperator()
    {
        assertQuery("SELECT '12' || '34' FROM orders LIMIT 1");
    }

    @Test
    public void testQuotedIdentifiers()
    {
        assertQuery("SELECT \"TOTALPRICE\" \"my price\" FROM \"ORDERS\"");
    }

    @Test
    public void testInvalidColumn()
    {
        assertQueryFails(
                "select * from lineitem l join (select orderkey_1, custkey from orders) o on l.orderkey = o.orderkey_1",
                "line 1:39: Column 'orderkey_1' cannot be resolved");
    }

    @Test
    public void testUnaliasedSubqueries()
    {
        assertQuery("SELECT orderkey FROM (SELECT orderkey FROM orders)");
    }

    @Test
    public void testUnaliasedSubqueries1()
    {
        assertQuery("SELECT a FROM (SELECT orderkey a FROM orders)");
    }

    @Test
    public void testJoinUnaliasedSubqueries()
    {
        assertQuery(
                "SELECT COUNT(*) FROM (SELECT * FROM lineitem) join (SELECT * FROM orders) using (orderkey)",
                "SELECT COUNT(*) FROM lineitem join orders on lineitem.orderkey = orders.orderkey"
        );
    }

    @Test
    public void testWith()
    {
        assertQuery("" +
                        "WITH a AS (SELECT * FROM orders) " +
                        "SELECT * FROM a",
                "SELECT * FROM orders");
    }

    @Test
    public void testWithQualifiedPrefix()
    {
        assertQuery("" +
                        "WITH a AS (SELECT 123 FROM orders LIMIT 1)" +
                        "SELECT a.* FROM a",
                "SELECT 123 FROM orders LIMIT 1");
    }

    @Test
    public void testWithAliased()
    {
        assertQuery("" +
                        "WITH a AS (SELECT * FROM orders) " +
                        "SELECT * FROM a x",
                "SELECT * FROM orders");
    }

    @Test
    public void testReferenceToWithQueryInFromClause()
    {
        assertQuery(
                "WITH a AS (SELECT * FROM orders)" +
                        "SELECT * FROM (" +
                        "   SELECT * FROM a" +
                        ")",
                "SELECT * FROM orders");
    }

    @Test
    public void testWithChaining()
    {
        assertQuery("" +
                        "WITH a AS (SELECT orderkey n FROM orders)\n" +
                        ", b AS (SELECT n + 1 n FROM a)\n" +
                        ", c AS (SELECT n + 1 n FROM b)\n" +
                        "SELECT n + 1 FROM c",
                "SELECT orderkey + 3 FROM orders");
    }

    @Test
    public void testWithSelfJoin()
    {
        assertQuery("" +
                "WITH x AS (SELECT DISTINCT orderkey FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "SELECT count(*) FROM x a JOIN x b USING (orderkey)", "" +
                "SELECT count(*)\n" +
                "FROM (SELECT DISTINCT orderkey FROM orders ORDER BY orderkey LIMIT 10) a\n" +
                "JOIN (SELECT DISTINCT orderkey FROM orders ORDER BY orderkey LIMIT 10) b ON a.orderkey = b.orderkey");
    }

    @Test
    public void testWithNestedSubqueries()
    {
        assertQuery("" +
                "WITH a AS (\n" +
                "  WITH aa AS (SELECT 123 x FROM orders LIMIT 1)\n" +
                "  SELECT x y FROM aa\n" +
                "), b AS (\n" +
                "  WITH bb AS (\n" +
                "    WITH bbb AS (SELECT y FROM a)\n" +
                "    SELECT bbb.* FROM bbb\n" +
                "  )\n" +
                "  SELECT y z FROM bb\n" +
                ")\n" +
                "SELECT *\n" +
                "FROM (\n" +
                "  WITH q AS (SELECT z w FROM b)\n" +
                "  SELECT j.*, k.*\n" +
                "  FROM a j\n" +
                "  JOIN q k ON (j.y = k.w)\n" +
                ") t", "" +
                "SELECT 123, 123 FROM orders LIMIT 1");
    }

    @Test
    public void testWithColumnAliasing()
    {
        assertQuery(
                "WITH a (id) AS (SELECT 123 FROM orders LIMIT 1) SELECT id FROM a",
                "SELECT 123 FROM orders LIMIT 1");

        assertQuery(
                "WITH t (a, b, c) AS (SELECT 1, custkey x, orderkey FROM orders) SELECT c, b, a FROM t",
                "SELECT orderkey, custkey, 1 FROM orders");
    }

    @Test
    public void testWithHiding()
    {
        assertQuery("" +
                        "WITH a AS (SELECT 1), " +
                        "     b AS (" +
                        "         WITH a AS (SELECT 2)" +
                        "         SELECT * FROM a" +
                        "    )" +
                        "SELECT * FROM b",
                "SELECT 2"
        );
        assertQueryFails("" +
                        "WITH a AS (VALUES 1), " +
                        "     a AS (VALUES 2)" +
                        "SELECT * FROM a",
                "line 1:28: WITH query name 'a' specified more than once"
        );
    }

    @Test
    public void testWithRecursive()
    {
        assertQueryFails(
                "WITH RECURSIVE a AS (SELECT 123) SELECT * FROM a",
                "line 1:1: Recursive WITH queries are not supported");
    }

    @Test
    public void testCaseNoElse()
    {
        assertQuery("SELECT orderkey, CASE orderstatus WHEN 'O' THEN 'a' END FROM orders");
    }

    @Test
    public void testCaseNoElseInconsistentResultType()
    {
        assertQueryFails(
                "SELECT orderkey, CASE orderstatus WHEN 'O' THEN 'a' WHEN '1' THEN 2 END FROM orders",
                "\\Qline 1:67: All CASE results must be the same type: varchar(1)\\E");
    }

    @Test
    public void testCaseWithSupertypeCast()
    {
        assertQuery(" SELECT CASE x WHEN 1 THEN cast(1 as decimal(4,1)) WHEN 2 THEN cast(1 as decimal(4,2)) ELSE cast(1 as decimal(4,3)) END FROM (values 1) t(x)", "SELECT 1.000");
    }

    @Test
    public void testIfExpression()
    {
        assertQuery(
                "SELECT sum(IF(orderstatus = 'F', totalprice, 0.0)) FROM orders",
                "SELECT sum(CASE WHEN orderstatus = 'F' THEN totalprice ELSE 0.0 END) FROM orders");
        assertQuery(
                "SELECT sum(IF(orderstatus = 'Z', totalprice)) FROM orders",
                "SELECT sum(CASE WHEN orderstatus = 'Z' THEN totalprice END) FROM orders");
        assertQuery(
                "SELECT sum(IF(orderstatus = 'F', NULL, totalprice)) FROM orders",
                "SELECT sum(CASE WHEN orderstatus = 'F' THEN NULL ELSE totalprice END) FROM orders");
        assertQuery(
                "SELECT IF(orderstatus = 'Z', orderkey / 0, orderkey) FROM orders",
                "SELECT CASE WHEN orderstatus = 'Z' THEN orderkey / 0 ELSE orderkey END FROM orders");
        assertQuery(
                "SELECT sum(IF(NULLIF(orderstatus, 'F') <> 'F', totalprice, 5.1)) FROM orders",
                "SELECT sum(CASE WHEN NULLIF(orderstatus, 'F') <> 'F' THEN totalprice ELSE 5.1 END) FROM orders");

        // coercions to supertype
        assertQuery("SELECT if(true, cast(1 as decimal(2,1)), 1)", "SELECT 1.0");
    }

    @Test
    public void testIn()
    {
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1, 2, 3)");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1.5, 2.3)", "SELECT orderkey FROM orders LIMIT 0"); // H2 incorrectly matches rows
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1, 2.0, 3)");
        assertQuery("SELECT orderkey FROM orders WHERE totalprice IN (1, 2, 3)");
        assertQuery("SELECT x FROM (values 3, 100) t(x) WHERE x IN (2147483649)", "SELECT * WHERE false");
        assertQuery("SELECT x FROM (values 3, 100, 2147483648, 2147483649, 2147483650) t(x) WHERE x IN (2147483648, 2147483650)", "values 2147483648, 2147483650");
        assertQuery("SELECT x FROM (values 3, 100, 2147483648, 2147483649, 2147483650) t(x) WHERE x IN (3, 4, 2147483648, 2147483650)", "values 3, 2147483648, 2147483650");
        assertQuery("SELECT x FROM (values 1, 2, 3) t(x) WHERE x IN (1 + cast(rand() < 0 as bigint), 2 + cast(rand() < 0 as bigint))", "values 1, 2");
        assertQuery("SELECT x FROM (values 1, 2, 3, 4) t(x) WHERE x IN (1 + cast(rand() < 0 as bigint), 2 + cast(rand() < 0 as bigint), 4)", "values 1, 2, 4");
        assertQuery("SELECT x FROM (values 1, 2, 3, 4) t(x) WHERE x IN (4, 2, 1)", "values 1, 2, 4");
        assertQuery("SELECT x FROM (values 1, 2, 3, 2147483648) t(x) WHERE x IN (1 + cast(rand() < 0 as bigint), 2 + cast(rand() < 0 as bigint), 2147483648)", "values 1, 2, 2147483648");
        assertQuery("SELECT x IN (0) FROM (values 4294967296) t(x)", "values false");
        assertQuery("SELECT x IN (0, 4294967297 + cast(rand() < 0 as bigint)) FROM (values 4294967296, 4294967297) t(x)", "values false, true");
        assertQuery("SELECT NULL in (1, 2, 3)", "values null");
        assertQuery("SELECT 1 in (1, NULL, 3)", "values true");
        assertQuery("SELECT 2 in (1, NULL, 3)", "values null");
        assertQuery("SELECT x FROM (values DATE '1970-01-01', DATE '1970-01-03') t(x) WHERE x IN (DATE '1970-01-01')", "values DATE '1970-01-01'");
        assertQuery("SELECT x FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1970-01-01 00:01:00+08:00') t(x) WHERE x IN (TIMESTAMP '1970-01-01 00:01:00+00:00')", "values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00'");
        assertQuery("SELECT COUNT(*) FROM (values 1) t(x) WHERE x IN (null, 0)", "SELECT 0");
        assertQuery("SELECT d IN (DECIMAL '2.0', DECIMAL '30.0') FROM (VALUES (DOUBLE '2.0')) t(d)", "SELECT true"); // coercion with type only coercion inside IN list
    }

    @Test
    public void testLargeIn()
    {
        String longValues = range(0, 5000)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (" + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (" + longValues + ")");

        String arrayValues = range(0, 5000)
                .mapToObj(i -> format("ARRAY[%s, %s, %s]", i, i + 1, i + 2))
                .collect(joining(", "));
        assertQuery("SELECT ARRAY[0, 0, 0] in (ARRAY[0, 0, 0], " + arrayValues + ")", "values true");
        assertQuery("SELECT ARRAY[0, 0, 0] in (" + arrayValues + ")", "values false");
    }

    @Test
    public void testNullOnLhsOfInPredicateDisallowed()
    {
        String errorMessage = "\\QNULL values are not allowed on the probe side of SemiJoin operator\\E.*";
        assertQueryFails("SELECT NULL IN (SELECT 1)", errorMessage);
        assertQueryFails("SELECT x FROM (VALUES NULL) t(x) WHERE x IN (SELECT 1)", errorMessage);
    }

    @Test
    public void testInSubqueryWithCrossJoin()
    {
        assertQuery("SELECT a FROM (VALUES (1),(2)) t(a) WHERE a IN " +
                "(SELECT b FROM (VALUES (ARRAY[2])) AS t1 (a) CROSS JOIN UNNEST(a) as t2(b))", "SELECT 2");
    }

    @Test
    public void testGroupByIf()
    {
        assertQuery(
                "SELECT IF(orderkey between 1 and 5, 'orders', 'others'), sum(totalprice) FROM orders GROUP BY 1",
                "SELECT CASE WHEN orderkey BETWEEN 1 AND 5 THEN 'orders' ELSE 'others' END, sum(totalprice)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE WHEN orderkey BETWEEN 1 AND 5 THEN 'orders' ELSE 'others' END");
    }

    @Test
    public void testDuplicateFields()
    {
        assertQuery(
                "SELECT * FROM (SELECT orderkey, orderkey FROM orders)",
                "SELECT orderkey, orderkey FROM orders");
    }

    @Test
    public void testDuplicateColumnsInWindowOrderByClause()
    {
        MaterializedResult actual = computeActual("SELECT a, row_number() OVER (ORDER BY a, a) FROM (VALUES 3, 2, 1) t(a)");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1, 1L)
                .row(2, 2L)
                .row(3, 3L)
                .build();

        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testWildcardFromSubquery()
    {
        assertQuery("SELECT * FROM (SELECT orderkey X FROM orders)");
    }

    @Test
    public void testCaseInsensitiveOutputAliasInOrderBy()
    {
        assertQueryOrdered("SELECT orderkey X FROM orders ORDER BY x");
    }

    @Test
    public void testCaseInsensitiveAttribute()
    {
        assertQuery("SELECT x FROM (SELECT orderkey X FROM orders)");
    }

    @Test
    public void testCaseInsensitiveAliasedRelation()
    {
        assertQuery("SELECT A.* FROM orders a");
    }

    @Test
    public void testCaseInsensitiveRowFieldReference()
    {
        assertQuery("SELECT a.Col0 FROM (VALUES row(cast(ROW(1,2) as ROW(col0 integer, col1 integer)))) AS t (a)", "SELECT 1");
    }

    @Test
    public void testSubqueryBody()
    {
        assertQuery("(SELECT orderkey, custkey FROM ORDERS)");
    }

    @Test
    public void testSubqueryBodyOrderLimit()
    {
        assertQueryOrdered("(SELECT orderkey AS a, custkey AS b FROM ORDERS) ORDER BY a LIMIT 1");
    }

    @Test
    public void testSubqueryBodyProjectedOrderby()
    {
        assertQueryOrdered("(SELECT orderkey, custkey FROM ORDERS) ORDER BY orderkey * -1");
    }

    @Test
    public void testSubqueryBodyDoubleOrderby()
    {
        assertQueryOrdered("(SELECT orderkey, custkey FROM ORDERS ORDER BY custkey) ORDER BY orderkey");
    }

    @Test
    public void testNodeRoster()
    {
        List<MaterializedRow> result = computeActual("SELECT * FROM system.runtime.nodes").getMaterializedRows();
        assertEquals(result.size(), getNodeCount());
    }

    @Test
    public void testCountOnInternalTables()
    {
        List<MaterializedRow> rows = computeActual("SELECT count(*) FROM system.runtime.nodes").getMaterializedRows();
        assertEquals(((Long) rows.get(0).getField(0)).longValue(), getNodeCount());
    }

    @Test
    public void testTransactionsTable()
    {
        List<MaterializedRow> result = computeActual("SELECT * FROM system.runtime.transactions").getMaterializedRows();
        assertTrue(result.size() >= 1); // At least one row for the current transaction.
    }

    @Test
    public void testDefaultExplainTextFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testDefaultExplainGraphvizFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (FORMAT GRAPHVIZ) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getGraphvizExplainPlan(query, LOGICAL));
    }

    @Test
    public void testLogicalExplain()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testLogicalExplainTextFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT TEXT) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testLogicalExplainGraphvizFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT GRAPHVIZ) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getGraphvizExplainPlan(query, LOGICAL));
    }

    @Test
    public void testDistributedExplain()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testDistributedExplainTextFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED, FORMAT TEXT) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testDistributedExplainGraphvizFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED, FORMAT GRAPHVIZ) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getGraphvizExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testExplainValidate()
    {
        MaterializedResult result = computeActual("EXPLAIN (TYPE VALIDATE) SELECT 1");
        assertEquals(result.getOnlyValue(), true);
    }

    @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = "line 1:32: Column 'x' cannot be resolved")
    public void testExplainValidateThrows()
    {
        computeActual("EXPLAIN (TYPE VALIDATE) SELECT x");
    }

    @Test
    public void testExplainOfExplain()
    {
        String query = "EXPLAIN SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testExplainOfExplainAnalyze()
    {
        String query = "EXPLAIN ANALYZE SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testExplainDdl()
    {
        assertExplainDdl("CREATE TABLE foo (pk bigint)", "CREATE TABLE foo");
        assertExplainDdl("CREATE VIEW foo AS SELECT * FROM orders", "CREATE VIEW foo");
        assertExplainDdl("DROP TABLE orders");
        assertExplainDdl("DROP VIEW view");
        assertExplainDdl("ALTER TABLE orders RENAME TO new_name");
        assertExplainDdl("ALTER TABLE orders RENAME COLUMN orderkey TO new_column_name");
        assertExplainDdl("SET SESSION foo = 'bar'");
        assertExplainDdl("PREPARE my_query FROM SELECT * FROM orders", "PREPARE my_query");
        assertExplainDdl("DEALLOCATE PREPARE my_query");
        assertExplainDdl("RESET SESSION foo");
        assertExplainDdl("START TRANSACTION");
        assertExplainDdl("COMMIT");
        assertExplainDdl("ROLLBACK");
    }

    private void assertExplainDdl(String query)
    {
        assertExplainDdl(query, query);
    }

    private void assertExplainDdl(String query, String expected)
    {
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), expected);
    }

    @Test
    public void testExplainExecute()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM orders")
                .build();
        MaterializedResult result = computeActual(session, "EXPLAIN (TYPE LOGICAL) EXECUTE my_query");
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("SELECT * FROM orders", LOGICAL));
    }

    @Test
    public void testExplainExecuteWithUsing()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM orders where orderkey < ?")
                .build();
        MaterializedResult result = computeActual(session, "EXPLAIN (TYPE LOGICAL) EXECUTE my_query USING 7");
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("SELECT * FROM orders where orderkey < 7", LOGICAL));
    }

    @Test
    public void testExplainSetSessionWithUsing()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SET SESSION foo = ?")
                .build();
        MaterializedResult result = computeActual(session, "EXPLAIN (TYPE LOGICAL) EXECUTE my_query USING 7");
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), "SET SESSION foo = 7");
    }

    @Test
    public void testShowCatalogs()
    {
        MaterializedResult result = computeActual("SHOW CATALOGS");
        assertTrue(result.getOnlyColumnAsSet().contains(getSession().getCatalog().get()));
    }

    @Test
    public void testShowCatalogsLike()
    {
        MaterializedResult result = computeActual(format("SHOW CATALOGS LIKE '%s'", getSession().getCatalog().get()));
        assertEquals(result.getOnlyColumnAsSet(), ImmutableSet.of(getSession().getCatalog().get()));
    }

    @Test
    public void testShowSchemas()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS");
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(getSession().getSchema().get(), INFORMATION_SCHEMA)));
    }

    @Test
    public void testShowSchemasFrom()
    {
        MaterializedResult result = computeActual(format("SHOW SCHEMAS FROM %s", getSession().getCatalog().get()));
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(getSession().getSchema().get(), INFORMATION_SCHEMA)));
    }

    @Test
    public void testShowSchemasLike()
    {
        MaterializedResult result = computeActual(format("SHOW SCHEMAS LIKE '%s'", getSession().getSchema().get()));
        assertEquals(result.getOnlyColumnAsSet(), ImmutableSet.of(getSession().getSchema().get()));
    }

    @Test
    public void testShowTables()
    {
        Set<String> expectedTables = ImmutableSet.copyOf(transform(TpchTable.getTables(), TpchTable::getTableName));

        MaterializedResult result = computeActual("SHOW TABLES");
        assertTrue(result.getOnlyColumnAsSet().containsAll(expectedTables));
    }

    @Test
    public void testShowTablesFrom()
    {
        Set<String> expectedTables = ImmutableSet.copyOf(transform(TpchTable.getTables(), TpchTable::getTableName));

        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();

        MaterializedResult result = computeActual("SHOW TABLES FROM " + schema);
        assertTrue(result.getOnlyColumnAsSet().containsAll(expectedTables));

        result = computeActual("SHOW TABLES FROM " + catalog + "." + schema);
        assertTrue(result.getOnlyColumnAsSet().containsAll(expectedTables));

        try {
            computeActual("SHOW TABLES FROM UNKNOWN");
            fail("Showing tables in an unknown schema should fail");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), MISSING_SCHEMA);
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "line 1:1: Schema 'unknown' does not exist");
        }
    }

    @Test
    public void testShowTablesLike()
    {
        MaterializedResult result = computeActual("SHOW TABLES LIKE 'or%'");
        assertEquals(result.getOnlyColumnAsSet(), ImmutableSet.of(ORDERS.getTableName()));
    }

    @Test
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedUnparametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        // Until we migrate all connectors to parametrized varchar we check two options
        assertTrue(actual.equals(expectedParametrizedVarchar) || actual.equals(expectedUnparametrizedVarchar),
                format("%s does not matche neither of %s and %s", actual, expectedParametrizedVarchar, expectedUnparametrizedVarchar));
    }

    @Test
    public void testAtTimeZone()
    {
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE INTERVAL '07:09' hour to minute", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'Asia/Oral'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT MIN(x) AT TIME ZONE 'America/Chicago' from (VALUES TIMESTAMP '1970-01-01 00:01:00+00:00') t(x)", "values TIMESTAMP '1970-01-01 00:01:00+00:00'");
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE '+07:09'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'America/Los_Angeles'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT x AT TIME ZONE 'America/Los_Angeles' FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00') t(x)",
                "values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00'");
        assertQuery("SELECT x AT TIME ZONE 'America/Los_Angeles' FROM (values TIMESTAMP '1970-01-01 00:01:00', TIMESTAMP '1970-01-01 08:01:00', TIMESTAMP '1969-12-31 16:01:00') t(x)",
                "values TIMESTAMP '1969-12-31 16:01:00-08:00', TIMESTAMP '1970-01-01 00:01:00-08:00', TIMESTAMP '1969-12-31 08:01:00-08:00'");
        assertQuery("SELECT min(x) AT TIME ZONE 'America/Los_Angeles' FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00') t(x)",
                "values TIMESTAMP '1969-12-31 16:01:00-08:00'");

        // with chained AT TIME ZONE
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'UTC'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'Asia/Tokyo' AT TIME ZONE 'America/Los_Angeles'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'Asia/Shanghai'", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
        assertQuery("SELECT min(x) AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'UTC' FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00') t(x)",
                "values TIMESTAMP '1969-12-31 16:01:00-08:00'");

        // with AT TIME ZONE in VALUES
        assertQuery("SELECT * from (VALUES TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'Asia/Oral')", "SELECT TIMESTAMP '2012-10-30 18:00:00.000 America/Los_Angeles'");
    }

    @Test
    public void testShowFunctions()
    {
        MaterializedResult result = computeActual("SHOW FUNCTIONS");
        ImmutableMultimap<String, MaterializedRow> functions = Multimaps.index(result.getMaterializedRows(), input -> {
            assertEquals(input.getFieldCount(), 6);
            return (String) input.getField(0);
        });

        assertTrue(functions.containsKey("avg"), "Expected function names " + functions + " to contain 'avg'");
        assertEquals(functions.get("avg").asList().size(), 4);
        assertEquals(functions.get("avg").asList().get(0).getField(1), "decimal(p,s)");
        assertEquals(functions.get("avg").asList().get(0).getField(2), "decimal(p,s)");
        assertEquals(functions.get("avg").asList().get(0).getField(3), "aggregate");
        assertEquals(functions.get("avg").asList().get(1).getField(1), "double");
        assertEquals(functions.get("avg").asList().get(1).getField(2), "bigint");
        assertEquals(functions.get("avg").asList().get(1).getField(3), "aggregate");
        assertEquals(functions.get("avg").asList().get(2).getField(1), "double");
        assertEquals(functions.get("avg").asList().get(2).getField(2), "double");
        assertEquals(functions.get("avg").asList().get(2).getField(3), "aggregate");
        assertEquals(functions.get("avg").asList().get(3).getField(1), "real");
        assertEquals(functions.get("avg").asList().get(3).getField(2), "real");
        assertEquals(functions.get("avg").asList().get(3).getField(3), "aggregate");

        assertTrue(functions.containsKey("abs"), "Expected function names " + functions + " to contain 'abs'");
        assertEquals(functions.get("abs").asList().get(0).getField(3), "scalar");
        assertEquals(functions.get("abs").asList().get(0).getField(4), true);

        assertTrue(functions.containsKey("rand"), "Expected function names " + functions + " to contain 'rand'");
        assertEquals(functions.get("rand").asList().get(0).getField(3), "scalar");
        assertEquals(functions.get("rand").asList().get(0).getField(4), false);

        assertTrue(functions.containsKey("rank"), "Expected function names " + functions + " to contain 'rank'");
        assertEquals(functions.get("rank").asList().get(0).getField(3), "window");

        assertTrue(functions.containsKey("rank"), "Expected function names " + functions + " to contain 'split_part'");
        assertEquals(functions.get("split_part").asList().get(0).getField(1), "varchar(x)");
        assertEquals(functions.get("split_part").asList().get(0).getField(2), "varchar(x), varchar(y), bigint");
        assertEquals(functions.get("split_part").asList().get(0).getField(3), "scalar");

        assertFalse(functions.containsKey("like"), "Expected function names " + functions + " not to contain 'like'");
    }

    @Test
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
    }

    @Test
    public void testSelectColumnOfNulls()
    {
        // Currently nulls can confuse the local planner, so select some
        assertQueryOrdered("SELECT \n" +
                " CAST(NULL AS VARCHAR),\n" +
                " CAST(NULL AS BIGINT)\n" +
                "FROM ORDERS\n" +
                " ORDER BY 1");
    }

    @Test
    public void testSelectCaseInsensitive()
    {
        assertQuery("SELECT ORDERKEY FROM ORDERS");
        assertQuery("SELECT OrDeRkEy FROM OrDeRs");
    }

    @Test
    public void testShowSession()
    {
        Session session = new Session(
                getSession().getQueryId(),
                Optional.empty(),
                getSession().isClientTransactionSupport(),
                getSession().getIdentity(),
                getSession().getSource(),
                getSession().getCatalog(),
                getSession().getSchema(),
                getSession().getTimeZoneKey(),
                getSession().getLocale(),
                getSession().getRemoteUserAddress(),
                getSession().getUserAgent(),
                getSession().getClientInfo(),
                getSession().getStartTime(),
                ImmutableMap.<String, String>builder()
                        .put("test_string", "foo string")
                        .put("test_long", "424242")
                        .build(),
                ImmutableMap.of(),
                ImmutableMap.of(TESTING_CATALOG, ImmutableMap.<String, String>builder()
                        .put("connector_string", "bar string")
                        .put("connector_long", "11")
                        .build()),
                getQueryRunner().getMetadata().getSessionPropertyManager(),
                getSession().getPreparedStatements());
        MaterializedResult result = computeActual(session, "SHOW SESSION");

        ImmutableMap<String, MaterializedRow> properties = Maps.uniqueIndex(result.getMaterializedRows(), input -> {
            assertEquals(input.getFieldCount(), 5);
            return (String) input.getField(0);
        });

        assertEquals(properties.get("test_string"), new MaterializedRow(1, "test_string", "foo string", "test default", "varchar", "test string property"));
        assertEquals(properties.get("test_long"), new MaterializedRow(1, "test_long", "424242", "42", "bigint", "test long property"));
        assertEquals(properties.get(TESTING_CATALOG + ".connector_string"),
                new MaterializedRow(1, TESTING_CATALOG + ".connector_string", "bar string", "connector default", "varchar", "connector string property"));
        assertEquals(properties.get(TESTING_CATALOG + ".connector_long"),
                new MaterializedRow(1, TESTING_CATALOG + ".connector_long", "11", "33", "bigint", "connector long property"));
    }

    @Test
    public void testTry()
    {
        // divide by zero
        assertQuery(
                "SELECT linenumber, sum(TRY(100/(CAST (tax*10 AS BIGINT)))) FROM lineitem GROUP BY linenumber",
                "SELECT linenumber, sum(100/(CAST (tax*10 AS BIGINT))) FROM lineitem WHERE CAST(tax*10 AS BIGINT) <> 0 GROUP BY linenumber");

        // invalid cast
        assertQuery(
                "SELECT TRY(CAST(IF(round(totalprice) % 2 = 0, CAST(totalprice AS VARCHAR), '^&$' || CAST(totalprice AS VARCHAR)) AS DOUBLE)) FROM orders",
                "SELECT CASE WHEN round(totalprice) % 2 = 0 THEN totalprice ELSE null END FROM orders");

        // invalid function argument
        assertQuery(
                "SELECT COUNT(TRY(to_base(100, CAST(round(totalprice/100) AS BIGINT)))) FROM orders",
                "SELECT SUM(CASE WHEN CAST(round(totalprice/100) AS BIGINT) BETWEEN 2 AND 36 THEN 1 ELSE 0 END) FROM orders");

        // as part of a complex expression
        assertQuery(
                "SELECT COUNT(CAST(orderkey AS VARCHAR) || TRY(to_base(100, CAST(round(totalprice/100) AS BIGINT)))) FROM orders",
                "SELECT SUM(CASE WHEN CAST(round(totalprice/100) AS BIGINT) BETWEEN 2 AND 36 THEN 1 ELSE 0 END) FROM orders");

        // missing function argument
        assertQueryFails("SELECT TRY()", "line 1:8: The 'try' function must have exactly one argument");

        // check that TRY is not pushed down
        assertQueryFails("SELECT TRY(x) IS NULL FROM (SELECT 1/y as x FROM (VALUES 1, 2, 3, 0, 4) t(y))", "/ by zero");
        assertQuery("SELECT x IS NULL FROM (SELECT TRY(1/y) as x FROM (VALUES 3, 0, 4) t(y))", "VALUES false, true, false");
    }

    @Test
    public void testTryNoMergeProjections()
    {
        // no regexp specified because the JVM optimizes away exception message constructor if run enough times
        assertQueryFails("SELECT TRY(x) FROM (SELECT 1/y as x FROM (VALUES 1, 2, 3, 0, 4) t(y))", ".*");
    }

    @Test
    public void testNoFrom()
    {
        assertQuery("SELECT 1 + 2, 3 + 4", "SELECT 1 + 2, 3 + 4 FROM orders LIMIT 1");
    }

    @Test
    public void testTopNByMultipleFields()
    {
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY orderkey ASC, custkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY orderkey ASC, custkey DESC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY orderkey DESC, custkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY orderkey DESC, custkey DESC LIMIT 10");

        // now try with order by fields swapped
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY custkey ASC, orderkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY custkey ASC, orderkey DESC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY custkey DESC, orderkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY custkey DESC, orderkey DESC LIMIT 10");

        // nulls first
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS FIRST, custkey ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) DESC NULLS FIRST, custkey ASC LIMIT 10");

        // nulls last
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS LAST LIMIT 10");
        assertQueryOrdered("SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) DESC NULLS LAST, custkey ASC LIMIT 10");

        // assure that default is nulls last
        assertQueryOrdered(
                "SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC, custkey ASC LIMIT 10",
                "SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS LAST, custkey ASC LIMIT 10");
    }

    @Test
    public void testExchangeWithProjectionPushDown()
    {
        assertQuery(
                "SELECT * FROM \n" +
                        "  (SELECT orderkey + 1 orderkey FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 100)) o \n" +
                        "JOIN \n" +
                        "  (SELECT orderkey + 1 orderkey FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 100)) o1 \n" +
                        "ON (o.orderkey = o1.orderkey)");
    }

    @Test
    public void testUnionWithProjectionPushDown()
    {
        assertQuery("SELECT key + 5, status FROM (SELECT orderkey key, orderstatus status FROM orders UNION ALL SELECT orderkey key, linestatus status FROM lineitem)");
    }

    @Test
    public void testJoinProjectionPushDown()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM\n" +
                "  (SELECT orderkey, abs(orderkey) a FROM orders) t\n" +
                "JOIN\n" +
                "  (SELECT orderkey, abs(orderkey) a FROM orders) u\n" +
                "ON\n" +
                "  t.orderkey = u.orderkey");
    }

    @Test
    public void testUnion()
    {
        assertQuery("SELECT orderkey FROM orders UNION SELECT custkey FROM orders");
        assertQuery("SELECT 123 UNION DISTINCT SELECT 123 UNION ALL SELECT 123");
        assertQuery("SELECT NULL UNION SELECT NULL");
        assertQuery("SELECT NULL, NULL UNION ALL SELECT NULL, NULL FROM nation");
        assertQuery("SELECT 'x', 'y' UNION ALL SELECT name, name FROM nation");

        // mixed single-node vs fixed vs source-distributed
        assertQuery("SELECT orderkey FROM orders UNION ALL SELECT 123 UNION ALL (SELECT custkey FROM orders GROUP BY custkey)");
    }

    @Test
    public void testUnionDistinct()
    {
        assertQuery("SELECT orderkey FROM orders UNION DISTINCT SELECT custkey FROM orders");
    }

    @Test
    public void testUnionAll()
    {
        assertQuery("SELECT orderkey FROM orders UNION ALL SELECT custkey FROM orders");
    }

    @Test
    public void testUnionArray()
    {
        assertQuery("SELECT a[1] FROM (SELECT ARRAY[1] UNION ALL SELECT ARRAY[1]) t(a) LIMIT 1", "SELECT 1");
    }

    @Test
    public void testChainedUnionsWithOrder()
    {
        assertQueryOrdered(
                "SELECT orderkey FROM orders UNION (SELECT custkey FROM orders UNION SELECT linenumber FROM lineitem) UNION ALL SELECT orderkey FROM lineitem ORDER BY orderkey");
    }

    @Test
    public void testUnionWithJoin()
    {
        assertQuery(
                "SELECT * FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION ALL " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "JOIN orders o ON (a.orderkey = o.orderkey)");
    }

    @Test
    public void testUnionWithAggregation()
    {
        assertQuery(
                "SELECT ds, count(*) FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION ALL " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "GROUP BY ds");
        assertQuery(
                "SELECT ds, count(*) FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "GROUP BY ds");
        assertQuery(
                "SELECT ds, count(DISTINCT orderkey) FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "GROUP BY ds");
        assertQuery(
                "SELECT clerk, count(DISTINCT orderstatus) FROM (" +
                        "SELECT * FROM orders WHERE orderkey=0 " +
                        " UNION ALL " +
                        "SELECT * FROM orders WHERE orderkey<>0) " +
                        "GROUP BY clerk");
        assertQuery(
                "SELECT count(clerk) FROM (" +
                        "SELECT clerk FROM orders WHERE orderkey=0 " +
                        " UNION ALL " +
                        "SELECT clerk FROM orders WHERE orderkey<>0) " +
                        "GROUP BY clerk");
        assertQuery(
                "SELECT count(orderkey), sum(sc) FROM (" +
                        "    SELECT sum(custkey) sc, orderkey FROM (" +
                        "        SELECT custkey,orderkey, orderkey+1 from orders where orderkey=0" +
                        "        UNION ALL " +
                        "        SELECT custkey,orderkey,orderkey+1 from orders where orderkey<>0) " +
                        "    GROUP BY orderkey)"
        );

        assertQuery(
                "SELECT count(orderkey), sum(sc) FROM (\n" +
                        "    SELECT sum(custkey) sc, orderkey FROM (\n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus from orders where orderkey=0\n" +
                        "        UNION ALL \n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus from orders where orderkey<>0) \n" +
                        "    GROUP BY GROUPING SETS ((orderkey, orderstatus), (orderkey)))",
                "SELECT count(orderkey), sum(sc) FROM (\n" +
                        "    SELECT sum(custkey) sc, orderkey FROM (\n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus from orders where orderkey=0\n" +
                        "        UNION ALL \n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus from orders where orderkey<>0) \n" +
                        "    GROUP BY orderkey, orderstatus \n" +
                        "    \n" +
                        "    UNION ALL \n" +
                        "    \n" +
                        "    SELECT sum(custkey) sc, orderkey FROM (\n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus from orders where orderkey=0\n" +
                        "        UNION ALL \n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus from orders where orderkey<>0) \n" +
                        "    GROUP BY orderkey)"
        );
    }

    @Test
    public void testUnionWithAggregationAndJoin()
    {
        assertQuery(
                "SELECT * FROM ( " +
                        "SELECT orderkey, count(*) FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION ALL " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "GROUP BY orderkey) t " +
                        "JOIN orders o " +
                        "ON (o.orderkey = t.orderkey)");
    }

    @Test
    public void testUnionWithJoinOnNonTranslateableSymbols()
    {
        assertQuery("SELECT *\n" +
                "FROM (SELECT orderdate ds, orderkey\n" +
                "      FROM orders\n" +
                "      UNION ALL\n" +
                "      SELECT shipdate ds, orderkey\n" +
                "      FROM lineitem) a\n" +
                "JOIN orders o\n" +
                "ON (substr(cast(a.ds AS VARCHAR), 6, 2) = substr(cast(o.orderdate AS VARCHAR), 6, 2) AND a.orderkey = o.orderkey)");
    }

    @Test
    public void testSubqueryUnion()
    {
        assertQueryOrdered("SELECT * FROM (SELECT orderkey FROM orders UNION SELECT custkey FROM orders UNION SELECT orderkey FROM orders) ORDER BY orderkey LIMIT 1000");
    }

    @Test
    public void testUnionWithFilterNotInSelect()
    {
        assertQuery("SELECT orderkey, orderdate FROM orders WHERE custkey < 1000 UNION ALL SELECT orderkey, shipdate FROM lineitem WHERE linenumber < 2000");
        assertQuery("SELECT orderkey, orderdate FROM orders UNION ALL SELECT orderkey, shipdate FROM lineitem WHERE linenumber < 2000");
        assertQuery("SELECT orderkey, orderdate FROM orders WHERE custkey < 1000 UNION ALL SELECT orderkey, shipdate FROM lineitem");
    }

    @Test
    public void testSelectOnlyUnion()
    {
        assertQuery("SELECT 123, 'foo' UNION ALL SELECT 999, 'bar'");
    }

    @Test
    public void testMultiColumnUnionAll()
    {
        assertQuery("SELECT * FROM orders UNION ALL SELECT * FROM orders");
    }

    @Test
    public void testUnionRequiringCoercion()
    {
        assertQuery("VALUES 1 UNION ALL VALUES 1.0, 2", "SELECT * FROM (VALUES 1) UNION ALL SELECT * FROM (VALUES 1.0, 2)");
        assertQuery("(VALUES 1) UNION ALL (VALUES 1.0, 2)", "SELECT * FROM (VALUES 1) UNION ALL SELECT * FROM (VALUES 1.0, 2)");
        assertQuery("SELECT 0, 0 UNION ALL SELECT 1.0, 0"); // This test case generates a RelationPlan whose .outputSymbols is different .root.outputSymbols
        assertQuery("SELECT 0, 0, 0, 0 UNION ALL SELECT 0.0, 0.0, 0, 0"); // This test case generates a RelationPlan where multiple positions share the same symbol
        assertQuery("SELECT * FROM (VALUES 1) UNION ALL SELECT * FROM (VALUES 1.0, 2)");

        assertQuery("SELECT * FROM (VALUES 1) UNION SELECT * FROM (VALUES 1.0, 2)", "VALUES 1.0, 2.0"); // H2 produces incorrect result for the original query: 1.0 1.0 2.0
        assertQuery("SELECT * FROM (VALUES (2, 2)) UNION SELECT * FROM (VALUES (1, 1.0))");
        assertQuery("SELECT * FROM (VALUES (NULL, NULL)) UNION SELECT * FROM (VALUES (1, 1.0))");
        assertQuery("SELECT * FROM (VALUES (NULL, NULL)) UNION ALL SELECT * FROM (VALUES (NULL, 1.0))");

        // Test for https://github.com/prestodb/presto/issues/7496
        // Cast varchar(1) -> varchar(4) for orderstatus in first source of union was not added. It was not done for type-only coercions.
        // Then as a result of predicate pushdown orderstatus (without cast) was compared with cast('aaa' as varchar(4)) which trigger checkArgument that
        // both types of comparison should be equal in DomainTranslator.
        assertQuery("SELECT a FROM " +
                "(" +
                "  (SELECT orderstatus as a FROM orders LIMIT 1) " +
                "UNION ALL " +
                "  SELECT 'aaaa' as a" +
                ") " +
                "WHERE  a = 'aaa'");
    }

    @Test
    public void testTableQuery()
    {
        assertQuery("TABLE orders", "SELECT * FROM orders");
    }

    @Test
    public void testTableQueryOrderLimit()
    {
        assertQueryOrdered("TABLE orders ORDER BY orderkey LIMIT 10", "SELECT * FROM orders ORDER BY orderkey LIMIT 10");
    }

    @Test
    public void testTableQueryInUnion()
    {
        assertQuery("(SELECT * FROM orders ORDER BY orderkey LIMIT 10) UNION ALL TABLE orders", "(SELECT * FROM orders ORDER BY orderkey LIMIT 10) UNION ALL SELECT * FROM orders");
    }

    @Test
    public void testTableAsSubquery()
    {
        assertQueryOrdered("(TABLE orders) ORDER BY orderkey", "(SELECT * FROM orders) ORDER BY orderkey");
    }

    @Test
    public void testLimitPushDown()
    {
        MaterializedResult actual = computeActual(
                "(TABLE orders ORDER BY orderkey) UNION ALL " +
                        "SELECT * FROM orders WHERE orderstatus = 'F' UNION ALL " +
                        "(TABLE orders ORDER BY orderkey LIMIT 20) UNION ALL " +
                        "(TABLE orders LIMIT 5) UNION ALL " +
                        "TABLE orders LIMIT 10");
        MaterializedResult all = computeExpected("SELECT * FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testOrderLimitCompaction()
    {
        assertQueryOrdered("SELECT * FROM (SELECT * FROM orders ORDER BY orderkey) LIMIT 10");
    }

    @Test
    public void testUnaliasSymbolReferencesWithUnion()
    {
        assertQuery("SELECT 1, 1, 'a', 'a' UNION ALL SELECT 1, 2, 'a', 'b'");
    }

    @Test
    public void testRandCrossJoins()
    {
        assertQuery("" +
                "SELECT COUNT(*) " +
                "FROM (SELECT * FROM orders ORDER BY rand() LIMIT 5) a " +
                "CROSS JOIN (SELECT * FROM lineitem ORDER BY rand() LIMIT 5) b");
    }

    @Test
    public void testCrossJoins()
    {
        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a " +
                "CROSS JOIN (SELECT * FROM lineitem ORDER BY orderkey LIMIT 5) b");
    }

    @Test
    public void testCrossJoinEmptyProbePage()
    {
        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders WHERE orderkey < 0) a " +
                "CROSS JOIN (SELECT * FROM lineitem WHERE orderkey < 100) b");
    }

    @Test
    public void testCrossJoinEmptyBuildPage()
    {
        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders WHERE orderkey < 100) a " +
                "CROSS JOIN (SELECT * FROM lineitem WHERE orderkey < 0) b");
    }

    @Test
    public void testSimpleCrossJoins()
    {
        assertQuery("SELECT * FROM (SELECT 1 a) x CROSS JOIN (SELECT 2 b) y");
    }

    @Test
    public void testCrossJoinsWithWhereClause()
    {
        assertQuery("" +
                        "SELECT a, b, c, d " +
                        "FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')) t1 (a, b) " +
                        "CROSS JOIN (VALUES (1, 1.1), (3, 3.3), (5, 5.5)) t2 (c, d) " +
                        "WHERE t1.a > t2.c",
                "SELECT * FROM (VALUES  (2, 'b', 1, 1.1), (3, 'c', 1, 1.1), (4, 'd', 1, 1.1), (4, 'd', 3, 3.3))");
    }

    @Test
    public void testCrossJoinsDifferentDataTypes()
    {
        assertQuery("" +
                "SELECT * " +
                "FROM (SELECT 'AAA' a1, 11 b1, 33.3 c1, true as d1, 21 e1) x " +
                "CROSS JOIN (SELECT 4444.4 a2, false as b2, 'BBB' c2, 22 d2) y");
    }

    @Test
    public void testCrossJoinWithNulls()
    {
        assertQuery("SELECT a, b FROM (VALUES (1), (2)) t (a) CROSS JOIN (VALUES (1), (3)) u (b)",
                "SELECT * FROM (VALUES  (1, 1), (1, 3), (2, 1), (2, 3))");
        assertQuery("SELECT a, b FROM (VALUES (1), (2), (null)) t (a), (VALUES (11), (null), (13)) u (b)",
                "SELECT * FROM (VALUES (1, 11), (1, null), (1, 13), (2, 11), (2, null), (2, 13), (null, 11), (null, null), (null, 13))");
        assertQuery("SELECT a, b FROM (VALUES ('AA'), ('BB'), (null)) t (a), (VALUES ('111'), (null), ('333')) u (b)",
                "SELECT * FROM (VALUES ('AA', '111'), ('AA', null), ('AA', '333'), ('BB', '111'), ('BB', null), ('BB', '333'), (null, '111'), (null, null), (null, '333'))");
    }

    @Test
    public void testImplicitCrossJoin()
    {
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 3) a, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 4) b");
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 2) b");
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 5) b, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 5) c ");

        // Inner Join converted to cross join because all join conditions are pushed down.
        assertQuery("" +
                "SELECT l.orderkey, l.linenumber " +
                "FROM orders o INNER JOIN lineitem l " +
                "ON o.custkey = l.linenumber " +
                "WHERE o.custkey IN (5) AND l.orderkey IN (7522)");

        assertQuery("" +
                "SELECT o.custkey " +
                "FROM orders o INNER JOIN lineitem l " +
                "ON o.custkey = l.linenumber " +
                "WHERE o.custkey IN (5) AND l.orderkey IN (7522)");

        assertQuery("" +
                "SELECT COUNT(*) " +
                "FROM orders o INNER JOIN lineitem l " +
                "ON o.custkey = l.linenumber " +
                "WHERE o.custkey IN (5) AND l.orderkey IN (7522)");
    }

    @Test
    public void testCrossJoinUnion()
    {
        assertQuery("" +
                "SELECT t.c " +
                "FROM (SELECT 1) " +
                "CROSS JOIN (SELECT 0 AS c UNION ALL SELECT 1) t");
        assertQuery("" +
                "SELECT a, b " +
                "FROM (VALUES (1, 1)) " +
                "CROSS JOIN (SELECT 0 AS a, 0 AS b UNION ALL SELECT 1, 1) t");
    }

    @Test
    public void testCrossJoinUnnestWithUnion()
    {
        assertQuery("" +
                        "SELECT col, COUNT(*)\n" +
                        "FROM ((\n" +
                        "    SELECT ARRAY[1, 2] AS a\n" +
                        "    UNION ALL\n" +
                        "    SELECT ARRAY[1, 3] AS a)  unionresult\n" +
                        "  CROSS JOIN UNNEST(unionresult.a) t(col))\n" +
                        "GROUP BY col",
                "SELECT * FROM VALUES (1, 2), (2, 1), (3, 1)");
    }

    @Test
    public void testJoinOnConstantExpression()
    {
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a " +
                "   JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) b " +
                "   ON 123 = 123");
    }

    @Test
    public void testSemiJoin()
    {
        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING min(orderkey) IN (SELECT orderkey FROM orders WHERE orderkey > 1)");
        //
        // constant literal versus a subquery
        assertQuery("SELECT 10 in (SELECT orderkey FROM orders)");

        // the same IN subquery used twice
        assertQuery(
                "SELECT * FROM (VALUES (1,1), (2,2), (3, 3)) t(x, y) WHERE (x+y in (VALUES 4, 5)) AND (x*y in (VALUES 4, 5))",
                "VALUES (2,2)");

        // test multiple IN subqueries with coercions
        assertQuery("SELECT 1.0 IN (SELECT 1), 1 IN (SELECT 1)");
        assertQuery("SELECT 1 WHERE 1 IN (SELECT 1) AND 1.0 IN (SELECT 1)");
        assertQuery("select 1.0 in (values (1), (2), (3))", "SELECT true");

        // test IN subqueries with supertype coercions
        assertQuery("SELECT cast(1 as decimal(3,2)) IN (SELECT cast(1 as decimal(3,1)))", "SELECT true");
        assertQuery("SELECT cast(1 as decimal(3,2)) IN (values (cast(1 as decimal(3,1))), (cast (2 as decimal(3,1))))", "SELECT true");

        // test multi level IN subqueries
        assertQuery("SELECT 1 IN (SELECT 1), 2 IN (SELECT 1) WHERE 1 IN (SELECT 1)");

         // test with subqueries on left
        assertQuery("SELECT (select 1) IN (SELECT 1)");
        assertQuery("SELECT (select 2) IN (1, (SELECT 2))");
        assertQuery("SELECT (2 + (select 1)) IN (SELECT 1)");
        assertQuery("SELECT (1 IN (SELECT 1)) IN (SELECT TRUE)");
        assertQuery("SELECT ((SELECT 1) IN (SELECT 1)) IN (SELECT TRUE)");
        assertQuery("SELECT (EXISTS(SELECT 1)) IN (SELECT TRUE)");
        assertQuery("SELECT (1 = ANY(SELECT 1)) IN (SELECT TRUE)");

        // Throw in a bunch of IN subquery predicates
        assertQuery("" +
                "SELECT *, o2.custkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 5 = 0)\n" +
                "FROM (SELECT * FROM orders WHERE custkey % 256 = 0) o1\n" +
                "JOIN (SELECT * FROM orders WHERE custkey % 256 = 0) o2\n" +
                "  ON (o1.orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0)) = (o2.orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0))\n" +
                "WHERE o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 4 = 0)\n" +
                "ORDER BY o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 7 = 0)");
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE partkey % 4 = 0),\n" +
                "  SUM(\n" +
                "    CASE\n" +
                "      WHEN orderkey\n" +
                "        IN (\n" +
                "          SELECT orderkey\n" +
                "          FROM lineitem\n" +
                "          WHERE suppkey % 4 = 0)\n" +
                "      THEN 1\n" +
                "      ELSE 0\n" +
                "      END)\n" +
                "FROM orders\n" +
                "GROUP BY orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE partkey % 4 = 0)\n" +
                "HAVING SUM(\n" +
                "  CASE\n" +
                "    WHEN orderkey\n" +
                "      IN (\n" +
                "        SELECT orderkey\n" +
                "        FROM lineitem\n" +
                "        WHERE suppkey % 4 = 0)\n" +
                "      THEN 1\n" +
                "      ELSE 0\n" +
                "      END) > 1");
    }

    @Test
    public void testJoinConstantPropagation()
    {
        assertQuery("" +
                "SELECT x, y, COUNT(*)\n" +
                "FROM (SELECT orderkey, 0 AS x FROM orders) a \n" +
                "JOIN (SELECT orderkey, 1 AS y FROM orders) b \n" +
                "ON a.orderkey = b.orderkey\n" +
                "GROUP BY 1, 2");
    }

    @Test
    public void testAntiJoin()
    {
        assertQuery("" +
                "SELECT *, orderkey\n" +
                "  NOT IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 3 = 0)\n" +
                "FROM orders");
    }

    @Test
    public void testSemiJoinLimitPushDown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 2 = 0)\n" +
                "  FROM orders\n" +
                "  LIMIT 10)");
    }

    //Disabled till #6622 is fixed
    @Test(enabled = false)
    public void testSemiJoinNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem)\n" +
                "FROM orders");
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem)\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders)");
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem)\n" +
                "FROM (\n" +
                "  SELECT CASE WHEN orderkey % 4 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "  FROM orders)");
    }

    @Test
    public void testSemiJoinWithGroupBy()
    {
        // using the same subquery in query
        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber");

        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)");

        // using different subqueries
        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT sum(orderkey) FROM orders WHERE orderkey < 5)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey > 3)");

        assertQuery("SELECT linenumber, min(orderkey), 6 IN (SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, 6 IN (SELECT orderkey FROM orders WHERE orderkey < 5)" +
                "HAVING 6 IN (SELECT orderkey FROM orders WHERE orderkey > 3)");
    }

    @Test
    public void testSemiJoinUnionNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 500 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM orders\n" +
                "    WHERE orderkey % 200 = 0\n" +
                "    UNION ALL\n" +
                "    SELECT CASE WHEN orderkey % 600 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM orders\n" +
                "    WHERE orderkey % 300 = 0\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE orderkey % 100 = 0)");
    }

    @Test
    public void testSemiJoinAggregationNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 10 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 2 = 0\n" +
                "    GROUP BY orderkey\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 3 = 0)");
    }

    @Test
    public void testSemiJoinUnionAggregationNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 500 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 250 = 0\n" +
                "    UNION ALL\n" +
                "    SELECT CASE WHEN orderkey % 300 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 200 = 0\n" +
                "    GROUP BY orderkey\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 100 = 0)\n");
    }

    @Test
    public void testSemiJoinAggregationUnionNullHandling()
    {
        assertQuery("" +
                "SELECT orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM (\n" +
                "      SELECT CASE WHEN orderkey % 500 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "      FROM orders\n" +
                "      WHERE orderkey % 200 = 0\n" +
                "      UNION ALL\n" +
                "      SELECT CASE WHEN orderkey % 600 = 0 THEN NULL ELSE orderkey END AS orderkey\n" +
                "      FROM orders\n" +
                "      WHERE orderkey % 300 = 0\n" +
                "    )\n" +
                "    GROUP BY orderkey\n" +
                "  )\n" +
                "FROM (\n" +
                "  SELECT orderkey\n" +
                "  FROM lineitem\n" +
                "  WHERE orderkey % 100 = 0)");
    }

    @Test
    public void testSameInPredicateInProjectionAndFilter()
    {
        assertQuery("SELECT x IN (SELECT * FROM (VALUES 1))\n" +
                        "FROM (VALUES 1) t(x)\n" +
                        "WHERE x IN (SELECT * FROM (VALUES 1))",
                "SELECT 1");

        assertQuery("SELECT x IN (SELECT * FROM (VALUES 1))\n" +
                        "FROM (VALUES 2) t(x)\n" +
                        "WHERE x IN (SELECT * FROM (VALUES 1))",
                "SELECT 1 WHERE false");
    }

    @Test
    public void testScalarSubquery()
    {
        // nested
        assertQuery("SELECT (SELECT (SELECT (SELECT 1)))");

        // aggregation
        assertQuery("SELECT * FROM lineitem WHERE orderkey = \n" +
                "(SELECT max(orderkey) FROM orders)");

        // no output
        assertQuery("SELECT * FROM lineitem WHERE orderkey = \n" +
                "(SELECT orderkey FROM orders WHERE 0=1)");

        // no output matching with null test
        assertQuery("SELECT * FROM lineitem WHERE \n" +
                "(SELECT orderkey FROM orders WHERE 0=1) " +
                "is null");
        assertQuery("SELECT * FROM lineitem WHERE \n" +
                "(SELECT orderkey FROM orders WHERE 0=1) " +
                "is not null");

        // subquery results and in in-predicate
        assertQuery("SELECT (SELECT 1) IN (1, 2, 3)");
        assertQuery("SELECT (SELECT 1) IN (   2, 3)");

        // multiple subqueries
        assertQuery("SELECT (SELECT 1) = (SELECT 3)");
        assertQuery("SELECT (SELECT 1) < (SELECT 3)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "(SELECT min(orderkey) FROM orders)" +
                "<" +
                "(SELECT max(orderkey) FROM orders)");
        assertQuery("SELECT (SELECT 1), (SELECT 2), (SELECT 3)");

        // distinct
        assertQuery("SELECT DISTINCT orderkey FROM lineitem " +
                "WHERE orderkey BETWEEN" +
                "   (SELECT avg(orderkey) FROM orders) - 10 " +
                "   AND" +
                "   (SELECT avg(orderkey) FROM orders) + 10");

        // subqueries with joins
        assertQuery("SELECT o1.orderkey, COUNT(*) " +
                "FROM orders o1 " +
                "INNER JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                "ON o1.orderkey " +
                "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                "GROUP BY o1.orderkey");
        assertQuery("SELECT o1.orderkey, COUNT(*) " +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o1 " +
                "LEFT JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                "ON o1.orderkey " +
                "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                "GROUP BY o1.orderkey");
        assertQuery("SELECT o1.orderkey, COUNT(*) " +
                "FROM orders o1 RIGHT JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                "ON o1.orderkey " +
                "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                "GROUP BY o1.orderkey");
        assertQuery("SELECT DISTINCT COUNT(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o1 " +
                        "FULL JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                        "ON o1.orderkey " +
                        "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                        "GROUP BY o1.orderkey",
                "VALUES 1, 10");

        // subqueries with ORDER BY
        assertQuery("SELECT orderkey, totalprice FROM orders ORDER BY (SELECT 2)");

        // subquery returns multiple rows
        String multipleRowsErrorMsg = "Scalar sub-query has returned multiple rows";
        assertQueryFails("SELECT * FROM lineitem WHERE orderkey = (\n" +
                        "SELECT orderkey FROM orders ORDER BY totalprice)",
                multipleRowsErrorMsg);
        assertQueryFails("SELECT orderkey, totalprice FROM orders ORDER BY (VALUES 1, 2)",
                multipleRowsErrorMsg);

        // exposes a bug in optimize hash generation because EnforceSingleNode does not
        // support more than one column from the underlying query
        assertQuery("SELECT custkey, (SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 1) FROM orders");

        // cast scalar sub-query
        assertQuery("SELECT 1.0/(SELECT 1), CAST(1.0 AS REAL)/(SELECT 1), 1/(SELECT 1)");
        assertQuery("SELECT 1.0 = (SELECT 1) AND 1 = (SELECT 1), 2.0 = (SELECT 1) WHERE 1.0 = (SELECT 1) AND 1 = (SELECT 1)");
        assertQuery("SELECT 1.0 = (SELECT 1), 2.0 = (SELECT 1), CAST(2.0 AS REAL) = (SELECT 1) WHERE 1.0 = (SELECT 1)");

        // coerce correlated symbols
        assertQuery("SELECT * FROM (VALUES 1) t(a) WHERE 1=(SELECT count(*) WHERE 1.0 = a)", "SELECT 1");
        assertQuery("SELECT * FROM (VALUES 1.0) t(a) WHERE 1=(SELECT count(*) WHERE 1 = a)", "SELECT 1.0");
    }

    @Test
    public void testExistsSubquery()
    {
        // nested
        assertQuery("SELECT EXISTS(SELECT NOT EXISTS(SELECT EXISTS(SELECT 1)))");

        // aggregation
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "EXISTS(SELECT max(orderkey) FROM orders)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "NOT EXISTS(SELECT max(orderkey) FROM orders)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "NOT EXISTS(SELECT orderkey FROM orders WHERE false)");

        // no output
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "EXISTS(SELECT orderkey FROM orders WHERE false)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "NOT EXISTS(SELECT orderkey FROM orders WHERE false)");

        // exists with in-predicate
        assertQuery("SELECT (EXISTS(SELECT 1)) IN (false)", "SELECT false");
        assertQuery("SELECT (NOT EXISTS(SELECT 1)) IN (false)", "SELECT true");

        assertQuery("SELECT (EXISTS(SELECT 1)) IN (true, false)", "SELECT true");
        assertQuery("SELECT (NOT EXISTS(SELECT 1)) IN (true, false)", "SELECT true");

        assertQuery("SELECT (EXISTS(SELECT 1 WHERE false)) IN (true, false)", "SELECT true");
        assertQuery("SELECT (NOT EXISTS(SELECT 1 WHERE false)) IN (true, false)", "SELECT true");

        assertQuery("SELECT (EXISTS(SELECT 1 WHERE false)) IN (false)", "SELECT true");
        assertQuery("SELECT (NOT EXISTS(SELECT 1 WHERE false)) IN (false)", "SELECT false");

        // multiple exists
        assertQuery("SELECT (EXISTS(SELECT 1)) = (EXISTS(SELECT 1)) WHERE NOT EXISTS(SELECT 1)", "SELECT true WHERE false");
        assertQuery("SELECT (EXISTS(SELECT 1)) = (EXISTS(SELECT 3)) WHERE NOT EXISTS(SELECT 1 WHERE false)", "SELECT true");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                        "(EXISTS(SELECT min(orderkey) FROM orders))" +
                        "=" +
                        "(NOT EXISTS(SELECT orderkey FROM orders WHERE false))",
                "SELECT count(*) FROM lineitem");
        assertQuery("SELECT EXISTS(SELECT 1), EXISTS(SELECT 1), EXISTS(SELECT 3), NOT EXISTS(SELECT 1), NOT EXISTS(SELECT 1 WHERE false)");

        // distinct
        assertQuery("SELECT DISTINCT orderkey FROM lineitem " +
                "WHERE EXISTS(SELECT avg(orderkey) FROM orders)");

        // subqueries used with joins
        QueryTemplate.Parameter joinType = parameter("join_type");
        QueryTemplate.Parameter condition = parameter("condition");
        QueryTemplate queryTemplate = queryTemplate(
                "SELECT o1.orderkey, COUNT(*) " +
                        "FROM orders o1 %join_type% JOIN (SELECT * FROM orders LIMIT 10) o2 ON %condition% " +
                        "GROUP BY o1.orderkey ORDER BY o1.orderkey LIMIT 5",
                joinType,
                condition);
        List<QueryTemplate.Parameter> conditions = condition.of(
                "EXISTS(SELECT avg(orderkey) FROM ORDERS)",
                "(SELECT avg(orderkey) FROM ORDERS) > 3");
        for (QueryTemplate.Parameter actualCondition : conditions) {
            for (QueryTemplate.Parameter actualJoinType : joinType.of("", "LEFT", "RIGHT")) {
                assertQuery(queryTemplate.replace(actualJoinType, actualCondition));
            }
            assertQuery(
                    queryTemplate.replace(joinType.of("FULL"), actualCondition),
                    "VALUES (1, 10), (2, 10), (3, 10), (4, 10), (5, 10)");
        }

        // subqueries with ORDER BY
        assertQuery("SELECT orderkey, totalprice FROM orders ORDER BY EXISTS(SELECT 2)");
        assertQuery("SELECT orderkey, totalprice FROM orders ORDER BY NOT(EXISTS(SELECT 2))");
    }

    @Test
    public void testScalarSubqueryWithGroupBy()
    {
        // using the same subquery in query
        assertQuery("SELECT linenumber, min(orderkey), (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber");

        assertQuery("SELECT linenumber, min(orderkey), (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, (SELECT max(orderkey) FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber, (SELECT max(orderkey) FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING min(orderkey) < (SELECT avg(orderkey) FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey), (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "HAVING min(orderkey) < (SELECT max(orderkey) FROM orders WHERE orderkey < 7)");

        // using different subqueries
        assertQuery("SELECT linenumber, min(orderkey), (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, (SELECT sum(orderkey) FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, max(orderkey), (SELECT min(orderkey) FROM orders WHERE orderkey < 5)" +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING sum(orderkey) > (SELECT min(orderkey) FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey), (SELECT max(orderkey) FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, (SELECT count(orderkey) FROM orders WHERE orderkey < 7)" +
                "HAVING min(orderkey) < (SELECT sum(orderkey) FROM orders WHERE orderkey < 7)");
    }

    @Test
    public void testOutputInEnforceSingleRow()
    {
        assertQuery("SELECT count(*) FROM (SELECT (SELECT 1))");
        assertQuery("SELECT * FROM (SELECT (SELECT 1))");
        assertQueryFails(
                "SELECT * FROM (SELECT (SELECT 1, 2))",
                "line 1:23: Multiple columns returned by subquery are not yet supported. Found 2");
    }

    @Test
    public void testExistsSubqueryWithGroupBy()
    {
        // using the same subquery in query
        assertQuery("SELECT linenumber, min(orderkey), EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber");

        assertQuery("SELECT linenumber, min(orderkey), EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber, EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey), EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "HAVING EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)");

        // using different subqueries
        assertQuery("SELECT linenumber, min(orderkey), EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)" +
                "FROM lineitem " +
                "GROUP BY linenumber, EXISTS(SELECT orderkey FROM orders WHERE orderkey < 17)");

        assertQuery("SELECT linenumber, max(orderkey), EXISTS(SELECT orderkey FROM orders WHERE orderkey < 5)" +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING EXISTS(SELECT orderkey FROM orders WHERE orderkey < 7)");

        assertQuery("SELECT linenumber, min(orderkey), EXISTS(SELECT orderkey FROM orders WHERE orderkey < 17)" +
                "FROM lineitem " +
                "GROUP BY linenumber, EXISTS(SELECT orderkey FROM orders WHERE orderkey < 17)" +
                "HAVING EXISTS(SELECT orderkey FROM orders WHERE orderkey < 27)");
    }

    @Test
    public void testCorrelatedScalarSubqueries()
    {
        String errorMsg = "Unsupported correlated subquery type";

        assertQueryFails("SELECT (SELECT l.orderkey) FROM lineitem l", errorMsg);
        assertQueryFails("SELECT (SELECT 2 * l.orderkey) FROM lineitem l", errorMsg);
        assertQueryFails("SELECT * FROM lineitem l WHERE 1 = (SELECT 2 * l.orderkey)", errorMsg);
        assertQueryFails("SELECT * FROM lineitem l ORDER BY (SELECT 2 * l.orderkey)", errorMsg);

        // group by
        assertQueryFails("SELECT max(l.quantity), 2 * l.orderkey, (SELECT l.orderkey) FROM lineitem l GROUP BY l.orderkey", errorMsg);
        assertQueryFails("SELECT max(l.quantity), 2 * l.orderkey FROM lineitem l GROUP BY l.orderkey HAVING max(l.quantity) < (SELECT l.orderkey)", errorMsg);
        assertQueryFails("SELECT max(l.quantity), 2 * l.orderkey FROM lineitem l GROUP BY l.orderkey, (SELECT l.orderkey)", errorMsg);

        // join
        assertQueryFails("SELECT * FROM lineitem l1 JOIN lineitem l2 ON l1.orderkey= (SELECT l2.orderkey)", errorMsg);
        assertQueryFails("SELECT (SELECT l3.* FROM lineitem l2 CROSS JOIN (SELECT l1.orderkey) l3 LIMIT 1) FROM lineitem l1", errorMsg);

        // subrelation
        assertQueryFails("SELECT * FROM lineitem l WHERE 2 * l.orderkey = (SELECT * FROM (SELECT l.orderkey))", errorMsg);

        // two level of nesting
        assertQueryFails("SELECT * FROM lineitem l WHERE 1 = (SELECT (SELECT 2 * l.orderkey))", errorMsg);
    }

    @Test
    public void testCorrelatedScalarSubqueriesWithCountScalarAggregationAndEqualityPredicatesInWhere()
    {
        assertQuery("SELECT (SELECT count(*) WHERE o.orderkey = 1) FROM orders o");
        assertQuery("SELECT count(*) FROM orders o WHERE 1 = (SELECT count(*) WHERE o.orderkey = 0)");
        assertQuery("SELECT * FROM orders o ORDER BY (SELECT count(*) WHERE o.orderkey = 0)");
        assertQuery(
                "SELECT count(*) FROM nation n WHERE " +
                        "(SELECT count(*) FROM region r WHERE n.regionkey = r.regionkey) > 1");
        assertQueryFails(
                "SELECT count(*) FROM nation n WHERE " +
                        "(SELECT count(*) FROM (SELECT count(*) FROM region r WHERE n.regionkey = r.regionkey)) > 1",
                "Unsupported correlated subquery type");

        // with duplicated rows
        assertQuery(
                "SELECT (SELECT count(*) WHERE a = 1) FROM (VALUES 1, 1, 2, 3) t(a)",
                "VALUES true, true, false, false");

        // group by
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey, (SELECT count(*) WHERE o.orderkey = 0) " +
                        "FROM orders o GROUP BY o.orderkey");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey " +
                        "FROM orders o GROUP BY o.orderkey HAVING 1 = (SELECT count(*) WHERE o.orderkey = 0)");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey FROM orders o " +
                        "GROUP BY o.orderkey, (SELECT count(*) WHERE o.orderkey = 0)");

        // join
        assertQuery(
                "SELECT count(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 " +
                        "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 " +
                        "ON NOT 1 = (SELECT count(*) WHERE o1.orderkey = o2.orderkey)");
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 " +
                        "ON NOT 1 = (SELECT count(*) WHERE o1.orderkey = o2.orderkey)",
                "line .*: Correlated subquery in given context is not supported");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE 1 = (SELECT * FROM (SELECT (SELECT count(*) WHERE o.orderkey = 0)))",
                "SELECT count(*) FROM orders o WHERE o.orderkey = 0");
    }

    @Test
    public void testCorrelatedScalarSubqueriesWithScalarAggregation()
    {
        // projection
        assertQuery(
                "SELECT (SELECT round(3 * avg(i.a)) FROM (VALUES 1, 1, 1, 2, 2, 3, 4) i(a) WHERE i.a < o.a AND i.a < 4) " +
                        "FROM (VALUES 0, 3, 3, 5) o(a)",
                "VALUES null, 4, 4, 5");

        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE (SELECT avg(i.orderkey) FROM orders i " +
                        "WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0) > 100",
                "VALUES 14999"); // h2 is slow

        // order by
        assertQuery(
                "SELECT orderkey FROM orders o " +
                        "ORDER BY " +
                        "   (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0), " +
                        "   orderkey " +
                        "LIMIT 1",
                "VALUES 1"); // h2 is slow

        // group by
        assertQuery(
                "SELECT max(o.orderdate), o.orderkey, " +
                        "(SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0) " +
                        "FROM orders o GROUP BY o.orderkey ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1, 40000.0)"); // h2 is slow
        assertQuery(
                "SELECT max(o.orderdate), o.orderkey " +
                        "FROM orders o " +
                        "GROUP BY o.orderkey " +
                        "HAVING 40000 < (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-07-24', 20000)"); // h2 is slow
        assertQuery(
                "SELECT max(o.orderdate), o.orderkey FROM orders o " +
                        "GROUP BY o.orderkey, (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1)"); // h2 is slow

        // join
        assertQuery(
                "SELECT count(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 " +
                        "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 " +
                        "ON NOT 1 = (SELECT avg(i.orderkey) FROM orders i WHERE o1.orderkey < o2.orderkey AND i.orderkey % 10000 = 0)");
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 " +
                        "ON NOT 1 = (SELECT avg(i.orderkey) FROM orders i WHERE o1.orderkey < o2.orderkey)",
                "line .*: Correlated subquery in given context is not supported");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE 100 < (SELECT * " +
                        "FROM (SELECT (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)))",
                "VALUES 14999"); // h2 is slow

        // consecutive correlated subqueries with scalar aggregation
        assertQuery("SELECT " +
                "(SELECT avg(regionkey) " +
                " FROM nation n2" +
                " WHERE n2.nationkey = n1.nationkey)," +
                "(SELECT avg(regionkey)" +
                " FROM nation n3" +
                " WHERE n3.nationkey = n1.nationkey)" +
                "FROM nation n1");
        assertQuery("SELECT" +
                "(SELECT avg(regionkey)" +
                " FROM nation n2 " +
                " WHERE n2.nationkey = n1.nationkey)," +
                "(SELECT avg(regionkey)+1 " +
                " FROM nation n3 " +
                " WHERE n3.nationkey = n1.nationkey)" +
                "FROM nation n1");
    }

    @Test
    public void testCorrelatedInPredicateSubqueries()
    {
        String errorMsg = "Unsupported correlated subquery type";

        assertQueryFails("SELECT 1 IN (SELECT l.orderkey) FROM lineitem l", errorMsg);
        assertQueryFails("SELECT 1 IN (SELECT 2 * l.orderkey) FROM lineitem l", errorMsg);
        assertQueryFails("SELECT * FROM lineitem l WHERE 1 IN (SELECT 2 * l.orderkey)", errorMsg);
        assertQueryFails("SELECT * FROM lineitem l ORDER BY 1 IN (SELECT 2 * l.orderkey)", errorMsg);

        // group by
        assertQueryFails("SELECT max(l.quantity), 2 * l.orderkey, 1 IN (SELECT l.orderkey) FROM lineitem l GROUP BY l.orderkey", errorMsg);
        assertQueryFails("SELECT max(l.quantity), 2 * l.orderkey FROM lineitem l GROUP BY l.orderkey HAVING max(l.quantity) IN (SELECT l.orderkey)", errorMsg);
        assertQueryFails("SELECT max(l.quantity), 2 * l.orderkey FROM lineitem l GROUP BY l.orderkey, 1 IN (SELECT l.orderkey)", errorMsg);

        // join
        assertQueryFails("SELECT * FROM lineitem l1 JOIN lineitem l2 ON l1.orderkey IN (SELECT l2.orderkey)", errorMsg);

        // subrelation
        assertQueryFails("SELECT * FROM lineitem l WHERE (SELECT * FROM (SELECT 1 IN (SELECT 2 * l.orderkey)))", errorMsg);

        // two level of nesting
        assertQueryFails("SELECT * FROM lineitem l WHERE true IN (SELECT 1 IN (SELECT 2 * l.orderkey))", errorMsg);
    }

    @Test
    public void testCorrelatedExistsSubqueriesWithPrunedCorrelationSymbols()
    {
        assertQuery("SELECT EXISTS(SELECT o.orderkey) FROM orders o");
        assertQuery("SELECT count(*) FROM orders o WHERE EXISTS(SELECT o.orderkey)");
        assertQuery("SELECT * FROM orders o ORDER BY EXISTS(SELECT o.orderkey)");

        // group by
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey, EXISTS(SELECT o.orderkey) FROM orders o GROUP BY o.orderkey");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey " +
                        "FROM orders o GROUP BY o.orderkey HAVING EXISTS (SELECT o.orderkey)");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey FROM orders o GROUP BY o.orderkey, EXISTS (SELECT o.orderkey)");

        // join
        assertQuery(
                "SELECT * FROM orders o JOIN (SELECT * FROM lineitem ORDER BY orderkey LIMIT 2) l " +
                        "ON NOT EXISTS(SELECT o.orderkey = l.orderkey)");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o WHERE (SELECT * FROM (SELECT EXISTS(SELECT o.orderkey)))",
                "VALUES 15000");
    }

    @Test
    public void testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere()
    {
        assertQuery("SELECT EXISTS(SELECT 1 WHERE o.orderkey = 1) FROM orders o");
        assertQuery("SELECT EXISTS(SELECT null WHERE o.orderkey = 1) FROM orders o");
        assertQuery("SELECT count(*) FROM orders o WHERE EXISTS(SELECT 1 WHERE o.orderkey = 0)");
        assertQuery("SELECT * FROM orders o ORDER BY EXISTS(SELECT 1 WHERE o.orderkey = 0)");
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE EXISTS (SELECT avg(l.orderkey) FROM lineitem l WHERE o.orderkey = l.orderkey)");
        assertQueryFails(
                "SELECT count(*) FROM orders o " +
                        "WHERE EXISTS (SELECT avg(l.orderkey) FROM lineitem l WHERE o.orderkey = l.orderkey GROUP BY l.linenumber)",
                "Unsupported correlated subquery type");
        assertQueryFails(
                "SELECT count(*) FROM orders o " +
                        "WHERE EXISTS (SELECT count(*) FROM lineitem l WHERE o.orderkey = l.orderkey HAVING count(*) > 3)",
                "Unsupported correlated subquery type");

        // with duplicated rows
        assertQuery(
                "SELECT EXISTS(SELECT 1 WHERE a = 1) FROM (VALUES 1, 1, 2, 3) t(a)",
                "VALUES true, true, false, false");

        // group by
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey, EXISTS(SELECT 1 WHERE o.orderkey = 0) " +
                        "FROM orders o GROUP BY o.orderkey");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey " +
                        "FROM orders o GROUP BY o.orderkey HAVING EXISTS (SELECT 1 WHERE o.orderkey = 0)");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey " +
                        "FROM orders o GROUP BY o.orderkey, EXISTS (SELECT 1 WHERE o.orderkey = 0)");

        // join
        assertQuery(
                "SELECT count(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 " +
                        "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 " +
                        "ON NOT EXISTS(SELECT 1 WHERE o1.orderkey = o2.orderkey)");
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 " +
                        "ON NOT EXISTS(SELECT 1 WHERE o1.orderkey = o2.orderkey)",
                "line .*: Correlated subquery in given context is not supported");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o WHERE (SELECT * FROM (SELECT EXISTS(SELECT 1 WHERE o.orderkey = 0)))",
                "SELECT count(*) FROM orders o WHERE o.orderkey = 0");
    }

    @Test
    public void testCorrelatedExistsSubqueries()
    {
        // projection
        assertQuery(
                "SELECT EXISTS(SELECT 1 FROM (VALUES 1, 1, 1, 2, 2, 3, 4) i(a) WHERE i.a < o.a AND i.a < 4) " +
                        "FROM (VALUES 0, 3, 3, 5) o(a)",
                "VALUES false, true, true, true");

        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 1000 = 0)",
                "VALUES 14999"); // h2 is slow

        // order by
        assertQuery(
                "SELECT orderkey FROM orders o ORDER BY " +
                        "EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "LIMIT 1",
                "VALUES 60000"); // h2 is slow

        // group by
        assertQuery(
                "SELECT max(o.orderdate), o.orderkey, " +
                        "EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0) " +
                        "FROM orders o GROUP BY o.orderkey ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1, true)"); // h2 is slow
        assertQuery(
                "SELECT max(o.orderdate), o.orderkey " +
                        "FROM orders o " +
                        "GROUP BY o.orderkey " +
                        "HAVING EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1)"); // h2 is slow
        assertQuery(
                "SELECT max(o.orderdate), o.orderkey FROM orders o " +
                        "GROUP BY o.orderkey, EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "ORDER BY o.orderkey LIMIT 1",
                "VALUES ('1996-01-02', 1)"); // h2 is slow

        // join
        assertQuery(
                "SELECT count(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 " +
                        "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 " +
                        "ON NOT EXISTS(SELECT 1 FROM orders i WHERE o1.orderkey < o2.orderkey AND i.orderkey % 10000 = 0)");
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 " +
                        "ON NOT EXISTS(SELECT 1 FROM orders i WHERE o1.orderkey < o2.orderkey)",
                "line .*: Correlated subquery in given context is not supported");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE (SELECT * FROM (SELECT EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)))",
                "VALUES 14999"); // h2 is slow
    }

    @Test
    public void testUnsupportedCorrelatedExistsSubqueries()
    {
        String errorMsg = "Unsupported correlated subquery type";

        assertQueryFails("SELECT EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3) FROM lineitem l", errorMsg);
        assertQueryFails("SELECT count(*) FROM lineitem l WHERE EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)", errorMsg);
        assertQueryFails("SELECT * FROM lineitem l ORDER BY EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)", errorMsg);

        // group by
        assertQueryFails("SELECT max(l.quantity), l.orderkey, EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3) FROM lineitem l GROUP BY l.orderkey", errorMsg);
        assertQueryFails("SELECT max(l.quantity), l.orderkey FROM lineitem l GROUP BY l.orderkey HAVING EXISTS (SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)", errorMsg);
        assertQueryFails("SELECT max(l.quantity), l.orderkey FROM lineitem l GROUP BY l.orderkey, EXISTS (SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)", errorMsg);

        // join
        assertQueryFails("SELECT * FROM lineitem l1 JOIN lineitem l2 ON NOT EXISTS(SELECT 1 WHERE l1.orderkey != l2.orderkey OR l1.orderkey = 3)", errorMsg);

        // subrelation
        assertQueryFails("SELECT count(*) FROM lineitem l WHERE (SELECT * FROM (SELECT EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)))", errorMsg);
    }

    @Test
    public void testTwoCorrelatedExistsSubqueries()
    {
        // This is simpliefied TPC-H q21
        assertQuery("SELECT\n" +
                        "  count(*) AS numwait\n" +
                        "FROM\n" +
                        "  nation l1\n" +
                        "WHERE\n" +
                        "  EXISTS(\n" +
                        "    SELECT *\n" +
                        "    FROM\n" +
                        "      nation l2\n" +
                        "    WHERE\n" +
                        "      l2.nationkey = l1.nationkey\n" +
                        "  )\n" +
                        "  AND NOT EXISTS(\n" +
                        "    SELECT *\n" +
                        "    FROM\n" +
                        "      nation l3\n" +
                        "    WHERE\n" +
                        "      l3.nationkey= l1.nationkey\n" +
                        "  )\n",
                "VALUES 0"); // EXISTS predicates are contradictory
    }

    @Test
    public void testPredicatePushdown()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT orderkey+1 as a FROM orders WHERE orderstatus = 'F' UNION ALL \n" +
                "  SELECT orderkey FROM orders WHERE orderkey % 2 = 0 UNION ALL \n" +
                "  (SELECT orderkey+custkey FROM orders ORDER BY orderkey LIMIT 10)\n" +
                ") \n" +
                "WHERE a < 20 OR a > 100 \n" +
                "ORDER BY a");
    }

    @Test
    public void testJoinPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM lineitem \n" +
                "JOIN (\n" +
                "  SELECT * FROM orders\n" +
                ") orders \n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey % 4 = 0\n" +
                "  AND lineitem.suppkey > orders.orderkey");
    }

    @Test
    public void testLeftJoinAsInnerPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.custkey IS NULL)");
    }

    @Test
    public void testPlainLeftJoinPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE lineitem.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testLeftJoinPredicatePushdownWithSelfEquality()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM lineitem \n" +
                "LEFT JOIN (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey = orders.orderkey\n" +
                "  AND lineitem.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testLeftJoinPredicatePushdownWithNullConstant()
    {
        assertQuery("" +
                "SELECT count(*)\n" +
                "FROM orders a\n" +
                "LEFT OUTER JOIN orders b\n" +
                "  ON a.clerk = b.clerk\n" +
                "WHERE a.orderpriority='5-LOW'\n" +
                "  AND b.orderpriority='1-URGENT'\n" +
                "  AND b.clerk is null\n" +
                "  AND a.orderkey % 4 = 0\n");
    }

    @Test
    public void testRightJoinAsInnerPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders\n" +
                "RIGHT JOIN lineitem\n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.custkey IS NULL)");
    }

    @Test
    public void testPlainRightJoinPredicatePushdown()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "RIGHT JOIN lineitem\n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE lineitem.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testRightJoinPredicatePushdownWithSelfEquality()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT * FROM orders WHERE orders.orderkey % 2 = 0\n" +
                ") orders \n" +
                "RIGHT JOIN lineitem\n" +
                "ON lineitem.orderkey = orders.orderkey \n" +
                "WHERE orders.orderkey = orders.orderkey\n" +
                "  AND lineitem.orderkey % 4 = 0\n" +
                "  AND (lineitem.suppkey % 2 = orders.orderkey % 2 OR orders.orderkey IS NULL)");
    }

    @Test
    public void testPredicatePushdownJoinEqualityGroups()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT custkey custkey1, custkey%4 custkey1a, custkey%8 custkey1b, custkey%16 custkey1c\n" +
                "  FROM orders\n" +
                ") orders1 \n" +
                "JOIN (\n" +
                "  SELECT custkey custkey2, custkey%4 custkey2a, custkey%8 custkey2b\n" +
                "  FROM orders\n" +
                ") orders2 ON orders1.custkey1 = orders2.custkey2\n" +
                "WHERE custkey2a = custkey2b\n" +
                "  AND custkey1 = custkey1a\n" +
                "  AND custkey2 = custkey2a\n" +
                "  AND custkey1a = custkey1c\n" +
                "  AND custkey1b = custkey1c\n" +
                "  AND custkey1b % 2 = 0");
    }

    @Test
    public void testGroupByKeyPredicatePushdown()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT custkey1, orderstatus1, SUM(totalprice1) totalprice, MAX(custkey2) maxcustkey\n" +
                "  FROM (\n" +
                "    SELECT *\n" +
                "    FROM (\n" +
                "      SELECT custkey custkey1, orderstatus orderstatus1, CAST(totalprice AS BIGINT) totalprice1, orderkey orderkey1\n" +
                "      FROM orders\n" +
                "    ) orders1 \n" +
                "    JOIN (\n" +
                "      SELECT custkey custkey2, orderstatus orderstatus2, CAST(totalprice AS BIGINT) totalprice2, orderkey orderkey2\n" +
                "      FROM orders\n" +
                "    ) orders2 ON orders1.orderkey1 = orders2.orderkey2\n" +
                "  ) \n" +
                "  GROUP BY custkey1, orderstatus1\n" +
                ")\n" +
                "WHERE custkey1 = maxcustkey\n" +
                "AND maxcustkey % 2 = 0 \n" +
                "AND orderstatus1 = 'F'\n" +
                "AND totalprice > 10000\n" +
                "ORDER BY custkey1, orderstatus1, totalprice, maxcustkey");
    }

    @Test
    public void testNonDeterministicJoinPredicatePushdown()
    {
        MaterializedResult materializedResult = computeActual("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT DISTINCT *\n" +
                "  FROM (\n" +
                "    SELECT 'abc' as col1a, 500 as col1b FROM lineitem limit 1\n" +
                "  ) table1\n" +
                "  JOIN (\n" +
                "    SELECT 'abc' as col2a FROM lineitem limit 1000000\n" +
                "  ) table2\n" +
                "  ON table1.col1a = table2.col2a\n" +
                "  WHERE rand() * 1000 > table1.col1b\n" +
                ")");
        MaterializedRow row = getOnlyElement(materializedResult.getMaterializedRows());
        assertEquals(row.getFieldCount(), 1);
        long count = (Long) row.getField(0);
        // Technically non-deterministic unit test but has essentially a next to impossible chance of a false positive
        assertTrue(count > 0 && count < 1000000);
    }

    @Test
    public void testTrivialNonDeterministicPredicatePushdown()
    {
        assertQuery("SELECT COUNT(*) WHERE rand() >= 0");
    }

    @Test
    public void testNonDeterministicTableScanPredicatePushdown()
    {
        MaterializedResult materializedResult = computeActual("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  LIMIT 1000\n" +
                ")\n" +
                "WHERE rand() > 0.5");
        MaterializedRow row = getOnlyElement(materializedResult.getMaterializedRows());
        assertEquals(row.getFieldCount(), 1);
        long count = (Long) row.getField(0);
        // Technically non-deterministic unit test but has essentially a next to impossible chance of a false positive
        assertTrue(count > 0 && count < 1000);
    }

    @Test
    public void testNonDeterministicAggregationPredicatePushdown()
    {
        MaterializedResult materializedResult = computeActual("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT orderkey, COUNT(*)\n" +
                "  FROM lineitem\n" +
                "  GROUP BY orderkey\n" +
                "  LIMIT 1000\n" +
                ")\n" +
                "WHERE rand() > 0.5");
        MaterializedRow row = getOnlyElement(materializedResult.getMaterializedRows());
        assertEquals(row.getFieldCount(), 1);
        long count = (Long) row.getField(0);
        // Technically non-deterministic unit test but has essentially a next to impossible chance of a false positive
        assertTrue(count > 0 && count < 1000);
    }

    @Test
    public void testSemiJoinPredicateMoveAround()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM orders WHERE custkey % 2 = 0 AND orderkey % 3 = 0)\n" +
                "WHERE orderkey\n" +
                "  IN (\n" +
                "    SELECT CASE WHEN orderkey % 7 = 0 THEN NULL ELSE orderkey END\n" +
                "    FROM lineitem\n" +
                "    WHERE partkey % 2 = 0)\n" +
                "  AND\n" +
                "    orderkey % 2 = 0");
    }

    @Test
    public void testUnionAllPredicateMoveAroundWithOverlappingProjections()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT orderkey AS x, orderkey as y\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 3 = 0\n" +
                "  UNION ALL\n" +
                "  SELECT orderkey AS x, orderkey as y\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 2 = 0\n" +
                ") a\n" +
                "JOIN (\n" +
                "  SELECT orderkey AS x, orderkey as y\n" +
                "  FROM orders\n" +
                ") b\n" +
                "ON a.x = b.x");
    }

    @Test
    public void testTableSampleBernoulliBoundaryValues()
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE BERNOULLI (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey FROM orders TABLESAMPLE BERNOULLI (0)");
        MaterializedResult all = computeExpected("SELECT orderkey FROM orders", fullSample.getTypes());

        assertContains(all, fullSample);
        assertEquals(emptySample.getMaterializedRows().size(), 0);
    }

    @Test
    public void testTableSampleBernoulli()
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        int total = computeExpected("SELECT orderkey FROM orders", ImmutableList.of(BIGINT)).getMaterializedRows().size();

        for (int i = 0; i < 100; i++) {
            List<MaterializedRow> values = computeActual("SELECT orderkey FROM ORDERS TABLESAMPLE BERNOULLI (50)").getMaterializedRows();

            assertEquals(values.size(), ImmutableSet.copyOf(values).size(), "TABLESAMPLE produced duplicate rows");
            stats.addValue(values.size() * 1.0 / total);
        }

        double mean = stats.getGeometricMean();
        assertTrue(mean > 0.45 && mean < 0.55, format("Expected mean sampling rate to be ~0.5, but was %s", mean));
    }

    @Test
    public void testFunctionNotRegistered()
    {
        assertQueryFails(
                "SELECT length(1)",
                "\\Qline 1:8: Unexpected parameters (integer) for function length. Expected:\\E.*");
    }

    @Test
    public void testFunctionArgumentTypeConstraint()
    {
        assertQueryFails(
                "SELECT greatest(rgb(255, 0, 0))",
                "\\Qline 1:8: Unexpected parameters (color) for function greatest. Expected: greatest(E) E:orderable\\E.*");
    }

    @Test
    public void testTypeMismatch()
    {
        assertQueryFails("SELECT 1 <> 'x'", "\\Qline 1:10: '<>' cannot be applied to integer, varchar(1)\\E");
    }

    @Test
    public void testInvalidType()
    {
        assertQueryFails("SELECT CAST(null AS array(foo))", "\\Qline 1:8: Unknown type: array(foo)\\E");
    }

    @Test
    public void testInvalidTypeInfixOperator()
    {
        // Comment on why error message references varchar(214783647) instead of varchar(2) which seems expected result type for concatenation in expression.
        // Currently variable argument functions do not play well with arguments using parametrized types.
        // The variable argument functions mechanism requires that all the arguments are of exactly same type. We cannot enforce that base must match but parameters may differ.
        assertQueryFails("SELECT ('a' || 'z') + (3 * 4) / 5", "\\Qline 1:21: '+' cannot be applied to varchar, integer\\E");
    }

    @Test
    public void testInvalidTypeBetweenOperator()
    {
        assertQueryFails("SELECT 'a' BETWEEN 3 AND 'z'", "\\Qline 1:12: Cannot check if varchar(1) is BETWEEN integer and varchar(1)\\E");
    }

    @Test
    public void testInvalidTypeArray()
    {
        assertQueryFails("SELECT ARRAY[1, 2, 'a']", "\\Qline 1:20: All ARRAY elements must be the same type: integer\\E");
    }

    @Test
    public void testTimeLiterals()
    {
        MaterializedResult.Builder builder = resultBuilder(getSession(), DATE, TIME, TIME_WITH_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE);

        DateTimeZone sessionTimeZone = DateTimeZoneIndex.getDateTimeZone(getSession().getTimeZoneKey());
        DateTimeZone utcPlus6 = DateTimeZoneIndex.getDateTimeZone(TimeZoneKey.getTimeZoneKeyForOffset(6 * 60));

        builder.row(
                new Date(new DateTime(2013, 3, 22, 0, 0, sessionTimeZone).getMillis()),
                new Time(new DateTime(1970, 1, 1, 3, 4, 5, sessionTimeZone).getMillisOfDay()),
                new Time(new DateTime(1970, 1, 1, 3, 4, 5, utcPlus6).getMillis()), // hack because java.sql.Time compares based on actual number of ms since epoch instead of ms since midnight
                new Timestamp(new DateTime(1960, 1, 22, 3, 4, 5, sessionTimeZone).getMillis()),
                new Timestamp(new DateTime(1960, 1, 22, 3, 4, 5, utcPlus6).getMillis()));

        MaterializedResult actual = computeActual("SELECT DATE '2013-03-22', TIME '3:04:05', TIME '3:04:05 +06:00', TIMESTAMP '1960-01-22 3:04:05', TIMESTAMP '1960-01-22 3:04:05 +06:00'");

        assertEquals(actual, builder.build());
    }

    @Test
    public void testArrayShuffle()
    {
        List<Integer> expected = IntStream.rangeClosed(1, 500).boxed().collect(toList());
        Set<List<Integer>> distinctResults = new HashSet<>();

        distinctResults.add(expected);
        for (int i = 0; i < 3; i++) {
            MaterializedResult results = computeActual(format("SELECT shuffle(ARRAY %s) from orders limit 10", expected));
            List<MaterializedRow> rows = results.getMaterializedRows();
            assertTrue(rows.size() == 10);

            for (MaterializedRow row : rows) {
                List<Integer> actual = (List<Integer>) row.getField(0);

                // check if the result is a correct permutation
                assertEqualsIgnoreOrder(actual, expected);

                distinctResults.add(actual);
            }
        }
        assertTrue(distinctResults.size() >= 24, "shuffle must produce at least 24 distinct results");
    }

    @Test
    public void testNonReservedTimeWords()
    {
        assertQuery("" +
                "SELECT TIME, TIMESTAMP, DATE, INTERVAL\n" +
                "FROM (SELECT 1 TIME, 2 TIMESTAMP, 3 DATE, 4 INTERVAL)");
    }

    @Test
    public void testCustomAdd()
    {
        assertQuery(
                "SELECT custom_add(orderkey, custkey) FROM orders",
                "SELECT orderkey + custkey FROM orders");
    }

    @Test
    public void testCustomSum()
    {
        @Language("SQL") String sql = "SELECT orderstatus, custom_sum(orderkey) FROM orders GROUP BY orderstatus";
        assertQuery(sql, sql.replace("custom_sum", "sum"));
    }

    @Test
    public void testCustomRank()
    {
        @Language("SQL") String sql = "" +
                "SELECT orderstatus, clerk, sales\n" +
                ", custom_rank() OVER (PARTITION BY orderstatus ORDER BY sales DESC) rnk\n" +
                "FROM (\n" +
                "  SELECT orderstatus, clerk, sum(totalprice) sales\n" +
                "  FROM orders\n" +
                "  GROUP BY orderstatus, clerk\n" +
                ")\n" +
                "ORDER BY orderstatus, clerk";

        assertEquals(computeActual(sql), computeActual(sql.replace("custom_rank", "rank")));
    }

    @Test
    public void testApproxSetBigint()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(custkey)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1002L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetVarchar()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(CAST(custkey AS VARCHAR))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1024L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetDouble()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(CAST(custkey AS DOUBLE))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1014L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetBigintGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(custkey)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1001L)
                .row("F", 998L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetVarcharGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(CAST(custkey AS VARCHAR))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1021L)
                .row("F", 1019L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetDoubleGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(CAST(custkey AS DOUBLE))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1011L)
                .row("F", 1011L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetWithNulls()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(IF(orderstatus = 'O', custkey))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(1001L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetOnlyNulls()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(null)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(new Object[] {null})
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(IF(orderstatus != 'O', custkey))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", null)
                .row("F", 998L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetGroupByWithNulls()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(IF(custkey % 2 <> 0, custkey))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 499L)
                .row("F", 496L)
                .row("P", 153L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLog()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(create_hll(custkey))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1002L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLogGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(merge(create_hll(custkey))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1001L)
                .row("F", 998L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLogWithNulls()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(create_hll(IF(orderstatus = 'O', custkey)))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1001L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLogGroupByWithNulls()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(merge(create_hll(IF(orderstatus != 'O', custkey)))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", null)
                .row("F", 998L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLogOnlyNulls()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(null)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(new Object[] {null})
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testEmptyApproxSet()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(empty_approx_set())");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(0L)
                .build();
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeEmptyApproxSet()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(empty_approx_set())) FROM orders");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(0L)
                .build();
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeEmptyNonEmptyApproxSet()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey) c FROM ORDERS UNION ALL SELECT empty_approx_set())");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1002L)
                .build();
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetBigint()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(cast(approx_set(custkey) AS P4HYPERLOGLOG)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1002L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetVarchar()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(cast(approx_set(CAST(custkey AS VARCHAR)) AS P4HYPERLOGLOG)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1024L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetDouble()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(cast(approx_set(CAST(custkey AS DOUBLE)) AS P4HYPERLOGLOG)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1014L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetBigintGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(cast(approx_set(custkey) AS P4HYPERLOGLOG)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1001L)
                .row("F", 998L)
                .row("P", 308L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetVarcharGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(cast(approx_set(CAST(custkey AS VARCHAR)) AS P4HYPERLOGLOG)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1021L)
                .row("F", 1019L)
                .row("P", 302L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetDoubleGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(cast(approx_set(CAST(custkey AS DOUBLE)) AS P4HYPERLOGLOG)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1011L)
                .row("F", 1011L)
                .row("P", 306L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetWithNulls()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(cast(approx_set(IF(orderstatus = 'O', custkey)) AS P4HYPERLOGLOG)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(1001L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetOnlyNulls()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(cast(approx_set(null) AS P4HYPERLOGLOG)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(new Object[] {null})
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(cast(approx_set(IF(orderstatus != 'O', custkey)) AS P4HYPERLOGLOG)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", null)
                .row("F", 998L)
                .row("P", 308L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testP4ApproxSetGroupByWithNulls()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(cast(approx_set(IF(custkey % 2 <> 0, custkey)) AS P4HYPERLOGLOG)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 495L)
                .row("F", 491L)
                .row("P", 153L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testValuesWithNonTrivialType()
    {
        MaterializedResult actual = computeActual("VALUES (0.0/0.0, 1.0/0.0, -1.0/0.0)");

        List<MaterializedRow> rows = actual.getMaterializedRows();
        assertEquals(rows.size(), 1);

        MaterializedRow row = rows.get(0);
        assertTrue(((Double) row.getField(0)).isNaN());
        assertEquals(row.getField(1), Double.POSITIVE_INFINITY);
        assertEquals(row.getField(2), Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testValuesWithTimestamp()
    {
        MaterializedResult actual = computeActual("VALUES (current_timestamp, now())");

        List<MaterializedRow> rows = actual.getMaterializedRows();
        assertEquals(rows.size(), 1);

        MaterializedRow row = rows.get(0);
        assertEquals(row.getField(0), row.getField(1));
    }

    @Test
    public void testValuesWithUnusedColumns()
    {
        MaterializedResult actual = computeActual("SELECT foo from (values (1, 2)) a(foo, bar)");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(1)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testFilterPushdownWithAggregation()
    {
        assertQuery("SELECT * FROM (SELECT count(*) FROM orders) WHERE 0=1");
        assertQuery("SELECT * FROM (SELECT count(*) FROM orders) WHERE null");
    }

    @Test
    public void testAccessControl()
    {
        assertAccessDenied("SELECT COUNT(true) FROM orders", "Cannot select from table .*.orders.*", privilege("orders", SELECT_TABLE));
        assertAccessDenied("INSERT INTO orders SELECT * FROM orders", "Cannot insert into table .*.orders.*", privilege("orders", INSERT_TABLE));
        assertAccessDenied("DELETE FROM orders", "Cannot delete from table .*.orders.*", privilege("orders", DELETE_TABLE));
        assertAccessDenied("CREATE TABLE foo AS SELECT * FROM orders", "Cannot create table .*.foo.*", privilege("foo", CREATE_TABLE));
    }

    @Test
    public void testEmptyInputForUnnest()
    {
        assertQuery("select val from (select distinct vals from (values (array[2])) t(vals) where false) tmp cross join unnest(tmp.vals) tt(val)", "select 1 where 1=2");
    }

    @Test
    public void testCoercions()
    {
        // VARCHAR
        assertQuery("SELECT length(NULL)");
        assertQuery("SELECT CAST('abc' AS VARCHAR(255)) || CAST('abc' AS VARCHAR(252))");
        assertQuery("SELECT CAST('abc' AS VARCHAR(255)) || 'abc'");

        // DECIMAL - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + NULL");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) + CAST(292.1 AS DECIMAL(5,1))");
        assertEqualsIgnoreOrder(
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1))] || CAST(292 AS DECIMAL(5,1))"),
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1)), CAST(292 AS DECIMAL(5,1))]"));

        // BIGINT - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(292 AS BIGINT)");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) = CAST(292 AS BIGINT)");
        assertEqualsIgnoreOrder(
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1))] || CAST(292 AS BIGINT)"),
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1)), CAST(292 AS DECIMAL(19,0))]"));

        // DECIMAL - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(1.1 AS DOUBLE)");
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) = CAST(1.1 AS DOUBLE)");
        assertQuery("SELECT SIN(CAST(1.1 AS DECIMAL(38,1)))");
        assertEqualsIgnoreOrder(
                computeActual("SELECT ARRAY[CAST(282.1 AS DOUBLE), CAST(283.2 AS DOUBLE)] || CAST(101.3 AS DECIMAL(5,1))"),
                computeActual("SELECT ARRAY[CAST(282.1 AS DOUBLE), CAST(283.2 AS DOUBLE), CAST(101.3 AS DOUBLE)]"));

        // INTEGER - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(292 AS INTEGER)");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) = CAST(292 AS INTEGER)");
        assertEqualsIgnoreOrder(
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1))] || CAST(292 AS INTEGER)"),
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1)), CAST(292 AS DECIMAL(19,0))]"));

        // TINYINT - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(CAST(121 AS DECIMAL(30,1)) AS TINYINT)");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) = CAST(CAST(121 AS DECIMAL(30,1)) AS TINYINT)");

        // SMALLINT - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(CAST(121 AS DECIMAL(30,1)) AS SMALLINT)");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) = CAST(CAST(121 AS DECIMAL(30,1)) AS SMALLINT)");

        // Complex coercions across joins
        assertQuery("SELECT * FROM (" +
                        "  SELECT t2.x || t2.z cc FROM (" +
                        "    SELECT *" +
                        "    FROM (VALUES (CAST('a' as VARCHAR), CAST('c' as VARCHAR))) t(x, z)" +
                        "  ) t2" +
                        "  JOIN (" +
                        "    SELECT *" +
                        "    FROM (VALUES (CAST('a' as VARCHAR), CAST('c' as VARCHAR))) u(x, z)" +
                        "    WHERE z='c'" +
                        "  ) u2" +
                        "  ON t2.z = u2.z" +
                        ") tt " +
                        "WHERE cc = 'ac'",
                "SELECT 'ac'");

        assertQuery("SELECT * FROM (" +
                        "  SELECT greatest (t.x, t.z) cc FROM (" +
                        "    SELECT *" +
                        "    FROM (VALUES (VARCHAR 'a', VARCHAR 'c')) t(x, z)" +
                        "  ) t" +
                        "  JOIN (" +
                        "    SELECT *" +
                        "    FROM (VALUES (VARCHAR 'a', VARCHAR 'c')) u(x, z)" +
                        "    WHERE z='c'" +
                        "  ) u" +
                        "  ON t.z = u.z" +
                        ")" +
                        "WHERE cc = 'c'",
                "SELECT 'c'");

        assertQuery("SELECT cc[1], cc[2] FROM (" +
                        " SELECT * FROM (" +
                        "  SELECT array[t.x, t.z] cc FROM (" +
                        "    SELECT *" +
                        "    FROM (VALUES (VARCHAR 'a', VARCHAR 'c')) t(x, z)" +
                        "  ) t" +
                        "  JOIN (" +
                        "    SELECT *" +
                        "    FROM (VALUES (VARCHAR 'a', VARCHAR 'c')) u(x, z)" +
                        "    WHERE z='c'" +
                        "  ) u" +
                        "  ON t.z = u.z)" +
                        " WHERE cc = array['a', 'c'])",
                "SELECT 'a', 'c'");

        assertQuery("SELECT c = 'x'" +
                "FROM (" +
                "    SELECT 'x' as c" +
                "    UNION ALL" +
                "    SELECT 'yy' as c" +
                ")");
    }

    @Test
    public void testExecute()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT 123, 'abc'")
                .build();
        assertQuery(session, "EXECUTE my_query", "SELECT 123, 'abc'");
    }

    @Test
    public void testExecuteUsing()
    {
        String query = "SELECT a + 1, count(?) FROM (VALUES 1, 2, 3, 2) t1(a) JOIN (VALUES 1, 2, 3, 4) t2(b) ON b < ? WHERE a < ? GROUP BY a + 1 HAVING count(1) > ?";
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", query)
                .build();
        assertQuery(session,
                "EXECUTE my_query USING 1, 5, 4, 0",
                "VALUES (2, 4), (3, 8), (4, 4)");
    }

    @Test
    public void testExecuteUsingComplexJoinCriteria()
    {
        String query = "SELECT * FROM (VALUES 1) t(a) JOIN (VALUES 2) u(a) ON t.a + u.a < ?";
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", query)
                .build();
        assertQuery(session,
                "EXECUTE my_query USING 5",
                "VALUES (1, 2)");
    }

    @Test
    public void testExecuteUsingWithSubquery()
    {
        String query = "SELECT ? in (SELECT orderkey FROM orders)";
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", query)
                .build();

        assertQuery(session,
                "EXECUTE my_query USING 10",
                "SELECT 10 in (SELECT orderkey FROM orders)"
        );
    }

    @Test
    public void testExecuteUsingWithSubqueryInJoin()
    {
        String query = "SELECT * " +
                "FROM " +
                "    (VALUES ?,2,3) t(x) " +
                "  JOIN " +
                "    (VALUES 1,2,3) t2(y) " +
                "  ON " +
                "(x in (VALUES 1,2,?)) = (y in (VALUES 1,2,3)) AND (x in (VALUES 1,?)) = (y in (VALUES 1,2))";

        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", query)
                .build();
        assertQuery(session,
                "EXECUTE my_query USING 1, 3, 2",
                "VALUES (1,1), (1,2), (2,2), (2,1), (3,3)");
    }

    @Test
    public void testExecuteWithParametersInGroupBy()
    {
        try {
            String query = "SELECT a + ?, count(1) FROM (VALUES 1, 2, 3, 2) t(a) GROUP BY a + ?";
            Session session = Session.builder(getSession())
                    .addPreparedStatement("my_query", query)
                    .build();
            computeActual(session, "EXECUTE my_query USING 1, 1");
            fail("parameters in group by and select should fail");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), MUST_BE_AGGREGATE_OR_GROUP_BY);
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "line 1:10: '(\"a\" + ?)' must be an aggregate expression or appear in GROUP BY clause");
        }
    }

    @Test
    public void testExecuteNoSuchQuery()
    {
        assertQueryFails("EXECUTE my_query", "Prepared statement not found: my_query");
    }

    @Test
    public void testParametersNonPreparedStatement()
    {
        try {
            computeActual("SELECT ?, 1");
            fail("parameters not in prepared statements should fail");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_PARAMETER_USAGE);
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "line 1:1: Incorrect number of parameters: expected 1 but found 0");
        }
    }

    @Test
    public void testDescribeInput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "select ? from nation where nationkey = ? and name < ?")
                .build();
        MaterializedResult actual = computeActual(session, "DESCRIBE INPUT my_query");
        MaterializedResult expected = resultBuilder(session, BIGINT, VARCHAR)
                .row(0, "unknown")
                .row(1, "bigint")
                .row(2, "varchar")
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeInputWithAggregation()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "select count(*) + ? from nation")
                .build();
        MaterializedResult actual = computeActual(session, "DESCRIBE INPUT my_query");
        MaterializedResult expected = resultBuilder(session, BIGINT, VARCHAR)
                .row(0, "bigint")
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeInputNoParameters()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "select * from nation")
                .build();
        MaterializedResult actual = computeActual(session, "DESCRIBE INPUT my_query");
        MaterializedResult expected = resultBuilder(session, UNKNOWN, UNKNOWN).build();
        assertEquals(actual, expected);
    }

    @Test
    public void testDescribeInputNoSuchQuery()
    {
        assertQueryFails("DESCRIBE INPUT my_query", "Prepared statement not found: my_query");
    }

    @Test
    public void testQuantifiedComparison()
    {
        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey = ANY (SELECT regionkey FROM region WHERE name IN ('ASIA', 'EUROPE'))");
        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey = ALL (SELECT regionkey FROM region WHERE name IN ('ASIA', 'EUROPE'))");

        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey <> ANY (SELECT regionkey FROM region WHERE name IN ('ASIA', 'EUROPE'))");
        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey <> ALL (SELECT regionkey FROM region WHERE name IN ('ASIA', 'EUROPE'))");

        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey = ALL (SELECT regionkey FROM region WHERE name IN ('ASIA'))");
        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey <> ALL (SELECT regionkey FROM region WHERE name IN ('ASIA'))");
        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey = ANY (SELECT regionkey FROM region WHERE name IN ('EUROPE'))");
        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey <> ANY (SELECT regionkey FROM region WHERE name IN ('EUROPE'))");

        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey < SOME (SELECT regionkey FROM region WHERE name IN ('ASIA', 'EUROPE'))");
        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey <= ANY (SELECT regionkey FROM region WHERE name IN ('ASIA', 'EUROPE'))");
        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey > ANY (SELECT regionkey FROM region WHERE name IN ('ASIA', 'EUROPE'))");
        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey >= SOME (SELECT regionkey FROM region WHERE name IN ('ASIA', 'EUROPE'))");

        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey < ALL (SELECT regionkey FROM region WHERE name IN ('ASIA', 'EUROPE'))");
        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey <= ALL (SELECT regionkey FROM region WHERE name IN ('ASIA', 'EUROPE'))");
        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey > ALL (SELECT regionkey FROM region WHERE name IN ('ASIA', 'EUROPE'))");
        assertQuery("SELECT nationkey, name, regionkey FROM nation WHERE regionkey >= ALL (SELECT regionkey FROM region WHERE name IN ('ASIA', 'EUROPE'))");

        // subquery with coercion
        assertQuery("SELECT 1.0 < ALL(SELECT 1), 1 < ALL(SELECT 1)");
        assertQuery("SELECT 1.0 < ANY(SELECT 1), 1 < ANY(SELECT 1)");
        assertQuery("SELECT 1.0 <= ALL(SELECT 1) WHERE 1 <= ALL(SELECT 1)");
        assertQuery("SELECT 1.0 <= ANY(SELECT 1) WHERE 1 <= ANY(SELECT 1)");
        assertQuery("SELECT 1.0 <= ALL(SELECT 1), 1 <= ALL(SELECT 1) WHERE 1 <= ALL(SELECT 1)");
        assertQuery("SELECT 1.0 <= ANY(SELECT 1), 1 <= ANY(SELECT 1) WHERE 1 <= ANY(SELECT 1)");
        assertQuery("SELECT 1.0 = ALL(SELECT 1) WHERE 1 = ALL(SELECT 1)");
        assertQuery("SELECT 1.0 = ANY(SELECT 1) WHERE 1 = ANY(SELECT 1)");
        assertQuery("SELECT 1.0 = ALL(SELECT 1), 2 = ALL(SELECT 1) WHERE 1 = ALL(SELECT 1)");
        assertQuery("SELECT 1.0 = ANY(SELECT 1), 2 = ANY(SELECT 1) WHERE 1 = ANY(SELECT 1)");

        // subquery with supertype coercion
        assertQuery("SELECT cast(1 as decimal(3,2)) < ALL(SELECT cast(1 as decimal(3,1)))");
        assertQuery("SELECT cast(1 as decimal(3,2)) < ANY(SELECT cast(1 as decimal(3,1)))");
        assertQuery("SELECT cast(1 as decimal(3,2)) <= ALL(SELECT cast(1 as decimal(3,1)))");
        assertQuery("SELECT cast(1 as decimal(3,2)) <= ANY(SELECT cast(1 as decimal(3,1)))");
        assertQuery("SELECT cast(1 as decimal(3,2)) = ALL(SELECT cast(1 as decimal(3,1)))");
        assertQuery("SELECT cast(1 as decimal(3,2)) = ANY(SELECT cast(1 as decimal(3,1)))", "SELECT true");
        assertQuery("SELECT cast(1 as decimal(3,2)) <> ALL(SELECT cast(1 as decimal(3,1)))");
        assertQuery("SELECT cast(1 as decimal(3,2)) <> ANY(SELECT cast(1 as decimal(3,1)))");
    }

    @Test(dataProvider = "quantified_comparisons_corner_cases")
    public void testQuantifiedComparisonCornerCases(String query)
    {
        assertQuery(query);
    }

    @DataProvider(name = "quantified_comparisons_corner_cases")
    public Object[][] qualifiedComparisonsCornerCases()
    {
        //the %subquery% is wrapped in a SELECT so that H2 does not blow up on the VALUES subquery
        Stream<String> queries = queryTemplate("SELECT %value% %operator% %quantifier% (SELECT * FROM (%subquery%))")
                .replaceAll(
                        parameter("subquery").of(
                                "SELECT 1 WHERE false",
                                "SELECT CAST(NULL AS INTEGER)",
                                "VALUES (1), (NULL)"
                        ),
                        parameter("quantifier").of("ALL", "ANY"),
                        parameter("value").of("1", "NULL"),
                        parameter("operator").of("=", "!=", "<", ">", "<=", ">="));
        //the following are disabled till #6622 is fixed
        List<String> excludedInPredicateQueries = ImmutableList.of(
                "SELECT NULL != ALL (SELECT * FROM (SELECT 1 WHERE false))",
                "SELECT NULL = ANY (SELECT * FROM (SELECT 1 WHERE false))",
                "SELECT NULL != ALL (SELECT * FROM (SELECT CAST(NULL AS INTEGER)))",
                "SELECT NULL = ANY (SELECT * FROM (SELECT CAST(NULL AS INTEGER)))",
                "SELECT NULL = ANY (SELECT * FROM (VALUES (1), (NULL)))",
                "SELECT NULL != ALL (SELECT * FROM (VALUES (1), (NULL)))"
        );
        Predicate<String> isExcluded = excludedInPredicateQueries::contains;
        return toArgumentsArrays(queries.filter(isExcluded.negate()).map(Arguments::of));
    }

    @Test
    public void testPreparedStatementWithSubqueries()
    {
        List<QueryTemplate.Parameter> leftValues = parameter("left").of(
                "", "1 = ",
                "EXISTS",
                "1 IN",
                "1 = ANY", "1 = ALL",
                "2 <> ANY", "2 <> ALL",
                "0 < ALL", "0 < ANY",
                "1 <= ALL", "1 <= ANY");

        queryTemplate("SELECT %left% (SELECT 1 WHERE 2 = ?)")
                .replaceAll(leftValues)
                .forEach(query -> {
                    Session session = Session.builder(getSession())
                            .addPreparedStatement("my_query", query)
                            .build();
                    assertQuery(session, "EXECUTE my_query USING 2", "SELECT true");
                });
    }

    @Test
    public void testDescribeOutput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("nationkey", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("regionkey", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, false)
                .row("comment", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(152)", 0, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutputNamedAndUnnamed()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "select 1, name, regionkey as my_alias from nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("_col0", "", "", "", "integer", 4, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("my_alias", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, true)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutputNonSelect()
    {
        assertDescribeOutputRowCount("CREATE TABLE foo AS SELECT * FROM nation");
        assertDescribeOutputRowCount("DELETE FROM orders");

        assertDescribeOutputEmpty("CALL foo()");
        assertDescribeOutputEmpty("SET SESSION optimize_hash_generation=false");
        assertDescribeOutputEmpty("RESET SESSION optimize_hash_generation");
        assertDescribeOutputEmpty("START TRANSACTION");
        assertDescribeOutputEmpty("COMMIT");
        assertDescribeOutputEmpty("ROLLBACK");
        assertDescribeOutputEmpty("GRANT INSERT ON foo TO bar");
        assertDescribeOutputEmpty("REVOKE INSERT ON foo FROM bar");
        assertDescribeOutputEmpty("CREATE SCHEMA foo");
        assertDescribeOutputEmpty("ALTER SCHEMA foo RENAME TO bar");
        assertDescribeOutputEmpty("DROP SCHEMA foo");
        assertDescribeOutputEmpty("CREATE TABLE foo (x bigint)");
        assertDescribeOutputEmpty("ALTER TABLE foo ADD COLUMN y bigint");
        assertDescribeOutputEmpty("ALTER TABLE foo RENAME TO bar");
        assertDescribeOutputEmpty("DROP TABLE foo");
        assertDescribeOutputEmpty("CREATE VIEW foo AS SELECT * FROM nation");
        assertDescribeOutputEmpty("DROP VIEW foo");
        assertDescribeOutputEmpty("PREPARE test FROM SELECT * FROM orders");
        assertDescribeOutputEmpty("EXECUTE test");
        assertDescribeOutputEmpty("DEALLOCATE PREPARE test");
    }

    private void assertDescribeOutputRowCount(@Language("SQL") String sql)
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", sql)
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("rows", "", "", "", "bigint", 8, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    private void assertDescribeOutputEmpty(@Language("SQL") String sql)
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", sql)
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutputShowTables()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SHOW TABLES")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("Table", session.getCatalog().get(), "information_schema", "tables", "varchar", 0, true)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutputOnAliasedColumnsAndExpressions()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "select count(*) as this_is_aliased, 1 + 2 from nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("this_is_aliased", "", "", "", "bigint", 8, true)
                .row("_col1", "", "", "", "integer", 4, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutputNoSuchQuery()
    {
        assertQueryFails("DESCRIBE OUTPUT my_query", "Prepared statement not found: my_query");
    }

    @Test
    public void testSubqueriesWithDisjunction()
    {
        List<QueryTemplate.Parameter> projections = parameter("projection").of("count(*)", "*", "%condition%");
        List<QueryTemplate.Parameter> conditions = parameter("condition").of(
                "nationkey IN (SELECT 1) OR TRUE",
                "EXISTS(SELECT 1) OR TRUE");

        queryTemplate("SELECT %projection% FROM nation WHERE %condition%")
                .replaceAll(projections, conditions)
                .forEach(this::assertQuery);

        queryTemplate("SELECT %projection% FROM nation WHERE (%condition%) AND nationkey <3")
                .replaceAll(projections, conditions)
                .forEach(this::assertQuery);

        assertQuery(
                "SELECT count(*) FROM nation WHERE (SELECT true FROM (SELECT 1) t(a) WHERE a = nationkey) OR TRUE",
                "SELECT 25");
        assertQueryFails(
                "SELECT (SELECT true FROM (SELECT 1) t(a) WHERE a = nationkey) " +
                        "FROM nation " +
                        "WHERE (SELECT true FROM (SELECT 1) t(a) WHERE a = nationkey) OR TRUE",
                "Unsupported correlated subquery type");
    }

    @Test
    public void testAssignUniqueId()
    {
        String unionLineitem50Times = IntStream.range(0, 50)
                .mapToObj(i -> "SELECT * FROM lineitem")
                .collect(joining(" UNION ALL "));

        assertQuery(
                "SELECT count(*) FROM (" +
                        "SELECT * FROM (" +
                        "   SELECT (SELECT count(*) WHERE c = 1) " +
                        "   FROM (SELECT CASE orderkey WHEN 1 THEN orderkey ELSE 1 END " +
                        "       FROM (" + unionLineitem50Times + ")) o(c)) result(a) " +
                        "WHERE a = 1)",
                "VALUES 3008750");
    }
}
