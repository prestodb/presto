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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.type.SqlIntervalDayTime;
import com.facebook.presto.type.SqlIntervalYearMonth;
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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD;
import static com.facebook.presto.SystemSessionProperties.ADD_PARTIAL_NODE_FOR_ROW_NUMBER_WITH_LIMIT;
import static com.facebook.presto.SystemSessionProperties.ENABLE_INTERMEDIATE_AGGREGATIONS;
import static com.facebook.presto.SystemSessionProperties.FIELD_NAMES_IN_JSON_CAST_ENABLED;
import static com.facebook.presto.SystemSessionProperties.GENERATE_DOMAIN_FILTERS;
import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.INLINE_PROJECTIONS_ON_VALUES;
import static com.facebook.presto.SystemSessionProperties.ITERATIVE_OPTIMIZER_TIMEOUT;
import static com.facebook.presto.SystemSessionProperties.JOIN_PREFILTER_BUILD_SIDE;
import static com.facebook.presto.SystemSessionProperties.LEGACY_UNNEST;
import static com.facebook.presto.SystemSessionProperties.MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER;
import static com.facebook.presto.SystemSessionProperties.MERGE_DUPLICATE_AGGREGATIONS;
import static com.facebook.presto.SystemSessionProperties.OFFSET_CLAUSE_ENABLED;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZER_USE_HISTOGRAMS;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_CASE_EXPRESSION_PREDICATE;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_CONDITIONAL_CONSTANT_APPROXIMATE_DISTINCT;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static com.facebook.presto.SystemSessionProperties.PREFILTER_FOR_GROUPBY_LIMIT;
import static com.facebook.presto.SystemSessionProperties.PREFILTER_FOR_GROUPBY_LIMIT_TIMEOUT_MS;
import static com.facebook.presto.SystemSessionProperties.PRE_PROCESS_METADATA_CALLS;
import static com.facebook.presto.SystemSessionProperties.PULL_EXPRESSION_FROM_LAMBDA_ENABLED;
import static com.facebook.presto.SystemSessionProperties.PUSH_DOWN_FILTER_EXPRESSION_EVALUATION_THROUGH_CROSS_JOIN;
import static com.facebook.presto.SystemSessionProperties.PUSH_REMOTE_EXCHANGE_THROUGH_GROUP_ID;
import static com.facebook.presto.SystemSessionProperties.QUICK_DISTINCT_LIMIT_ENABLED;
import static com.facebook.presto.SystemSessionProperties.RANDOMIZE_OUTER_JOIN_NULL_KEY;
import static com.facebook.presto.SystemSessionProperties.RANDOMIZE_OUTER_JOIN_NULL_KEY_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT;
import static com.facebook.presto.SystemSessionProperties.REMOVE_MAP_CAST;
import static com.facebook.presto.SystemSessionProperties.REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN;
import static com.facebook.presto.SystemSessionProperties.REWRITE_CASE_TO_MAP_ENABLED;
import static com.facebook.presto.SystemSessionProperties.REWRITE_CONSTANT_ARRAY_CONTAINS_TO_IN_EXPRESSION;
import static com.facebook.presto.SystemSessionProperties.REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN;
import static com.facebook.presto.SystemSessionProperties.REWRITE_CROSS_JOIN_OR_TO_INNER_JOIN;
import static com.facebook.presto.SystemSessionProperties.REWRITE_EXPRESSION_WITH_CONSTANT_EXPRESSION;
import static com.facebook.presto.SystemSessionProperties.REWRITE_LEFT_JOIN_NULL_FILTER_TO_SEMI_JOIN;
import static com.facebook.presto.SystemSessionProperties.REWRITE_MIN_MAX_BY_TO_TOP_N;
import static com.facebook.presto.SystemSessionProperties.SIMPLIFY_PLAN_WITH_EMPTY_INPUT;
import static com.facebook.presto.SystemSessionProperties.USE_DEFAULTS_FOR_CORRELATED_AGGREGATION_PUSHDOWN_THROUGH_OUTER_JOINS;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static com.facebook.presto.operator.scalar.InvokeFunction.INVOKE_FUNCTION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.EXPRESSION_NOT_CONSTANT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_AGGREGATE_OR_GROUP_BY;
import static com.facebook.presto.sql.tree.ExplainType.Type.DISTRIBUTED;
import static com.facebook.presto.sql.tree.ExplainType.Type.IO;
import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SHOW_CREATE_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;
import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestngUtils.toDataProvider;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.tests.QueryTemplate.parameter;
import static com.facebook.presto.tests.QueryTemplate.queryTemplate;
import static com.facebook.presto.tests.StatefulSleepingSum.STATEFUL_SLEEPING_SUM;
import static com.facebook.presto.tests.StructuralTestUtil.mapType;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestQueries
        extends AbstractTestQueryFramework
{
    // We can just use the default type registry, since we don't use any parametric types
    public static final List<SqlFunction> CUSTOM_FUNCTIONS = new FunctionListBuilder()
            .aggregates(CustomSum.class)
            .window(CustomRank.class)
            .scalars(CustomAdd.class)
            .scalars(CreateHll.class)
            .scalars(CustomStructWithPassthrough.class)
            .scalars(CustomStructWithoutPassthrough.class)
            .functions(APPLY_FUNCTION, INVOKE_FUNCTION, STATEFUL_SLEEPING_SUM)
            .getFunctions();

    public static final List<PropertyMetadata<?>> TEST_SYSTEM_PROPERTIES = ImmutableList.of(
            PropertyMetadata.stringProperty(
                    "test_string",
                    "test string property",
                    "test default",
                    false),
            PropertyMetadata.longProperty(
                    "test_long",
                    "test long property",
                    42L,
                    false));
    public static final List<PropertyMetadata<?>> TEST_CATALOG_PROPERTIES = ImmutableList.of(
            PropertyMetadata.stringProperty(
                    "connector_string",
                    "connector string property",
                    "connector default",
                    false),
            PropertyMetadata.longProperty(
                    "connector_long",
                    "connector long property",
                    33L,
                    false),
            PropertyMetadata.booleanProperty(
                    "connector_boolean",
                    "connector boolean property",
                    true,
                    false),
            PropertyMetadata.doubleProperty(
                    "connector_double",
                    "connector double property",
                    99.0,
                    false));

    public static final String UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG = "line .*: Given correlated subquery is not supported";

    private static final DateTimeFormatter ZONED_DATE_TIME_FORMAT = DateTimeFormatter.ofPattern(SqlTimestampWithTimeZone.JSON_FORMAT);

    @Test
    public void testParsingError()
    {
        assertQueryFails("SELECT foo FROM", "line 1:16: mismatched input '<EOF>'. Expecting: .*");
    }

    @Test
    public void testSelectLargeInterval()
    {
        MaterializedResult result = computeActual("SELECT INTERVAL '30' DAY");
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), new SqlIntervalDayTime(30, 0, 0, 0, 0));

        result = computeActual("SELECT INTERVAL '" + Short.MAX_VALUE + "' YEAR");
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), new SqlIntervalYearMonth(Short.MAX_VALUE, 0));
    }

    private void emptyJoinQueries(Session session)
    {
        // Empty predicate and inner join
        assertQuery(session, "select 1 from (select * from orders where 1 = 0) DT join customer on DT.custkey=customer.custkey",
                "select 1 from orders where 1 =0");

        // Empty non-null producing side for outer join.
        assertQuery(session, "select 1 from (select * from orders where 1 = 0) DT left outer join customer on DT.custkey=customer.custkey",
                "select 1 from orders where 1 =0");

        // 3 way join with empty non-null producing side for outer join
        assertQuery(session, "select 1 from (select * from orders where 1 = 0) DT"
                        + " left outer join customer C1 on DT.custkey=C1.custkey"
                        + " left outer join customer C2 on C1.custkey=C2.custkey",
                "select 1 from orders where 1 =0");

        // Zero limit
        assertQuery(session, "select 1 from (select * from orders LIMIT 0) DT join customer on DT.custkey=customer.custkey",
                "select 1 from orders where 1 =0");

        // Negative test.
        assertQuery(session, "select 1 from (select * from orders) DT join customer on DT.custkey=customer.custkey",
                "select 1 from orders");

        // Empty null producing side for outer join.
        assertQuery(session, "select 1 from (select * from orders) ORD left outer join (select custkey from customer where 1=0) " +
                        "CUST on ORD.custkey=CUST.custkey",
                "select 1 from orders");

        // Empty null producing side for left outer join with constant field.
        assertQuery(session, "select One from (select * from orders) ORD left outer join (select 1 as One, custkey from customer where 1=0) " +
                        "CUST on ORD.custkey=CUST.custkey",
                "select null as One from orders");

        // Empty null producing side for right outer join with constant field.
        assertQuery(session, "select One from (select 1 as One, custkey from customer where 1=0) CUST right outer join (select * from orders) ORD " +
                        " ON ORD.custkey=CUST.custkey",
                "select null as One from orders");

        // 3 way join with mix of left and right outer joins. DT left outer join C1 right outer join O2.
        // DT is empty which produces DT right outer join O2 which produce O2 as final result.
        assertQuery(session, "select 1 from (select * from orders where 1 = 0) DT"
                        + " left outer join customer C1 on DT.custkey=C1.custkey"
                        + " right outer join orders O2 on C1.custkey=O2.custkey",
                "select 1 from orders");

        // Empty side for full outer join.
        assertQuery(session, "select 1 from (select * from orders) ORD full outer join (select custkey from customer where 1=0) " +
                        "CUST on ORD.custkey=CUST.custkey",
                "select 1 from orders");

        // Empty side for full outer join as input to aggregation.
        assertQuery(session, "select count(*), orderkey from (select * from orders) ORD full outer join (select custkey from customer where 1=0) " +
                        "CUST on ORD.custkey=CUST.custkey group by orderkey order by orderkey",
                "select count(*), orderkey from orders group by orderkey order by orderkey");
    }

    @Test
    public void testSelectNull()
    {
        assertQuery("SELECT NULL");
    }

    @Test
    public void testAggregationOverUnknown()
    {
        assertQuery("SELECT clerk, min(totalprice), max(totalprice), min(nullvalue), max(nullvalue) " +
                "FROM (SELECT clerk, totalprice, null AS nullvalue FROM orders) " +
                "GROUP BY clerk");
    }

    @Test
    public void testLimitIntMax()
    {
        assertQuery("SELECT orderkey FROM orders LIMIT " + Integer.MAX_VALUE);
        assertQuery("SELECT orderkey FROM orders ORDER BY orderkey LIMIT " + Integer.MAX_VALUE);
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
    }

    @Test
    public void testNonDeterministicInLambda()
    {
        MaterializedResult materializedResult = computeActual("SELECT apply(1, x -> x + rand()) FROM orders LIMIT 10");
        long distinctCount = materializedResult.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .distinct()
                .count();
        assertTrue(distinctCount >= 8, "rand() must produce different rows");
    }

    @Test
    public void testLambdaCapture()
    {
        // Test for lambda expression without capture can be found in TestLambdaExpression

        assertQuery("SELECT apply(0, x -> x + c1) FROM (VALUES 1) t(c1)", "VALUES 1");
        assertQuery("SELECT apply(0, x -> x + t.c1) FROM (VALUES 1) t(c1)", "VALUES 1");
        assertQuery("SELECT apply(c1, x -> x + c2) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(c1, c2)", "VALUES 3, 7, 11");
        assertQuery("SELECT apply(c1 + 10, x -> apply(x + 100, y -> c1)) FROM (VALUES 1) t(c1)", "VALUES 1");
        assertQuery("SELECT apply(c1 + 10, x -> apply(x + 100, y -> t.c1)) FROM (VALUES 1) t(c1)", "VALUES 1");
        assertQuery("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> r.x)", "VALUES 10");
        assertQuery("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> r.x) FROM (VALUES 1) u(x)", "VALUES 10");
        assertQuery("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> r.x) FROM (VALUES 1) r(x)", "VALUES 10");
        assertQuery("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> apply(3, y -> y + r.x)) FROM (VALUES 1) u(x)", "VALUES 13");
        assertQuery("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> apply(3, y -> y + r.x)) FROM (VALUES 1) r(x)", "VALUES 13");
        assertQuery("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> apply(3, y -> y + r.x)) FROM (VALUES 'a') r(x)", "VALUES 13");
        assertQuery("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), z -> apply(3, y -> y + r.x)) FROM (VALUES 1) r(x)", "VALUES 4");

        // reference lambda variable of the not-immediately-enclosing lambda
        assertQuery("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 1000) t(x)", "VALUES 1");
        assertQuery("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 'abc') t(x)", "VALUES 1");
        assertQuery("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 1000) t(x)", "VALUES 1");
        assertQuery("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 'abc') t(x)", "VALUES 1");

        // in join post-filter
        assertQuery("SELECT * FROM (VALUES true) t(x) left JOIN (VALUES 1001) t2(y) ON (apply(false, z -> apply(false, y -> x)))", "SELECT true, 1001");
    }

    @Test
    public void testLambdaInAggregationContext()
    {
        assertQuery("SELECT apply(sum(x), i -> i * i) FROM (VALUES 1, 2, 3, 4, 5) t(x)", "SELECT 225");
        assertQuery("SELECT apply(x, i -> i - 1), sum(y) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) GROUP BY x", "VALUES (0, 30), (1, 50)");
        assertQuery("SELECT x, apply(sum(y), i -> i * 10) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) GROUP BY x", "VALUES (1, 300), (2, 500)");
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
    public void testLambdaInValuesAndUnnest()
    {
        assertQuery("SELECT * FROM UNNEST(transform(sequence(1, 5), x -> x * x))", "SELECT * FROM (VALUES 1, 4, 9, 16, 25)");
        assertQuery("SELECT x[5] FROM (VALUES transform(sequence(1, 5), x -> x * x)) t(x)", "SELECT 25");
    }

    @Test
    public void testApplyLambdaRepeated()
    {
        assertQuery("SELECT x + x FROM (SELECT apply(a, i -> i * i) x FROM (VALUES 3) t(a))", "SELECT 18");
        assertQuery("SELECT apply(a, i -> i * i) + apply(a, i -> i * i) FROM (VALUES 3) t(a)", "SELECT 18");
        assertQuery("SELECT apply(a, i -> i * i), apply(a, i -> i * i) FROM (VALUES 3) t(a)", "SELECT 9, 9");
    }

    @Test
    public void testTryLambdaRepeated()
    {
        assertQuery("SELECT try(10 / a) + try(10 / a) FROM (VALUES 5) t(a)", "SELECT 4");
        assertQuery("SELECT try(10 / a), try(10 / a) FROM (VALUES 5) t(a)", "SELECT 2, 2");
    }

    @Test
    public void testTryLambdaWithCast()
    {
        assertQuery(
                "SELECT IF(TRY(CAST(a AS INT)) IN (1, 5), TRY(CAST(b AS DOUBLE)), 0.0) FROM (VALUES (varchar'1', varchar'2.1'), (varchar'5', varchar'3.4')) t(a, b)",
                "VALUES 2.1, 3.4");
    }

    @Test
    public void testNonDeterministicFilter()
    {
        MaterializedResult materializedResult = computeActual("SELECT u FROM ( SELECT if(rand() > 0.5, 0, 1) AS u ) WHERE u <> u");
        assertEquals(materializedResult.getRowCount(), 0);

        materializedResult = computeActual("SELECT u, v FROM ( SELECT if(rand() > 0.5, 0, 1) AS u, 4*4 AS v ) WHERE u <> u and v > 10");
        assertEquals(materializedResult.getRowCount(), 0);

        materializedResult = computeActual("SELECT u, v, w FROM ( SELECT if(rand() > 0.5, 0, 1) AS u, 4*4 AS v, 'abc' AS w ) WHERE v > 10");
        assertEquals(materializedResult.getRowCount(), 1);
    }

    @Test
    public void testNonDeterministicProjection()
    {
        MaterializedResult materializedResult = computeActual("SELECT r, r + 1 FROM (SELECT rand(100) r FROM orders) LIMIT 10");
        assertEquals(materializedResult.getRowCount(), 10);
        for (MaterializedRow materializedRow : materializedResult) {
            assertEquals(materializedRow.getFieldCount(), 2);
            assertEquals(((Number) materializedRow.getField(0)).intValue() + 1, materializedRow.getField(1));
        }
    }

    @Test
    public void testMapSubscript()
    {
        assertQuery("SELECT map(array[1], array['aa'])[1]", "SELECT 'aa'");
        assertQuery("SELECT map(array['a'], array['aa'])['a']", "SELECT 'aa'");
        assertQuery("SELECT map(array[array[1,1]], array['a'])[array[1,1]]", "SELECT 'a'");
        assertQuery("SELECT map(array[(1,2)], array['a'])[(1,2)]", "SELECT 'a'");
    }

    @Test
    public void testTableSampleWithFraction()
    {
        assertQuerySucceeds("SELECT count(*) FROM orders TABLESAMPLE BERNOULLI (0.000000000000000000001)");
        assertQuerySucceeds("SELECT count(*) FROM orders TABLESAMPLE BERNOULLI (0.02)");
    }

    @Test
    public void testRowSubscript()
    {
        // Subscript on Row with unnamed fields
        assertQuery("SELECT ROW (1, 'a', true)[2]", "SELECT 'a'");
        assertQuery("SELECT r[2] FROM (VALUES (ROW (ROW (1, 'a', true)))) AS v(r)", "SELECT 'a'");
        assertQuery("SELECT r[1], r[2] FROM (SELECT ROW (name, regionkey) FROM nation ORDER BY name LIMIT 1) t(r)", "VALUES ('ALGERIA', 0)");

        // Subscript on Row with named fields
        assertQuery("SELECT (CAST (ROW (1, 'a', 2 ) AS ROW (field1 bigint, field2 varchar(1), field3 bigint)))[2]", "SELECT 'a'");

        // Subscript on nested Row
        assertQuery("SELECT ROW (1, 'a', ROW (false, 2, 'b'))[3][3]", "SELECT 'b'");

        // Row subscript in filter condition
        assertQuery("SELECT orderstatus FROM orders WHERE ROW (orderkey, custkey)[1] = 100", "SELECT 'O'");

        // Row subscript in join condition
        assertQuery("SELECT n.name, r.name FROM nation n JOIN region r ON ROW (n.name, n.regionkey)[2] = ROW (r.name, r.regionkey)[2] ORDER BY n.name LIMIT 1", "VALUES ('ALGERIA', 'AFRICA')");
    }

    @Test
    public void testRowSubscriptInLambda()
    {
        assertQuery("SELECT apply(ROW (1, 2), r -> r[2])", "SELECT 2");
    }

    @Test
    public void testVarbinary()
    {
        assertQuery("SELECT LENGTH(x) FROM (SELECT from_base64('gw==') AS x)", "SELECT 1");
        assertQuery("SELECT LENGTH(from_base64('gw=='))", "SELECT 1");
    }

    @Test
    public void testRowFieldAccessor()
    {
        //Dereference only
        assertQuery("SELECT a FROM (VALUES ROW (CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)).col0)) AS t (a)", "SELECT 1");
        assertQuery("SELECT a.col0 FROM (VALUES ROW (CAST(ROW(1, 2) AS ROW(col0 integer, col1 integer)))) AS t (a)", "SELECT 1");
        assertQuery("SELECT a.col0 FROM (VALUES ROW (CAST(ROW(1.0E0, 2.0E0) AS ROW(col0 integer, col1 integer)))) AS t (a)", "SELECT 1.0");
        assertQuery("SELECT a.col0 FROM (VALUES ROW (CAST(ROW(TRUE, FALSE) AS ROW(col0 boolean, col1 boolean)))) AS t (a)", "SELECT TRUE");
        assertQuery("SELECT a.col1 FROM (VALUES ROW (CAST(ROW(1.0, 'kittens') AS ROW(col0 varchar, col1 varchar)))) AS t (a)", "SELECT 'kittens'");
        assertQuery("SELECT a.col2.col1 FROM (VALUES ROW(CAST(ROW(1.0, ARRAY[2], row(3, 4.0)) AS ROW(col0 double, col1 array(int), col2 row(col0 integer, col1 double))))) t(a)", "SELECT 4.0");

        // mixture of row field reference and table field reference
        assertQuery("SELECT CAST(row(1, t.x) AS row(col0 bigint, col1 bigint)).col1 FROM (VALUES 1, 2, 3) t(x)", "SELECT * FROM (VALUES 1, 2, 3)");
        assertQuery("SELECT Y.col1 FROM (SELECT CAST(row(1, t.x) AS row(col0 bigint, col1 bigint)) AS Y FROM (VALUES 1, 2, 3) t(x)) test_t", "SELECT * FROM (VALUES 1, 2, 3)");

        // Subscript + Dereference
        assertQuery("SELECT a.col1[2] FROM (VALUES ROW(CAST(ROW(1.0, ARRAY[22, 33, 44, 55], row(3, 4.0E0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a)", "SELECT 33");
        assertQuery("SELECT a.col1[2].col0, a.col1[2].col1 FROM (VALUES ROW(cast(row(1.0, ARRAY[row(31, 4.1E0), row(32, 4.2E0)], row(3, 4.0E0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double))))) t(a)", "SELECT 32, 4.2");

        assertQuery("SELECT CAST(row(11, 12) AS row(col0 bigint, col1 bigint)).col0", "SELECT 11");

        // Dereference in VALUES node
        assertQuery("SELECT v FROM ( VALUES (ARRAY[ CAST( ROW(2, 'a') AS ROW( int_field BIGINT, str_field VARCHAR ) )][1].str_field)) AS t (v)", "SELECT 'a'");
    }

    @Test
    public void testRowFieldAccessorInAggregate()
    {
        assertQuery("SELECT a.col0, SUM(a.col1[2]), SUM(a.col2.col0), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[2, 13, 4], row(11, 4.1E0))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.0, ARRAY[2, 23, 4], row(12, 14.0E0))  AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.0, ARRAY[22, 33, 44], row(13, 5.0E0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a) " +
                        "GROUP BY a.col0",
                "SELECT * FROM VALUES (1.0, 46, 24, 9.1), (2.0, 23, 12, 14.0)");

        assertQuery("SELECT a.col2.col0, SUM(a.col0), SUM(a.col1[2]), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[2, 13, 4], row(11, 4.1E0))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.0, ARRAY[2, 23, 4], row(11, 14.0E0))  AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(7.0, ARRAY[22, 33, 44], row(13, 5.0E0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a) " +
                        "GROUP BY a.col2.col0",
                "SELECT * FROM VALUES (11, 3.0, 36, 18.1), (13, 7.0, 33, 5.0)");

        assertQuery("SELECT a.col1[1].col0, SUM(a.col0), SUM(a.col1[1].col1), SUM(a.col1[2].col0), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[row(31, 4.5E0), row(12, 4.2E0)], row(3, 4.0E0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.1, ARRAY[row(41, 3.1E0), row(32, 4.2E0)], row(6, 6.0E0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(31, 4.2E0), row(22, 4.2E0)], row(5, 4.0E0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double))))) t(a) " +
                        "GROUP BY a.col1[1].col0",
                "SELECT * FROM VALUES (31, 3.2, 8.7, 34, 8.0), (41, 3.1, 3.1, 32, 6.0)");

        assertQuery("SELECT a.col1[1].col0, SUM(a.col0), SUM(a.col1[1].col1), SUM(a.col1[2].col0), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(31, 4.2E0), row(22, 4.2E0)], row(5, 4.0E0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.0, ARRAY[row(31, 4.5E0), row(12, 4.2E0)], row(3, 4.1E0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.1, ARRAY[row(41, 3.1E0), row(32, 4.2E0)], row(6, 6.0E0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.3, ARRAY[row(41, 3.1E0), row(32, 4.2E0)], row(6, 6.0E0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))) " +
                        ") t(a) " +
                        "GROUP BY a.col1[1]",
                "SELECT * FROM VALUES (31, 2.2, 4.2, 22, 4.0), (31, 1.0, 4.5, 12, 4.1), (41, 6.4, 6.2, 64, 12.0)");

        assertQuery("SELECT a.col1[2], SUM(a.col0), SUM(a.col1[1]), SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[2, 13, 4], row(11, 4.1E0))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.0, ARRAY[2, 13, 4], row(12, 14.0E0))  AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(7.0, ARRAY[22, 33, 44], row(13, 5.0E0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a) " +
                        "GROUP BY a.col1[2]",
                "SELECT * FROM VALUES (13, 3.0, 4, 18.1), (33, 7.0, 22, 5.0)");

        assertQuery("SELECT a.col2.col0, SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(2.2, ARRAY[row(31, 4.2E0), row(22, 4.2E0)], row(5, 4.0E0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.0, ARRAY[row(31, 4.5E0), row(12, 4.2E0)], row(3, 4.1E0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.1, ARRAY[row(41, 3.1E0), row(32, 4.2E0)], row(6, 6.0E0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(3.3, ARRAY[row(41, 3.1E0), row(32, 4.2E0)], row(6, 6.0E0)) AS ROW(col0 double, col1 array(row(col0 integer, col1 double)), col2 row(col0 integer, col1 double)))) " +
                        ") t(a) " +
                        "GROUP BY a.col2",
                "SELECT * FROM VALUES (5, 4.0), (3, 4.1), (6, 12.0)");

        assertQuery("SELECT a.col2.col0, a.col0, SUM(a.col2.col1) FROM " +
                        "(VALUES " +
                        "ROW(CAST(ROW(1.0, ARRAY[2, 13, 4], row(11, 4.1E0))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(2.0, ARRAY[2, 23, 4], row(11, 14.0E0))  AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.5, ARRAY[2, 13, 4], row(11, 4.1E0))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(1.5, ARRAY[2, 13, 4], row(11, 4.1E0))   AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double)))), " +
                        "ROW(CAST(ROW(7.0, ARRAY[22, 33, 44], row(13, 5.0E0)) AS ROW(col0 double, col1 array(integer), col2 row(col0 integer, col1 double))))) t(a) " +
                        "WHERE a.col1[2] < 30 " +
                        "GROUP BY 1, 2 ORDER BY 1",
                "SELECT * FROM VALUES (11, 1.0, 4.1), (11, 1.5, 8.2), (11, 2.0, 14.0)");

        assertQuery("SELECT a[1].col0, COUNT(1) FROM " +
                        "(VALUES " +
                        "(ROW(CAST(ARRAY[row(31, 4.2E0), row(22, 4.2E0)] AS ARRAY(ROW(col0 integer, col1 double))))), " +
                        "(ROW(CAST(ARRAY[row(31, 4.5E0), row(12, 4.2E0)] AS ARRAY(ROW(col0 integer, col1 double))))), " +
                        "(ROW(CAST(ARRAY[row(41, 3.1E0), row(32, 4.2E0)] AS ARRAY(ROW(col0 integer, col1 double))))), " +
                        "(ROW(CAST(ARRAY[row(31, 3.1E0), row(32, 4.2E0)] AS ARRAY(ROW(col0 integer, col1 double))))) " +
                        ") t(a) " +
                        "GROUP BY 1 " +
                        "ORDER BY 2 DESC",
                "SELECT * FROM VALUES (31, 3), (41, 1)");
    }

    @Test
    public void testMapTransformKeys()
    {
        assertQuery(
                "SELECT\n" +
                        "   MAP_KEYS(TRANSFORM_KEYS(features, (k, v) -> MAP(ARRAY[1, 2], ARRAY[10, 20])[k])) as k1, \n" +
                        "   MAP_KEYS(TRANSFORM_KEYS(features, (k, v) -> MAP(ARRAY[1, 2], ARRAY[30, 40])[k])) as k2 \n" +
                        "FROM (SELECT MAP(ARRAY[1], ARRAY[1]) as features) ",
                "VALUES ((10), (30))");
    }

    @Test
    public void testTryMapTransformValueFunction()
    {
        // MaterializedResult#Builder doesn't support null row. Coalesce null value to empty map for comparison.
        MaterializedResult actual = computeActual("" +
                "SELECT COALESCE( TRY( TRANSFORM_VALUES( id, (k, v) -> k / v ) ) , MAP() )" +
                "FROM ( VALUES " +
                "(MAP(ARRAY[1, 2], ARRAY[0, 0])), " +
                "(MAP(ARRAY[1, 2], ARRAY[1, 2])), " +
                "(MAP(ARRAY[28, 56], ARRAY[2, 4])), " +
                "(MAP(ARRAY[4, 5], ARRAY[0, 0])), " +
                "(MAP(ARRAY[12, 72], ARRAY[3, 6]))) AS t (id)");

        MaterializedResult expected = resultBuilder(getSession(), mapType(INTEGER, INTEGER))
                .row(ImmutableMap.of())
                .row(ImmutableMap.of(1, 1, 2, 1))
                .row(ImmutableMap.of(28, 14, 56, 14))
                .row(ImmutableMap.of())
                .row(ImmutableMap.of(12, 4, 72, 12))
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testRowCast()
    {
        assertQuery("SELECT CAST(row(1, 2) AS row(aa bigint, bb boolean)).aa", "SELECT 1");
        assertQuery("SELECT CAST(row(1, 2) AS row(aa bigint, bb varchar)).bb", "SELECT '2'");
        assertQuery("SELECT CAST(row(1, 2) AS row(aa bigint, bb boolean)).bb", "SELECT true");
        assertQuery("SELECT CAST(row(true, array[0, 2]) AS row(aa boolean, bb array(boolean))).bb[1]", "SELECT false");
        assertQuery("SELECT CAST(row(0.1, array[0, 2], row(1, 0.5)) AS row(aa bigint, bb array(boolean), cc row(dd varchar, ee varchar))).cc.ee", "SELECT '0.5'");
        assertQuery("SELECT CAST(array[row(0.1, array[0, 2], row(1, 0.5))] AS array<row(aa bigint, bb array(boolean), cc row(dd varchar, ee varchar))>)[1].cc.ee", "SELECT '0.5'");
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
                        "SELECT t2.*, max(t1.b) AS max_b " +
                        "FROM (VALUES (1, 'a'),  (2, 'b'), (1, 'c'), (3, 'd')) t1(a, b) " +
                        "INNER JOIN " +
                        "(VALUES 1, 2, 3, 4) t2(a) " +
                        "ON t1.a = t2.a " +
                        "GROUP BY t2.a",
                "SELECT * FROM VALUES (1, 'c'), (2, 'b'), (3, 'd')");

        assertQuery("" +
                        "SELECT t2.*, max(t1.b1) AS max_b1 " +
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
                "      FROM orders x " +
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
                "  FROM orders x " +
                "  WHERE custkey < 100 " +
                ") t");
    }

    @Test
    public void testDereferenceInComparison()
    {
        assertQuery("" +
                "SELECT orders.custkey, orders.orderkey " +
                "FROM orders " +
                "WHERE orders.custkey > orders.orderkey AND orders.custkey < 200.3");
    }

    @Test
    public void testMissingRowFieldInGroupBy()
    {
        assertQueryFails(
                "SELECT a.col0, count(*) FROM (VALUES ROW(cast(ROW(1, 1) AS ROW(col0 integer, col1 integer)))) t(a)",
                "line 1:8: 'a.col0' must be an aggregate expression or appear in GROUP BY clause");
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
        assertQuery(
                "WITH unioned AS ( SELECT 1 UNION ALL SELECT 2 ) SELECT * FROM unioned CROSS JOIN UNNEST(ARRAY[3]) steps (step)",
                "SELECT * FROM (VALUES (1, 3), (2, 3))");
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
        assertQuery("SELECT a FROM (" +
                        "    SELECT l.arr AS arr FROM (" +
                        "        SELECT orderkey, ARRAY[1,2,3] AS arr FROM orders ORDER BY orderkey LIMIT 1) l" +
                        "    FULL OUTER JOIN (" +
                        "        SELECT orderkey, ARRAY[1,2,3] AS arr FROM orders ORDER BY orderkey LIMIT 1) o" +
                        "    ON l.orderkey = o.orderkey) " +
                        "CROSS JOIN UNNEST(arr) AS t (a)",
                "SELECT * FROM (VALUES (1), (2), (3))");

        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) LEFT OUTER JOIN UNNEST(x) ON true",
                "line .*: UNNEST on other than the right side of CROSS JOIN is not supported");
        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) RIGHT OUTER JOIN UNNEST(x) ON true",
                "line .*: UNNEST on other than the right side of CROSS JOIN is not supported");
        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) FULL OUTER JOIN UNNEST(x) ON true",
                "line .*: UNNEST on other than the right side of CROSS JOIN is not supported");
    }

    @Test
    public void testArrays()
    {
        assertQuery("SELECT a[1] FROM (SELECT ARRAY[orderkey] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey FROM orders");
        assertQuery("SELECT a[1 + CAST(round(rand()) AS BIGINT)] FROM (SELECT ARRAY[orderkey, orderkey] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey FROM orders");
        assertQuery("SELECT a[1] + 1 FROM (SELECT ARRAY[orderkey] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey + 1 FROM orders");
        assertQuery("SELECT a[1] FROM (SELECT ARRAY[orderkey + 1] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey + 1 FROM orders");
        assertQuery("SELECT a[1][1] FROM (SELECT ARRAY[ARRAY[orderkey + 1]] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey + 1 FROM orders");
        assertQuery("SELECT CARDINALITY(a) FROM (SELECT ARRAY[orderkey, orderkey + 1] AS a FROM orders ORDER BY orderkey) t", "SELECT 2 FROM orders");
    }

    @Test
    public void testArrayAgg()
    {
        assertQuery("SELECT clerk, cardinality(array_agg(orderkey)) FROM orders GROUP BY clerk", "SELECT clerk, count(*) FROM orders GROUP BY clerk");
    }

    @Test
    public void testReduceAgg()
    {
        assertQuery(
                "SELECT x, reduce_agg(y, 1, (a, b) -> a * b, (a, b) -> a * b) " +
                        "FROM (VALUES (1, 5), (1, 6), (1, 7), (2, 8), (2, 9), (3, 10)) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, 5 * 6 * 7), (2, 8 * 9), (3, 10)");
        assertQuery(
                "SELECT x, reduce_agg(y, 0, (a, b) -> a + b, (a, b) -> a + b) " +
                        "FROM (VALUES (1, 5), (1, 6), (1, 7), (2, 8), (2, 9), (3, 10)) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, 5 + 6 + 7), (2, 8 + 9), (3, 10)");

        assertQuery(
                "SELECT x, reduce_agg(y, 1, (a, b) -> a * b, (a, b) -> a * b) " +
                        "FROM (VALUES (1, CAST(5 AS DOUBLE)), (1, 6), (1, 7), (2, 8), (2, 9), (3, 10)) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, CAST(5 AS DOUBLE) * 6 * 7), (2, 8 * 9), (3, 10)");
        assertQuery(
                "SELECT x, reduce_agg(y, 0, (a, b) -> a + b, (a, b) -> a + b) " +
                        "FROM (VALUES (1, CAST(5 AS DOUBLE)), (1, 6), (1, 7), (2, 8), (2, 9), (3, 10)) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, CAST(5 AS DOUBLE) + 6 + 7), (2, 8 + 9), (3, 10)");

        assertQuery(
                "SELECT " +
                        "x, " +
                        "array_join(" +
                        "   array_sort(" +
                        "       split(reduce_agg(y, '', (a, b) -> a || b, (a, b) -> a || b), '')" +
                        "   ), " +
                        "   ''" +
                        ") " +
                        "FROM (VALUES (1, 'a'), (1, 'b'), (1, 'c'), (2, 'd'), (2, 'e'), (3, 'f')) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, 'abc'), (2, 'de'), (3, 'f')");

        assertQuery(
                "SELECT " +
                        "x, " +
                        "array_join(" +
                        "   array_sort(" +
                        "       reduce_agg(y, ARRAY['x'], (a, b) -> a || b, (a, b) -> a || b)" +
                        "   ), " +
                        "   ''" +
                        ") " +
                        "FROM (VALUES (1, ARRAY['a']), (1, ARRAY['b']), (1, ARRAY['c']), (2, ARRAY['d']), (2, ARRAY['e']), (3, ARRAY['f'])) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, 'abcx'), (2, 'dex'), (3, 'fx')");

        assertQuery("SELECT REDUCE_AGG((x,y), (0,0), (x, y)->(x[1],y[1]), (x,y)->(x[1],y[1]))[1] from (select 1 x, 2 y)", "select 0");
    }

    @Test
    public void testRows()
    {
        // Using JSON_FORMAT(CAST(_ AS JSON)) because H2 does not support ROW type
        Session session = Session.builder(getSession()).setSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, "true").build();
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(1 + 2, CONCAT('a', 'b')) AS JSON))", "SELECT '{\"\":3,\"\":\"ab\"}'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(a + b) AS JSON)) FROM (VALUES (1, 2)) AS t(a, b)", "SELECT '{\"\":3}'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(1, ROW(9, a, ARRAY[], NULL), ROW(1, 2)) AS JSON)) FROM (VALUES ('a')) t(a)",
                "SELECT '{\"\":1,\"\":{\"\":9,\"\":\"a\",\"\":[],\"\":null},\"\":{\"\":1,\"\":2}}'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(ROW(ROW(ROW(ROW(a, b), c), d), e), f) AS JSON)) FROM (VALUES (ROW(0, 1), 2, '3', NULL, ARRAY[5], ARRAY[])) t(a, b, c, d, e, f)",
                "SELECT '{\"\":{\"\":{\"\":{\"\":{\"\":{\"\":0,\"\":1},\"\":2},\"\":\"3\"},\"\":null},\"\":[5]},\"\":[]}'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ARRAY_AGG(ROW(a, b)) AS JSON)) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(a, b)",
                "SELECT '[{\"\":1,\"\":2},{\"\":3,\"\":4},{\"\":5,\"\":6}]'");
        assertQuery(session, "SELECT CONTAINS(ARRAY_AGG(ROW(a, b)), ROW(1, 2)) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(a, b)", "SELECT TRUE");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ARRAY_AGG(ROW(c, d)) AS JSON)) FROM (VALUES (ARRAY[1, 3, 5], ARRAY[2, 4, 6])) AS t(a, b) CROSS JOIN UNNEST(a, b) AS u(c, d)",
                "SELECT '[{\"\":1,\"\":2},{\"\":3,\"\":4},{\"\":5,\"\":6}]'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(x, y, z) AS JSON)) FROM (VALUES ROW(1, NULL, '3')) t(x,y,z)", "SELECT '{\"\":1,\"\":null,\"\":\"3\"}'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(x, y, z) AS JSON)) FROM (VALUES ROW(1, CAST(NULL AS INTEGER), '3')) t(x,y,z)", "SELECT '{\"\":1,\"\":null,\"\":\"3\"}'");
    }

    @Test
    public void testMaps()
    {
        assertQuery("SELECT m[max_key] FROM (SELECT map_agg(orderkey, orderkey) m, max(orderkey) max_key FROM orders)", "SELECT max(orderkey) FROM orders");
        // Make sure that even if the map constructor throws with the NULL key the block builders are left in a consistent state
        // and the TRY() call eventually succeeds and return NULL values.
        assertQuery("SELECT JSON_FORMAT(CAST(TRY(MAP(ARRAY[NULL], ARRAY[x])) AS JSON)) FROM (VALUES 1, 2) t(x)", "SELECT * FROM (VALUES NULL, NULL)");

        assertQuery("SELECT cardinality(m) FROM (SELECT map_agg(orderkey, orderkey) m FROM orders)", "SELECT count(orderkey) FROM orders");
        assertQuery("SELECT cast(cardinality(m) as varchar) FROM (SELECT map_agg(orderkey, orderkey) m FROM orders)", "SELECT cast(count(orderkey) as varchar) FROM orders");
        assertQuery("SELECT cardinality(map_keys(m)) FROM (SELECT map_agg(orderkey, orderkey) m FROM orders)", "SELECT count(orderkey) FROM orders");
        assertQuery("SELECT cardinality(map_values(m)) FROM (SELECT map_agg(orderkey, orderkey) m FROM orders)", "SELECT count(orderkey) FROM orders");
        assertQuery("SELECT cardinality(map_keys(m)) + cardinality(map_values(m)) FROM (SELECT map_agg(cast(orderkey as varchar), cast(orderkey as varchar)) m FROM orders)", "SELECT count(orderkey) * 2 FROM orders");
        assertQuery("SELECT cardinality(map(array[cardinality(map_values(m))], array[cardinality(map_values(m))])) FROM (SELECT map_agg(orderkey, orderkey) m FROM orders)", "SELECT 1");
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
        assertQuery("SELECT 1.1 in (VALUES (1.1), (2.2))", "VALUES (TRUE)");

        assertQuery("" +
                        "WITH a AS (VALUES (1.1, 2), (sin(3.3), 2+2)) " +
                        "SELECT * FROM a",
                "VALUES (1.1, 2), (sin(3.3), 2+2)");

        // implicit coercions
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
        MaterializedResult raw = computeActual("SELECT orderstatus, orderkey, totalprice FROM orders");

        Multimap<String, Long> orderKeyByStatus = ArrayListMultimap.create();
        Multimap<String, Double> totalPriceByStatus = ArrayListMultimap.create();
        for (MaterializedRow row : raw.getMaterializedRows()) {
            orderKeyByStatus.put((String) row.getField(0), ((Number) row.getField(1)).longValue());
            totalPriceByStatus.put((String) row.getField(0), (Double) row.getField(2));
        }

        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, " +
                "   approx_percentile(orderkey, 0.5), " +
                "   approx_percentile(totalprice, 0.5)," +
                "   approx_percentile(orderkey, 2, 0.5)," +
                "   approx_percentile(totalprice, 2, 0.5)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        for (MaterializedRow row : actual.getMaterializedRows()) {
            String status = (String) row.getField(0);
            Long orderKey = ((Number) row.getField(1)).longValue();
            Double totalPrice = (Double) row.getField(2);
            Long orderKeyWeighted = ((Number) row.getField(3)).longValue();
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
        assertQueryOrdered(
                "SELECT sum(orderkey), row_number() OVER (ORDER BY orderkey) " +
                        "FROM orders " +
                        "WHERE orderkey <= 10 " +
                        "GROUP BY orderkey " +
                        "HAVING sum(orderkey) >= 3 " +
                        "ORDER BY orderkey DESC " +
                        "LIMIT 3",
                "VALUES (7, 5), (6, 4), (5, 3)");
    }

    @Test
    public void testWhereNull()
    {
        // This query is has this strange shape to force the compiler to leave a true on the stack
        // with the null flag set so if the filter method is not handling nulls correctly, this
        // query will fail
        assertQuery("SELECT custkey FROM orders WHERE custkey = custkey AND CAST(nullif(custkey, custkey) AS boolean) AND CAST(nullif(custkey, custkey) AS boolean)");
    }

    @Test
    public void testDistinctMultipleFields()
    {
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM orders");
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
    public void testDistinctHaving()
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) AS count " +
                "FROM orders " +
                "GROUP BY orderdate " +
                "HAVING COUNT(DISTINCT clerk) > 1");
    }

    @Test
    public void testDistinctLimitWithQuickDistinctLimitEnabled()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(QUICK_DISTINCT_LIMIT_ENABLED, "true")
                .build();
        testDistinctLimitInternal(session);
    }

    @Test
    public void testDistinctLimit()
    {
        testDistinctLimitInternal(getSession());
    }

    public void testDistinctLimitInternal(Session session)
    {
        assertQuery(session,
                "select cardinality(map_agg((orderstatus, custkey), true)) from (SELECT DISTINCT orderstatus, custkey " +
                        "FROM (SELECT orderstatus, custkey FROM orders ORDER BY orderkey LIMIT 10) " +
                        "LIMIT 10)", "select 10");
        assertQuery(session, "SELECT COUNT(*) FROM (SELECT DISTINCT orderstatus, custkey FROM orders LIMIT 10)");
        assertQuery(session, "SELECT DISTINCT custkey, orderstatus FROM orders WHERE custkey = 1268 LIMIT 2");
        assertQuery(session, "SELECT DISTINCT custkey, orderstatus FROM orders WHERE custkey = 1268 LIMIT 10000");
        assertQuery(session, "SELECT DISTINCT custkey, orderstatus FROM orders WHERE custkey = 1268 LIMIT 15000");
        assertQuerySucceeds(session, "SELECT DISTINCT custkey FROM orders LIMIT 2");
        assertQuerySucceeds(session, "SELECT DISTINCT custkey FROM orders LIMIT 10000");

        assertQuery(session, "" +
                        "SELECT DISTINCT x FROM (VALUES 1) t(x) JOIN (VALUES 10, 20) u(a) ON t.x < u.a LIMIT 100",
                "SELECT 1");
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
    public void testGroupByOrderByLimit()
    {
        assertQueryOrdered("SELECT custkey, SUM(totalprice) FROM orders GROUP BY custkey ORDER BY SUM(totalprice) DESC LIMIT 10");
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
    public void testOffset()
    {
        Session localSession = Session.builder(getSession())
                .setSystemProperty(OFFSET_CLAUSE_ENABLED, "true")
                .build();
        String values = "(VALUES ('A', 3), ('D', 2), ('C', 1), ('B', 4)) AS t(x, y)";

        MaterializedResult actual = computeActual(localSession, "SELECT x FROM " + values + " OFFSET 2 ROWS");
        MaterializedResult all = computeExpected("SELECT x FROM " + values, actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 2);
        assertNotEquals(actual.getMaterializedRows().get(0), actual.getMaterializedRows().get(1));
        assertContains(all, actual);
    }

    @Test
    public void testOffsetEmptyResult()
    {
        Session localSession = Session.builder(getSession())
                .setSystemProperty(OFFSET_CLAUSE_ENABLED, "true")
                .build();
        assertQueryReturnsEmptyResult(localSession, "SELECT name FROM nation OFFSET 100 ROWS");
        assertQueryReturnsEmptyResult(localSession, "SELECT name FROM nation ORDER BY regionkey OFFSET 100 ROWS");
        assertQueryReturnsEmptyResult(localSession, "SELECT name FROM nation OFFSET 100 ROWS LIMIT 20");
        assertQueryReturnsEmptyResult(localSession, "SELECT name FROM nation ORDER BY regionkey OFFSET 100 ROWS LIMIT 20");
    }

    @Test
    public void testOffsetLimitOrderByConsistency()
    {
        Session localSession = Session.builder(getSession())
                .setSystemProperty(OFFSET_CLAUSE_ENABLED, "true")
                .build();

        String query = "SELECT name FROM customer ORDER BY name OFFSET 1 LIMIT 256";
        MaterializedResult expectedResults = computeActual(localSession, query).toTestTypes();
        List<MaterializedRow> expectedRows = expectedResults.getMaterializedRows();

        for (int i = 0; i < 5; i++) {
            MaterializedResult actualResults = computeActual(localSession, query).toTestTypes();
            List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
            assertEquals(actualRows, expectedRows, "Mismatched results on run " + i);
        }
    }

    @Test
    public void testRepeatedAggregations()
    {
        assertQuery("SELECT SUM(orderkey), SUM(orderkey) FROM orders");
    }

    @Test
    public void testRepeatedOutputs()
    {
        assertQuery("SELECT orderkey a, orderkey b FROM orders WHERE orderstatus = 'F'");
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
        MaterializedResult actual = computeActual("SELECT orderkey FROM orders LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM orders", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testLimitWithAggregation()
    {
        MaterializedResult actual = computeActual("SELECT custkey, SUM(CAST(totalprice * 100 AS BIGINT)) FROM orders GROUP BY custkey LIMIT 10");
        MaterializedResult all = computeExpected("SELECT custkey, SUM(CAST(totalprice * 100 AS BIGINT)) FROM orders GROUP BY custkey", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testLimitInInlineView()
    {
        MaterializedResult actual = computeActual("SELECT orderkey FROM (SELECT orderkey FROM orders LIMIT 100) T LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM orders", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testCountAll()
    {
        assertQuery("SELECT COUNT(*) FROM orders");
        assertQuery("SELECT COUNT(42) FROM orders", "SELECT COUNT(*) FROM orders");
        assertQuery("SELECT COUNT(42 + 42) FROM orders", "SELECT COUNT(*) FROM orders");
        assertQuery("SELECT COUNT(null) FROM orders", "SELECT 0");
    }

    @Test
    public void testCountColumn()
    {
        assertQuery("SELECT COUNT(orderkey) FROM orders");
        assertQuery("SELECT COUNT(orderstatus) FROM orders");
        assertQuery("SELECT COUNT(orderdate) FROM orders");
        assertQuery("SELECT COUNT(1) FROM orders");

        assertQuery("SELECT COUNT(NULLIF(orderstatus, 'F')) FROM orders");
        assertQuery("SELECT COUNT(CAST(NULL AS BIGINT)) FROM orders"); // todo: make COUNT(null) work
    }

    @Test
    public void testCountOverGlobalAggregation()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT COUNT(*) FROM nation)");
    }

    @Test
    public void testCountOverGroupedAggregation()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT COUNT(*) FROM nation GROUP BY GROUPING SETS (nationkey, ()))", "SELECT 26");
    }

    @Test
    public void testWildcard()
    {
        assertQuery("SELECT * FROM orders");
    }

    @Test
    public void testMultipleWildcards()
    {
        assertQuery("SELECT *, 123, * FROM orders");
    }

    @Test
    public void testMixedWildcards()
    {
        assertQuery("SELECT *, orders.*, orderkey FROM orders");
    }

    @Test
    public void testQualifiedWildcardFromAlias()
    {
        assertQuery("SELECT T.* FROM orders T");
    }

    @Test
    public void testQualifiedWildcardFromInlineView()
    {
        assertQuery("SELECT T.* FROM (SELECT orderkey + custkey FROM orders) T");
    }

    @Test
    public void testQualifiedWildcard()
    {
        assertQuery("SELECT orders.* FROM orders");
    }

    @Test
    public void testAverageAll()
    {
        assertQuery("SELECT AVG(totalprice) FROM orders");
    }

    @Test
    public void testVariance()
    {
        // int64
        assertQuery("SELECT VAR_SAMP(custkey) FROM orders");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM orders ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM orders ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT VAR_SAMP(custkey) FROM (SELECT custkey FROM orders LIMIT 0) T");

        // double
        assertQuery("SELECT VAR_SAMP(totalprice) FROM orders");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM orders ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM orders ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT VAR_SAMP(totalprice) FROM (SELECT totalprice FROM orders LIMIT 0) T");
    }

    @Test
    public void testZeroVarianceSamp()
    {
        assertQuery("SELECT VAR_SAMP(6523763181031200) FROM LINEITEM");
    }

    @Test
    public void testZeroVariancePop()
    {
        assertQuery("SELECT VAR_POP(6523763181031200) FROM LINEITEM");
    }

    @Test
    public void testZeroStddevSamp()
    {
        assertQuery("SELECT STDDEV_SAMP(6523763181031200) FROM LINEITEM");
    }

    @Test
    public void testZeroStddevPop()
    {
        assertQuery("SELECT STDDEV_POP(6523763181031200) FROM LINEITEM");
    }

    @Test
    public void testVariancePop()
    {
        // int64
        assertQuery("SELECT VAR_POP(custkey) FROM orders");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM orders ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM orders ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT VAR_POP(custkey) FROM (SELECT custkey FROM orders LIMIT 0) T");

        // double
        assertQuery("SELECT VAR_POP(totalprice) FROM orders");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM orders ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM orders ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT VAR_POP(totalprice) FROM (SELECT totalprice FROM orders LIMIT 0) T");
    }

    @Test
    public void testStdDev()
    {
        // int64
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM orders");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM orders ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM orders ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT STDDEV_SAMP(custkey) FROM (SELECT custkey FROM orders LIMIT 0) T");

        // double
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM orders");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM orders ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM orders ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT STDDEV_SAMP(totalprice) FROM (SELECT totalprice FROM orders LIMIT 0) T");
    }

    @Test
    public void testStdDevPop()
    {
        // int64
        assertQuery("SELECT STDDEV_POP(custkey) FROM orders");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM orders ORDER BY custkey LIMIT 2) T");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM orders ORDER BY custkey LIMIT 1) T");
        assertQuery("SELECT STDDEV_POP(custkey) FROM (SELECT custkey FROM orders LIMIT 0) T");

        // double
        assertQuery("SELECT STDDEV_POP(totalprice) FROM orders");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM orders ORDER BY totalprice LIMIT 2) T");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM orders ORDER BY totalprice LIMIT 1) T");
        assertQuery("SELECT STDDEV_POP(totalprice) FROM (SELECT totalprice FROM orders LIMIT 0) T");
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

        assertQuery(
                "SELECT regionkey, count(*) FROM (" +
                        "   SELECT regionkey FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM (VALUES 2, 100) t(regionkey)) " +
                        "GROUP BY ROLLUP (regionkey)",
                "SELECT * FROM (VALUES  (0, 5), (1, 5), (2, 6), (3, 5), (4, 5), (100, 1), (NULL, 27))");
    }

    @Test
    public void testGrouping()
    {
        assertQuery(
                "SELECT a, b AS t, sum(c), grouping(a, b) + grouping(a) " +
                        "FROM (VALUES ('h', 'j', 11), ('k', 'l', 7)) AS t (a, b, c) " +
                        "GROUP BY GROUPING SETS ( (a), (b)) " +
                        "ORDER BY grouping(b) ASC",
                "VALUES (NULL, 'j', 11, 3), (NULL, 'l', 7, 3), ('h', NULL, 11, 1), ('k', NULL, 7, 1)");

        assertQuery(
                "SELECT a, sum(b), grouping(a) FROM (VALUES ('h', 11, 0), ('k', 7, 0)) AS t (a, b, c) GROUP BY GROUPING SETS (a)",
                "VALUES ('h', 11, 0), ('k', 7, 0)");

        // Grouping sets with limit
        assertQuery(
                "SELECT a, sum(b) as sum FROM (VALUES ('h', 11), ('h', 12), ('k', 7)) AS t (a, b) GROUP BY GROUPING SETS ((), a) order by sum limit 1",
                "VALUES ('k',7)");

        assertQuery(
                "SELECT a, b, sum(c), grouping(a, b) FROM (VALUES ('h', 'j', 11), ('k', 'l', 7) ) AS t (a, b, c) GROUP BY GROUPING SETS ( (a), (b)) HAVING grouping(a, b) > 1 ",
                "VALUES (NULL, 'j', 11, 2), (NULL, 'l', 7, 2)");

        assertQuery("SELECT a, grouping(a) * 1.0 FROM (VALUES (1) ) AS t (a) GROUP BY a",
                "VALUES (1, 0.0)");

        assertQuery("SELECT a, grouping(a), grouping(a) FROM (VALUES (1) ) AS t (a) GROUP BY a",
                "VALUES (1, 0, 0)");

        assertQuery("SELECT grouping(a) FROM (VALUES ('h', 'j', 11), ('k', 'l', 7)) AS t (a, b, c) GROUP BY GROUPING SETS (a,c), c*2",
                "VALUES (0), (1), (0), (1)");
    }

    @Test
    public void testGroupingSets()
    {
        Session sessionWithPushRemoteExchangeThroughGroupIdEnabled = Session.builder(getSession())
                .setSystemProperty(PUSH_REMOTE_EXCHANGE_THROUGH_GROUP_ID, "true")
                .build();
        Session sessionWithPushRemoteExchangeThroughGroupIdDisabled = Session.builder(getSession())
                .setSystemProperty(PUSH_REMOTE_EXCHANGE_THROUGH_GROUP_ID, "false")
                .build();
        testGroupingSets(sessionWithPushRemoteExchangeThroughGroupIdEnabled);
        testGroupingSets(sessionWithPushRemoteExchangeThroughGroupIdDisabled);
    }

    private void testGroupingSets(Session session)
    {
        assertQuery(session,
                "SELECT cast(sum(totalprice) as bigint), orderstatus, orderpriority FROM orders GROUP BY GROUPING SETS ((orderstatus), (orderstatus, orderpriority))",
                "VALUES (200872441, 'O', '5-LOW'), " +
                        "        (199678223, 'O', '3-MEDIUM'), " +
                        "        (206109275, 'F', '1-URGENT'), " +
                        "        (210258874, 'O', '2-HIGH'), " +
                        "        (13665497, 'P', '3-MEDIUM'), " +
                        "        (12650639, 'P', '2-HIGH'), " +
                        "        (1028376331, 'O', NULL), " +
                        "        (205922827, 'F', '4-NOT SPECIFIED'), " +
                        "        (1035681023, 'F', NULL), " +
                        "        (208560811, 'O', '4-NOT SPECIFIED'), " +
                        "        (210211977, 'F', '5-LOW'), " +
                        "        (63339475, 'P', NULL), " +
                        "        (202158746, 'F', '3-MEDIUM'), " +
                        "        (209005981, 'O', '1-URGENT'), " +
                        "        (211278198, 'F', '2-HIGH'), " +
                        "        (12098256, 'P', '5-LOW'), " +
                        "        (11233550, 'P', '1-URGENT'), " +
                        "        (13691534, 'P', '4-NOT SPECIFIED') ");

        assertQuery(session,
                "SELECT cast(sum(totalprice) as bigint), orderstatus, orderpriority FROM orders GROUP BY GROUPING SETS ((orderstatus), (orderpriority))",
                "VALUES (1028376331, 'O', NULL), " +
                        "        (1035681023, 'F', NULL), " +
                        "        (63339475, 'P', NULL), " +
                        "        (423182675, NULL, '5-LOW'), " +
                        "        (434187712, NULL, '2-HIGH'), " +
                        "        (426348806, NULL, '1-URGENT'), " +
                        "        (428175171, NULL, '4-NOT SPECIFIED'), " +
                        "        (415502467, NULL, '3-MEDIUM')");
    }

    @Test
    public void testGroupingWithFortyArguments()
    {
        // This test ensures we correctly pick the bigint implementation version of the grouping
        // function which supports up to 62 columns. Semantically it is exactly the same as
        // TestGroupingOperationFunction#testMoreThanThirtyTwoArguments. That test is a little easier to
        // understand and verify.
        String fortyLetterSequence = "aa, ab, ac, ad, ae, af, ag, ah, ai, aj, ak, al, am, an, ao, ap, aq, ar, asa, at, au, av, aw, ax, ay, az, " +
                "ba, bb, bc, bd, be, bf, bg, bh, bi, bj, bk, bl, bm, bn";
        String fortyIntegers = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, " +
                "31, 32, 33, 34, 35, 36, 37, 38, 39, 40";
        // 20, 2, 13, 33, 40, 9 , 14 (corresponding indices from Left to right in the above fortyLetterSequence)
        String groupingSet1 = "at, ab, am, bg, bn, ai, an";
        // 28, 4, 5, 29, 31, 10 (corresponding indices from left to right in the above fortyLetterSequence)
        String groupingSet2 = "bb, ad, ae, bc, be, aj";
        String query = String.format(
                "SELECT grouping(%s) FROM (VALUES (%s)) AS t(%s) GROUP BY GROUPING SETS ((%s), (%s), (%s))",
                fortyLetterSequence,
                fortyIntegers,
                fortyLetterSequence,
                fortyLetterSequence,
                groupingSet1,
                groupingSet2);

        assertQuery(query, "VALUES (0), (822283861886), (995358664191)");
    }

    @Test
    public void testGroupingInTableSubquery()
    {
        // In addition to testing grouping() in subqueries, the following tests also
        // ensure correct behavior in the case of alternating GROUPING SETS and GROUP BY
        // clauses in the same plan. This is significant because grouping() with GROUP BY
        // works only with a special re-write that should not happen in the presence of
        // GROUPING SETS.

        // Inner query has a single GROUP BY and outer query has GROUPING SETS
        assertQuery(
                "SELECT orderkey, custkey, sum(agg_price) AS outer_sum, grouping(orderkey, custkey), g " +
                        "FROM " +
                        "    (SELECT orderkey, custkey, sum(totalprice) AS agg_price, grouping(custkey, orderkey) AS g " +
                        "        FROM orders " +
                        "        GROUP BY orderkey, custkey " +
                        "        ORDER BY agg_price ASC " +
                        "        LIMIT 5) AS t " +
                        "GROUP BY GROUPING SETS ((orderkey, custkey), g) " +
                        "ORDER BY outer_sum",
                "VALUES (35271, 334, 874.89, 0, NULL), " +
                        "       (28647, 1351, 924.33, 0, NULL), " +
                        "       (58145, 862, 929.03, 0, NULL), " +
                        "       (8354, 634, 974.04, 0, NULL), " +
                        "       (37415, 301, 986.63, 0, NULL), " +
                        "       (NULL, NULL, 4688.92, 3, 0)");

        // Inner query has GROUPING SETS and outer query has GROUP BY
        assertQuery(
                "SELECT orderkey, custkey, g, sum(agg_price) AS outer_sum, grouping(orderkey, custkey) " +
                        "FROM " +
                        "    (SELECT orderkey, custkey, sum(totalprice) AS agg_price, grouping(custkey, orderkey) AS g " +
                        "     FROM orders " +
                        "     GROUP BY GROUPING SETS ((custkey), (orderkey)) " +
                        "     ORDER BY agg_price ASC " +
                        "     LIMIT 5) AS t " +
                        "GROUP BY orderkey, custkey, g",
                "VALUES (28647, NULL, 2, 924.33, 0), " +
                        "       (8354, NULL, 2, 974.04, 0), " +
                        "       (37415, NULL, 2, 986.63, 0), " +
                        "       (58145, NULL, 2, 929.03, 0), " +
                        "       (35271, NULL, 2, 874.89, 0)");

        // Inner query has GROUPING SETS but no grouping and outer query has a simple GROUP BY
        assertQuery(
                "SELECT orderkey, custkey, sum(agg_price) AS outer_sum, grouping(orderkey, custkey) " +
                        "FROM " +
                        "   (SELECT orderkey, custkey, sum(totalprice) AS agg_price " +
                        "    FROM orders " +
                        "    GROUP BY GROUPING SETS ((custkey), (orderkey)) " +
                        "    ORDER BY agg_price ASC NULLS FIRST) AS t " +
                        "GROUP BY orderkey, custkey " +
                        "ORDER BY outer_sum ASC NULLS FIRST " +
                        "LIMIT 5",
                "VALUES (35271, NULL, 874.89, 0), " +
                        "       (28647, NULL, 924.33, 0), " +
                        "       (58145, NULL, 929.03, 0), " +
                        "       (8354,  NULL, 974.04, 0), " +
                        "       (37415, NULL, 986.63, 0)");
    }

    @Test
    public void testIntersect()
    {
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM nation WHERE nationkey > 21");
        assertQuery(
                "SELECT regionkey FROM nation WHERE nationkey < 7 " +
                        "INTERSECT DISTINCT SELECT regionkey FROM nation WHERE nationkey > 21",
                "VALUES 1, 3");
        assertQuery(
                "WITH wnation AS (SELECT nationkey, regionkey FROM nation) " +
                        "SELECT regionkey FROM wnation WHERE nationkey < 7 " +
                        "INTERSECT SELECT regionkey FROM wnation WHERE nationkey > 21", "VALUES 1, 3");
        assertQuery(
                "SELECT num FROM (SELECT 1 AS num FROM nation WHERE nationkey=10 " +
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
                        "EXCEPT SELECT regionkey FROM nation WHERE nationkey > 21");
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
                "SELECT num FROM (SELECT 1 AS num FROM nation WHERE nationkey=10 " +
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
        assertQuery("SELECT COUNT(*) FROM nation EXCEPT SELECT COUNT(regionkey) FROM nation WHERE regionkey < 3 HAVING SUM(regionkey) IS NOT NULL");
        assertQuery("SELECT SUM(nationkey), COUNT(name) FROM (SELECT nationkey, name FROM nation WHERE nationkey < 6 EXCEPT SELECT regionkey, name FROM nation) n");
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
    public void testSelectWithComparison()
    {
        assertQuery("SELECT orderkey FROM lineitem WHERE tax < discount");
    }

    @Test
    public void testInlineView()
    {
        assertQuery("SELECT orderkey, custkey FROM (SELECT orderkey, custkey FROM orders) U");
    }

    @Test
    public void testAliasedInInlineView()
    {
        assertQuery("SELECT x, y FROM (SELECT orderkey x, custkey y FROM orders) U");
    }

    @Test
    public void testInlineViewWithProjections()
    {
        assertQuery("SELECT x + 1, y FROM (SELECT orderkey * 10 x, custkey y FROM orders) u");
    }

    @Test
    public void testInUncorrelatedSubquery()
    {
        assertQuery(
                "SELECT CASE WHEN false THEN 1 IN (VALUES 2) END",
                "SELECT NULL");
        assertQuery(
                "SELECT x FROM (VALUES 2) t(x) WHERE MAP(ARRAY[8589934592], ARRAY[x]) IN (VALUES MAP(ARRAY[8589934592],ARRAY[2]))",
                "SELECT 2");
        assertQuery(
                "SELECT a IN (VALUES 2), a FROM (VALUES (2)) t(a)",
                "SELECT TRUE, 2");
    }

    @Test
    public void testUncorrelatedSubqueryWithEmptyResult()
    {
        assertQuery(
                "SELECT regionkey, (select name from nation where false) from region",
                "SELECT regionkey, NULL from region");
        assertQuery(
                "SELECT regionkey, (select name from nation where nationkey = 5 and mod(nationkey,5) = 1) from region",
                "SELECT regionkey, NULL from region");
    }

    @Test
    public void testChecksum()
    {
        assertQuery("SELECT to_hex(checksum(0))", "SELECT '0000000000000000'");
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
    public void testColumnAliases()
    {
        assertQuery(
                "SELECT x, T.y, z + 1 FROM (SELECT custkey, orderstatus, totalprice FROM orders) T (x, y, z)",
                "SELECT custkey, orderstatus, totalprice + 1 FROM orders");
    }

    @Test
    public void testRowNumberNoOptimization()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE NOT rn <= 10");
        MaterializedResult all = computeExpected("SELECT orderkey, orderstatus FROM orders", actual.getTypes());
        assertEquals(actual.getMaterializedRows().size(), all.getMaterializedRows().size() - 10);
        assertContains(all, actual);

        actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn - 5 <= 10");
        all = computeExpected("SELECT orderkey, orderstatus FROM orders", actual.getTypes());
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
    public void testRowNumberSpecialFilters()
    {
        // Test "row_number() = negative number" filter with ORDER BY. This should create a Window Node with a Filter Node on top and return 0 rows.
        assertQueryReturnsEmptyResult("" +
                "SELECT * FROM (" +
                "   SELECT a, row_number() OVER (PARTITION BY a ORDER BY a) rn\n" +
                "   FROM (VALUES (1), (1), (1), (2), (2), (3)) t (a)) t " +
                "WHERE rn = -1");

        // Test "row_number() <= negative number" filter with ORDER BY. This should create a Window Node with a Filter Node on top and return 0 rows.
        assertQueryReturnsEmptyResult("" +
                "SELECT * FROM (" +
                "   SELECT a, row_number() OVER (PARTITION BY a ORDER BY a) rn\n" +
                "   FROM (VALUES (1), (1), (1), (2), (2), (3)) t (a)) t " +
                "WHERE rn <= -1");

        // Test "row_number() = 0" filter with ORDER BY. This should create a Window Node with a Filter Node on top and return 0 rows.
        assertQueryReturnsEmptyResult("" +
                "SELECT * FROM (" +
                "   SELECT a, row_number() OVER (PARTITION BY a ORDER BY a) rn\n" +
                "   FROM (VALUES (1), (1), (1), (2), (2), (3)) t (a)) t " +
                "WHERE rn = 0");

        // Test "row_number() = negative number" filter without ORDER BY. This should create a RowNumber Node with a Filter Node on top and return 0 rows.
        assertQueryReturnsEmptyResult("" +
                "SELECT * FROM (" +
                "   SELECT a, row_number() OVER (PARTITION BY a) rn\n" +
                "   FROM (VALUES (1), (1), (1), (2), (2), (3)) t (a)) t " +
                "WHERE rn = -1");

        // Test "row_number() <= negative number" filter without ORDER BY. This should create a RowNumber Node with a Filter Node on top and return 0 rows.
        assertQueryReturnsEmptyResult("" +
                "SELECT * FROM (" +
                "   SELECT a, row_number() OVER (PARTITION BY a) rn\n" +
                "   FROM (VALUES (1), (1), (1), (2), (2), (3)) t (a)) t " +
                "WHERE rn <= -1");

        // Test "row_number() = 0" filter without ORDER BY. This should create a RowNumber Node with a Filter Node on top and return 0 rows.
        assertQueryReturnsEmptyResult("" +
                "SELECT * FROM (" +
                "   SELECT a, row_number() OVER (PARTITION BY a) rn\n" +
                "   FROM (VALUES (1), (1), (1), (2), (2), (3)) t (a)) t " +
                "WHERE rn = 0");
    }

    @Test
    public void testMinMaxN()
    {
        assertQuery("" +
                        "SELECT x FROM (" +
                        "SELECT min(orderkey, 3) t FROM orders" +
                        ") CROSS JOIN UNNEST(t) AS a(x)",
                "VALUES 1, 2, 3");

        assertQuery(
                "SELECT orderkey, min(orderkey, 3) over() AS t FROM orders",
                "SELECT orderkey, ARRAY[cast(1 as bigint), cast(2 as bigint), cast(3 as bigint)] t FROM orders");
    }

    @Test
    public void testMinMaxByN()
    {
        assertQuery("SELECT MIN_BY(c0, c0, c1) OVER ( PARTITION BY c2 ORDER BY c3 ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) FROM " +
                "( VALUES (1, 10, FALSE, 0), (2, 10, FALSE, 1) ) AS t(c0, c1, c2, c3)", "values array[1], array[1, 2]");
        assertQuery("SELECT MIN_BY(c0, c3, c1) OVER ( PARTITION BY c2 ORDER BY c3 ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) FROM " +
                "( VALUES (1, 10, FALSE, 2), (2, 10, FALSE, 1) ) AS t(c0, c1, c2, c3)", "values array[2], array[2, 1]");
        assertQuery("select max_by(k1, k2, 2) over (partition by k3 order by k4) from (values (1, 'a', 1, 3), (2, 'b', 1, 2), (5, 'd', 2, 1), (8, 'c', 2, 0)) " +
                "t(k1, k2, k3, k4)", "values array[8], array[5, 8], array[2], array[2, 1]");
        assertQuery("select max_by(k1, k2, 2) over (partition by k3 order by k4) from (values (1, 'a', 1, 3), (2, 'b', 1, 2), (5, 'd', 2, 1), (8, 'c', 2, 0), (7, 'e', 1, 8), " +
                "(9, 'f', 2, 10), (0, 'g', 1, 9)) t(k1, k2, k3, k4)", "values array[8], array[5, 8], array[9, 5], array[2], array[2, 1], array[7, 2], array[0, 7]");
        assertQuery("select min_by(k1, k2, 2) over (partition by k3 order by k4) from (values (1, 'a', 1, 3), (2, 'b', 1, 2), (5, 'd', 2, 1), (8, 'c', 2, 0), (7, 'e', 1, 8), " +
                "(9, 'f', 2, 10), (0, 'g', 1, 9)) t(k1, k2, k3, k4)", "values array[2], array[1, 2], array[1, 2], array[1, 2], array[8], array[8, 5], array[8, 5]");
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
        MaterializedResult all = computeExpected("SELECT orderkey, orderstatus FROM orders", actual.getTypes());
        assertEquals(actual.getMaterializedRows().size(), 5);
        assertContains(all, actual);

        actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn < 5");
        all = computeExpected("SELECT orderkey, orderstatus FROM orders", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 4);
        assertContains(all, actual);

        actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") LIMIT 5");
        all = computeExpected("SELECT orderkey, orderstatus FROM orders", actual.getTypes());

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
        MaterializedResult all = computeExpected("SELECT orderkey, orderstatus FROM orders", actual.getTypes());

        // there are 3 DISTINCT orderstatus, so expect 15 rows.
        assertEquals(actual.getMaterializedRows().size(), 15);
        assertContains(all, actual);

        // Test for unreferenced outputs
        actual = computeActual("" +
                "SELECT orderkey FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus) rn, orderkey\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 5");
        all = computeExpected("SELECT orderkey FROM orders", actual.getTypes());

        // there are 3 distinct orderstatus, so expect 15 rows.
        assertEquals(actual.getMaterializedRows().size(), 15);
        assertContains(all, actual);
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
        assertQuery(
                "SELECT orderkey, orderstatus, SUM(rn) OVER (PARTITION BY orderstatus) c " +
                        "FROM ( " +
                        "   SELECT orderkey, orderstatus, row_number() OVER (PARTITION BY orderstatus) rn " +
                        "   FROM ( " +
                        "       SELECT * FROM orders ORDER BY orderkey LIMIT 10 " +
                        "   ) " +
                        ")",
                "VALUES " +
                        "(1, 'O', 21), " +
                        "(2, 'O', 21), " +
                        "(3, 'F', 10), " +
                        "(4, 'O', 21), " +
                        "(5, 'F', 10), " +
                        "(6, 'F', 10), " +
                        "(7, 'O', 21), " +
                        "(32, 'O', 21), " +
                        "(33, 'F', 10), " +
                        "(34, 'O', 21)");
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
        assertQuery(
                "SELECT * FROM ( " +
                        "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey, orderstatus " +
                        "   FROM orders " +
                        ") WHERE rn <= 2",
                "VALUES " +
                        "(1, 1, 'O'), " +
                        "(2, 2, 'O'), " +
                        "(1, 3, 'F'), " +
                        "(2, 5, 'F'), " +
                        "(1, 65, 'P'), " +
                        "(2, 197, 'P')");

        // Test for unreferenced outputs
        assertQuery(
                "SELECT * FROM ( " +
                        "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey " +
                        "   FROM orders " +
                        ") WHERE rn <= 2",
                "VALUES " +
                        "(1, 1), " +
                        "(2, 2), " +
                        "(1, 3), " +
                        "(2, 5), " +
                        "(1, 65), " +
                        "(2, 197)");

        assertQuery(
                "SELECT * FROM ( " +
                        "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderstatus " +
                        "   FROM orders " +
                        ") WHERE rn <= 2",
                "VALUES " +
                        "(1, 'O'), " +
                        "(2, 'O'), " +
                        "(1, 'F'), " +
                        "(2, 'F'), " +
                        "(1, 'P'), " +
                        "(2, 'P')");
    }

    @Test
    public void testTopNUnpartitionedWindowWithEqualityFilter()
    {
        assertQuery(
                "SELECT * FROM ( " +
                        "   SELECT row_number() OVER (ORDER BY orderkey) rn, orderkey, orderstatus " +
                        "   FROM orders " +
                        ") WHERE rn = 2",
                "VALUES (2, 2, 'O')");
    }

    @Test
    public void testTopNUnpartitionedWindowWithCompositeFilter()
    {
        assertQuery(
                "SELECT * FROM ( " +
                        "   SELECT row_number() OVER (ORDER BY orderkey) rn, orderkey, orderstatus " +
                        "   FROM orders " +
                        ") WHERE rn = 1 OR rn IN (3, 4) OR rn BETWEEN 6 AND 7",
                "VALUES " +
                        "(1, 1, 'O'), " +
                        "(3, 3, 'F'), " +
                        "(4, 4, 'O'), " +
                        "(6, 6, 'F'), " +
                        "(7, 7, 'O')");
    }

    @Test
    public void testTopNPartitionedWindowWithEqualityFilter()
    {
        assertQuery(
                "SELECT * FROM ( " +
                        "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey, orderstatus " +
                        "   FROM orders " +
                        ") WHERE rn = 2",
                "VALUES " +
                        "(2, 2, 'O'), " +
                        "(2, 5, 'F'), " +
                        "(2, 197, 'P')");

        // Test for unreferenced outputs
        assertQuery(
                "SELECT * FROM ( " +
                        "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey " +
                        "   FROM orders " +
                        ") WHERE rn = 2",
                "VALUES (2, 2), (2, 5), (2, 197)");

        assertQuery(
                "SELECT * FROM ( " +
                        "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderstatus " +
                        "   FROM orders " +
                        ") WHERE rn = 2",
                "VALUES (2, 'O'), (2, 'F'), (2, 'P')");
    }

    @Test
    public void testScalarFunction()
    {
        assertQuery("SELECT SUBSTR('Quadratically', 5, 6)");
    }

    @Test
    public void testCast()
    {
        assertQuery("SELECT CAST('1' AS BIGINT)");
        assertQuery("SELECT CAST(totalprice AS BIGINT) FROM orders");
        assertQuery("SELECT CAST(orderkey AS DOUBLE) FROM orders");
        assertQuery("SELECT CAST(orderkey AS VARCHAR) FROM orders");
        assertQuery("SELECT CAST(orderkey AS BOOLEAN) FROM orders");

        assertQuery("SELECT try_cast('1' AS BIGINT)", "SELECT CAST('1' AS BIGINT)");
        assertQuery("SELECT try_cast(totalprice AS BIGINT) FROM orders", "SELECT CAST(totalprice AS BIGINT) FROM orders");
        assertQuery("SELECT try_cast(orderkey AS DOUBLE) FROM orders", "SELECT CAST(orderkey AS DOUBLE) FROM orders");
        assertQuery("SELECT try_cast(orderkey AS VARCHAR) FROM orders", "SELECT CAST(orderkey AS VARCHAR) FROM orders");
        assertQuery("SELECT try_cast(orderkey AS BOOLEAN) FROM orders", "SELECT CAST(orderkey AS BOOLEAN) FROM orders");

        assertQuery("SELECT try_cast('foo' AS BIGINT)", "SELECT CAST(null AS BIGINT)");
        assertQuery("SELECT try_cast(clerk AS BIGINT) FROM orders", "SELECT CAST(null AS BIGINT) FROM orders");
        assertQuery("SELECT try_cast(orderkey * orderkey AS VARCHAR) FROM orders", "SELECT CAST(orderkey * orderkey AS VARCHAR) FROM orders");
        assertQuery("SELECT try_cast(try_cast(orderkey AS VARCHAR) AS BIGINT) FROM orders", "SELECT orderkey FROM orders");
        assertQuery("SELECT try_cast(clerk AS VARCHAR) || try_cast(clerk AS VARCHAR) FROM orders", "SELECT clerk || clerk FROM orders");

        assertQuery("SELECT coalesce(try_cast('foo' AS BIGINT), 456)", "SELECT 456");
        assertQuery("SELECT coalesce(try_cast(clerk AS BIGINT), 456) FROM orders", "SELECT 456 FROM orders");

        assertQuery("SELECT CAST(x AS BIGINT) FROM (VALUES 1, 2, 3, NULL) t (x)", "VALUES 1, 2, 3, NULL");
        assertQuery("SELECT try_cast(x AS BIGINT) FROM (VALUES 1, 2, 3, NULL) t (x)", "VALUES 1, 2, 3, NULL");
    }

    @Test
    public void testNestedCast()
    {
        assertQuery("select cast(varchar_value as varchar(3)) || ' sfd' from (values ('9898.122')) t(varchar_value)", "VALUES '989 sfd'");
        assertQuery("select cast(cast(varchar_value as varchar(3)) as varchar(5)) from (values ('9898.122')) t(varchar_value)", "VALUES '989'");
    }

    @Test
    public void testInvalidCast()
    {
        assertQueryFails(
                "SELECT CAST(1 AS DATE)",
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
        assertQuery("SELECT '12' || '34'");
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
                "SELECT * FROM lineitem l JOIN (SELECT orderkey_1, custkey FROM orders) o on l.orderkey = o.orderkey_1",
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
    public void testWith()
    {
        assertQuery("" +
                        "WITH a AS (SELECT * FROM orders) " +
                        "SELECT * FROM a",
                "SELECT * FROM orders");
        assertQuerySucceeds("WITH t(x, y, z) AS (TABLE region) SELECT * FROM t");
    }

    @Test
    public void testWithQualifiedPrefix()
    {
        assertQuery("WITH a AS (SELECT 123) SELECT a.* FROM a", "SELECT 123");
    }

    @Test
    public void testWithAliased()
    {
        assertQuery("WITH a AS (SELECT * FROM orders) SELECT * FROM a x", "SELECT * FROM orders");
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
        assertQuery("WITH a (id) AS (SELECT 123) SELECT id FROM a", "SELECT 123");

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
                "SELECT 2");
        assertQueryFails(
                "WITH a AS (VALUES 1), " +
                        "     a AS (VALUES 2)" +
                        "SELECT * FROM a",
                "line 1:28: WITH query name 'a' specified more than once");
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
        assertQuery(" SELECT CASE x WHEN 1 THEN CAST(1 AS decimal(4,1)) WHEN 2 THEN CAST(1 AS decimal(4,2)) ELSE CAST(1 AS decimal(4,3)) END FROM (values 1) t(x)", "SELECT 1.000");
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
        assertQuery("SELECT if(true, CAST(1 AS decimal(2,1)), 1)", "SELECT 1.0");
    }

    @Test
    public void testIn()
    {
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1, 2, 3)");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1.5, 2.3)", "SELECT orderkey FROM orders LIMIT 0"); // H2 incorrectly matches rows
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1, 2E0, 3)");
        assertQuery("SELECT orderkey FROM orders WHERE totalprice IN (1, 2, 3)");
        assertQuery("SELECT x FROM (values 3, 100) t(x) WHERE x IN (2147483649)", "SELECT * WHERE false");
        assertQuery("SELECT x FROM (values 3, 100, 2147483648, 2147483649, 2147483650) t(x) WHERE x IN (2147483648, 2147483650)", "values 2147483648, 2147483650");
        assertQuery("SELECT x FROM (values 3, 100, 2147483648, 2147483649, 2147483650) t(x) WHERE x IN (3, 4, 2147483648, 2147483650)", "values 3, 2147483648, 2147483650");
        assertQuery("SELECT x FROM (values 1, 2, 3) t(x) WHERE x IN (1 + CAST(rand() < 0 AS bigint), 2 + CAST(rand() < 0 AS bigint))", "values 1, 2");
        assertQuery("SELECT x FROM (values 1, 2, 3, 4) t(x) WHERE x IN (1 + CAST(rand() < 0 AS bigint), 2 + CAST(rand() < 0 AS bigint), 4)", "values 1, 2, 4");
        assertQuery("SELECT x FROM (values 1, 2, 3, 4) t(x) WHERE x IN (4, 2, 1)", "values 1, 2, 4");
        assertQuery("SELECT x FROM (values 1, 2, 3, 2147483648) t(x) WHERE x IN (1 + CAST(rand() < 0 AS bigint), 2 + CAST(rand() < 0 AS bigint), 2147483648)", "values 1, 2, 2147483648");
        assertQuery("SELECT x IN (0) FROM (values 4294967296) t(x)", "values false");
        assertQuery("SELECT x IN (0, 4294967297 + CAST(rand() < 0 AS bigint)) FROM (values 4294967296, 4294967297) t(x)", "values false, true");
        assertQuery("SELECT NULL in (1, 2, 3)", "values null");
        assertQuery("SELECT 1 in (1, NULL, 3)", "values true");
        assertQuery("SELECT 2 in (1, NULL, 3)", "values null");
        assertQuery("SELECT x FROM (values DATE '1970-01-01', DATE '1970-01-03') t(x) WHERE x IN (DATE '1970-01-01')", "values DATE '1970-01-01'");
        assertQuery("SELECT COUNT(*) FROM (values 1) t(x) WHERE x IN (null, 0)", "SELECT 0");
        assertQuery("SELECT d IN (DECIMAL '2.0', DECIMAL '30.0') FROM (VALUES (2.0E0)) t(d)", "SELECT true"); // coercion with type only coercion inside IN list
    }

    @Test
    public void testInTimestampWithTimezone()
    {
        assertEquals(
                computeActual("SELECT x FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1970-01-01 00:01:00+08:00') t(x) WHERE x IN (TIMESTAMP '1970-01-01 00:01:00+00:00')")
                        .getOnlyColumn().collect(toList()),
                ImmutableList.of(zonedDateTime("1970-01-01 00:01:00.000 UTC"), zonedDateTime("1970-01-01 08:01:00.000 +08:00")));
    }

    @Test
    public void testLargeIn()
    {
        String longValues = range(0, 5000)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        Session session = Session.builder(getSession())
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "15000ms")
                .build();
        assertQuery(session, "SELECT orderkey FROM orders WHERE orderkey IN (" + longValues + ")");
        assertQuery(session, "SELECT orderkey FROM orders WHERE orderkey NOT IN (" + longValues + ")");

        assertQuery(session, "SELECT orderkey FROM orders WHERE orderkey IN (mod(1000, orderkey), " + longValues + ")");
        assertQuery(session, "SELECT orderkey FROM orders WHERE orderkey NOT IN (mod(1000, orderkey), " + longValues + ")");

        String varcharValues = range(0, 5000)
                .mapToObj(i -> "'" + i + "'")
                .collect(joining(", "));
        assertQuery(session, "SELECT orderkey FROM orders WHERE cast(orderkey AS VARCHAR) IN (" + varcharValues + ")");
        assertQuery(session, "SELECT orderkey FROM orders WHERE cast(orderkey AS VARCHAR) NOT IN (" + varcharValues + ")");

        String arrayValues = range(0, 5000)
                .mapToObj(i -> format("ARRAY[%s, %s, %s]", i, i + 1, i + 2))
                .collect(joining(", "));
        assertQuery(session, "SELECT ARRAY[0, 0, 0] in (ARRAY[0, 0, 0], " + arrayValues + ")", "values true");
        assertQuery(session, "SELECT ARRAY[0, 0, 0] in (" + arrayValues + ")", "values false");
    }

    @Test
    public void testLargeInWithHistograms()
    {
        String longValues = range(0, 10_000)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        String query = "select orderpriority, sum(totalprice) from lineitem join orders on lineitem.orderkey = orders.orderkey where orders.orderkey in (" + longValues + ") group by 1";
        Session session = Session.builder(getSession())
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "30000ms")
                .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "true")
                .build();
        assertQuerySucceeds(session, query);
        session = Session.builder(getSession())
                .setSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, "20000ms")
                .setSystemProperty(OPTIMIZER_USE_HISTOGRAMS, "false")
                .build();
        assertQuerySucceeds(session, query);
    }

    @Test
    public void testNullOnLhsOfInPredicateAllowed()
    {
        assertQuery("SELECT NULL IN (1, 2, 3)", "SELECT NULL");
        assertQuery("SELECT NULL IN (SELECT 1)", "SELECT NULL");
        assertQuery("SELECT NULL IN (SELECT 1 WHERE FALSE)", "SELECT FALSE");
        assertQuery("SELECT x FROM (VALUES NULL) t(x) WHERE x IN (SELECT 1)", "SELECT 33 WHERE FALSE");
        assertQuery("SELECT NULL IN (SELECT CAST(NULL AS BIGINT))", "SELECT NULL");
        assertQuery("SELECT NULL IN (SELECT NULL WHERE FALSE)", "SELECT FALSE");
        assertQuery("SELECT NULL IN ((SELECT 1) UNION ALL (SELECT NULL))", "SELECT NULL");
        assertQuery("SELECT x IN (SELECT TRUE) FROM (SELECT * FROM (VALUES CAST(NULL AS BOOLEAN)) t(x) WHERE (x OR NULL) IS NULL)", "SELECT NULL");
        assertQuery("SELECT x IN (SELECT 1) FROM (SELECT * FROM (VALUES CAST(NULL AS INTEGER)) t(x) WHERE (x + 10 IS NULL) OR X = 2)", "SELECT NULL");
        assertQuery("SELECT x IN (SELECT 1 WHERE FALSE) FROM (SELECT * FROM (VALUES CAST(NULL AS INTEGER)) t(x) WHERE (x + 10 IS NULL) OR X = 2)", "SELECT FALSE");
    }

    @Test
    public void testDuplicateFields()
    {
        assertQuery(
                "SELECT * FROM (SELECT orderkey, orderkey FROM orders)",
                "SELECT orderkey, orderkey FROM orders");
    }

    @Test
    public void testWildcardFromSubquery()
    {
        assertQuery("SELECT * FROM (SELECT orderkey X FROM orders)");
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
        assertQuery("SELECT a.Col0 FROM (VALUES row(cast(ROW(1,2) AS ROW(col0 integer, col1 integer)))) AS t (a)", "SELECT 1");
    }

    @Test
    public void testSubqueryBody()
    {
        assertQuery("(SELECT orderkey, custkey FROM orders)");
    }

    @Test
    public void testSubqueryBodyOrderLimit()
    {
        assertQueryOrdered("(SELECT orderkey AS a, custkey AS b FROM orders) ORDER BY a LIMIT 1");
    }

    @Test
    public void testSubqueryBodyProjectedOrderby()
    {
        assertQueryOrdered("(SELECT orderkey, custkey FROM orders) ORDER BY orderkey * -1");
    }

    @Test
    public void testSubqueryBodyDoubleOrderby()
    {
        assertQueryOrdered("(SELECT orderkey, custkey FROM orders ORDER BY custkey) ORDER BY orderkey");
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
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("EXPLAIN ", query, LOGICAL));
    }

    @Test
    public void testDefaultExplainGraphvizFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (FORMAT GRAPHVIZ) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getGraphvizExplainPlan("EXPLAIN (FORMAT GRAPHVIZ) ", query, LOGICAL));
    }

    @Test
    public void testLogicalExplain()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("EXPLAIN (TYPE LOGICAL) ", query, LOGICAL));
    }

    @Test
    public void testIOExplain()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("EXPLAIN (TYPE IO) ", query, IO));
    }

    @Test
    public void testLogicalExplainTextFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT TEXT) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("EXPLAIN (TYPE LOGICAL, FORMAT TEXT) ", query, LOGICAL));
    }

    @Test
    public void testLogicalExplainGraphvizFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT GRAPHVIZ) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getGraphvizExplainPlan("EXPLAIN (TYPE LOGICAL, FORMAT GRAPHVIZ) ", query, LOGICAL));
    }

    @Test
    public void testLogicalExplainJsonFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT JSON) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getJsonExplainPlan("EXPLAIN (TYPE LOGICAL, FORMAT JSON) ", query, LOGICAL));
    }

    @Test
    public void testDistributedExplain()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("EXPLAIN (TYPE DISTRIBUTED) ", query, DISTRIBUTED));
    }

    @Test
    public void testDistributedExplainTextFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED, FORMAT TEXT) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("EXPLAIN (TYPE DISTRIBUTED, FORMAT TEXT) ", query, DISTRIBUTED));
    }

    @Test
    public void testDistributedExplainGraphvizFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED, FORMAT GRAPHVIZ) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getGraphvizExplainPlan("EXPLAIN (TYPE DISTRIBUTED, FORMAT GRAPHVIZ) ", query, DISTRIBUTED));
    }

    @Test
    public void testDistributedExplainJsonFormat()
    {
        String query = "SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getJsonExplainPlan("EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) ", query, DISTRIBUTED));
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
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("EXPLAIN ", query, LOGICAL));
    }

    @Test
    public void testExplainOfExplainAnalyze()
    {
        String query = "EXPLAIN ANALYZE SELECT * FROM orders";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("EXPLAIN ", query, LOGICAL));
    }

    @Test
    public void testExplainValidateOfExplain()
    {
        String query = "EXPLAIN SELECT 1";
        MaterializedResult result = computeActual("EXPLAIN (TYPE VALIDATE) " + query);
        assertEquals(result.getOnlyValue(), true);
    }

    @Test
    public void testExplainDdl()
    {
        assertExplainDdl("CREATE TABLE foo (pk bigint)", "CREATE TABLE foo");
        assertExplainDdl("CREATE VIEW foo AS SELECT * FROM orders", "CREATE VIEW foo");
        assertExplainDdl("CREATE OR REPLACE FUNCTION testing.default.tan (x int) RETURNS double COMMENT 'tangent trigonometric function' LANGUAGE SQL DETERMINISTIC CALLED ON NULL INPUT RETURN sin(x) / cos(x)", "CREATE FUNCTION testing.default.tan");
        assertExplainDdl("ALTER FUNCTION testing.default.tan CALLED ON NULL INPUT", "ALTER FUNCTION testing.default.tan");
        assertExplainDdl("DROP FUNCTION IF EXISTS testing.default.tan (int)", "DROP FUNCTION testing.default.tan");
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
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("", "SELECT * FROM orders", LOGICAL));
    }

    @Test
    public void testExplainExecuteWithUsing()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM orders WHERE orderkey < ?")
                .build();
        MaterializedResult result = computeActual(session, "EXPLAIN (TYPE LOGICAL) EXECUTE my_query USING 7");
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("", "SELECT * FROM orders WHERE orderkey < 7", LOGICAL));
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
    public void testShowCatalogsLikeWithEscape()
    {
        try {
            MaterializedResult result = computeActual(getSession(), "SHOW CATALOGS LIKE 't$_%' ESCAPE ''");
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Escape string must be a single character"));
        }

        try {
            MaterializedResult result = computeActual(getSession(), "SHOW CATALOGS LIKE 't$_%' ESCAPE '$$'");
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Escape string must be a single character"));
        }

        MaterializedResult result = computeActual(getSession(), "SHOW CATALOGS LIKE '%testing$_%' ESCAPE '$'");
        assertEquals("[[testing_catalog]]", result.getMaterializedRows().toString());

        result = computeActual(getSession(), "SHOW CATALOGS LIKE '$_%' ESCAPE '$'");
        assertEquals("[]", result.getMaterializedRows().toString());
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
    public void testUse()
    {
        Session sessionWithDefaultCatalogAndSchema = getSession();
        String catalog = sessionWithDefaultCatalogAndSchema.getCatalog().get();
        String schema = sessionWithDefaultCatalogAndSchema.getSchema().get();

        assertQueryFails(sessionWithDefaultCatalogAndSchema, "USE non_exist_schema", format("Schema does not exist: %s.non_exist_schema", catalog));
        assertQueryFails(sessionWithDefaultCatalogAndSchema, "USE non_exist_catalog.any_schema", "Catalog does not exist: non_exist_catalog");
        assertQueryFails(sessionWithDefaultCatalogAndSchema, format("USE %s.non_exist_schema", catalog), format("Schema does not exist: %s.non_exist_schema", catalog));
        assertUpdate(sessionWithDefaultCatalogAndSchema, format("USE %s.%s", catalog, schema));

        Session sessionWithoutDefaultCatalogAndSchema = Session.builder(getSession())
                .setCatalog(null)
                .setSchema(null)
                .build();
        assertQueryFails(sessionWithoutDefaultCatalogAndSchema, "USE any_schema", ".* Catalog must be specified when session catalog is not set");
        assertQueryFails(sessionWithoutDefaultCatalogAndSchema, "USE non_exist_catalog.any_schema", "Catalog does not exist: non_exist_catalog");
        assertQueryFails(sessionWithoutDefaultCatalogAndSchema, format("USE %s.non_exist_schema", catalog), format("Schema does not exist: %s.non_exist_schema", catalog));
        assertUpdate(sessionWithoutDefaultCatalogAndSchema, format("USE %s.%s", catalog, schema));
    }

    @Test
    public void testShowSchemasLike()
    {
        MaterializedResult result = computeActual(format("SHOW SCHEMAS LIKE '%s'", getSession().getSchema().get()));
        assertEquals(result.getOnlyColumnAsSet(), ImmutableSet.of(getSession().getSchema().get()));
    }

    @Test
    public void testShowSchemasLikeWithEscape()
    {
        assertQueryFails("SHOW SCHEMAS IN foo LIKE '%$_%' ESCAPE", "line 1:39: mismatched input '<EOF>'. Expecting: <string>");
        assertQueryFails("SHOW SCHEMAS LIKE 't$_%' ESCAPE ''", "Escape string must be a single character");
        assertQueryFails("SHOW SCHEMAS LIKE 't$_%' ESCAPE '$$'", "Escape string must be a single character");

        Set<Object> allSchemas = computeActual("SHOW SCHEMAS").getOnlyColumnAsSet();
        assertEquals(allSchemas, computeActual("SHOW SCHEMAS LIKE '%_%'").getOnlyColumnAsSet());
        Set<Object> result = computeActual("SHOW SCHEMAS LIKE '%$_%' ESCAPE '$'").getOnlyColumnAsSet();
        assertNotEquals(allSchemas, result);
        assertThat(result).contains("information_schema").allMatch(schemaName -> ((String) schemaName).contains("_"));
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

        assertQueryFails("SHOW TABLES FROM UNKNOWN", "line 1:1: Schema 'unknown' does not exist");
        assertQueryFails("SHOW TABLES FROM UNKNOWNCATALOG.UNKNOWNSCHEMA", "line 1:1: Catalog 'unknowncatalog' does not exist");
    }

    @Test
    public void testShowTablesLike()
    {
        assertThat(computeActual("SHOW TABLES LIKE 'or%'").getOnlyColumnAsSet())
                .contains("orders")
                .allMatch(tableName -> ((String) tableName).startsWith("or"));
    }

    @Test
    public void testShowTablesLikeWithEscape()
    {
        assertQueryFails("SHOW TABLES IN a LIKE '%$_%' ESCAPE", "line 1:36: mismatched input '<EOF>'. Expecting: <string>");
        assertQueryFails("SHOW TABLES LIKE 't$_%' ESCAPE ''", "Escape string must be a single character");
        assertQueryFails("SHOW TABLES LIKE 't$_%' ESCAPE '$$'", "Escape string must be a single character");

        Set<Object> allTables = computeActual("SHOW TABLES FROM information_schema").getOnlyColumnAsSet();
        assertEquals(allTables, computeActual("SHOW TABLES FROM information_schema LIKE '%_%'").getOnlyColumnAsSet());
        Set<Object> result = computeActual("SHOW TABLES FROM information_schema LIKE '%$_%' ESCAPE '$'").getOnlyColumnAsSet();
        assertNotEquals(allTables, result);
        assertThat(result).contains("table_privileges").allMatch(schemaName -> ((String) schemaName).contains("_"));
    }

    @Parameters("storageFormat")
    @Test
    public void testShowColumns(@Optional("PARQUET") String storageFormat)
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        // DWRF does not support date type.
        String format = (System.getProperty("storageFormat") == null) ? storageFormat : System.getProperty("storageFormat");
        String orderdateType = format.equals("DWRF") ? "varchar" : "date";
        MaterializedResult expectedUnparametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("orderkey", "bigint", "", "", 19L, null, null)
                .row("custkey", "bigint", "", "", 19L, null, null)
                .row("orderstatus", "varchar", "", "", null, null, 2147483647L)
                .row("totalprice", "double", "", "", 53L, null, null)
                .row("orderdate", orderdateType, "", "", null, null, format.equals("varchar") ? 2147483647L : null)
                .row("orderpriority", "varchar", "", "", null, null, 2147483647L)
                .row("clerk", "varchar", "", "", null, null, 2147483647L)
                .row("shippriority", "integer", "", "", 10L, null, null)
                .row("comment", "varchar", "", "", null, null, 2147483647L)
                .build();

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("orderkey", "bigint", "", "", 19L, null, null)
                .row("custkey", "bigint", "", "", 19L, null, null)
                .row("orderstatus", "varchar(1)", "", "", null, null, 1L)
                .row("totalprice", "double", "", "", 53L, null, null)
                .row("orderdate", orderdateType, "", "", null, null, format.equals("varchar") ? 2147483647L : null)
                .row("orderpriority", "varchar(15)", "", "", null, null, 15L)
                .row("clerk", "varchar(15)", "", "", null, null, 15L)
                .row("shippriority", "integer", "", "", 10L, null, null)
                .row("comment", "varchar(79)", "", "", null, null, 79L)
                .build();

        // Until we migrate all connectors to parametrized varchar we check two options
        assertTrue(actual.equals(expectedParametrizedVarchar) || actual.equals(expectedUnparametrizedVarchar),
                format("%s matches neither %s nor %s", actual, expectedParametrizedVarchar, expectedUnparametrizedVarchar));
    }

    @Test
    public void testAtTimeZone()
    {
        // TODO the expected values here are non-sensical due to https://github.com/prestodb/presto/issues/7122
        assertEquals(computeScalar("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'Asia/Oral'"), zonedDateTime("2012-10-30 16:00:00.000 Asia/Oral"));
        assertEquals(computeScalar("SELECT MIN(x) AT TIME ZONE 'America/Chicago' FROM (VALUES TIMESTAMP '1970-01-01 00:01:00+00:00') t(x)"), zonedDateTime("1969-12-31 18:01:00.000 America/Chicago"));
        assertEquals(computeScalar("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE '+07:09'"), zonedDateTime("2012-10-30 18:09:00.000 +07:09"));
        assertEquals(computeScalar("SELECT TIMESTAMP '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles'"), zonedDateTime("2012-10-30 18:00:00.000 America/Los_Angeles"));
        assertEquals(computeScalar("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'America/Los_Angeles'"), zonedDateTime("2012-10-30 04:00:00.000 America/Los_Angeles"));
        assertEquals(computeActual("SELECT x AT TIME ZONE 'America/Los_Angeles' FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00') t(x)").getOnlyColumnAsSet(),
                ImmutableSet.of(zonedDateTime("1969-12-31 16:01:00.000 America/Los_Angeles")));
        assertEquals(computeActual("SELECT x AT TIME ZONE 'America/Los_Angeles' FROM (values TIMESTAMP '1970-01-01 00:01:00', TIMESTAMP '1970-01-01 08:01:00', TIMESTAMP '1969-12-31 16:01:00') t(x)").getOnlyColumn().collect(toList()),
                ImmutableList.of(zonedDateTime("1970-01-01 03:01:00.000 America/Los_Angeles"), zonedDateTime("1970-01-01 11:01:00.000 America/Los_Angeles"), zonedDateTime("1969-12-31 19:01:00.000 America/Los_Angeles")));
        assertEquals(computeScalar("SELECT min(x) AT TIME ZONE 'America/Los_Angeles' FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00') t(x)"),
                zonedDateTime("1969-12-31 16:01:00.000 America/Los_Angeles"));

        // with chained AT TIME ZONE
        assertEquals(computeScalar("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'UTC'"), zonedDateTime("2012-10-30 11:00:00.000 UTC"));
        assertEquals(computeScalar("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'Asia/Tokyo' AT TIME ZONE 'America/Los_Angeles'"), zonedDateTime("2012-10-30 04:00:00.000 America/Los_Angeles"));
        assertEquals(computeScalar("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'Asia/Shanghai'"), zonedDateTime("2012-10-30 19:00:00.000 Asia/Shanghai"));
        assertEquals(computeScalar("SELECT min(x) AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'UTC' FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00') t(x)"),
                zonedDateTime("1970-01-01 00:01:00.000 UTC"));

        // with AT TIME ZONE in VALUES
        assertEquals(computeScalar("SELECT * FROM (VALUES TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'Asia/Oral')"), zonedDateTime("2012-10-30 16:00:00.000 Asia/Oral"));
    }

    @Test
    public void testAtTimeZoneWithInterval()
    {
        assertEquals(computeScalar("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE INTERVAL '07:09' hour to minute"), zonedDateTime("2012-10-30 18:09:00.000 +07:09"));
    }

    public ZonedDateTime zonedDateTime(String value)
    {
        return ZONED_DATE_TIME_FORMAT.parse(value, ZonedDateTime::from);
    }

    @Test
    public void testShowFunctions()
    {
        MaterializedResult result = computeActual("SHOW FUNCTIONS");
        ImmutableMultimap<String, MaterializedRow> functions = Multimaps.index(result.getMaterializedRows(), input -> {
            assertEquals(input.getFieldCount(), 10);
            return (String) input.getField(0);
        });

        assertTrue(functions.containsKey("avg"), "Expected function names " + functions + " to contain 'avg'");
        assertEquals(functions.get("avg").asList().size(), 6);
        assertEquals(functions.get("avg").asList().get(0).getField(1), "decimal(p,s)");
        assertEquals(functions.get("avg").asList().get(0).getField(2), "decimal(p,s)");
        assertEquals(functions.get("avg").asList().get(0).getField(3), "aggregate");
        assertEquals(functions.get("avg").asList().get(1).getField(1), "double");
        assertEquals(functions.get("avg").asList().get(1).getField(2), "bigint");
        assertEquals(functions.get("avg").asList().get(1).getField(3), "aggregate");
        assertEquals(functions.get("avg").asList().get(2).getField(1), "double");
        assertEquals(functions.get("avg").asList().get(2).getField(2), "double");
        assertEquals(functions.get("avg").asList().get(2).getField(3), "aggregate");
        assertEquals(functions.get("avg").asList().get(3).getField(1), "interval day to second");
        assertEquals(functions.get("avg").asList().get(3).getField(2), "interval day to second");
        assertEquals(functions.get("avg").asList().get(3).getField(3), "aggregate");
        assertEquals(functions.get("avg").asList().get(4).getField(1), "interval year to month");
        assertEquals(functions.get("avg").asList().get(4).getField(2), "interval year to month");
        assertEquals(functions.get("avg").asList().get(4).getField(3), "aggregate");
        assertEquals(functions.get("avg").asList().get(5).getField(1), "real");
        assertEquals(functions.get("avg").asList().get(5).getField(2), "real");
        assertEquals(functions.get("avg").asList().get(5).getField(3), "aggregate");

        assertTrue(functions.containsKey("abs"), "Expected function names " + functions + " to contain 'abs'");
        assertEquals(functions.get("abs").asList().get(0).getField(3), "scalar");
        assertEquals(functions.get("abs").asList().get(0).getField(4), true);
        assertEquals(functions.get("abs").asList().get(0).getField(6), false);
        assertEquals(functions.get("abs").asList().get(0).getField(7), true);
        assertEquals(functions.get("abs").asList().get(0).getField(8), false);
        assertEquals(functions.get("abs").asList().get(0).getField(9), "");

        assertTrue(functions.containsKey("rand"), "Expected function names " + functions + " to contain 'rand'");
        assertEquals(functions.get("rand").asList().get(0).getField(3), "scalar");
        assertEquals(functions.get("rand").asList().get(0).getField(4), false);
        assertEquals(functions.get("rand").asList().get(0).getField(6), false);
        assertEquals(functions.get("rand").asList().get(0).getField(7), true);
        assertEquals(functions.get("rand").asList().get(0).getField(8), false);
        assertEquals(functions.get("rand").asList().get(0).getField(9), "");

        assertTrue(functions.containsKey("rank"), "Expected function names " + functions + " to contain 'rank'");
        assertEquals(functions.get("rank").asList().get(0).getField(3), "window");
        assertEquals(functions.get("rank").asList().get(0).getField(4), true);
        assertEquals(functions.get("rank").asList().get(0).getField(6), false);
        assertEquals(functions.get("rank").asList().get(0).getField(7), true);
        assertEquals(functions.get("rank").asList().get(0).getField(8), false);
        assertEquals(functions.get("rank").asList().get(0).getField(9), "");

        assertTrue(functions.containsKey("greatest"), "Expected function names " + functions + " to contain 'greatest'");
        assertEquals(functions.get("greatest").asList().get(0).getField(3), "scalar");
        assertEquals(functions.get("greatest").asList().get(0).getField(4), true);
        assertEquals(functions.get("greatest").asList().get(0).getField(6), true);
        assertEquals(functions.get("greatest").asList().get(0).getField(7), true);
        assertEquals(functions.get("greatest").asList().get(0).getField(8), false);
        assertEquals(functions.get("greatest").asList().get(0).getField(9), "");

        assertTrue(functions.containsKey("split_part"), "Expected function names " + functions + " to contain 'split_part'");
        assertEquals(functions.get("split_part").asList().get(0).getField(1), "varchar(x)");
        assertEquals(functions.get("split_part").asList().get(0).getField(2), "varchar(x), varchar(y), bigint");
        assertEquals(functions.get("split_part").asList().get(0).getField(3), "scalar");
        assertEquals(functions.get("split_part").asList().get(0).getField(4), true);
        assertEquals(functions.get("split_part").asList().get(0).getField(6), false);
        assertEquals(functions.get("split_part").asList().get(0).getField(7), true);
        assertEquals(functions.get("split_part").asList().get(0).getField(8), false);
        assertEquals(functions.get("split_part").asList().get(0).getField(9), "");

        assertFalse(functions.containsKey("like"), "Expected function names " + functions + " not to contain 'like'");
    }

    @Test
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'bigint' AND table_name = 'customer' and column_name = 'custkey' LIMIT 1",
                "SELECT 'customer' table_name");
    }

    @Test
    public void testInformationSchemaUppercaseName()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'LOCAL'",
                "SELECT '' WHERE false");
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'TINY'",
                "SELECT '' WHERE false");
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ORDERS'",
                "SELECT '' WHERE false");
    }

    @Test
    public void testSelectColumnOfNulls()
    {
        // Currently nulls can confuse the local planner, so select some
        assertQueryOrdered("SELECT CAST(NULL AS VARCHAR), CAST(NULL AS BIGINT) FROM orders ORDER BY 1");
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
                java.util.Optional.empty(),
                getSession().isClientTransactionSupport(),
                getSession().getIdentity(),
                getSession().getSource(),
                getSession().getCatalog(),
                getSession().getSchema(),
                getSession().getTraceToken(),
                getSession().getTimeZoneKey(),
                getSession().getLocale(),
                getSession().getRemoteUserAddress(),
                getSession().getUserAgent(),
                getSession().getClientInfo(),
                getSession().getClientTags(),
                getSession().getResourceEstimates(),
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
                getSession().getPreparedStatements(),
                ImmutableMap.of(),
                getSession().getTracer(),
                getSession().getWarningCollector(),
                getSession().getRuntimeStats(),
                getSession().getQueryType());
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

        // Show session like
        result = computeActual(session, "SHOW SESSION LIKE '%test%'");

        properties = Maps.uniqueIndex(result.getMaterializedRows(), input -> {
            assertEquals(input.getFieldCount(), 5);
            return (String) input.getField(0);
        });

        assertEquals(properties.get("test_string"), new MaterializedRow(1, "test_string", "foo string", "test default", "varchar", "test string property"));
        assertEquals(properties.get("test_long"), new MaterializedRow(1, "test_long", "424242", "42", "bigint", "test long property"));

        // Show session like with escape
        result = computeActual(session, "SHOW SESSION LIKE '%test$_long%' ESCAPE '$'");

        properties = Maps.uniqueIndex(result.getMaterializedRows(), input -> {
            assertEquals(input.getFieldCount(), 5);
            return (String) input.getField(0);
        });

        assertEquals(properties.get("test_long"), new MaterializedRow(1, "test_long", "424242", "42", "bigint", "test long property"));

        try {
            computeActual(session, "SHOW SESSION LIKE 't$_%' ESCAPE ''");
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Escape string must be a single character"));
        }

        try {
            computeActual(session, "SHOW SESSION LIKE 't$_%' ESCAPE '$$'");
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Escape string must be a single character"));
        }
    }

    @Test
    public void testShowSessionWithoutNativeSessionProperties()
    {
        // SHOW SESSION will exclude native-worker session properties
        @Language("SQL") String sql = "SHOW SESSION";
        MaterializedResult actualResult = computeActual(sql);
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        String nativeSessionProperty = "native_expression_max_array_size_in_reduce";
        List<MaterializedRow> filteredRows = getNativeWorkerSessionProperties(actualRows, nativeSessionProperty);
        assertTrue(filteredRows.isEmpty());
    }

    @Test
    public void testSetSessionNativeWorkerSessionProperty()
    {
        // SET SESSION on a native-worker session property
        @Language("SQL") String setSession = "SET SESSION native_expression_max_array_size_in_reduce=50000";
        MaterializedResult setSessionResult = computeActual(setSession);
        assertEquals(
                setSessionResult.toString(),
                "MaterializedResult{rows=[[true]], " +
                        "types=[boolean], " +
                        "setSessionProperties={native_expression_max_array_size_in_reduce=50000}, " +
                        "resetSessionProperties=[], updateType=SET SESSION, " +
                        "clearTransactionId=false}");
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
        assertQueryFails("SELECT TRY(x) IS NULL FROM (SELECT 1/y AS x FROM (VALUES 1, 2, 3, 0, 4) t(y))", ".*(/|division) by zero.*");
        assertQuery("SELECT x IS NULL FROM (SELECT TRY(1/y) AS x FROM (VALUES 3, 0, 4) t(y))", "VALUES false, true, false");

        // test try with invalid JSON
        assertQuery("SELECT JSON_FORMAT(TRY(JSON 'INVALID'))", "SELECT NULL");
        assertQuery("SELECT JSON_FORMAT(TRY (JSON_PARSE('INVALID')))", "SELECT NULL");

        // tests that might be constant folded
        assertQuery("SELECT TRY(CAST(NULL AS BIGINT))", "SELECT NULL");
        assertQuery("SELECT TRY(CAST('123' AS BIGINT))", "SELECT 123L");
        assertQuery("SELECT TRY(CAST('foo' AS BIGINT))", "SELECT NULL");
        assertQuery("SELECT TRY(CAST('foo' AS BIGINT)) + TRY(CAST('123' AS BIGINT))", "SELECT NULL");
        assertQuery("SELECT TRY(CAST(CAST(123 AS VARCHAR) AS BIGINT))", "SELECT 123L");
        assertQuery("SELECT COALESCE(CAST(CONCAT('123', CAST(123 AS VARCHAR)) AS BIGINT), 0)", "SELECT 123123L");
        assertQuery("SELECT TRY(CAST(CONCAT('hello', CAST(123 AS VARCHAR)) AS BIGINT))", "SELECT NULL");
        assertQuery("SELECT COALESCE(TRY(CAST(CONCAT('a', CAST(123 AS VARCHAR)) AS INTEGER)), 0)", "SELECT 0");
        assertQuery("SELECT COALESCE(TRY(CAST(CONCAT('a', CAST(123 AS VARCHAR)) AS BIGINT)), 0)", "SELECT 0L");
        assertQuery("SELECT 123 + TRY(ABS(-9223372036854775807 - 1))", "SELECT NULL");
        assertQuery("SELECT JSON_FORMAT(TRY(JSON '[]')) || '123'", "SELECT '[]123'");
        assertQuery("SELECT JSON_FORMAT(TRY(JSON 'INVALID')) || '123'", "SELECT NULL");
        assertQuery("SELECT TRY(2/1)", "SELECT 2");
        assertQuery("SELECT TRY(2/0)", "SELECT null");
        assertQuery("SELECT COALESCE(TRY(2/0), 0)", "SELECT 0");
        assertQuery("SELECT TRY(ABS(-2))", "SELECT 2");

        // test try with null
        assertQuery("SELECT TRY(1 / x) FROM (SELECT NULL as x)", "SELECT NULL");
    }

    @Test
    public void testTryWithLambda()
    {
        assertQuery("SELECT TRY(apply(5, x -> x + 1) / 0)", "SELECT NULL");
        assertQuery("SELECT TRY(apply(5 + RANDOM(1), x -> x + 1) / 0)", "SELECT NULL");
        assertQuery("SELECT apply(5 + RANDOM(1), x -> x + TRY(1 / 0))", "SELECT NULL");
    }

    @Test
    public void testTryNoMergeProjections()
    {
        // no regexp specified because the JVM optimizes away exception message constructor if run enough times
        assertQueryFails("SELECT TRY(x) FROM (SELECT 1/y AS x FROM (VALUES 1, 2, 3, 0, 4) t(y))", ".*");
    }

    @Test
    public void testNoFrom()
    {
        assertQuery("SELECT 1 + 2, 3 + 4");
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
    public void testUnionWithTopN()
    {
        assertQuery("SELECT * FROM (" +
                        "   SELECT regionkey FROM nation " +
                        "   UNION ALL " +
                        "   SELECT nationkey FROM nation" +
                        ") t(a) " +
                        "ORDER BY a LIMIT 1",
                "SELECT 0");
    }

    @Test
    public void testUnionWithAggregation()
    {
        assertQuery(
                "SELECT regionkey, count(*) FROM (" +
                        "   SELECT regionkey FROM nation " +
                        "   UNION ALL " +
                        "   SELECT * FROM (VALUES 2, 100) t(regionkey)) " +
                        "GROUP BY regionkey",
                "SELECT * FROM (VALUES  (0, 5), (1, 5), (2, 6), (3, 5), (4, 5), (100, 1))");

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
                        "        SELECT custkey,orderkey, orderkey+1 FROM orders WHERE orderkey=0" +
                        "        UNION ALL " +
                        "        SELECT custkey,orderkey,orderkey+1 FROM orders WHERE orderkey<>0) " +
                        "    GROUP BY orderkey)");

        assertQuery(
                "SELECT count(orderkey), sum(sc) FROM (\n" +
                        "    SELECT sum(custkey) sc, orderkey FROM (\n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey=0\n" +
                        "        UNION ALL \n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey<>0) \n" +
                        "    GROUP BY GROUPING SETS ((orderkey, orderstatus), (orderkey)))",
                "SELECT count(orderkey), sum(sc) FROM (\n" +
                        "    SELECT sum(custkey) sc, orderkey FROM (\n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey=0\n" +
                        "        UNION ALL \n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey<>0) \n" +
                        "    GROUP BY orderkey, orderstatus \n" +
                        "    \n" +
                        "    UNION ALL \n" +
                        "    \n" +
                        "    SELECT sum(custkey) sc, orderkey FROM (\n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey=0\n" +
                        "        UNION ALL \n" +
                        "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey<>0) \n" +
                        "    GROUP BY orderkey)");
    }

    @Test
    public void testUnionWithUnionAndAggregation()
    {
        assertQuery(
                "SELECT count(*) FROM (" +
                        "SELECT 1 FROM nation GROUP BY regionkey " +
                        "UNION ALL " +
                        "SELECT 1 FROM (" +
                        "   SELECT 1 FROM nation " +
                        "   UNION ALL " +
                        "   SELECT 1 FROM nation))");
        assertQuery(
                "SELECT count(*) FROM (" +
                        "SELECT 1 FROM (" +
                        "   SELECT 1 FROM nation " +
                        "   UNION ALL " +
                        "   SELECT 1 FROM nation)" +
                        "UNION ALL " +
                        "SELECT 1 FROM nation GROUP BY regionkey)");
    }

    @Test
    public void testUnionWithAggregationAndTableScan()
    {
        assertQuery(
                "SELECT orderkey, 1 FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, count(*) FROM orders GROUP BY 1",
                "SELECT orderkey, 1 FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, count(*) FROM orders GROUP BY orderkey");

        assertQuery(
                "SELECT orderkey, count(*) FROM orders GROUP BY 1 " +
                        "UNION ALL " +
                        "SELECT orderkey, 1 FROM orders",
                "SELECT orderkey, count(*) FROM orders GROUP BY orderkey " +
                        "UNION ALL " +
                        "SELECT orderkey, 1 FROM orders");
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
        // Then as a result of predicate pushdown orderstatus (without cast) was compared with CAST('aaa' AS varchar(4)) which trigger checkArgument that
        // both types of comparison should be equal in DomainTranslator.
        assertQuery("SELECT a FROM " +
                "(" +
                "  (SELECT orderstatus AS a FROM orders LIMIT 1) " +
                "UNION ALL " +
                "  SELECT 'aaaa' AS a" +
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
        MaterializedResult all = computeExpected("SELECT * FROM orders", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testUnaliasSymbolReferencesWithUnion()
    {
        assertQuery("SELECT 1, 1, 'a', 'a' UNION ALL SELECT 1, 2, 'a', 'b'");
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
                "EXISTS(SELECT avg(orderkey) FROM orders)",
                "(SELECT avg(orderkey) FROM orders) > 3");
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
        assertQuery("SELECT (SELECT n.nationkey + n.NATIONKEY) FROM nation n");
        assertQuery("SELECT (SELECT 2 * n.nationkey) FROM nation n");
        assertQuery("SELECT nationkey FROM nation n WHERE 2 = (SELECT 2 * n.nationkey)");
        assertQuery("SELECT nationkey FROM nation n ORDER BY (SELECT 2 * n.nationkey)");

        // group by
        assertQuery("SELECT max(n.regionkey), 2 * n.nationkey, (SELECT n.nationkey) FROM nation n GROUP BY n.nationkey");
        assertQuery(
                "SELECT max(l.quantity), 2 * l.orderkey FROM lineitem l GROUP BY l.orderkey HAVING max(l.quantity) < (SELECT l.orderkey)");
        assertQuery("SELECT max(l.quantity), 2 * l.orderkey FROM lineitem l GROUP BY l.orderkey, (SELECT l.orderkey)");

        // join
        assertQuery("SELECT * FROM nation n1 JOIN nation n2 ON n1.nationkey = (SELECT n2.nationkey)");
        assertQueryFails(
                "SELECT (SELECT l3.* FROM lineitem l2 CROSS JOIN (SELECT l1.orderkey) l3 LIMIT 1) FROM lineitem l1",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // subrelation
        assertQuery(
                "SELECT 1 FROM nation n WHERE 2 * nationkey - 1  = (SELECT * FROM (SELECT n.nationkey))",
                "SELECT 1"); // h2 fails to parse this query

        // two level of nesting
        assertQuery("SELECT * FROM nation n WHERE 2 = (SELECT (SELECT 2 * n.nationkey))");

        // explicit LIMIT in subquery
        assertQueryFails(
                "SELECT (SELECT count(*) FROM (VALUES (7,1)) t(orderkey, value) WHERE orderkey = corr_key LIMIT 1) FROM (values 7) t(corr_key)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        assertQuery(
                "SELECT (SELECT count(*) FROM (VALUES (7,1)) t(orderkey, value) WHERE orderkey = corr_key GROUP BY value LIMIT 2) FROM (values 7) t(corr_key)");

        // Limit(1) and non-constant output symbol of the subquery (count)
        assertQueryFails("SELECT (SELECT count(*) FROM (VALUES (7,1), (7,2)) t(orderkey, value) WHERE orderkey = corr_key GROUP BY value LIMIT 1) FROM (values 7) t(corr_key)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testCorrelatedNonAggregationScalarSubqueries()
    {
        String subqueryReturnedTooManyRows = "Scalar sub-query has returned multiple rows";

        assertQuery("SELECT (SELECT 1 WHERE a = 2) FROM (VALUES 1) t(a)", "SELECT null");
        assertQuery("SELECT (SELECT 2 WHERE a = 1) FROM (VALUES 1) t(a)", "SELECT 2");
        assertQueryFails(
                "SELECT (SELECT 2 FROM (VALUES 3, 4) WHERE a = 1) FROM (VALUES 1) t(a)",
                subqueryReturnedTooManyRows);

        // multiple subquery output projections
        assertQueryFails(
                "SELECT name FROM nation n WHERE 'AFRICA' = (SELECT 'bleh' FROM region WHERE regionkey > n.regionkey)",
                subqueryReturnedTooManyRows);
        assertQueryFails(
                "SELECT name FROM nation n WHERE 'AFRICA' = (SELECT name FROM region WHERE regionkey > n.regionkey)",
                subqueryReturnedTooManyRows);
        assertQueryFails(
                "SELECT name FROM nation n WHERE 1 = (SELECT 1 FROM region WHERE regionkey > n.regionkey)",
                subqueryReturnedTooManyRows);

        // correlation used in subquery output
        assertQueryFails(
                "SELECT name FROM nation n WHERE 'AFRICA' = (SELECT n.name FROM region WHERE regionkey > n.regionkey)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        assertQuery(
                "SELECT (SELECT 2 WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                "VALUES 2, null, null, null, null");
        // outputs plain correlated orderkey symbol which causes ambiguity with outer query orderkey symbol
        assertQueryFails(
                "SELECT (SELECT o.orderkey WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertQueryFails(
                "SELECT (SELECT o.orderkey * 2 WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        // correlation used outside the subquery
        assertQueryFails(
                "SELECT o.orderkey, (SELECT o.orderkey * 2 WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // aggregation with having
//        TODO: uncomment below test once #8456 is fixed
//        assertQuery("SELECT (SELECT avg(totalprice) FROM orders GROUP BY custkey, orderdate HAVING avg(totalprice) < a) FROM (VALUES 900) t(a)");

        // correlation in predicate
        assertQuery("SELECT name FROM nation n WHERE 'AFRICA' = (SELECT name FROM region WHERE regionkey = n.regionkey)");

        // same correlation in predicate and projection
        assertQueryFails(
                "SELECT nationkey FROM nation n WHERE " +
                        "(SELECT n.regionkey * 2 FROM region r WHERE n.regionkey = r.regionkey) > 6",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // different correlation in predicate and projection
        assertQueryFails(
                "SELECT nationkey FROM nation n WHERE " +
                        "(SELECT n.nationkey * 2 FROM region r WHERE n.regionkey = r.regionkey) > 6",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // correlation used in subrelation
        assertQuery(
                "SELECT nationkey FROM nation n WHERE " +
                        "(SELECT regionkey * 2 FROM (SELECT regionkey FROM region r WHERE n.regionkey = r.regionkey)) > 6 " +
                        "ORDER BY 1 LIMIT 3",
                "VALUES 4, 10, 11"); // h2 didn't make it

        // with duplicated rows
        assertQuery(
                "SELECT (SELECT name FROM nation WHERE nationkey = a) FROM (VALUES 1, 1, 2, 3) t(a)",
                "VALUES 'ARGENTINA', 'ARGENTINA', 'BRAZIL', 'CANADA'"); // h2 didn't make it

        // returning null when nothing matched
        assertQuery(
                "SELECT (SELECT name FROM nation WHERE nationkey = a) FROM (VALUES 31) t(a)",
                "VALUES null");

        assertQuery(
                "SELECT (SELECT r.name FROM nation n, region r WHERE r.regionkey = n.regionkey AND n.nationkey = a) FROM (VALUES 1) t(a)",
                "VALUES 'AMERICA'");
    }

    @Test
    public void testCorrelatedScalarSubqueriesWithScalarAggregationAndEqualityPredicatesInWhere()
    {
        assertQuery("SELECT (SELECT count(*) WHERE o.orderkey = 1) FROM orders o");
        assertQuery("SELECT count(*) FROM orders o WHERE 1 = (SELECT count(*) WHERE o.orderkey = 0)");
        assertQuery("SELECT * FROM orders o ORDER BY (SELECT count(*) WHERE o.orderkey = 0)");
        assertQuery(
                "SELECT count(*) FROM nation n WHERE " +
                        "(SELECT count(*) FROM region r WHERE n.regionkey = r.regionkey) > 1");
        assertQueryFails(
                "SELECT count(*) FROM nation n WHERE " +
                        "(SELECT avg(a) FROM (SELECT count(*) FROM region r WHERE n.regionkey = r.regionkey) t(a)) > 1",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

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
                "VALUES ('1996-01-02', 1, 40000)"); // h2 is slow
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

        //count in subquery
        assertQuery("SELECT * " +
                        "FROM (VALUES (0),( 1), (2), (7)) AS v1(c1) " +
                        "WHERE v1.c1 > (SELECT count(c1) FROM (VALUES (0),( 1), (2)) AS v2(c1) WHERE v1.c1 = v2.c1)",
                "VALUES (2), (7)");
    }

    @Test
    public void testCorrelatedInPredicateSubqueries()
    {
        assertQuery("SELECT orderkey, clerk IN (SELECT clerk FROM orders s WHERE s.custkey = o.custkey AND s.orderkey < o.orderkey) FROM orders o");
        assertQuery("SELECT orderkey FROM orders o WHERE clerk IN (SELECT clerk FROM orders s WHERE s.custkey = o.custkey AND s.orderkey < o.orderkey)");

        // all cases of IN (as one test query to avoid pruning, over-eager push down)
        assertQuery(
                "SELECT t1.a, t1.b, " +
                        "  t1.b in (SELECT t2.b " +
                        "    FROM (values (2, 3), (2, 4), (3, 0), (30,NULL)) t2(a, b) " +
                        "    WHERE t1.a - 5 <= t2.a and t2.a <= t1.a and 0 <= t2.a) " +
                        "from (values (1,1), (2,4), (3,5), (4,NULL), (30,2), (40,NULL) ) t1(a, b) " +
                        "order by t1.a",
                "VALUES (1,1,FALSE), (2,4,TRUE), (3,5,FALSE), (4,NULL,NULL), (30,2,NULL), (40,NULL,FALSE)");

        // subquery with LIMIT (correlated filter below any unhandled node type)
        assertQueryFails(
                "SELECT orderkey FROM orders o WHERE clerk IN (SELECT clerk FROM orders s WHERE s.custkey = o.custkey AND s.orderkey < o.orderkey ORDER BY 1 LIMIT 1)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        assertQueryFails("SELECT 1 IN (SELECT l.orderkey) FROM lineitem l", UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertQueryFails("SELECT 1 IN (SELECT 2 * l.orderkey) FROM lineitem l", UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertQueryFails("SELECT * FROM lineitem l WHERE 1 IN (SELECT 2 * l.orderkey)", UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertQueryFails("SELECT * FROM lineitem l ORDER BY 1 IN (SELECT 2 * l.orderkey)", UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // group by
        assertQueryFails("SELECT max(l.quantity), 2 * l.orderkey, 1 IN (SELECT l.orderkey) FROM lineitem l GROUP BY l.orderkey", UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertQueryFails("SELECT max(l.quantity), 2 * l.orderkey FROM lineitem l GROUP BY l.orderkey HAVING max(l.quantity) IN (SELECT l.orderkey)", UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertQueryFails("SELECT max(l.quantity), 2 * l.orderkey FROM lineitem l GROUP BY l.orderkey, 1 IN (SELECT l.orderkey)", UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // join
        assertQueryFails("SELECT * FROM lineitem l1 JOIN lineitem l2 ON l1.orderkey IN (SELECT l2.orderkey)", UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // subrelation
        assertQueryFails(
                "SELECT * FROM lineitem l WHERE (SELECT * FROM (SELECT 1 IN (SELECT 2 * l.orderkey)))",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // two level of nesting
        assertQueryFails("SELECT * FROM lineitem l WHERE true IN (SELECT 1 IN (SELECT 2 * l.orderkey))", UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
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
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE EXISTS (SELECT avg(l.orderkey) FROM lineitem l WHERE o.orderkey = l.orderkey GROUP BY l.linenumber)");
        assertQueryFails(
                "SELECT count(*) FROM orders o " +
                        "WHERE EXISTS (SELECT count(*) FROM lineitem l WHERE o.orderkey = l.orderkey HAVING count(*) > 3)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

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

        // not exists
        assertQuery(
                "SELECT count(*) FROM customer WHERE NOT EXISTS(SELECT * FROM orders WHERE orders.custkey=customer.custkey)",
                "VALUES 500");
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
                "SELECT EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3) " +
                        "FROM lineitem l LIMIT 1");

        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 1000 = 0)",
                "VALUES 14999"); // h2 is slow
        assertQuery(
                "SELECT count(*) FROM lineitem l " +
                        "WHERE EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");

        // order by
        assertQuery(
                "SELECT orderkey FROM orders o ORDER BY " +
                        "EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)" +
                        "LIMIT 1",
                "VALUES 60000"); // h2 is slow
        assertQuery(
                "SELECT orderkey FROM lineitem l ORDER BY " +
                        "EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");

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
        assertQuery(
                "SELECT max(l.quantity), l.orderkey, EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3) FROM lineitem l " +
                        "GROUP BY l.orderkey");
        assertQuery(
                "SELECT max(l.quantity), l.orderkey FROM lineitem l " +
                        "GROUP BY l.orderkey " +
                        "HAVING EXISTS (SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");
        assertQuery(
                "SELECT max(l.quantity), l.orderkey FROM lineitem l " +
                        "GROUP BY l.orderkey, EXISTS (SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");

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
        assertQuery(
                "SELECT count(*) FROM orders o " +
                        "WHERE (SELECT * FROM (SELECT EXISTS(SELECT 1 WHERE o.orderkey > 10 OR o.orderkey != 3)))",
                "VALUES 14999");
    }

    @Test
    public void testCorrelatedJoin()
    {
        // TopN in correlated subquery
        assertQuery(
                "SELECT regionkey, n.name FROM region CROSS JOIN LATERAL (SELECT name FROM nation WHERE region.regionkey = regionkey ORDER BY nationkey LIMIT 2) n",
                "VALUES " +
                        "(0, 'ETHIOPIA'), " +
                        "(0, 'ALGERIA'), " +
                        "(1, 'BRAZIL'), " +
                        "(1, 'ARGENTINA'), " +
                        "(2, 'INDONESIA'), " +
                        "(2, 'INDIA'), " +
                        "(3, 'GERMANY'), " +
                        "(3, 'FRANCE'), " +
                        "(4, 'IRAN'), " +
                        "(4, 'EGYPT')");
    }

    @Test
    public void testTwoCorrelatedExistsSubqueries()
    {
        // This is simplified TPC-H q21
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
                "  SELECT orderkey+1 AS a FROM orders WHERE orderstatus = 'F' UNION ALL \n" +
                "  SELECT orderkey FROM orders WHERE orderkey % 2 = 0 UNION ALL \n" +
                "  (SELECT orderkey+custkey FROM orders ORDER BY orderkey LIMIT 10)\n" +
                ") \n" +
                "WHERE a < 20 OR a > 100 \n" +
                "ORDER BY a");
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
    public void testUnionAllPredicateMoveAroundWithOverlappingProjections()
    {
        assertQuery("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT orderkey AS x, orderkey AS y\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 3 = 0\n" +
                "  UNION ALL\n" +
                "  SELECT orderkey AS x, orderkey AS y\n" +
                "  FROM orders\n" +
                "  WHERE orderkey % 2 = 0\n" +
                ") a\n" +
                "JOIN (\n" +
                "  SELECT orderkey AS x, orderkey AS y\n" +
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
            List<MaterializedRow> values = computeActual("SELECT orderkey FROM orders TABLESAMPLE BERNOULLI (50)").getMaterializedRows();

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
                "SELECT length(1)", ".*Unexpected parameters \\(integer\\) for function .*length. Expected:.*");
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
    public void testArrayShuffle()
    {
        List<Integer> expected = IntStream.rangeClosed(1, 500).boxed().collect(toList());
        Set<List<Integer>> distinctResults = new HashSet<>();

        distinctResults.add(expected);
        for (int i = 0; i < 3; i++) {
            MaterializedResult results = computeActual(format("SELECT shuffle(ARRAY %s) FROM orders LIMIT 10", expected));
            List<MaterializedRow> rows = results.getMaterializedRows();
            assertEquals(rows.size(), 10);

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
        assertQuery(
                "SELECT TIME, TIMESTAMP, DATE, INTERVAL FROM (SELECT 1 TIME, 2 TIMESTAMP, 3 DATE, 4 INTERVAL)",
                "SELECT 1, 2, 3, 4");
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
    public void testApproxSetBigintWithMaxError()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(custkey, 0.01)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1000L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetVarcharWithMaxError()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(CAST(custkey AS VARCHAR), 0.01)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1000L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetDoubleWithMaxError()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(CAST(custkey AS DOUBLE), 0.01)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1000L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
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
        MaterializedResult actual = computeActual("SELECT cardinality(merge(CAST (null AS HyperLogLog))) FROM orders");

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
    public void testEmptyApproxSetWithMaxError()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(empty_approx_set(0.1))");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(0L)
                .build();
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test(expectedExceptions = {RuntimeException.class, PrestoException.class}, expectedExceptionsMessageRegExp = ".*Max standard error must be in.*")
    public void testEmptyApproxSetWithMaxErrorOutsideBounds()
    {
        computeActual("SELECT cardinality(empty_approx_set(0.3))");
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
        MaterializedResult actual = computeActual("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey) c FROM orders UNION ALL SELECT empty_approx_set())");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1002L)
                .build();
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeEmptyNonEmptyApproxSetWithSameMaxError()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey, 0.1) c FROM orders UNION ALL SELECT empty_approx_set(0.1))");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1046L)
                .build();
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeEmptyNonEmptyApproxSetWithDifferentMaxError()
    {
        assertQueryFails("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey, 0.1) c FROM orders UNION ALL SELECT empty_approx_set(0.2))",
                "Cannot merge HLLs with different number of buckets.*");
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
        MaterializedResult actual = computeActual("VALUES (0E0/0E0, 1E0/0E0, -1E0/0E0)");

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
        MaterializedResult actual = computeActual("SELECT foo FROM (values (1, 2)) a(foo, bar)");

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
    public void testGroupByWithConstants()
    {
        assertQuery("SELECT * FROM (SELECT regionkey, col, count(*) FROM (SELECT regionkey, 'bla' as col FROM nation) GROUP BY regionkey, col)");
        assertQuery("select 'blah', * from (select regionkey, count(*) FROM nation GROUP BY regionkey)");
        assertQuery("SELECT * FROM (SELECT col, count(*) FROM (SELECT 'bla' as col FROM nation) GROUP BY col)");
        assertQuery("SELECT cnt FROM (SELECT col, count(*) as cnt FROM (SELECT 'bla' as col from nation) GROUP BY col)");
        assertQuery("SELECT MIN(10), 1 as col1 GROUP BY 2");
        assertQuery("SELECT col, 'bla' as const_col, count(*) FROM (SELECT 1 as col) GROUP BY 1,2");
        assertQuery("SELECT 'bla' as const_col, count(*) FROM (SELECT 1 as col LIMIT 0) GROUP BY 1");
        assertQuery("SELECT AVG(x) FROM (SELECT 1 AS x, orderstatus FROM orders) GROUP BY x, orderstatus");
    }

    @Test
    public void testAccessControl()
    {
        assertAccessDenied("INSERT INTO orders SELECT * FROM orders", "Cannot insert into table .*.orders.*", privilege("orders", INSERT_TABLE));
        assertAccessDenied("DELETE FROM orders", "Cannot delete from table .*.orders.*", privilege("orders", DELETE_TABLE));
        assertAccessDenied("CREATE TABLE foo AS SELECT * FROM orders", "Cannot create table .*.foo.*", privilege("foo", CREATE_TABLE));
        assertAccessDenied("SELECT * FROM nation", "Cannot select from columns \\[comment, name, nationkey, regionkey\\] in table .*.nation.*", privilege("nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT * FROM (SELECT * FROM nation)", "Cannot select from columns \\[comment, name, nationkey, regionkey\\] in table .*.nation.*", privilege("nationkey", SELECT_COLUMN));
        assertAccessAllowed("SELECT name FROM (SELECT * FROM nation)", privilege("nationkey", SELECT_COLUMN));
        assertAccessAllowed("SELECT name FROM nation", privilege("nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT n1.nationkey, n2.regionkey FROM nation n1, nation n2", "Cannot select from columns \\[nationkey, regionkey\\] in table .*.nation.*", privilege("nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT count(name) as c FROM nation where comment > 'abc' GROUP BY regionkey having max(nationkey) > 10", "Cannot select from columns \\[comment, name, nationkey, regionkey\\] in table .*.nation.*", privilege("nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT 1 FROM region, nation where region.regionkey = nation.nationkey", "Cannot select from columns \\[nationkey\\] in table .*.nation.*", privilege("nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT count(*) FROM nation", "Cannot select from columns \\[\\] in table .*.nation.*", privilege("nation", SELECT_COLUMN));
        assertAccessDenied("WITH t1 AS (SELECT * FROM nation) SELECT * FROM t1", "Cannot select from columns \\[comment, name, nationkey, regionkey\\] in table .*.nation.*", privilege("nationkey", SELECT_COLUMN));
        assertAccessAllowed("SELECT name AS my_alias FROM nation", privilege("my_alias", SELECT_COLUMN));
        assertAccessAllowed("SELECT my_alias from (SELECT name AS my_alias FROM nation)", privilege("my_alias", SELECT_COLUMN));
        assertAccessDenied("SELECT name AS my_alias FROM nation", "Cannot select from columns \\[name\\] in table .*.nation.*", privilege("name", SELECT_COLUMN));
        assertAccessDenied("SELECT regionkey FROM nation as n1 join region as r1 using (regionkey)", "Cannot select from columns \\[regionkey\\] in table .*", privilege("regionkey", SELECT_COLUMN));
        assertAccessDenied("SELECT array_agg(regionkey ORDER BY regionkey) FROM nation JOIN region USING (regionkey)", "Cannot select from columns \\[regionkey\\] in table .*", privilege("regionkey", SELECT_COLUMN));
        assertAccessDenied("SHOW CREATE TABLE orders", "Cannot show create table for .*.orders.*", privilege("orders", SHOW_CREATE_TABLE));
        assertAccessAllowed("SHOW CREATE TABLE lineitem", privilege("orders", SHOW_CREATE_TABLE));
    }

    @Test
    public void testEmptyInputForUnnest()
    {
        assertQuery("SELECT val FROM (SELECT DISTINCT vals FROM (values (array[2])) t(vals) WHERE false) tmp CROSS JOIN unnest(tmp.vals) tt(val)", "SELECT 1 WHERE 1=2");
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
    }

    @Test
    public void testComplexCoercions()
    {
        assertQuery("SELECT * FROM (" +
                        "  SELECT t2.x || t2.z cc FROM (" +
                        "    SELECT *" +
                        "    FROM (VALUES (CAST('a' AS VARCHAR), CAST('c' AS VARCHAR))) t(x, z)" +
                        "  ) t2" +
                        "  JOIN (" +
                        "    SELECT *" +
                        "    FROM (VALUES (CAST('a' AS VARCHAR), CAST('c' AS VARCHAR))) u(x, z)" +
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
                "    SELECT 'x' AS c" +
                "    UNION ALL" +
                "    SELECT 'yy' AS c" +
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
    public void testExecuteUsingWithDifferentDatatypes()
    {
        String query = "SELECT c1, c2, c3, c4, c5, c6, c7['alice'], c8[1], c9 FROM (SELECT ? as c1, ? as c2, ? as c3, ? as c4, ? as c5, ? as c6, ? as c7, ? as c8, ? as c9)";
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", query)
                .build();
        assertQuery(session,
                "EXECUTE my_query USING -100, 'hello', (DATE '2020-10-15'), (TIMESTAMP '2020-03-14 10:12:12'), true, ARRAY [1, 2], MAP(ARRAY['alice', 'bob'], ARRAY [100, 300]), ROW('hi!'), null",
                "VALUES (-100, 'hello', DATE '2020-10-15', TIMESTAMP '2020-03-14 10:12:12', true, ARRAY [1, 2], 100, 'hi!', null)");
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
                "SELECT 10 in (SELECT orderkey FROM orders)");
    }

    @Test
    public void testExecuteWithParametersInLambda()
    {
        String query = "SELECT filter(array[1, 2, 3], x -> x > ?)";
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", query)
                .build();

        assertQuery(session, "EXECUTE my_query USING 2", "SELECT array[3]");
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
            fail("parameters in GROUP BY and SELECT should fail");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), MUST_BE_AGGREGATE_OR_GROUP_BY);
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "line 1:10: '(a + ?)' must be an aggregate expression or appear in GROUP BY clause");
        }
    }

    @Test
    public void testExecuteUsingFunction()
    {
        String query = "SELECT ?";
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", query)
                .build();
        assertQuery(session,
                "EXECUTE my_query USING length('Presto!')",
                "VALUES (7)");
    }

    @Test
    public void testExecuteUsingSubqueryFails()
    {
        try {
            String query = "SELECT ?";
            Session session = Session.builder(getSession())
                    .addPreparedStatement("my_query", query)
                    .build();
            computeActual(session, "EXECUTE my_query USING (SELECT 1 from nation)");
            fail("nonLiteral parameters should fail");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), EXPRESSION_NOT_CONSTANT);
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "line 1:24: Constant expression cannot contain table references");
        }
    }

    @Test
    public void testExecuteUsingSelectStarFails()
    {
        try {
            String query = "SELECT ?";
            Session session = Session.builder(getSession())
                    .addPreparedStatement("my_query", query)
                    .build();
            computeActual(session, "EXECUTE my_query USING (SELECT * from nation)");
            fail("nonLiteral parameters should fail");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), EXPRESSION_NOT_CONSTANT);
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "line 1:24: Constant expression cannot contain column references");
        }
    }

    @Test
    public void testExecuteUsingColumnReferenceFails()
    {
        try {
            String query = "SELECT ? from nation";
            Session session = Session.builder(getSession())
                    .addPreparedStatement("my_query", query)
                    .build();
            computeActual(session, "EXECUTE my_query USING \"nationkey\"");
            fail("nonLiteral parameters should fail");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), EXPRESSION_NOT_CONSTANT);
        }
        catch (RuntimeException e) {
            assertEquals(e.getMessage(), "line 1:24: Constant expression cannot contain column references");
        }
    }

    @Test
    public void testExecuteUsingWithWithClause()
    {
        String query = "WITH src AS (SELECT * FROM (VALUES (1, 4),(2, 5), (3, 6)) AS t(id1, id2) WHERE id2 = ?)" +
                " SELECT * from src WHERE id1 between ? and ?";

        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", query)
                .build();
        assertQuery(session,
                "EXECUTE my_query USING 6, 0, 10",
                "VALUES (3, 6)");
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
                .addPreparedStatement("my_query", "SELECT ? FROM nation WHERE nationkey = ? and name < ?")
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
                .addPreparedStatement("my_query", "SELECT count(*) + ? FROM nation")
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
                .addPreparedStatement("my_query", "SELECT * FROM nation")
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
        assertQuery("SELECT CAST(1 AS decimal(3,2)) < ALL(SELECT CAST(1 AS decimal(3,1)))");
        assertQuery("SELECT CAST(1 AS decimal(3,2)) < ANY(SELECT CAST(1 AS decimal(3,1)))");
        assertQuery("SELECT CAST(1 AS decimal(3,2)) <= ALL(SELECT CAST(1 AS decimal(3,1)))");
        assertQuery("SELECT CAST(1 AS decimal(3,2)) <= ANY(SELECT CAST(1 AS decimal(3,1)))");
        assertQuery("SELECT CAST(1 AS decimal(3,2)) = ALL(SELECT CAST(1 AS decimal(3,1)))");
        assertQuery("SELECT CAST(1 AS decimal(3,2)) = ANY(SELECT CAST(1 AS decimal(3,1)))", "SELECT true");
        assertQuery("SELECT CAST(1 AS decimal(3,2)) <> ALL(SELECT CAST(1 AS decimal(3,1)))");
        assertQuery("SELECT CAST(1 AS decimal(3,2)) <> ANY(SELECT CAST(1 AS decimal(3,1)))");
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
        return queryTemplate("SELECT %value% %operator% %quantifier% (SELECT * FROM (%subquery%))")
                .replaceAll(
                        parameter("subquery").of(
                                "SELECT 1 WHERE false",
                                "SELECT CAST(NULL AS INTEGER)",
                                "VALUES (1), (NULL)"),
                        parameter("quantifier").of("ALL", "ANY"),
                        parameter("value").of("1", "NULL"),
                        parameter("operator").of("=", "!=", "<", ">", "<=", ">="))
                .collect(toDataProvider());
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
                .addPreparedStatement("my_query", "SELECT 1, name, regionkey AS my_alias FROM nation")
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
        assertDescribeOutputEmpty("CREATE FUNCTION testing.default.tan (x int) RETURNS double COMMENT 'tangent trigonometric function' LANGUAGE SQL DETERMINISTIC CALLED ON NULL INPUT RETURN sin(x) / cos(x)");
        assertDescribeOutputEmpty("ALTER FUNCTION testing.default.tan CALLED ON NULL INPUT");
        assertDescribeOutputEmpty("DROP FUNCTION IF EXISTS testing.default.tan (int)");

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
                .addPreparedStatement("my_query", "SELECT count(*) AS this_is_aliased, 1 + 2 FROM nation")
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
        assertQuery(
                "SELECT (SELECT true FROM (SELECT 1) t(a) WHERE a = nationkey) " +
                        "FROM nation " +
                        "WHERE (SELECT true FROM (SELECT 1) t(a) WHERE a = nationkey) OR TRUE " +
                        "ORDER BY nationkey " +
                        "LIMIT 2",
                "VALUES true, null");
    }

    @Test
    public void testAssignUniqueId()
    {
        String unionLineitem25Times = IntStream.range(0, 25)
                .mapToObj(i -> "SELECT * FROM lineitem")
                .collect(joining(" UNION ALL "));

        assertQuery(
                "SELECT count(*) FROM (" +
                        "SELECT * FROM (" +
                        "   SELECT (SELECT count(*) WHERE c = 1) " +
                        "   FROM (SELECT CASE orderkey WHEN 1 THEN orderkey ELSE 1 END " +
                        "       FROM (" + unionLineitem25Times + ")) o(c)) result(a) " +
                        "WHERE a = 1)",
                "VALUES 1504375");
    }

    @Test
    public void testPruningCountAggregationOverScalar()
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT SUM(orderkey) FROM orders)");
        assertQuery(
                "SELECT COUNT(*) FROM (SELECT SUM(orderkey) FROM orders GROUP BY custkey)",
                "VALUES 1000");
        assertQuery("SELECT count(*) FROM (VALUES 2) t(a) GROUP BY a", "VALUES 1");
        assertQuery("SELECT a, count(*) FROM (VALUES 2) t(a) GROUP BY a", "VALUES (2, 1)");
        assertQuery("SELECT count(*) FROM (VALUES 2) t(a) GROUP BY a+1", "VALUES 1");
    }

    @Test
    public void testDefaultDecimalLiteralSwitch()
    {
        Session decimalLiteral = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.PARSE_DECIMAL_LITERALS_AS_DOUBLE, "false")
                .build();
        MaterializedResult decimalColumnResult = computeActual(decimalLiteral, "SELECT 1.0");

        assertEquals(decimalColumnResult.getRowCount(), 1);
        assertEquals(decimalColumnResult.getTypes().get(0), createDecimalType(2, 1));
        assertEquals(decimalColumnResult.getMaterializedRows().get(0).getField(0), new BigDecimal("1.0"));

        Session doubleLiteral = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.PARSE_DECIMAL_LITERALS_AS_DOUBLE, "true")
                .build();
        MaterializedResult doubleColumnResult = computeActual(doubleLiteral, "SELECT 1.0");

        assertEquals(doubleColumnResult.getRowCount(), 1);
        assertEquals(doubleColumnResult.getTypes().get(0), DOUBLE);
        assertEquals(doubleColumnResult.getMaterializedRows().get(0).getField(0), 1.0);
    }

    @Test
    public void testLastValueIgnoreNulls()
    {
        assertQuery(
                "WITH T AS (" +
                        "    SELECT" +
                        "        p," +
                        "        v" +
                        "    FROM (" +
                        "        VALUES" +
                        "            (2, 2)," +
                        "            (1, 1)," +
                        "            (3, NULL)" +
                        "    ) T(p, v)" +
                        ")" +
                        "SELECT" +
                        "    LAST_VALUE(v) IGNORE NULLS OVER (" +
                        "        PARTITION BY 1" +
                        "        ORDER BY" +
                        "            p ASC" +
                        "    )" +
                        "FROM T",
                "Values 1, 2, 2");
    }

    @Test
    public void testLargeBytecode()
    {
        StringBuilder stringBuilder = new StringBuilder("SELECT x FROM (SELECT orderkey x, custkey y from orders limit 10) WHERE CASE true ");
        // Generate 100 cases.
        for (int i = 0; i < 100; i++) {
            stringBuilder.append(" when x in (");
            for (int j = 0; j < 20; j++) {
                stringBuilder.append("random(" + (i * 100 + j) + "), ");
            }

            stringBuilder.append("random(" + i + ")) then x = random()");
        }

        stringBuilder.append("else x = random() end");
        assertQueryFails(stringBuilder.toString(), "Query results in large bytecode exceeding the limits imposed by JVM|Compiler failed");
    }

    @Test(timeOut = 60_000)
    public void testLargeQuery()
    {
        StringBuilder query = new StringBuilder("SELECT * FROM (VALUES ROW(0, '0')");
        for (int i = 1; i <= 30000; ++i) {
            query.append(format(", ROW(%d, '%d')", i, i));
        }
        assertQuery(query + ")");
    }

    @Test
    public void testInComplexTypes()
    {
        //test cases to trigger the SET_CONTAINS path of InCodeGenerator.java with complex types
        StringBuilder query = new StringBuilder("select * from (values('a'), (null)) as t (name) where ROW('1', name) IN ( ");
        for (int i = 2; i < 32; i++) {
            query.append(String.format("ROW('1','%s'), ", i));
        }
        query.append("ROW('1', name), ROW('2',name), ROW('3',name))");
        assertQuerySucceeds(query.toString());

        query = new StringBuilder("select ROW(null_value) IN ( ");
        for (int i = 0; i < 32; i++) {
            query.append(String.format("ROW(%s), ", i));
        }
        query.append("ROW(32)) ");
        query.append("FROM (values(null)) as t (null_value)");
        assertQuery(query.toString(), "SELECT NULL");
    }

    @Test
    public void testRowExpressionInterpreterStackOverflow()
    {
        StringBuilder stringBuilder = new StringBuilder("SELECT  CASE");
        for (int i = 1; i <= 500; i++) {
            stringBuilder.append(" when x = random(" + i + ") then " + i);
        }

        stringBuilder.append(" else x end from (select -1 x)");
        assertQuery(stringBuilder.toString(), "values -1");
    }

    @Test
    public void testSwitchOptimization()
    {
        assertQuery("select 1", "select 1");
        assertQuery(
                "SELECT CASE WHEN x = 1 THEN 1 WHEN x = 5 THEN 5 WHEN x = IF(RANDOM() >= 0, 3, 5) THEN 10 ELSE -1 END FROM (SELECT ORDERKEY x FROM orders where orderkey <= 10)",
                "SELECT CASE x WHEN 1 THEN 1 WHEN 5 THEN 5 WHEN 3 THEN 10 ELSE -1 END FROM (SELECT ORDERKEY x FROM orders where orderkey <= 10)");

        assertQuery(
                "SELECT CASE x WHEN 1 THEN 1 WHEN 5 THEN 5 WHEN 3 THEN 10 ELSE -1 END FROM (SELECT ORDERKEY x FROM orders where orderkey <= 10)",
                "SELECT CASE x WHEN 1 THEN 1 WHEN 5 THEN 5 WHEN 3 THEN 10 ELSE -1 END FROM (SELECT ORDERKEY x FROM orders where orderkey <= 10)");
    }

    @Test
    public void testCasePredicateRewrite()
    {
        Session caseExpressionRewriteEnabled = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_CASE_EXPRESSION_PREDICATE, "true")
                .build();

        assertQuery(
                caseExpressionRewriteEnabled,
                "SELECT LINENUMBER FROM LINEITEM WHERE (CASE WHEN QUANTITY <= 15 THEN 'SMALL' WHEN (QUANTITY > 15 AND QUANTITY <= 30) THEN 'MEDIUM' ELSE 'LARGE' END) = 'SMALL'");

        assertQuery(
                caseExpressionRewriteEnabled,
                "SELECT LINENUMBER FROM LINEITEM WHERE 'MEDIUM' = (CASE WHEN QUANTITY <= 15 THEN 'SMALL' WHEN (QUANTITY > 15 AND QUANTITY <= 30) THEN 'MEDIUM' ELSE 'LARGE' END)");

        assertQuery(
                caseExpressionRewriteEnabled,
                "SELECT LINENUMBER FROM LINEITEM WHERE (CASE WHEN QUANTITY <= 15 THEN 'SMALL' WHEN (QUANTITY > 15 AND QUANTITY <= 30) THEN 'MEDIUM' ELSE 'LARGE' END) = 'LARGE'");

        assertQuery(
                caseExpressionRewriteEnabled,
                "SELECT SUM(TOTALPRICE) FROM ORDERS WHERE (CASE ORDERSTATUS WHEN 'F' THEN 1 WHEN 'O' THEN 2 WHEN 'P' THEN 3 ELSE -1 END) = 2");

        assertQuery(
                caseExpressionRewriteEnabled,
                "SELECT SUM(TOTALPRICE) FROM ORDERS WHERE 1 < (CASE ORDERSTATUS WHEN 'F' THEN 1 WHEN 'O' THEN 2 WHEN 'P' THEN 3 ELSE -1 END)");

        assertQuery(
                caseExpressionRewriteEnabled,
                "SELECT SUM(TOTALPRICE) FROM ORDERS WHERE (CASE ORDERSTATUS WHEN 'F' THEN 1 WHEN 'O' THEN 2 WHEN 'P' THEN 3 ELSE 2 END) = 2");

        assertQuery(
                caseExpressionRewriteEnabled,
                "SELECT ORDERSTATUS, ORDERPRIORITY, TOTALPRICE FROM ORDERS WHERE (CASE WHEN ORDERSTATUS='F' THEN 1 WHEN (CASE WHEN ORDERPRIORITY = '5-LOW' THEN true ELSE false END) THEN 2 WHEN ORDERSTATUS='O' THEN 3 ELSE -1 END) > 1");
    }

    @Test
    public void testSwitchReturnsNull()
    {
        assertQuery(
                "SELECT CASE true WHEN random() < 0 THEN true END",
                "SELECT CAST(NULL AS BOOLEAN)");

        assertQuery(
                "SELECT TRUE AND CAST(NULL AS BOOLEAN) AND RANDOM() >= 0",
                "SELECT CAST(NULL AS BOOLEAN)");

        assertQuery(
                "SELECT TRUE AND CAST(NULL AS BOOLEAN) AND RANDOM() < 0",
                "SELECT FALSE");

        assertQuery(
                "SELECT TRUE AND CAST(NULL AS BOOLEAN) IS NULL AND RANDOM() >= 0",
                "SELECT TRUE");

        assertQuery(
                "SELECT 1 = ALL (SELECT CAST(NULL AS INTEGER))",
                "SELECT CAST(NULL AS BOOLEAN)");
    }

    @Test
    public void testAndInFilter()
    {
        assertQuery(
                "SELECT count() from (select * from orders where orderkey < 10) where ((orderkey > 100 and custkey > 100) or (orderkey > 200 and custkey < 200))",
                "values 0");

        assertQuery(
                "SELECT ((orderkey > 100 and custkey > 100) or (orderkey > 200 and custkey < 200)) from (select * from orders where orderkey < 10 limit 1)",
                "values false");
    }

    @Test
    public void testSetAgg()
    {
        final String input = "(select 1 x, 2 y union all select 1 x, 2 y union all select 2 x, 1 y)";
        assertQuery(
                "select count() from (select set_agg(x) = array_distinct(array_agg(x)) equals" +
                        " from " + input + " group by y) where equals",
                "select count(distinct y) from " + input);

        assertQuery(
                "select count() from " +
                        "(select set_agg(orderkey) = array_agg(distinct orderkey) eq from orders group by custkey) where eq",
                "select count(distinct custkey) from orders");
        assertQuery(
                "select cardinality(set_agg(orderkey)) from orders",
                "select count(distinct orderkey) from orders");

        assertQuery(
                "select count() from " +
                        "(select set_agg(comment) = array_agg(distinct comment) eq from orders group by orderkey) where eq",
                "select count(distinct orderkey) from orders");
        assertQuery(
                "select cardinality(set_agg(comment)) from orders",
                "select count(distinct comment) from orders");

        assertQuery(
                "select count() from " +
                        "(select set_agg(cast(orderdate as date)) = array_agg(distinct cast(orderdate as date)) eq from orders group by orderkey) where eq",
                "select count(distinct orderkey) from orders");
        assertQuery(
                "select cardinality(set_agg(cast(orderdate as date))) from orders",
                "select count(distinct orderdate) from orders");
    }

    @Test
    public void testSetAggIndeterminateRows()
    {
        // union all is to force usage of the serialized state
        assertQuery("SELECT unnested from (SELECT set_agg(x) as agg_result from (" +
                        "SELECT ARRAY[CAST(row(null, 2) AS ROW(INTEGER, INTEGER))] x " +
                        "UNION ALL " +
                        "SELECT ARRAY[null, CAST(row(1, null) AS ROW(INTEGER, INTEGER))] " +
                        "UNION ALL " +
                        "SELECT ARRAY[CAST(row(null, 2) AS ROW(INTEGER, INTEGER))])) " +
                        "CROSS JOIN unnest(agg_result) as r(unnested)",
                "SELECT * FROM (VALUES (ARRAY[null, row(1,null)]), (ARRAY[row(null, 2)]))");
    }

    @Test
    public void testSetAggIndeterminateArrays()
    {
        // union all is to force usage of the serialized state
        assertQuery("SELECT unnested from (SELECT set_agg(x) as agg_result from (" +
                        "SELECT ARRAY[ARRAY[null, 2]] x " +
                        "UNION ALL " +
                        "SELECT ARRAY[null, ARRAY[1, null]] " +
                        "UNION ALL " +
                        "SELECT ARRAY[ARRAY[null, 2]])) " +
                        "CROSS JOIN unnest(agg_result) as r(unnested)",
                "SELECT * FROM (VALUES (ARRAY[null, ARRAY[1,null]]), (ARRAY[ARRAY[null, 2]]))");
    }

    @Test
    public void testRedundantProjection()
    {
        assertQuery(
                "SELECT x, reduce(x, 0, (s, x) -> s + x, s -> s), reduce(x, 0, (s, x) -> s + x, s -> s) FROM (VALUES (array[1, 2, 3])) t(x)",
                "SELECT array[1, 2, 3], 6, 6");
        assertQuery(
                "SELECT x, filter(x, v -> date(v) BETWEEN date'2020-01-01' AND date'2020-06-30'), filter(x, v -> date(v) BETWEEN date'2020-01-01' AND date_add('day', 2, date'2020-06-28')) FROM (VALUES (array['2020-03-01', '2020-07-01'])) t(x)",
                "SELECT array['2020-03-01', '2020-07-01'], array['2020-03-01'], array['2020-03-01']");
        assertQuerySucceeds(
                "SELECT DISTINCT null AS a, NULL AS b, orderstatus FROM (SELECT orderstatus FROM orders GROUP BY orderstatus)");
    }

    @Test
    public void testComparisonWithLike()
    {
        assertQuery("SELECT t1.custkey, t2.comment " +
                "FROM " +
                "(SELECT * FROM orders WHERE (comment LIKE '%shipping_onsite%') = FALSE) t1 " +
                "JOIN " +
                "(SELECT * FROM orders WHERE (comment LIKE '%shipping_onsite%') = FALSE) t2 " +
                "ON t1.orderkey=t2.orderkey");
    }

    @Test
    public void testSetUnion()
    {
        // sanity
        assertQuery(
                "select set_union(x) from (values array[1, 2], array[3, 4], array[5, 6]) as t(x)",
                "select array[1, 2, 3, 4, 5, 6]");
        assertQuery(
                "select set_union(x) from (values array[1, 2, 3], array[2, 3, 4], array[7, 8]) as t(x)",
                "select array[1, 2, 3, 4, 7, 8]");
        assertQuery(
                "select group_id, set_union(numbers) from (values (1, array[1, 2]), (1, array[2, 3]), (2, array[4, 5]), (2, array[5, 6])) as t(group_id, numbers) group by group_id",
                "select group_id, numbers from (values (1, array[1, 2, 3]), (2, array[4, 5, 6])) as t(group_id, numbers)");
        assertQuery(
                "select group_id, set_union(numbers) from (values (1, array[1, 2]), (2, array[2, 3]), (3, array[4, 5]), (4, array[5, 6])) as t(group_id, numbers) group by group_id",
                "select group_id, numbers from (values (1, array[1, 2]), (2, array[2, 3]), (3, array[4, 5]), (4, array[5, 6])) as t(group_id, numbers)");
    }

    @Test
    public void testSetUnionWithNulls()
    {
        // all nulls should return empty array to match behavior of array_distinct(flatten(array_agg(x)))
        assertQuery(
                "select set_union(x) from (values null, null, null) as t(x)",
                "select array[]");
        // nulls inside arrays should be captured while pure nulls should be ignored
        assertQuery(
                "select set_union(x) from (values null, array[null], null) as t(x)",
                "select array[null]");
        // null inside arrays should be captured
        assertQuery(
                "select set_union(x) from (values array[1, 2, 3], array[null], null) as t(x)",
                "select array[1, 2, 3, null]");
        // return null for empty rows
        assertQuery(
                "select set_union(x) from (values null, array[null], null) as t(x) where x != null",
                "select null");
    }

    @Test
    public void testSetUnionIndeterminateRows()
    {
        // union all is to force usage of the serialized state
        assertQuery("SELECT c1, c2 from (SELECT set_union(x) as agg_result from (" +
                        "SELECT ARRAY[CAST(row(null, 2) AS ROW(INTEGER, INTEGER))] x " +
                        "UNION ALL " +
                        "SELECT ARRAY[null, CAST(row(1, null) AS ROW(INTEGER, INTEGER))] " +
                        "UNION ALL " +
                        "SELECT ARRAY[CAST(row(null, 2) AS ROW(INTEGER, INTEGER))])) " +
                        "CROSS JOIN unnest(agg_result) as r(c1, c2)",
                "SELECT * FROM (VALUES (1,null) , (null, 2), (null, null))");
    }

    @Test
    public void testSetUnionIndeterminateArrays()
    {
        // union all is to force usage of the serialized state
        assertQuery("SELECT unnested from (SELECT set_union(x) as agg_result from (" +
                        "SELECT ARRAY[ARRAY[null, 2]] x " +
                        "UNION ALL " +
                        "SELECT ARRAY[null, ARRAY[1, null]] " +
                        "UNION ALL " +
                        "SELECT ARRAY[ARRAY[null, 2]])) " +
                        "CROSS JOIN unnest(agg_result) as r(unnested)",
                "SELECT * FROM (VALUES (null), (ARRAY[1,null]), (ARRAY[null, 2]))");
    }

    @Test
    public void testMultipleSqlFunctionsWithLambda()
    {
        assertQuery(
                "SELECT array_sum(zip_with(a, b, (x, y) -> x * y)), array_sum(zip_with(a, b, (x, y) -> x * y)) + array_sum(zip_with(a, a, (x, y) -> x * y)) FROM (VALUES (ARRAY[1, 2, 3], ARRAY[1, 0, 0])) t(a, b)",
                "SELECT 1, 15");
    }

    @Test
    public void testDistinctFrom()
    {
        assertQuery(
                "SELECT x IS DISTINCT FROM NULL FROM (SELECT CAST(NULL AS VARCHAR)) T(x)",
                "SELECT FALSE");
        assertQuery(
                "SELECT NULL IS DISTINCT FROM x FROM (SELECT CAST(NULL AS VARCHAR)) T(x)",
                "SELECT FALSE");
        assertQuery(
                "SELECT x IS DISTINCT FROM NULL FROM (SELECT CAST('something' AS VARCHAR)) T(x)",
                "SELECT TRUE");
        assertQuery(
                "SELECT NULL IS DISTINCT FROM x FROM (SELECT CAST('something' AS VARCHAR)) T(x)",
                "SELECT TRUE");
        assertQuery(
                "SELECT R.name IS DISTINCT FROM NULL FROM nation N LEFT OUTER JOIN region R ON N.regionkey = R.regionkey AND R.regionkey = 2 WHERE N.name='GERMANY'",
                "SELECT FALSE");
        assertQuery(
                "SELECT NULL IS DISTINCT FROM R.name FROM nation N LEFT OUTER JOIN region R ON N.regionkey = R.regionkey AND R.regionkey = 2 WHERE N.name='KENYA'",
                "SELECT FALSE");
        assertQuery(
                "SELECT NULL IS DISTINCT FROM R.name FROM nation N LEFT OUTER JOIN region R ON N.regionkey = R.regionkey AND R.regionkey = 2 WHERE N.name='KENYA'",
                "SELECT FALSE");
        assertQuery(
                "SELECT NULL IS DISTINCT FROM R.name FROM nation N LEFT OUTER JOIN region R ON N.regionkey = R.regionkey AND R.regionkey = 2 WHERE N.name='KENYA'",
                "SELECT FALSE");
        assertQuery(
                "SELECT NULL IS DISTINCT FROM R.name FROM nation N LEFT OUTER JOIN region R ON N.regionkey = R.regionkey AND R.regionkey = 2 WHERE N.name='INDIA'",
                "SELECT TRUE");
        assertQuery(
                "SELECT NULL IS DISTINCT FROM R.name FROM nation N LEFT OUTER JOIN region R ON N.regionkey = R.regionkey AND R.regionkey = 2 WHERE N.name='JAPAN'",
                "SELECT TRUE");
    }

    @Test
    public void testDereference()
    {
        assertQuery(
                "select cast(row(row(row(random(10), if(random(10) >= 0, 2)), random(10)), random(100)) AS row(x row(y row(a int, b int), c int), d int)).x.y.b",
                "select 2");
        assertQuery(
                "select cast(row(row(row(random(10), if(random(10) < 0, 2)), random(10)), random(100)) AS row(x row(y row(a int, b int), c int), d int)).x.y.b",
                "select null");
        assertQuery(
                "select cast(row(row(null, random(10)), random(100)) AS row(x row(y row(a int, b int), c int), d int)).x.y.b",
                "select null");
        assertQuery(
                "select cast(row(row(null, if(random(100) >= 0, 4)), random(10)) AS row(x row(y row(a int, b int), c int), d int)).x.c",
                "select 4");
    }

    @Test
    public void testApproxMostFrequentWithLong()
    {
        MaterializedResult actual1 = computeActual("SELECT approx_most_frequent(3, cast(x as bigint), 15) FROM (values 1, 2, 1, 3, 1, 2, 3, 4, 5) t(x)");
        assertEquals(actual1.getRowCount(), 1);
        assertEquals(actual1.getMaterializedRows().get(0).getFields().get(0), ImmutableMap.of(1L, 3L, 2L, 2L, 3L, 2L));

        MaterializedResult actual2 = computeActual("SELECT approx_most_frequent(2, cast(x as bigint), 15) FROM (values 1, 2, 1, 3, 1, 2, 2, 1, 3, 4, 5) t(x)");
        assertEquals(actual2.getRowCount(), 1);
        assertEquals(actual2.getMaterializedRows().get(0).getFields().get(0), ImmutableMap.of(1L, 4L, 2L, 3L));
    }

    @Test
    public void testApproxMostFrequentWithVarchar()
    {
        MaterializedResult actual1 = computeActual("SELECT approx_most_frequent(3, x, 15) FROM (values 'A', 'B', 'A', 'C', 'A', 'B', 'C', 'D', 'E') t(x)");
        assertEquals(actual1.getRowCount(), 1);
        assertEquals(actual1.getMaterializedRows().get(0).getFields().get(0), ImmutableMap.of("A", 3L, "B", 2L, "C", 2L));

        MaterializedResult actual2 = computeActual("SELECT approx_most_frequent(2, x, 15) FROM (values 'A', 'B', 'A', 'C', 'A', 'B', 'C', 'D', 'E') t(x)");
        assertEquals(actual2.getRowCount(), 1);
        Object actual2Value = actual2.getMaterializedRows().get(0).getFields().get(0);
        assertTrue(actual2Value.equals(ImmutableMap.of("A", 3L, "B", 2L)) || actual2Value.equals(ImmutableMap.of("A", 3L, "C", 2L)));
    }

    @Test
    public void testApproxMostFrequentWithLongGroupBy()
    {
        MaterializedResult actual1 = computeActual("SELECT k, approx_most_frequent(3, cast(v as bigint), 15) FROM (values ('a', 1), ('b', 2), ('a', 1), ('c', 3), ('a', 1), ('b', 2), ('c', 3), ('a', 4), ('b', 5)) t(k, v) GROUP BY 1 ORDER BY 1");

        assertEquals(actual1.getRowCount(), 3);
        assertEquals(actual1.getMaterializedRows().get(0).getFields().get(0), "a");
        assertEquals(actual1.getMaterializedRows().get(0).getFields().get(1), ImmutableMap.of(1L, 3L, 4L, 1L));
        assertEquals(actual1.getMaterializedRows().get(1).getFields().get(0), "b");
        assertEquals(actual1.getMaterializedRows().get(1).getFields().get(1), ImmutableMap.of(2L, 2L, 5L, 1L));
        assertEquals(actual1.getMaterializedRows().get(2).getFields().get(0), "c");
        assertEquals(actual1.getMaterializedRows().get(2).getFields().get(1), ImmutableMap.of(3L, 2L));
    }

    @Test
    public void testApproxMostFrequentWithStringGroupBy()
    {
        MaterializedResult actual1 = computeActual("SELECT k, approx_most_frequent(3, v, 15) FROM (values ('a', 'A'), ('b', 'B'), ('a', 'A'), ('c', 'C'), ('a', 'A'), ('b', 'B'), ('c', 'C'), ('a', 'D'), ('b', 'E')) t(k, v) GROUP BY 1 ORDER BY 1");

        assertEquals(actual1.getRowCount(), 3);
        assertEquals(actual1.getMaterializedRows().get(0).getFields().get(0), "a");
        assertEquals(actual1.getMaterializedRows().get(0).getFields().get(1), ImmutableMap.of("A", 3L, "D", 1L));
        assertEquals(actual1.getMaterializedRows().get(1).getFields().get(0), "b");
        assertEquals(actual1.getMaterializedRows().get(1).getFields().get(1), ImmutableMap.of("B", 2L, "E", 1L));
        assertEquals(actual1.getMaterializedRows().get(2).getFields().get(0), "c");
        assertEquals(actual1.getMaterializedRows().get(2).getFields().get(1), ImmutableMap.of("C", 2L));
    }

    @Test
    public void testApproxMostFrequentWithNullGroupBy()
    {
        MaterializedResult actual1 = computeActual("select k, approx_most_frequent(2, v, 10) from (values (1, null), (2, 3)) t(k, v) group by k order by k");

        assertEquals(actual1.getRowCount(), 2);
        assertEquals(actual1.getMaterializedRows().get(0).getFields().get(0), 1);
        assertNull(actual1.getMaterializedRows().get(0).getFields().get(1));
        assertEquals(actual1.getMaterializedRows().get(1).getFields().get(0), 2);
        assertEquals(actual1.getMaterializedRows().get(1).getFields().get(1), ImmutableMap.of(3, 1L));
    }

    @Test
    public void testUnknownMaxBy()
    {
        assertQuery("select max_by(x, y) from (select 1 x, null y)", "select null");
        assertQuery("select max_by(x, y) from (select null x, 1 y)", "select null");
        assertQuery("select max_by(x, y) from (select null x, null y)", "select null");
    }

    @Test
    public void testMapUnionSum()
    {
        MaterializedResult actual = computeActual("select map_union_sum(x) from (select map(array['x', 'y'], cast(array[1.1,2] as array<real>) ) x union all select map(array['x', 'y'], cast(array[10,20] as array<real>)))");
        assertEquals(actual.getRowCount(), 1);
        assertEquals(actual.getMaterializedRows().get(0).getField(0), ImmutableMap.of("x", 11.1f, "y", 22.0f));

        actual = computeActual("select map_union_sum(x) from (select map(array['x', 'y'], cast(array[1.1,2.58] as array<double>)) x union all select map(array['x', 'y'], cast(array[10.1,20.1] as array<double>)))");
        assertEquals(actual.getRowCount(), 1);
        assertEquals(actual.getMaterializedRows().get(0).getField(0), ImmutableMap.of("x", 11.2, "y", 22.68));

        actual = computeActual("select map_union_sum(x) from (select map(array['x', 'y'], cast(array[1,2] as array<bigint>)) x union all select map(array['x', 'y'], cast(array[10,20] as array<bigint>)))");
        assertEquals(actual.getRowCount(), 1);
        assertEquals(actual.getMaterializedRows().get(0).getField(0), ImmutableMap.of("x", 11L, "y", 22L));

        actual = computeActual("select y, map_union_sum(x) from (select 1 y, map(array['x', 'y'], cast(array[1,2] as array<bigint>)) x union all select 1 y, map(array['x', 'z', 'y'], cast(array[10,30,20] as array<bigint>))) group by y");
        assertEquals(actual.getRowCount(), 1);
        assertEquals(actual.getMaterializedRows().get(0).getField(1), ImmutableMap.of("x", 11L, "y", 22L, "z", 30L));

        actual = computeActual("select y, map_union_sum(x) from (select 1 y, map(array['x', 'y'], cast(array[1, null] as array<bigint>))x ) group by y");
        assertEquals(actual.getRowCount(), 1);
        assertEquals(actual.getMaterializedRows().get(0).getField(1), ImmutableMap.of("x", 1L, "y", 0L));

        actual = computeActual("select y, map_union_sum(x) from (select 1 y, map(array['x', 'z', 'y'], cast(array[null,30,20] as array<integer>)) x " +
                "union all select 1 y, map(array['x', 'y'], cast(array[1,null] as array<integer>))x) group by y");
        assertEquals(actual.getRowCount(), 1);
        assertEquals(actual.getMaterializedRows().get(0).getField(1), ImmutableMap.of("x", 1, "y", 20, "z", 30));

        actual = computeActual("select y, map_union_sum(x) from (select 1 y, map(array['x', 'z', 'y'], cast(array[null,30,20] as array<smallint>)) x " +
                "union all select 1 y, map(array['x', 'y'], cast(array[1,null] as array<smallint>))x) group by y");
        assertEquals(actual.getRowCount(), 1);
        assertEquals(actual.getMaterializedRows().get(0).getField(1), ImmutableMap.of("x", (short) 1, "y", (short) 20, "z", (short) 30));

        actual = computeActual("select y, map_union_sum(x) from (select 1 y, map(array['x', 'z', 'y'], cast(array[null,30,20] as array<tinyint>)) x " +
                "union all select 1 y, map(array['x', 'y'], cast(array[1,null] as array<tinyint>))x) group by y");
        assertEquals(actual.getRowCount(), 1);
        assertEquals(actual.getMaterializedRows().get(0).getField(1), ImmutableMap.of("x", (byte) 1, "y", (byte) 20, "z", (byte) 30));

        actual = computeActual("select y, map_union_sum(x) from (select 1 y, map(array['x', 'z', 'y'], cast(array[null,30,20] as array<bigint>)) x union all select 1 y, map(array['x', 'y'], cast(array[1,null] as array<bigint>))x) group by y");
        assertEquals(actual.getRowCount(), 1);
        assertEquals(actual.getMaterializedRows().get(0).getField(1), ImmutableMap.of("x", 1L, "y", 20L, "z", 30L));

        actual = computeActual("select y, map_union_sum(x) from (select 1 y, map(array['x', 'z', 'y'], cast(array[null,30,20] as array<real>)) x union all select 1 y, map(array['x', 'y'], cast(array[1,null] as array<real>))x) group by y");
        assertEquals(actual.getRowCount(), 1);
        assertEquals(actual.getMaterializedRows().get(0).getField(1), ImmutableMap.of("x", 1.0f, "y", 20.0f, "z", 30.0f));

        actual = computeActual("select y, map_union_sum(x) from (select 1 y, map(array['x', 'y'], cast(array[1,null] as array<bigint>)) x " +
                "union all select 1 y, map(array['x', 'z', 'y'], cast(array[null,30,20] as array<bigint>)) " +
                "union all select 1 y, map(array['a', 'y', 'x'], cast(array[100, 400, 200] as array<bigint>)) " +
                "union all select 3 y, map(array['a', 'm', 'x'], cast(array[-1, 2, -3] as array<bigint>)) " +
                ") group by y order by y");
        assertEquals(actual.getRowCount(), 2);
        assertEquals(actual.getMaterializedRows().get(0).getField(0), 1);
        assertEquals(actual.getMaterializedRows().get(0).getField(1), ImmutableMap.of("a", 100L, "x", 201L, "y", 420L, "z", 30L));
        assertEquals(actual.getMaterializedRows().get(1).getField(0), 3);
        assertEquals(actual.getMaterializedRows().get(1).getField(1), ImmutableMap.of("a", -1L, "m", 2L, "x", -3L));
    }

    @Test
    public void testInvalidMapUnionSum()
    {
        assertQueryFails(
                "SELECT map_union_sum(x) from (select cast(MAP() as map<varchar, varchar>) x)",
                ".*line 1:8: Unexpected parameters \\(map\\(varchar,varchar\\)\\) for function (?:native.default.)?map_union_sum. Expected: (?:native.default.)?map_union_sum\\(map\\((K|k),(V|v)\\)\\) (K|k):comparable, (V|v):nonDecimalNumeric.*");
        assertQueryFails(
                "SELECT map_union_sum(x) from (select cast(MAP() as map<varchar, decimal(10,2)>) x)",
                ".*line 1:8: Unexpected parameters \\(map\\(varchar,decimal\\(10,2\\)\\)\\) for function (?:native.default.)?map_union_sum. Expected: (?:native.default.)?map_union_sum\\(map\\((K|k),(V|v)\\)\\) (K|k):comparable, (V|v):nonDecimalNumeric.*");
    }

    @Test
    public void testMapUnionSumOverflow()
    {
        assertQueryFails(
                "select y, map_union_sum(x) from (select 1 y, map(array['x', 'z', 'y'], cast(array[null,30,100] as array<tinyint>)) x " +
                        "union all select 1 y, map(array['x', 'y'], cast(array[1,100] as array<tinyint>))x) group by y", ".*Value 200 exceeds.*");
        assertQueryFails(
                "select y, map_union_sum(x) from (select 1 y, map(array['x', 'z', 'y'], cast(array[null,30, 32760] as array<smallint>)) x " +
                        "union all select 1 y, map(array['x', 'y'], cast(array[1,100] as array<smallint>))x) group by y", ".*Value 32860 exceeds.*");
    }

    @Test
    public void testMultipleOrderingOnSameCanonicalVariables()
    {
        assertQuerySucceeds("SELECT ARRAY_AGG( x ORDER BY x ASC, x DESC ) FROM ( SELECT 0 as x, 0 AS y)");
    }

    @Test
    public void testMultipleConcat()
    {
        assertQuery("select concat('a', '','','', 'b', '', '', 'c', 'd', '', '', '', '')", "select 'abcd'");
        assertQuery("select concat('', '','','', '', '', '', '', '', '', '')", "select ''");
        assertQuery("select concat('', '','','', 'x', '', '', '', '', '', '')", "select 'x'");
    }

    @Test
    public void testReduceAggWithNulls()
    {
        assertQueryFails("select reduce_agg(x, null, (x,y)->try(x+y), (x,y)->try(x+y)) from (select 1 union all select 10) T(x)", ".*REDUCE_AGG only supports non-NULL literal as the initial value.*");
        assertQueryFails("select reduce_agg(x, cast(null as bigint), (x,y)->coalesce(x, 0)+coalesce(y, 0), (x,y)->coalesce(x, 0)+coalesce(y, 0)) from (values cast(10 as bigint),10)T(x)", ".*REDUCE_AGG only supports non-NULL literal as the initial value.*");

        // here some reduce_aggs coalesce overflow/zero-divide errors to null in the input/combine functions
        assertQuery("select reduce_agg(x, 0, (x,y)->try(1/x+1/y), (x,y)->try(1/x+1/y)) from ((select 0) union all select 10.) T(x)", "select null");
        assertQuery("select reduce_agg(x, 0, (x, y)->try(x+y), (x, y)->try(x+y)) from (values 2817, 9223372036854775807) AS T(x)", "select null");
        assertQuery("select reduce_agg(x, array[], (x, y)->array[element_at(x, 2)],  (x, y)->array[element_at(x, 2)]) from (select array[array[1]]) T(x)", "select array[null]");
    }

    @Test
    public void testReduceAggWithMapZip()
    {
        Session sessionWithIntermediateAggEnabled = Session.builder(getSession())
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "true")
                .build();
        assertQuerySucceeds(sessionWithIntermediateAggEnabled, "select reduce_agg(x, map(), (s,x)->map_zip_with( s, x, (k1,v1,v2)->if(v1>v2,v1,v2)), (s,x)->map_zip_with( s, x, (k1,v1,v2)->if(v1>v2,v1,v2))) from (select map(array['k1', 'k2'], array[1e-2, 0.06]) x union all select map(array['k1', 'k2'], array[2e-05, 1e-2]) x)");
    }

    @Test
    public void testReduceAggWithArrayConcat()
    {
        assertQuerySucceeds("select sum(cardinality(s)) from (SELECT REDUCE_AGG(x, cast(array[] as array<bigint>), (x,y)->x||sequence(1, y), (x,y)->x||y) s FROM (SELECT x FROM (SELECT 1) CROSS JOIN UNNEST(SEQUENCE(1, 7000)) T(x)) group by random(100))");
        assertQuerySucceeds("select sum(cardinality(s)) from (SELECT REDUCE_AGG(x, cast(map() as map<bigint, bigint>), (x,y) -> map_concat(x, map(sequence(1, y), repeat(1, cast(y as int)))), (x,y)->map_concat(x,y)) s FROM (SELECT x FROM (SELECT 1) CROSS JOIN UNNEST(SEQUENCE(1, 7000)) T(x)) group by random(100))");
    }

    @Test
    public void testGroupByWithLambdaExpression()
    {
        assertQueryFails(
                "SELECT reduce(a, 0, (s, x) -> x, s->s), count(*) FROM (VALUES (array[1]), (array[1, 2, 3]), (array[3])) t(a) GROUP BY reduce(a, 0, (s, x) -> x, s->s)",
                "GROUP BY does not support lambda expressions, please use GROUP BY # instead");
        assertQuery(
                "SELECT reduce(a, 0, (s, x) -> x, s->s), count(*) FROM (VALUES (array[1]), (array[1, 2, 3]), (array[3])) t(a) GROUP BY 1",
                "VALUES (3, 2), (1, 1)");
    }

    public void testSourceLocationInPlan()
    {
        MaterializedResult result = computeActual("explain(type distributed) select max(orderkey + 1) over(partition by custkey) m from orders");
        assertEquals(result.getRowCount(), 1);
        String plan = (String) result.getMaterializedRows().get(0).getField(0);
        assertTrue(plan.contains("expr := (orderkey) + (BIGINT'1') (1:88)"));
        assertTrue(plan.contains("m := max (1:34)"));
        assertTrue(plan.contains("max := max(expr) RANGE UNBOUNDED_PRECEDING CURRENT_ROW (1:34)"));
    }

    @Test
    public void testCSECompilation()
    {
        String sql = "SELECT " +
                "FILTER( MAP_VALUES(map1), x -> x >= ELEMENT_AT( MAP(ARRAY['low','medium','high'], ARRAY[40000,40000,40000]), COALESCE( ELEMENT_AT( other_map, name ), 'low' ) ) ) AS v1, " +
                "FILTER( MAP_VALUES(map1), x -> x >= ELEMENT_AT( MAP(ARRAY['low','medium','high'], ARRAY[40000,40000,40000]), COALESCE( ELEMENT_AT( other_map, name ), 'low' ) ) ) AS v2 " +
                "FROM (select MAP(ARRAY['low'], ARRAY[100000]) map1, '' name) CROSS JOIN ( SELECT MAP() AS other_map ) risk_map";
        assertQuery(sql, "VALUES (100000, 100000)");
    }

    @Test
    public void testApproxPercentileMerged()
    {
        MaterializedResult raw = computeActual("SELECT orderstatus, orderkey, totalprice FROM orders");

        Multimap<String, Long> orderKeyByStatus = ArrayListMultimap.create();
        Multimap<String, Double> totalPriceByStatus = ArrayListMultimap.create();
        for (MaterializedRow row : raw.getMaterializedRows()) {
            orderKeyByStatus.put((String) row.getField(0), ((Number) row.getField(1)).longValue());
            totalPriceByStatus.put((String) row.getField(0), (Double) row.getField(2));
        }

        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, " +
                "   approx_percentile(orderkey, 0.5), " +
                "   approx_percentile(orderkey, 0.8)," +
                "   approx_percentile(totalprice, 0.1), " +
                "   approx_percentile(totalprice, 0.4)," +
                "   approx_percentile(totalprice, 0.3, 0.001), " +
                "   approx_percentile(totalprice, 0.6, 0.001)," +
                "   approx_percentile(totalprice, 2, 0.5)," +
                "   approx_percentile(totalprice, 2, 0.8)," +
                "   approx_percentile(totalprice, 2, 0.4, 0.001)," +
                "   approx_percentile(totalprice, 2, 0.7, 0.001)," +
                "   approx_percentile(orderkey, 0.9)," +
                "   approx_percentile(orderkey, 0.8+0.1)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        for (MaterializedRow row : actual.getMaterializedRows()) {
            String status = (String) row.getField(0);
            Long orderKey05 = ((Number) row.getField(1)).longValue();
            Long orderKey08 = ((Number) row.getField(2)).longValue();
            Double totalPrice01 = (Double) row.getField(3);
            Double totalPrice04 = (Double) row.getField(4);
            Double totalPrice03Accuracy = (Double) row.getField(5);
            Double totalPrice06Accuracy = (Double) row.getField(6);
            Double totalPrice05Weighted = (Double) row.getField(7);
            Double totalPrice08Weighted = (Double) row.getField(8);
            Double totalPrice04WeightedAccuracy = (Double) row.getField(9);
            Double totalPrice07WeightedAccuracy = (Double) row.getField(10);
            Long orderKey09v1 = ((Number) row.getField(11)).longValue();
            Long orderKey09v2 = ((Number) row.getField(12)).longValue();

            List<Long> orderKeys = Ordering.natural().sortedCopy(orderKeyByStatus.get(status));
            List<Double> totalPrices = Ordering.natural().sortedCopy(totalPriceByStatus.get(status));

            // verify real rank of returned value is within 1% of requested rank
            assertTrue(orderKey05 >= orderKeys.get((int) (0.49 * orderKeys.size())));
            assertTrue(orderKey05 <= orderKeys.get((int) (0.51 * orderKeys.size())));

            assertTrue(orderKey08 >= orderKeys.get((int) (0.79 * orderKeys.size())));
            assertTrue(orderKey08 <= orderKeys.get((int) (0.81 * orderKeys.size())));

            assertTrue(totalPrice01 >= totalPrices.get((int) (0.09 * totalPrices.size())));
            assertTrue(totalPrice01 <= totalPrices.get((int) (0.11 * totalPrices.size())));

            assertTrue(totalPrice04 >= totalPrices.get((int) (0.39 * totalPrices.size())));
            assertTrue(totalPrice04 <= totalPrices.get((int) (0.41 * totalPrices.size())));

            assertTrue(totalPrice03Accuracy >= totalPrices.get((int) (0.29 * totalPrices.size())));
            assertTrue(totalPrice03Accuracy <= totalPrices.get((int) (0.31 * totalPrices.size())));

            assertTrue(totalPrice06Accuracy >= totalPrices.get((int) (0.59 * totalPrices.size())));
            assertTrue(totalPrice06Accuracy <= totalPrices.get((int) (0.61 * totalPrices.size())));

            assertTrue(totalPrice05Weighted >= totalPrices.get((int) (0.49 * totalPrices.size())));
            assertTrue(totalPrice05Weighted <= totalPrices.get((int) (0.51 * totalPrices.size())));

            assertTrue(totalPrice08Weighted >= totalPrices.get((int) (0.79 * totalPrices.size())));
            assertTrue(totalPrice08Weighted <= totalPrices.get((int) (0.81 * totalPrices.size())));

            assertTrue(totalPrice04WeightedAccuracy >= totalPrices.get((int) (0.39 * totalPrices.size())));
            assertTrue(totalPrice04WeightedAccuracy <= totalPrices.get((int) (0.41 * totalPrices.size())));

            assertTrue(totalPrice07WeightedAccuracy >= totalPrices.get((int) (0.69 * totalPrices.size())));
            assertTrue(totalPrice07WeightedAccuracy <= totalPrices.get((int) (0.71 * totalPrices.size())));

            assertTrue(orderKey09v1 >= orderKeys.get((int) (0.89 * orderKeys.size())));
            assertTrue(orderKey09v1 <= orderKeys.get((int) (0.91 * orderKeys.size())));

            assertTrue(orderKey09v2 >= orderKeys.get((int) (0.89 * orderKeys.size())));
            assertTrue(orderKey09v2 <= orderKeys.get((int) (0.91 * orderKeys.size())));
        }
    }

    @Test
    public void testOrderByCompilation()
    {
        String sql = "SELECT orderkey, array_agg(abs(1) ORDER BY orderkey) FROM orders GROUP BY orderkey ORDER BY orderkey";
        assertQuery(sql, "SELECT orderkey, array_agg(1) FROM orders GROUP BY orderkey ORDER BY orderkey");
    }

    @Test
    public void testRandomizeNullKeyOuterJoin()
    {
        Session enableRandomize = Session.builder(getSession())
                .setSystemProperty(RANDOMIZE_OUTER_JOIN_NULL_KEY, "true")
                .build();

        String leftJoin = "SELECT o.orderkey, l.discount FROM orders o LEFT JOIN lineitem l ON o.orderkey = l.orderkey";
        assertQueryWithSameQueryRunner(enableRandomize, leftJoin, getSession());

        String multipleJoin = "SELECT o.orderkey, l.partkey, p.name FROM orders o LEFT JOIN lineitem l ON o.orderkey = l.orderkey LEFT JOIN part p ON l.partkey=p.partkey";
        assertQueryWithSameQueryRunner(enableRandomize, multipleJoin, getSession());

        Session enableKeyFromOuterJoin = Session.builder(getSession())
                .setSystemProperty(RANDOMIZE_OUTER_JOIN_NULL_KEY_STRATEGY, "key_from_outer_join")
                .build();
        assertQueryWithSameQueryRunner(enableKeyFromOuterJoin, multipleJoin, getSession());

        Session enableRandomizeFourPartition = Session.builder(getSession())
                .setSystemProperty(RANDOMIZE_OUTER_JOIN_NULL_KEY, "true")
                .setSystemProperty(HASH_PARTITION_COUNT, "1")
                .build();
        String varcharJoinKey = "select t.k, t2.k, t2.v from (values 'r0', 'r1', 'r2', 'r3') t(k) left join (values (null, 1), (null, 2), (null, 3), (null, 4)) t2(k, v) on t.k = t2.k";
        assertQueryWithSameQueryRunner(enableRandomizeFourPartition, varcharJoinKey, "values ('r0', null, null), ('r1', null, null), ('r2', null, null), ('r3', null, null)");
    }

    @Test
    public void testMapSubset()
    {
        assertQuery("select m[1], m[3] from (select map_subset(map(array[1,2,3,4], array['a', 'b', 'c', 'd']), array[1,3,10]) m)", "select 'a', 'c'");
        assertQuery("select cardinality(map_subset(map(array[1,2,3,4], array['a', 'b', 'c', 'd']), array[10,20]))", "select 0");
        assertQuery("select cardinality(map_subset(map(), array[10,20]))", "select 0");
        assertQuerySucceeds("select map_subset(map(), array[10,20])"); // cannot compare to other map as the type is unknown. Just test that the query succeeds!
        assertQuerySucceeds("select map_subset(map(array[1], array[1]), array[])");
    }

    @Test
    public void testMultiMapFromEntriesWihTransform()
    {
        assertQuerySucceeds("select multimap_from_entries(x) from (select zip(array[orderkey], array[array[custkey, custkey]]) x from (select 1 orderkey, 1 custkey))");
    }

    @Test
    public void testDuplicateUnnestItem()
    {
        // unnest with cross join
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3]) AS r(r1, r2)", "VALUES (1, 2, 2), (1, 3, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[2, 3]) AS r(r1, r2, r3)", "VALUES (1, 2, 2, 2), (1, 3, 3, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[10,11,12], ARRAY[2, 3]) AS r(r1, r2, r3)", "VALUES (1, 2, 10, 2), (1, 3, 11, 3), (1, NULL, 12, NULL)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[10,11,12]) AS r(r1, r2, r3)", "VALUES (1, 2, 2, 10), (1, 3, 3, 11), (1, NULL, NULL, 12)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b'])) AS r(r1, r2, r3, r4)", "VALUES (1, 2, 'a', 2, 'a'), (1, 3, 'b', 3, 'b')");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']), MAP(ARRAY[2, 3], ARRAY['a', 'b'])) AS r(r1, r2, r3, r4, r5, r6)",
                "VALUES (1, 2, 'a', 1, 'a', 2, 'a'), (1, 3, 'b', 2, 'b', 3, 'b'), (1, NULL, NULL, 3, 'c', NULL, NULL)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c'])) AS r(r1, r2, r3, r4, r5, r6)",
                "VALUES (1, 2, 'a', 2, 'a', 1, 'a'), (1, 3, 'b', 3, 'b', 2, 'b'), (1, NULL, NULL, NULL, NULL, 3, 'c')");
        assertQuery("SELECT * from ( SELECT ARRAY[1] AS kv FROM (select 1)) CROSS JOIN UNNEST( kv, kv )", "VALUES (ARRAY[1], 1, 1)");

        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, ord)", "VALUES (1, 2, 2, 1), (1, 3, 3, 2)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (1, 2, 2, 2, 1), (1, 3, 3, 3, 2)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[10,11,12], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (1, 2, 10, 2, 1), (1, 3, 11, 3, 2), (1, NULL, 12, NULL, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[10,11,12]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (1, 2, 2, 10, 1), (1, 3, 3, 11, 2), (1, NULL, NULL, 12, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b'])) WITH ORDINALITY AS r(r1, r2, r3, r4, ord)", "VALUES (1, 2, 'a', 2, 'a', 1), (1, 3, 'b', 3, 'b', 2)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']), MAP(ARRAY[2, 3], ARRAY['a', 'b'])) WITH ORDINALITY AS r(r1, r2, r3, r4, r5, r6, ord)",
                "VALUES (1, 2, 'a', 1, 'a', 2, 'a', 1), (1, 3, 'b', 2, 'b', 3, 'b', 2), (1, NULL, NULL, 3, 'c', NULL, NULL, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c'])) WITH ORDINALITY AS r(r1, r2, r3, r4, r5, r6, ord)",
                "VALUES (1, 2, 'a', 2, 'a', 1, 'a', 1), (1, 3, 'b', 3, 'b', 2, 'b', 2), (1, NULL, NULL, NULL, NULL, 3, 'c', 3)");
        assertQuery("SELECT * from ( SELECT ARRAY[1] AS kv FROM (select 1)) CROSS JOIN UNNEST( kv, kv ) WITH ORDINALITY AS t(r1, r2, ord)", "VALUES (ARRAY[1], 1, 1, 1)");

        Session useLegacyUnnest = Session.builder(getSession())
                .setSystemProperty(LEGACY_UNNEST, "true")
                .build();
        assertQuery(useLegacyUnnest, "SELECT k, cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)) from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2)",
                "VALUES (1, row(2, 3), row(2, 3)), (1, row(3, 5), row(3, 5))");
        assertQuery(useLegacyUnnest, "SELECT k, cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), cast(r3 as row(x int, y int, z int)) from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) AS r(r1, r2, r3)",
                "VALUES (1, row(2, 3), row(2, 3), row(10, 13, 15)), (1, row(3, 5), row(3, 5), row(23, 25, 20))");
        assertQuery(useLegacyUnnest, "SELECT k, cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), ord from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, ord)",
                "VALUES (1, row(2, 3), row(2, 3), 1), (1, row(3, 5), row(3, 5), 2)");
        assertQuery(useLegacyUnnest, "SELECT k, cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), cast(r3 as row(x int, y int, z int)), ord from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) WITH ORDINALITY AS r(r1, r2, r3, ord)",
                "VALUES (1, row(2, 3), row(2, 3), row(10, 13, 15), 1), (1, row(3, 5), row(3, 5), row(23, 25, 20), 2)");

        assertQuery("select orderkey, custkey, totalprice, t1, t2 from orders cross join unnest(array[custkey], array[custkey]) as t(t1, t2)",
                "select orderkey, custkey, totalprice, custkey as t1, custkey as t2 from orders");
        assertQuery("select orderkey, custkey, totalprice, t1, t2, t3 from orders cross join unnest(array[custkey], array[custkey], array[custkey]) as t(t1, t2, t3)",
                "select orderkey, custkey, totalprice, custkey as t1, custkey as t2, custkey as t3 from orders");
        assertQuery("select orderkey, custkey, totalprice, t1, t2, t3 from orders cross join unnest(array[custkey], array[custkey], array[shippriority]) as t(t1, t2, t3)",
                "select orderkey, custkey, totalprice, custkey as t1, custkey as t2, shippriority as t3 from orders");

        // SIMPLE UNNEST without cross join
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3])", "VALUES (2, 2), (3, 3)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[2, 3])", "VALUES (2, 2, 2), (3, 3, 3)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[10,11,12], ARRAY[2, 3])", "VALUES (2, 10, 2), (3, 11, 3), (NULL, 12, NULL)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[10,11,12])", "VALUES (2, 2, 10), (3, 3, 11), (NULL, NULL, 12)");
        assertQuery("SELECT * from unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b']))", "VALUES (2, 'a', 2, 'a'), (3, 'b', 3, 'b')");
        assertQuery("SELECT * from unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']), MAP(ARRAY[2, 3], ARRAY['a', 'b']))",
                "VALUES (2, 'a', 1, 'a', 2, 'a'), (3, 'b', 2, 'b', 3, 'b'), (NULL, NULL, 3, 'c', NULL, NULL)");
        assertQuery("SELECT * from unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']))",
                "VALUES (2, 'a', 2, 'a', 1, 'a'), (3, 'b', 3, 'b', 2, 'b'), (NULL, NULL, NULL, NULL, 3, 'c')");

        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, ord)", "VALUES (2, 2, 1), (3, 3, 2)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (2, 2, 2, 1), (3, 3, 3, 2)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[10,11,12], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (2, 10, 2, 1), (3, 11, 3, 2), (NULL, 12, NULL, 3)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[10,11,12]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (2, 2, 10, 1), (3, 3, 11, 2), (NULL, NULL, 12, 3)");

        assertQuery(useLegacyUnnest, "SELECT cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)) from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2)",
                "VALUES (row(2, 3), row(2, 3)), (row(3, 5), row(3, 5))");
        assertQuery(useLegacyUnnest, "SELECT cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), cast(r3 as row(x int, y int, z int)) from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) AS r(r1, r2, r3)",
                "VALUES (row(2, 3), row(2, 3), row(10, 13, 15)), (row(3, 5), row(3, 5), row(23, 25, 20))");
        assertQuery(useLegacyUnnest, "SELECT cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), ord from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, ord)",
                "VALUES (row(2, 3), row(2, 3), 1), (row(3, 5), row(3, 5), 2)");
        assertQuery(useLegacyUnnest, "SELECT cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), cast(r3 as row(x int, y int, z int)), ord from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) WITH ORDINALITY AS r(r1, r2, r3, ord)",
                "VALUES (row(2, 3), row(2, 3), row(10, 13, 15), 1), (row(3, 5), row(3, 5), row(23, 25, 20), 2)");

        // mixed
        assertQuery("select * from (SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, ord)) cross join unnest(ARRAY[2, 3], ARRAY[2, 3])",
                "VALUES (2, 2, 1, 2, 2), (2, 2, 1, 3, 3), (3, 3, 2, 2, 2), (3, 3, 2, 3, 3)");
    }

    @Test
    public void testDuplicateUnnestRows()
    {
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2, r3, r4)",
                "VALUES (1, 2, 3, 2, 3), (1, 3, 5, 3, 5)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) AS r(r1, r2, r3, r4, r5, r6, r7)",
                "VALUES (1, 2, 3, 2, 3, 10, 13, 15), (1, 3, 5, 3, 5, 23, 25, 20)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, r3, r4, ord)",
                "VALUES (1, 2, 3, 2, 3, 1), (1, 3, 5, 3, 5, 2)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) WITH ORDINALITY AS r(r1, r2, r3, r4, r5, r6, r7, ord)",
                "VALUES (1, 2, 3, 2, 3, 10, 13, 15, 1), (1, 3, 5, 3, 5, 23, 25, 20, 2)");

        assertQuery("SELECT * from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2, r3, r4)",
                "VALUES (2, 3, 2, 3), (3, 5, 3, 5)");
        assertQuery("SELECT * from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, r3, r4, ord)",
                "VALUES (2, 3, 2, 3, 1), (3, 5, 3, 5, 2)");
    }

    @Test
    public void testDependentWindowFunction()
    {
        // rank() from window function is used as input to parent window function
        String sql = "SELECT a, b, c, rnk, SUM(c) OVER (PARTITION BY a ORDER BY b rows BETWEEN rnk PRECEDING AND rnk FOLLOWING)" +
                "FROM (" +
                "    SELECT" +
                "        a, b, c, RANK() OVER (PARTITION BY a ORDER BY b) AS rnk" +
                "    FROM (" +
                "        VALUES (1, 1, 1), (1, 2, 1), (1, 3, 1), (2, 1, 1), (2, 2, 1), (2, 3, 1)" +
                "    ) AS t(a, b, c)" +
                ")";
        assertQuery(sql, "VALUES (1, 1, 1, 1, 2), (1, 2, 1, 2, 3), (1, 3, 1, 3, 3), (2, 1, 1, 1, 2), (2, 2, 1, 2, 3), (2, 3, 1, 3, 3)");

        sql = "select orderkey, orderpriority, totalprice, rnk, " +
                "avg(totalprice) over (partition by orderpriority order by orderkey rows between rnk preceding and rnk following) " +
                "from (select orderkey, orderpriority, totalprice, rank() over(partition by orderpriority order by orderkey) as rnk from orders)";
        assertQuery(sql);
    }

    @Test
    public void testGroupByLimit()
    {
        Session prefilter = Session.builder(getSession())
                .setSystemProperty(PREFILTER_FOR_GROUPBY_LIMIT, "true")
                .build();
        MaterializedResult result1 = computeActual(prefilter, "select count(shipdate), orderkey from lineitem group by orderkey limit 100000");
        MaterializedResult result2 = computeActual("select count(shipdate), orderkey from lineitem group by orderkey limit 100000");
        assertEqualsIgnoreOrder(result1, result2, "Prefilter and without prefilter don't give matching results");

        assertQuery(prefilter, "select count(custkey), orderkey from orders where orderstatus='F' and orderkey < 50 group by orderkey limit 100", "values (1, 3), (1, 6), (1, 37), (1, 33), (1, 5)");
        assertQuery(prefilter, "select count(1) from (select count(custkey), orderkey from orders where orderstatus='F' and orderkey < 50 group by orderkey limit 4)", "select 4");
        assertQuery(prefilter, "select count(comment), orderstatus from (select upper(comment) comment, upper(orderstatus) orderstatus from orders where orderkey < 50) group by orderstatus limit 100", "values (5, 'F'), (10, 'O')");

        assertQuery(prefilter, "select count(comment), orderstatus from (select upper(comment) comment, upper(orderstatus) orderstatus from orders where orderkey < 50) group by orderstatus having count(1) > 1 limit 100", "values (5, 'F'), (10, 'O')");

        prefilter = Session.builder(getSession())
                .setSystemProperty(PREFILTER_FOR_GROUPBY_LIMIT, "true")
                .setSystemProperty(PREFILTER_FOR_GROUPBY_LIMIT_TIMEOUT_MS, "1")
                .build();

        result1 = computeActual(prefilter, "select count(shipdate), orderkey from lineitem group by orderkey limit 100000");
        result2 = computeActual("select count(shipdate), orderkey from lineitem group by orderkey limit 100000");
        assertEqualsIgnoreOrder(result1, result2, "Prefilter and without prefilter don't give matching results");

        assertQuery(prefilter, "select count(1) from (select count(custkey), orderkey from orders group by orderkey limit 100000)", "values 15000");
        assertQuery(prefilter, "select count(1) from (select count(custkey), orderkey from orders group by orderkey limit 4)", "select 4");
        assertQuery(prefilter, "select count(1) from (select count(comment), orderstatus from (select upper(comment) comment, upper(orderstatus) orderstatus from orders) group by orderstatus limit 100000)", "values 3");
    }

    @Test
    public void testPrefilterGroupByLimitNotFiredForLowCardinalityKeys()
    {
        Session prefilter = Session.builder(getSession())
                .setSystemProperty("prefilter_for_groupby_limit", "true")
                .build();

        MaterializedResult plan = computeActual(prefilter, "explain(type distributed) select count(custkey), orderstatus from orders group by orderstatus limit 1000");
        assertEquals(((String) plan.getOnlyValue()).toUpperCase().indexOf("MAP_AGG"), -1);
        plan = computeActual(prefilter, "explain(type distributed) select count(custkey), orderkey from orders group by orderkey limit 100000");
        assertEquals(((String) plan.getOnlyValue()).toUpperCase().indexOf("MAP_AGG"), -1);
    }

    @Test
    public void testNestedExpressions()
    {
        assertQuery(
                "SELECT (true and coalesce(X, true) IN (true, false)) IN (true, false) FROM (VALUES true) T(X)",
                "SELECT true");
    }

    @DataProvider(name = "use_default_literal_coalesce")
    public static Object[][] useDefaultLiteralCoalesce()
    {
        return new Object[][] {
                {true},
                {false}
        };
    }

    @Test(dataProvider = "use_default_literal_coalesce")
    public void testCoalesceWithDefaultsForPushingAggregationThroughOuterJoins(boolean useDefaultLiteralCoalesce)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(USE_DEFAULTS_FOR_CORRELATED_AGGREGATION_PUSHDOWN_THROUGH_OUTER_JOINS, Boolean.toString(useDefaultLiteralCoalesce))
                .build();

        // Queries that will use the default literal optimization
        assertQuery(session,
                "select count(*) from nation n where (select count(*) from customer c where n.nationkey=c.nationkey)>0",
                "select 25");

        assertQuery(session,
                "select count(*) from nation n where (select count(name) from customer c where n.nationkey=c.nationkey)>0",
                "select 25");

        assertQuery(session,
                "select count(*) from nation n where (select max(custkey) from customer c where n.nationkey=c.nationkey)>0",
                "select 25");

        assertQuery(session, "select count(*) from nation n where (select sum(custkey) from customer c where n.nationkey=c.nationkey)>0",
                "select 25");

        // Queries that will perform a cross join to compute the aggregation over nulls
        // approx_distinct and max_by return true for isCalledOnNullInput
        assertQuery(session,
                "select count(*) from nation n where (select approx_distinct(custkey) from customer c where n.nationkey=c.nationkey)>0",
                "select 25");

        assertQuery(session,
                "select count(*) from nation n where (select max_by(custkey, c.name) from customer c where n.nationkey=c.nationkey)>2000",
                "select 0");
    }

    @Test
    public void testMergeDuplicateAggregations()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(MERGE_DUPLICATE_AGGREGATIONS, "true")
                .build();
        assertQuery(session, "SELECT linenumber, min(orderkey) " +
                "FROM lineitem " +
                "GROUP BY linenumber " +
                "HAVING min(orderkey) < (SELECT avg(orderkey) FROM orders WHERE orderkey < 7)");
    }

    @Test
    public void testReplaceConditionalApproxDistinct()
    {
        Session enabled = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_CONDITIONAL_CONSTANT_APPROXIMATE_DISTINCT, "true")
                .build();
        Session disabled = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_CONDITIONAL_CONSTANT_APPROXIMATE_DISTINCT, "false")
                .build();

        List<String> queries = ImmutableList.of(
                "select approx_distinct(if(%s, 1, 2)) from orders %s",
                "select approx_distinct(if(%s, 3)) from orders %s",
                "select approx_distinct(if(%s, 4, NULL)) from orders %s",
                "select approx_distinct(if(%s, NULL, 5)) from orders %s",
                "select approx_distinct(if(%s, NULL)) from orders %s",
                "select approx_distinct(if(%s, NULL, NULL)) from orders %s");
        List<String> conditions = ImmutableList.of(
                "orderkey = orderkey",
                "orderkey % 2 = 0");
        List<String> suffixes = ImmutableList.of(
                "",
                "where orderkey % 3 = 0",
                "group by orderkey");

        for (String query : queries) {
            for (String condition : conditions) {
                for (String suffix : suffixes) {
                    String sql = format(query, condition, suffix);
                    assertQueryWithSameQueryRunner(enabled, sql, disabled);
                }
            }
        }
    }

    @Test
    public void testSameAggregationWithAndWithoutFilter()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER, "true")
                .build();
        Session disableOptimization = Session.builder(getSession())
                .setSystemProperty(MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER, "false")
                .build();

        assertQuery(enableOptimization, "select regionkey, count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation group by regionkey", "values (3,4,5),(2,5,5),(0,1,5),(4,2,5),(1,3,5)");
        assertQuery(enableOptimization, "select count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation", "values (15,25)");
        assertQuery(enableOptimization, "select count(1), count(1) filter (where k > 5) from (values 1, null, 3, 5, null, 8, 10) t(k)", "values (7, 2)");

        String sql = "select regionkey, count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation group by regionkey";
        MaterializedResult resultWithOptimization = computeActual(enableOptimization, sql);
        MaterializedResult resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        sql = "select count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        sql = "select partkey, sum(quantity), sum(quantity) filter (where discount > 0.1) from lineitem group by grouping sets((), (partkey))";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);

        // multiple aggregations in query
        sql = "select partkey, sum(quantity), sum(quantity) filter (where discount < 0.05), sum(linenumber), sum(linenumber) filter (where discount < 0.05) from lineitem group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // aggregations in multiple levels
        sql = "select partkey, avg(sum), avg(sum) filter (where tax < 0.05), avg(filtersum) from (select partkey, suppkey, sum(quantity) sum, sum(quantity) filter (where discount > 0.05) filtersum, max(tax) tax from lineitem where partkey=1598 group by partkey, suppkey) t group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // global aggregation
        sql = "select sum(quantity), sum(quantity) filter (where discount < 0.05) from lineitem";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // order by
        sql = "select partkey, array_agg(suppkey order by suppkey), array_agg(suppkey order by suppkey) filter (where discount > 0.05) from lineitem group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // grouping sets
        sql = "SELECT partkey, suppkey, sum(quantity), sum(quantity) filter (where discount > 0.05) from lineitem group by grouping sets((), (partkey), (partkey, suppkey))";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // aggregation over union
        sql = "SELECT partkey, sum(quantity), sum(quantity) filter (where orderkey > 0) from (select quantity, orderkey, partkey from lineitem union all select totalprice as quantity, orderkey, custkey as partkey from orders) group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // aggregation over join
        sql = "select custkey, sum(quantity), sum(quantity) filter (where tax < 0.05) from lineitem l join orders o on l.orderkey=o.orderkey group by custkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);

        // Now we do not have aggregations which has no filter
        // multiple aggregations in query
        sql = "select partkey, sum(quantity) filter (where discount > 0.05), sum(quantity) filter (where discount < 0.05), sum(linenumber) filter (where discount > 0.05), sum(linenumber) filter (where discount < 0.05) from lineitem group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        sql = "select partkey, sum(quantity) filter (where discount > 0.05), sum(quantity) filter (where discount < 0.05), sum(linenumber), sum(linenumber) filter (where discount < 0.05) from lineitem group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // aggregations in multiple levels
        sql = "select partkey, avg(sum) filter (where tax > 0.05), avg(sum) filter (where tax < 0.05), avg(filtersum) from (select partkey, suppkey, sum(quantity) sum, sum(quantity) filter (where discount > 0.05) filtersum, max(tax) tax from lineitem where partkey=1598 group by partkey, suppkey) t group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        sql = "select partkey, avg(sum) filter (where tax > 0.05), avg(sum) filter (where tax < 0.05), avg(filtersum) from (select partkey, suppkey, sum(quantity) filter (where discount < 0.05) sum, sum(quantity) filter (where discount > 0.05) filtersum, max(tax) tax from lineitem where partkey=1598 group by partkey, suppkey) t group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // global aggregation
        sql = "select sum(quantity) filter (where discount > 0.05), sum(quantity) filter (where discount < 0.05) from lineitem";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // order by
        sql = "select partkey, array_agg(suppkey order by suppkey) filter (where discount < 0.05), array_agg(suppkey order by suppkey) filter (where discount > 0.05) from lineitem group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // grouping sets
        sql = "SELECT partkey, suppkey, sum(quantity) filter (where discount < 0.05), sum(quantity) filter (where discount > 0.05) from lineitem group by grouping sets((), (partkey), (partkey, suppkey))";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // aggregation over union
        sql = "SELECT partkey, sum(quantity) filter (where orderkey > 10), sum(quantity) filter (where orderkey > 0) from (select quantity, orderkey, partkey from lineitem union all select totalprice as quantity, orderkey, custkey as partkey from orders) group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // aggregation over join
        sql = "select custkey, sum(quantity) filter (where tax > 0.05), sum(quantity) filter (where tax < 0.05) from lineitem l join orders o on l.orderkey=o.orderkey group by custkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
    }

    @Parameters("storageFormat")
    @Test
    public void testQueryWithEmptyInput(@Optional("PARQUET") String storageFormat)
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(SIMPLIFY_PLAN_WITH_EMPTY_INPUT, "true")
                .build();

        // DWRF does not support date type.
        String format = (System.getProperty("storageFormat") == null) ? storageFormat : System.getProperty("storageFormat");
        String orderdate = getDateExpression(format, "o.orderdate");
        String shipdate = getDateExpression(format, "l.shipdate");

        assertQuery(enableOptimization, "select o.orderkey, o.custkey, l.linenumber from orders o join (select orderkey, linenumber from lineitem where false) l on o.orderkey = l.orderkey");
        assertQuery(enableOptimization, "select o.orderkey, o.custkey, l.linenumber from orders o left join (select orderkey, linenumber from lineitem where false) l on o.orderkey = l.orderkey");
        assertQuery(enableOptimization, "select o.orderkey, o.custkey, l.linenumber from (select orderkey, linenumber from lineitem where false) l right join orders o on l.orderkey = o.orderkey");
        assertQuery(enableOptimization, "select orderkey, partkey from lineitem union all select orderkey, custkey as partkey from orders where false");
        assertQuery(enableOptimization, "select orderkey, partkey from lineitem where orderkey in (select orderkey from orders where false)");
        assertQuery(enableOptimization, "select orderkey, partkey from lineitem where orderkey not in (select orderkey from orders where false)");
        assertQuery(enableOptimization, "select count(*) as count from (select orderkey from orders where false)");
        assertQuery(enableOptimization, "select orderkey, count(*) as count from (select orderkey from orders where false) group by orderkey");
        assertQuery(enableOptimization, "select o.orderkey, o.custkey, l.linenumber from orders o join (select orderkey, max(linenumber) as linenumber from lineitem where false group by orderkey) l on o.orderkey = l.orderkey");
        assertQuery(enableOptimization, "select orderkey from orders where orderkey = (select orderkey from lineitem where false)");
        assertQuery(enableOptimization, "SELECT (SELECT 1 FROM orders WHERE false LIMIT 1) FROM lineitem");
        assertQuery(enableOptimization, "SELECT (SELECT 1 FROM orders WHERE false LIMIT 1) FROM lineitem");
        assertQuery(enableOptimization, "WITH emptyorders as (select orderkey, totalprice, orderdate from orders where false) SELECT orderkey, orderdate, totalprice, " +
                "ROW_NUMBER() OVER (ORDER BY orderdate) as row_num FROM emptyorders WHERE totalprice > 10 ORDER BY orderdate ASC LIMIT 10");
        String sql = format("WITH emptyorders as (select * from orders where false) SELECT p.name, l.orderkey, l.partkey, l.quantity, RANK() OVER (PARTITION BY p.name ORDER BY l.quantity DESC) AS rank_quantity " +
                "FROM lineitem l JOIN emptyorders o ON l.orderkey = o.orderkey JOIN part p ON l.partkey = p.partkey WHERE %s BETWEEN DATE '1995-03-01' AND DATE '1995-03-31' " +
                "AND %s BETWEEN DATE '1995-03-01' AND DATE '1995-03-31' AND p.size = 15 ORDER BY p.name, rank_quantity LIMIT 100", orderdate, shipdate);
        assertQuery(enableOptimization, sql);
        assertQuery(enableOptimization, "select orderkey, row_number() over (partition by orderpriority), orderpriority from (select orderkey, orderpriority from orders where false)");
        assertQuery(enableOptimization, "select * from (select orderkey, row_number() over (partition by orderpriority order by orderkey) row_number, orderpriority from (select orderkey, orderpriority from orders where false)) where row_number < 2");

        emptyJoinQueries(enableOptimization);
    }

    @Test
    public void testPushFilterExpressionDownCrossJoin()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(PUSH_DOWN_FILTER_EXPRESSION_EVALUATION_THROUGH_CROSS_JOIN, "ALWAYS")
                .build();
        String sql = "with t1 as (select * from (values (1, 2), (null, 2), (1, null), (null, null)) t(k1, k2)), t2 as (select * from (values (1, 2), (null, 1), (null, 2), (null, null)) t(k1, k2)) " +
                "select * from t1 join t2 on t1.k1=t2.k1 or coalesce(t1.k2, 1)= coalesce(t2.k2, 2)";
        assertQuery(enableOptimization, sql);

        sql = "select o.orderkey, l.partkey, l.suppkey from lineitem l join orders o on l.orderkey = o.orderkey or (l.partkey + l.suppkey) = o.orderkey where l.quantity < 5 and o.totalprice < 2000";
        assertQuery(enableOptimization, sql);

        sql = "select o.orderkey, l.partkey, l.suppkey from lineitem l join orders o on (l.orderkey = o.orderkey or (l.partkey + l.suppkey) = o.orderkey) and l.quantity*totalprice > 1000 where l.quantity < 5 and o.totalprice < 2000";
        assertQuery(enableOptimization, sql);
    }

    @Test
    public void testInnerJoinWithOrCondition()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(PUSH_DOWN_FILTER_EXPRESSION_EVALUATION_THROUGH_CROSS_JOIN, "disabled")
                .setSystemProperty(REWRITE_CROSS_JOIN_OR_TO_INNER_JOIN, "true")
                .build();
        String sql = "with t1 as (select * from (values (1, 2), (null, 2), (1, null), (null, null)) t(k1, k2)), t2 as (select * from (values (1, 2), (null, 1), (null, 2), (null, null)) t(k1, k2)) " +
                "select * from t1 join t2 on t1.k1=t2.k1 or t1.k2=t2.k2";
        assertQuery(enableOptimization, sql);

        sql = "select o.orderkey, l.partkey from lineitem l join orders o on l.orderkey = o.orderkey or l.partkey = o.orderkey where l.quantity < 5 and o.totalprice < 2000";
        assertQuery(enableOptimization, sql);

        sql = "select o.orderkey, l.partkey from lineitem l join orders o on (l.orderkey = o.orderkey or l.partkey = o.orderkey) and l.quantity*totalprice > 1000 where l.quantity < 5 and o.totalprice < 2000";
        assertQuery(enableOptimization, sql);

        Session enablePushFilterOptimization = Session.builder(getSession())
                .setSystemProperty(PUSH_DOWN_FILTER_EXPRESSION_EVALUATION_THROUGH_CROSS_JOIN, "REWRITTEN_TO_INNER_JOIN")
                .setSystemProperty(REWRITE_CROSS_JOIN_OR_TO_INNER_JOIN, "true")
                .build();

        sql = "with t1 as (select * from (values (1, 2), (null, 2), (1, null), (null, null)) t(k1, k2)), t2 as (select * from (values (1, 2), (null, 1), (null, 2), (null, null)) t(k1, k2)) " +
                "select * from t1 join t2 on t1.k1=t2.k1 or coalesce(t1.k2, 1)= coalesce(t2.k2, 2)";
        assertQuery(enablePushFilterOptimization, sql);

        sql = "select o.orderkey, l.partkey, l.suppkey from lineitem l join orders o on l.orderkey = o.orderkey or (l.partkey + l.suppkey) = o.orderkey where l.quantity < 5 and o.totalprice < 2000";
        assertQuery(enablePushFilterOptimization, sql);

        sql = "select o.orderkey, l.partkey, l.suppkey from lineitem l join orders o on (l.orderkey = o.orderkey or (l.partkey + l.suppkey) = o.orderkey) and l.quantity*totalprice > 1000 where l.quantity < 5 and o.totalprice < 2000";
        assertQuery(enablePushFilterOptimization, sql);
    }

    @Test
    public void testArraySum()
    {
        // bigint cases
        String sql = "select array_sum(k) from (values " +
                "(array[cast(10 as bigint), 20, 30]), " +
                "(array[cast(40 as bigint), 60]), " +
                "(array[cast(100 as bigint), 200]), " +
                "(array[cast(1 as bigint), 2, 3, 4]), " +
                "(array[cast(-10 as bigint), -20]), " +
                "(array[cast(800 as bigint), 200]), " +
                "(array[cast(300 as bigint), 700, 1000]), " +
                "(array[cast(-500 as bigint), 500, 0]), " +
                "(array[cast(2147483647 as bigint), 1]), " +
                "(array[cast(-2147483648 as bigint), 0]), " +
                "(array[cast(null as bigint), 100, 200]), " + // null case
                "(array[cast(50 as bigint), null, 150])) t(k)"; // null case
        assertQuery(sql, "values cast(60 as bigint), cast(100 as bigint), cast(300 as bigint), cast(10 as bigint), cast(-30 as bigint), cast(1000 as bigint), cast(2000 as bigint), cast(0 as bigint), cast(2147483648 as bigint), cast(-2147483648 as bigint), cast(300 as bigint), cast(200 as bigint)");

        // double cases
        sql = "select array_sum(k) from (values " +
                "(array[cast(1.5 as double), 2.5]), " +
                "(array[cast(3.5 as double), 4.5]), " +
                "(array[cast(0.1 as double), 0.2, 0.3]), " +
                "(array[cast(-1.5 as double), -2.5]), " +
                "(array[cast(10.5 as double), 20.5]), " +
                "(array[cast(3.3 as double), 6.7]), " +
                "(array[cast(4.4 as double), 5.6, 10.0]), " +
                "(array[cast(-2.2 as double), 2.2]), " +
                "(array[cast(1e308 as double), -1e308]), " +
                "(array[cast(0.0000001 as double), 0.0000002]), " +
                "(array[cast(null as double), 1.1, 2.2]), " + // null case
                "(array[cast(0.5 as double), null, 1.5])) t(k)"; // null case
        assertQuery(sql, "values cast(4.0 as double), cast(8.0 as double), cast(0.6 as double), cast(-4.0 as double), cast(31.0 as double), cast(10.0 as double), cast(20.0 as double), cast(0.0 as double), cast(0.0 as double), cast(0.0000003 as double), cast(3.3 as double), cast(2.0 as double)");
    }

    @Test
    public void testArrayCumSumIntegers()
    {
        // int
        String sql = "select array_cum_sum(k) from (values (array[cast(5 as INTEGER), 6, 0]), (ARRAY[]), (CAST(NULL AS array(integer)))) t(k)";
        assertQuery(sql, "values array[cast(5 as integer), cast(11 as integer), cast(11 as integer)], array[], null");

        sql = "select array_cum_sum(k) from (values (array[cast(5 as INTEGER), 6, 0]), (ARRAY[]), (CAST(NULL AS array(integer))), (ARRAY [cast(2147483647 as INTEGER), 2147483647, 2147483647])) t(k)";
        assertQueryFails(sql, "integer (addition )?overflow:.*", true);

        sql = "select array_cum_sum(k) from (values (array[cast(5 as INTEGER), 6, null, 2, 3])) t(k)";
        assertQuery(sql, "values array[cast(5 as integer), cast(11 as integer), cast(null as integer), cast(null as integer), cast(null as integer)]");

        sql = "select array_cum_sum(k) from (values (array[cast(null as INTEGER), 6, null, 2, 3])) t(k)";
        assertQuery(sql, "values array[cast(null as integer), cast(null as integer), cast(null as integer), cast(null as integer), cast(null as integer)]");

        // bigint
        sql = "select array_cum_sum(k) from (values (array[cast(5 as bigint), 6, 0]), (ARRAY[]), (CAST(NULL AS array(bigint))), (ARRAY [cast(2147483647 as bigint), 2147483647, 2147483647])) t(k)";
        assertQuery(sql, "values array[cast(5 as bigint), cast(11 as bigint), cast(11 as bigint)], array[], null, array[cast(2147483647 as bigint), cast(4294967294 as bigint), cast(6442450941 as bigint)]");

        sql = "select array_cum_sum(k) from (values (array[cast(5 as bigint), 6, null, 2, 3])) t(k)";
        assertQuery(sql, "values array[cast(5 as bigint), cast(11 as bigint), cast(null as bigint), cast(null as bigint), cast(null as bigint)]");

        sql = "select array_cum_sum(k) from (values (array[cast(null as bigint), 6, null, 2, 3])) t(k)";
        assertQuery(sql, "values array[cast(null as bigint), cast(null as bigint), cast(null as bigint), cast(null as bigint), cast(null as bigint)]");
    }

    @Test
    public void testArrayCumSumDecimals()
    {
        String sql = "select array_cum_sum(k) from (values (array[cast(5.1 as decimal(38, 1)), 6, 0]), (ARRAY[]), (CAST(NULL AS array(decimal)))) t(k)";
        assertQuery(sql, "values array[cast(5.1 as decimal), cast(11.1 as decimal), cast(11.1 as decimal)], array[], null");

        sql = "select array_cum_sum(k) from (values (array[cast(5.1 as decimal(38, 1)), 6, null, 3]), (array[cast(null as decimal(38, 1)), 6, null, 3])) t(k)";
        assertQuery(sql, "values array[cast(5.1 as decimal), cast(11.1 as decimal), cast(null as decimal), cast(null as decimal)], " +
                "array[cast(null as decimal), cast(null as decimal), cast(null as decimal), cast(null as decimal)]");
    }

    @Test
    public void testArrayCumSumReals()
    {
        // real
        String sql = "select array_cum_sum(k) from (values (array[cast(null as real), 6, null, 2, 3])) t(k)";
        assertQuery(sql, "values array[cast(null as real), cast(null as real), cast(null as real), cast(null as real), cast(null as real)]");

        MaterializedResult raw = computeActual("SELECT array_cum_sum(k) FROM (values (ARRAY [cast(5.1 as real), 6.1, 0.5]), (ARRAY[]), (CAST(NULL AS array(real))), " +
                "(ARRAY [cast(null as real), 6.1, 0.5]), (ARRAY [cast(2.5 as real), 6.1, null, 3.2])) t(k)");
        List<MaterializedRow> rowList = raw.getMaterializedRows();
        List<Float> actualFloat = (List<Float>) rowList.get(0).getField(0);
        List<Float> expectedFloat = ImmutableList.of(5.1f, 11.2f, 11.7f);
        for (int i = 0; i < actualFloat.size(); ++i) {
            assertTrue(actualFloat.get(i) > expectedFloat.get(i) - 1e-5 && actualFloat.get(i) < expectedFloat.get(i) + 1e-5);
        }

        actualFloat = (List<Float>) rowList.get(1).getField(0);
        assertTrue(actualFloat.isEmpty());

        actualFloat = (List<Float>) rowList.get(2).getField(0);
        assertNull(actualFloat);

        actualFloat = (List<Float>) rowList.get(3).getField(0);
        for (int i = 0; i < actualFloat.size(); ++i) {
            assertNull(actualFloat.get(i));
        }

        actualFloat = (List<Float>) rowList.get(4).getField(0);
        expectedFloat = Arrays.asList(2.5f, 8.6f, null, null);
        for (int i = 0; i < 2; ++i) {
            assertTrue(actualFloat.get(i) > expectedFloat.get(i) - 1e-5 && actualFloat.get(i) < expectedFloat.get(i) + 1e-5);
        }
        for (int i = 2; i < actualFloat.size(); ++i) {
            assertNull(actualFloat.get(i));
        }

        // double
        raw = computeActual("SELECT array_cum_sum(k) FROM (values (ARRAY [cast(5.1 as double), 6.1, 0.5]), (ARRAY[]), (CAST(NULL AS array(double))), " +
                "(ARRAY [cast(null as double), 6.1, 0.5]), (ARRAY [cast(5.1 as double), 6.1, null, 3.2])) t(k)");
        rowList = raw.getMaterializedRows();
        List<Double> actualDouble = (List<Double>) rowList.get(0).getField(0);
        List<Double> expectedDouble = ImmutableList.of(5.1, 11.2, 11.7);
        for (int i = 0; i < actualDouble.size(); ++i) {
            assertTrue(actualDouble.get(i) > expectedDouble.get(i) - 1e-5 && actualDouble.get(i) < expectedDouble.get(i) + 1e-5);
        }

        actualDouble = (List<Double>) rowList.get(1).getField(0);
        assertTrue(actualDouble.isEmpty());

        actualDouble = (List<Double>) rowList.get(2).getField(0);
        assertNull(actualDouble);

        actualDouble = (List<Double>) rowList.get(3).getField(0);
        for (int i = 0; i < actualDouble.size(); ++i) {
            assertNull(actualDouble.get(i));
        }

        actualDouble = (List<Double>) rowList.get(4).getField(0);
        expectedDouble = Arrays.asList(5.1, 11.2, null, null);
        for (int i = 0; i < 2; ++i) {
            assertTrue(actualDouble.get(i) > expectedDouble.get(i) - 1e-5 && actualDouble.get(i) < expectedDouble.get(i) + 1e-5);
        }
        for (int i = 2; i < actualDouble.size(); ++i) {
            assertNull(actualDouble.get(i));
        }
    }

    @Test
    public void testArrayCumSumVarchar()
    {
        String sql = "select array_cum_sum(k) from (values (array[cast('5.1' as varchar), '6', '0']), (ARRAY[]), (CAST(NULL AS array(varchar)))) t(k)";
        assertQueryFails(sql, ".*cannot be applied to.*");

        sql = "select array_cum_sum(k) from (values (array[cast(null as varchar), '6', '0'])) t(k)";
        assertQueryFails(sql, ".*cannot be applied to.*");
    }

    @Test
    public void testPreProcessMetastoreCalls()
    {
        Session enablePreProcessMetadataCalls = Session.builder(getSession())
                .setSystemProperty(PRE_PROCESS_METADATA_CALLS, "true")
                .build();

        String query = "SELECT name from nation";
        assertEqualsIgnoreOrder(computeActual(enablePreProcessMetadataCalls, query), computeActual(getSession(), query));

        query = "SELECT orderkey, custkey, sum(agg_price) AS outer_sum, grouping(orderkey, custkey), g " +
                "FROM " +
                "    (SELECT orderkey, custkey, sum(totalprice) AS agg_price, grouping(custkey, orderkey) AS g " +
                "        FROM orders " +
                "        GROUP BY orderkey, custkey " +
                "        ORDER BY agg_price ASC " +
                "        LIMIT 5) AS t " +
                "GROUP BY GROUPING SETS ((orderkey, custkey), g) " +
                "ORDER BY outer_sum";
        assertEqualsIgnoreOrder(computeActual(enablePreProcessMetadataCalls, query), computeActual(getSession(), query));

        query = "SELECT c.custkey, c.name, l.orderkey, p.name FROM\n" +
                "    customer c\n" +
                "    JOIN orders o ON c.custkey = o.custkey\n" +
                "    JOIN lineitem l ON o.orderkey = l.orderkey\n" +
                "    JOIN part p ON l.partkey = p.partkey\n";
        assertEqualsIgnoreOrder(computeActual(enablePreProcessMetadataCalls, query), computeActual(getSession(), query));

        query = "SELECT\n" +
                "    c.custkey,\n" +
                "    c.name,\n" +
                "    o.orderkey,\n" +
                "    o.orderdate,\n" +
                "    l.extendedprice\n" +
                "FROM\n" +
                "    customer c\n" +
                "    JOIN orders o ON c.custkey = o.custkey\n" +
                "    JOIN lineitem l ON o.orderkey = l.orderkey\n" +
                "    JOIN (\n" +
                "        SELECT\n" +
                "            orderkey,\n" +
                "            MAX(shipdate) AS max_shipdate\n" +
                "        FROM\n" +
                "            lineitem\n" +
                "        GROUP BY\n" +
                "            orderkey\n" +
                "    ) max_ship ON l.orderkey = max_ship.orderkey AND l.shipdate = max_ship.max_shipdate\n" +
                "WHERE\n" +
                "    c.nationkey = 1\n";
        assertEqualsIgnoreOrder(computeActual(enablePreProcessMetadataCalls, query), computeActual(getSession(), query));

        query = "WITH temp_cte AS ( \n" +
                "   SELECT name from nation \n" +
                "   ) \n" +
                "   SELECT * FROM temp_cte";
        assertEqualsIgnoreOrder(computeActual(enablePreProcessMetadataCalls, query), computeActual(getSession(), query));
    }

    @Test
    public void testPreProcessMetastoreCallsForFailedQueries()
    {
        Session enablePreProcessMetadataCalls = Session.builder(getSession())
                .setSystemProperty(PRE_PROCESS_METADATA_CALLS, "true")
                .build();

        String query = "SELECT name from nation1";
        String errorMessage = "Table .*nation1 does not exist";
        assertQueryFails(query, errorMessage);
        assertQueryFails(enablePreProcessMetadataCalls, query, errorMessage);

        query = "SELECT name1 from nation";
        errorMessage = ".*Column 'name1' cannot be resolved";
        assertQueryFails(query, errorMessage);
        assertQueryFails(enablePreProcessMetadataCalls, query, errorMessage);
    }

    @Test
    public void testCrossJoinWithArrayContainsCondition()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(PUSH_DOWN_FILTER_EXPRESSION_EVALUATION_THROUGH_CROSS_JOIN, "REWRITTEN_TO_INNER_JOIN")
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_CONTAINS_TO_INNER_JOIN, "true")
                .build();

        String sql = "with t1 as (select * from (values (array[1, 2, 3], 10), (array[4, 5, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t1 join t2 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (11, 4, 'b'), (10, 1, 'a')");

        sql = "with t1 as (select * from (values (array[1, 1, 3], 10), (array[4, 4, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t1 join t2 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (11, 4, 'b'), (10, 1, 'a')");

        sql = "with t1 as (select * from (values (array[1, null, 3], 10), (array[4, null, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (null, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t1 join t2 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (10, 1, 'a')");

        sql = "with t1 as (select * from (values (array[1, 2, 3], 10), (array[4, 5, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t1 join t2 on contains(t1.arr, t2.k) and t1.k > 10";
        assertQuery(enableOptimization, sql, "values (11, 4, 'b')");

        sql = "with t1 as (select * from (values (array[1, 2, 3], 1), (array[4, 5, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t1 join t2 on contains(t1.arr, t2.k) or t1.k = t2.k";
        assertQuery(enableOptimization, sql, "values (1, 1, 'a'), (11, 4, 'b')");

        sql = "with t1 as (select array_agg(orderkey) orderkey, partkey from lineitem l where l.quantity < 5 group by partkey) " +
                "select t1.partkey, o.orderkey, o.totalprice from t1 join orders o on contains(t1.orderkey, o.orderkey) where o.totalprice < 2000";
        // Because the UDF has different names in H2, which is `array_contains`
        String h2Sql = "with t1 as (select array_agg(orderkey) orderkey, partkey from lineitem l where l.quantity < 5 group by partkey) " +
                "select t1.partkey, o.orderkey, o.totalprice from t1 join orders o on array_contains(t1.orderkey, o.orderkey) where o.totalprice < 2000";
        assertQuery(enableOptimization, sql, h2Sql);

        sql = "with t1 as (select array_agg(orderkey) orderkey, partkey from lineitem l where l.quantity < 5 group by partkey) " +
                "select t1.partkey, o.orderkey, o.totalprice from t1 join orders o on contains(t1.orderkey, o.orderkey) and t1.partkey < o.orderkey where o.totalprice < 2000";
        h2Sql = "with t1 as (select array_agg(orderkey) orderkey, partkey from lineitem l where l.quantity < 5 group by partkey) " +
                "select t1.partkey, o.orderkey, o.totalprice from t1 join orders o on array_contains(t1.orderkey, o.orderkey) and t1.partkey < o.orderkey where o.totalprice < 2000";
        assertQuery(enableOptimization, sql, h2Sql);

        // Arrray on build side
        sql = "with t1 as (select * from (values (array[1, 2, 3], 10), (array[4, 5, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 join t1 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (11, 4, 'b'), (10, 1, 'a')");

        sql = "with t1 as (select * from (values (array[1, 1, 3], 10), (array[4, 4, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 join t1 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (11, 4, 'b'), (10, 1, 'a')");

        sql = "with t1 as (select * from (values (array[1, null, 3], 10), (array[4, null, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (null, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 join t1 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (10, 1, 'a')");

        sql = "with t1 as (select * from (values (array[1, 2, 3], 10), (array[4, 5, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 join t1 on contains(t1.arr, t2.k) and t1.k > 10";
        assertQuery(enableOptimization, sql, "values (11, 4, 'b')");

        sql = "with t1 as (select * from (values (array[1, 2, 3], 1), (array[4, 5, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 join t1 on contains(t1.arr, t2.k) or t1.k = t2.k";
        assertQuery(enableOptimization, sql, "values (1, 1, 'a'), (11, 4, 'b')");

        sql = "with t1 as (select array_agg(orderkey) orderkey, partkey from lineitem l where l.quantity < 5 group by partkey) " +
                "select t1.partkey, o.orderkey, o.totalprice from t1 join orders o on contains(t1.orderkey, o.orderkey) where o.totalprice < 2000";
        h2Sql = "with t1 as (select array_agg(orderkey) orderkey, partkey from lineitem l where l.quantity < 5 group by partkey) " +
                "select t1.partkey, o.orderkey, o.totalprice from orders o join t1 on array_contains(t1.orderkey, o.orderkey) where o.totalprice < 2000";
        assertQuery(enableOptimization, sql, h2Sql);

        sql = "with t1 as (select array_agg(orderkey) orderkey, partkey from lineitem l where l.quantity < 5 group by partkey) " +
                "select t1.partkey, o.orderkey, o.totalprice from orders o join t1 on contains(t1.orderkey, o.orderkey) and t1.partkey < o.orderkey where o.totalprice < 2000";
        h2Sql = "with t1 as (select array_agg(orderkey) orderkey, partkey from lineitem l where l.quantity < 5 group by partkey) " +
                "select t1.partkey, o.orderkey, o.totalprice from orders o join t1 on array_contains(t1.orderkey, o.orderkey) and t1.partkey < o.orderkey where o.totalprice < 2000";
        assertQuery(enableOptimization, sql, h2Sql);

        // Element type and array type does not match
        sql = "with t1 as (select * from (values (array[cast(1 as bigint), 2, 3], 10), (array[4, 5, 6], 11)) t(arr, k)), t2 as (select * from (values (cast(1 as integer), 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t1 join t2 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (11, 4, 'b'), (10, 1, 'a')");

        sql = "with t1 as (select * from (values (array[cast(1 as integer), 2, 3], 10), (array[4, 5, 6], 11)) t(arr, k)), t2 as (select * from (values (cast(1 as bigint), 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t1 join t2 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (11, 4, 'b'), (10, 1, 'a')");

        sql = "with t1 as (select * from (values (1, 'JAPAN'), (2, 'invalid_nation')) t(k, nation)) " +
                "select t1.k, t1.nation from t1 where contains((select array_agg(name) from nation), t1.nation)";
        assertQuery(enableOptimization, sql, "values (1, 'JAPAN')");

        sql = "with t1 as (select * from (values (1, 'JAPAN'), (2, 'invalid_nation')) t(k, nation)) " +
                "select t1.k, t1.nation from t1 where contains(transform((select array_agg(name) from nation), (x) ->lower(x)), lower(t1.nation))";
        assertQuery(enableOptimization, sql, "values (1, 'JAPAN')");
    }

    @Test
    public void testLeftJoinNullFilterToSemiJoin()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(REWRITE_LEFT_JOIN_NULL_FILTER_TO_SEMI_JOIN, "true")
                .build();

        String sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3) t(k)), t2 as (select * from (values 1, 1, 2, 2) t(k)) select t1.* from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values 3, 3");

        // null in both sides
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3, null) t(k)), t2 as (select * from (values 1, 1, 2, 2, null) t(k)) select t1.* from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values 3, 3, null");
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3, null) t(k)), t2 as (select * from (values 1, 1, 2, 2, null, null, null, null) t(k)) select t1.* from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values 3, 3, null");
        assertNotEquals(computeActual("EXPLAIN(TYPE DISTRIBUTED) " + sql).getOnlyValue().toString().indexOf("Aggregate"), -1);

        // null in right side
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3) t(k)), t2 as (select * from (values 1, 1, 2, 2, null) t(k)) select t1.* from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values 3, 3");
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3) t(k)), t2 as (select * from (values 1, 1, 2, 2, null, null, null, null) t(k)) select t1.* from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values 3, 3");
        assertNotEquals(computeActual("EXPLAIN(TYPE DISTRIBUTED) " + sql).getOnlyValue().toString().indexOf("Aggregate"), -1);

        // right side already has distinct should apply no more distinct
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3) t(k)), t2 as (select distinct(k) from (values 1, 1, 2, 2, null) t(k)) select t1.* from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values 3, 3");
        assertNotEquals(computeActual("EXPLAIN(TYPE DISTRIBUTED) " + sql).getOnlyValue().toString().indexOf("Aggregate"), -1);

        // right side has group by but not distinct
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3) t(k)), t2 as (select k from (values 1, 1, 2, 2, null) t(k) group by k having count(1) > 1) select t1.* from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values 3, 3");
        assertNotEquals(computeActual("EXPLAIN(TYPE DISTRIBUTED) " + sql).getOnlyValue().toString().indexOf("Aggregate"), -1);

        // filter in right child of join
        sql = "with t1 as (select * from (values (1, 2), (2, 3), (1, 0)) t(k1, k2)), t2 as (select * from (values (1, 2), (2, 3)) t(k1, k2)) select t1.k1, t1.k2 from t1 left join t2 on t1.k2=t2.k2 and t2.k1 > 1 where t2.k2 is null";
        assertQuery(enableOptimization, sql, "values (1, 2), (1, 0)");

        // null in left side
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3, null) t(k)), t2 as (select * from (values 1, 1, 2, 2) t(k)) select t1.* from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values 3, 3, null");

        // right key also in output
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3) t(k)), t2 as (select * from (values 1, 1, 2, 2) t(k)) select t1.*, t2.k from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values (3, null), (3, null)");
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3, null) t(k)), t2 as (select * from (values 1, 1, 2, 2, null) t(k)) select t1.*, t2.k from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values (3, null), (3, null), (null, null)");
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3) t(k)), t2 as (select * from (values 1, 1, 2, 2, null) t(k)) select t1.*, t2.k from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values (3, null), (3, null)");
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3, null) t(k)), t2 as (select * from (values 1, 1, 2, 2) t(k)) select t1.*, t2.k from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values (3, null), (3, null), (null, null)");

        // Empty right input
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3) t(k)), t2 as (select * from (values 1, 1, 2, 2) t(k) where k > 3) select t1.* from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values 1, 1, 2, 2, 3, 3");
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3, null) t(k)), t2 as (select * from (values 1, 1, 2, 2, null) t(k) where k > 3) select t1.* from t1 left join t2 on t1.k=t2.k where t2.k is null";
        assertQuery(enableOptimization, sql, "values 1, 1, 2, 2, 3, 3, null");

        // join filter on left side
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3) t(k)), t2 as (select * from (values 1, 1, 2, 2) t(k)) select t1.* from t1 left join t2 on t1.k=t2.k and t1.k < 2 where t2.k is null";
        assertQuery(enableOptimization, sql, "values 2, 2, 3, 3");

        // join filter on right side
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3) t(k)), t2 as (select * from (values 1, 1, 2, 2) t(k)) select t1.* from t1 left join t2 on t1.k=t2.k and t2.k < 2 where t2.k is null";
        assertQuery(enableOptimization, sql, "values 3, 3, 2, 2");

        // join filter on both side
        sql = "with t1 as (select * from (values 1, 1, 2, 2, 3, 3) t(k)), t2 as (select * from (values 1, 1, 2, 2) t(k)) select t1.* from t1 left join t2 on t1.k=t2.k and t1.k+t2.k > 2 where t2.k is null";
        assertQuery(enableOptimization, sql, "values 1, 1, 3, 3");

        // rewrite with tpch dataset
        sql = "with t as (select orderkey from orders where custkey%5=1) select l.orderkey, l.partkey, l.quantity from lineitem l left join t on l.orderkey = t.orderkey where t.orderkey is null";
        assertQuery(enableOptimization, sql);

        // filter in where
        sql = "with t as (select orderkey from orders where custkey%5=1) select l.orderkey, l.partkey, l.quantity from lineitem l left join t on l.orderkey = t.orderkey where t.orderkey is null and l.suppkey%5=1";
        assertQuery(enableOptimization, sql);

        // filter in on condition
        sql = "with t as (select orderkey from orders where custkey%5=1) select l.orderkey, l.partkey, l.quantity from lineitem l left join t on l.orderkey = t.orderkey and l.suppkey%5=1 where t.orderkey is null";
        assertQuery(enableOptimization, sql);
    }

    @Test
    public void testParitalRowNumberNode()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(ADD_PARTIAL_NODE_FOR_ROW_NUMBER_WITH_LIMIT, "true")
                .build();

        String sql = "select orderkey from (select orderkey, row_number() over(partition by orderkey) <=2 as keep from lineitem) where keep";
        assertQuery(enableOptimization, sql);
    }

    @Test
    public void testCaseToMapOptimization()
    {
        assertNotEquals(computeActual("EXPLAIN(TYPE DISTRIBUTED) SELECT orderkey, CASE  WHEN orderstatus||'-'|| comment ='O' THEN 'a' " +
                "when  orderstatus||'-'|| comment='F' THEN 'b' else 'other' END, " +
                "sum(case when  orderstatus||'-'|| comment='F' THEN  totalprice END)" +
                "FROM orders group by 1,2").getOnlyValue().toString().indexOf("element_at"), -1);

        assertNotEquals(computeActual("EXPLAIN(TYPE DISTRIBUTED) SELECT orderkey, CASE  WHEN orderstatus in ('a', 'f', 'b') THEN 'a' " +
                "when  orderstatus='F' THEN 'b' else 'other' END, " +
                "sum(case when  orderstatus||'-'|| comment='F' THEN  totalprice END)" +
                "FROM orders group by 1,2").getOnlyValue().toString().indexOf("element_at"), -1);

        // Should not be applied below
        assertEquals(computeActual("EXPLAIN(TYPE DISTRIBUTED) SELECT orderkey, CASE  WHEN orderstatus in ('a', 'f', 'b') THEN comment||'a' " +
                "when  orderstatus='F' THEN comment||'b' else 'other' END, " +
                "sum(case when  orderstatus||'-'|| comment='F' THEN  totalprice END)" +
                "FROM orders group by 1,2").getOnlyValue().toString().indexOf("element_at"), -1);

        assertQuery("select case x when 1 then 1 when 2 then 2 when 1 then 3 end from (select 1 as x) t", "select 1");
        assertQuery("select x, case x when 1 then 1 when 2 then 2 else 3 end from (select x from (values 1, 2, 3, 4) t(x))");
        assertQuery("select x, case when x=1 then 1 when x=2 then 2 else 3 end from (select x from (values 1, 2, 3, 4) t(x))");
        assertQuery("select x, case when x=1 then 1 when x in (2, 3) then 2 else 3 end from (select x from (values 1, 2, 3, 4) t(x))");
        assertQuery("select case (case true when true then true else coalesce(false = any (values true), true) end) when false then true end limit 1");

        // disable the feature and test to make sure it doesn't fire

        Session session = Session.builder(getSession())
                .setSystemProperty(REWRITE_CASE_TO_MAP_ENABLED, "false")
                .build();

        assertEquals(computeActual(session, "EXPLAIN(TYPE DISTRIBUTED) SELECT orderkey, CASE  WHEN orderstatus in ('a', 'f', 'b') THEN 'a' " +
                "when  orderstatus='F' THEN 'b' else 'other' END, " +
                "sum(case when  orderstatus||'-'|| comment='F' THEN  totalprice END)" +
                "FROM orders group by 1,2").getOnlyValue().toString().indexOf("element_at"), -1);
    }

    @Test
    public void testArraySort()
    {
        assertQuery("select array_sort(cast(array[5,6,4,null,3,7] as array<integer>))", "select array[3, 4, 5, 6, 7, null]");
        assertQuery("select array_sort(array[5,6,4,null,3,7])", "select array[3, 4, 5, 6, 7, null]");

        MaterializedResult result = computeActual("select array_sort(array[5,6,4,null,3,7, cast(9223372036854775807 as bigint)])");
        assertSorted(result.getMaterializedRows().get(0).getFields());

        result = computeActual("select array_sort(array[5.4,6.1,4.9,null,4.5,7.8,5.2])");
        assertSorted(result.getMaterializedRows().get(0).getFields());

        result = computeActual("select array_sort(cast(array[5.4,6.1,4.9,null,4.5,7.8,5.2] as array<double>))");
        assertSorted(result.getMaterializedRows().get(0).getFields());

        result = computeActual("select array_sort(cast(array[null, 5.4,6.1,4.9,null,4.5,7.8,5.2] as array<double>))");
        assertSorted(result.getMaterializedRows().get(0).getFields());

        result = computeActual("select array_sort(cast(array[null, 5.4,6.1,4.9,null,4.5,7.8,5.2] as array<real>))");
        assertSorted(result.getMaterializedRows().get(0).getFields());

        result = computeActual("select array_sort(cast(array[5.4,6.1,4.9,4.5,7.8,5.2] as array<real>))");
        assertSorted(result.getMaterializedRows().get(0).getFields());

        result = computeActual("select array_sort(cast(array[5.4,6.1,4.9,4.5,7.8,5.2] as array<double>))");
        assertSorted(result.getMaterializedRows().get(0).getFields());
    }

    private static void assertSorted(List<Object> array)
    {
        int i = 1;
        for (; i < array.size(); i++) {
            if (array.get(i) == null) {
                break;
            }
            assertTrue(((Comparable) array.get(i)).compareTo((Comparable) array.get(i - 1)) >= 0);
        }

        while (i < array.size()) {
            assertNull(array.get(i++));
        }
    }

    @Test
    public void testLikePrefixAndSuffix()
    {
        assertQuery("select x like 'abc%' from (values 'abc', 'def', 'bcd') T(x)");
        assertQuery("select x like '%abc%' from (values 'xabcy', 'abxabcdef', 'bcd',  'xabcyabcz') T(x)");
        assertQuery("select x like '%abc' from (values 'xa bc', 'xabcy', 'abcd', 'xabc') T(x)");
        assertQuery("select x like '%ab_c' from (values 'xa bc', 'xabcy', 'abcd') T(x)");
        assertQuery("select x like '%' from (values 'xa bc', 'xabcy', 'abcd') T(x)");
        assertQuery("select x like '%_%' from (values 'xa bc', 'xabcy', 'abcd') T(x)");
        assertQuery("select x like '%a%' from (values 'xa bc', 'xabcy', 'abcd') T(x)");
        assertQuery("select x like '%acd%xy%' from (values 'xa bc', 'xabcy', 'abcd') T(x)");
    }

    @Test
    public void testLikePrefixAndSuffixWithChars()
    {
        assertQuery("select x like 'abc%' from (values CAST ('abc' AS CHAR(3)), CAST ('def' AS CHAR(3)), CAST ('bcd' AS CHAR(3))) T(x)");
        assertQuery("select x like '%abc%' from (values CAST ('xabcy' AS CHAR(5)), CAST ('abxabcdef' AS CHAR(9)), CAST ('bcd' AS CHAR(3)),  CAST ('xabcyabcz' AS CHAR(9))) T(x)");
        assertQuery(
                "select x like '%abc' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4)), CAST ('xabc' AS CHAR(4)), CAST (' xabc' AS CHAR(5))) T(x)",
                "SELECT * FROM (VALUES(false), (false), (false), (false), (true)) T");
        assertQuery("select x like '%ab_c' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)");
        assertQuery("select x like '%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)");
        assertQuery("select x like '%_%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)");
        assertQuery("select x like '%a%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)");
        assertQuery("select x like '%acd%xy%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)");
    }

    @Test
    public void testLambdaExpressionPullUp()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(PULL_EXPRESSION_FROM_LAMBDA_ENABLED, "true")
                .build();
        // H2 runner does not have map, run with the same query runner for this query
        assertQueryWithSameQueryRunner(session, "select map_filter(idmap, (k, v) -> array_position(array_sort(map_keys(idmap)), k) <= 3) from (values map(array[1, 4, 2, 8, 0], " +
                "array['a', 'a', 'a', 'a', 'a'])) t(idmap)", "values map(array[0, 1, 2], array['a', 'a', 'a'])");
        assertQuery(session, "select * from (values (1, 1, array[1, 2, 3]), (1, 2, array[0, 1, 4]), (2, 2, array[1, 2, 3]), (5, 0, array[1, 2, 3]))t(id1, id2, array) where any_match(array, x -> x > id1 + id2)",
                "values (1, 1, array[1, 2, 3]), (1, 2, array[0, 1, 4])");
        assertQueryWithSameQueryRunner(session, "select transform(arr1, x->transform(arr2, y->slice(arr2, 1, 3))) from (values (array[1, 2], array[1,2,3,4,5]), (array[1,2,3], array[0,1,2,3])) t(arr1, arr2)",
                "values array[array[array[1, 2, 3], array[1, 2, 3], array[1, 2, 3], array[1, 2, 3], array[1, 2, 3]], array[array[1, 2, 3], array[1, 2, 3], array[1, 2, 3], array[1, 2, 3], array[1, 2, 3]]], " +
                        "array[array[array[0, 1, 2], array[0, 1, 2], array[0, 1, 2], array[0, 1, 2]], array[array[0, 1, 2], array[0, 1, 2], array[0, 1, 2], array[0, 1, 2]], array[array[0, 1, 2], array[0, 1, 2], array[0, 1, 2], array[0, 1, 2]]]");
        assertQuery(session, "select transform(col1, x -> concat(case when col2 is null then '*' when contains(col2, x) then '+' else ' ' end, x)) from (values (array['1'], array['1'])) t(col1, col2)",
                "values ('+1')");
        assertQuery(session, "select transform(col1, x -> if(x, col2[2], 0)) from (values (array[false], array[0])) t(col1, col2)", "values array[0]");
        assertQuery(session, "select transform(arr1, x -> arr2[2]) from (values (array[], array[0])) t(arr1, arr2)", "values array[]");
        assertQuery(session, "SELECT ANY_MATCH(a, x -> (x LIKE CONCAT('a', b))) AS rejected_by_control_model, FILTER(a, x -> x LIKE CONCAT('a', b)) FROM ( SELECT ARRAY[CAST(random() AS VARCHAR)] a, CAST(random() AS VARCHAR) AS b )", "values (false, array[])");
        assertQuery(session, "SELECT ANY_MATCH(a, x -> regexp_like(x, CONCAT('a', b))) AS rejected_by_control_model, FILTER(a, x -> regexp_like(x, CONCAT('a', b))) FROM ( SELECT ARRAY[CAST(random() AS VARCHAR)] a, CAST(random() AS VARCHAR) AS b )", "values (false, array[])");
        assertQuery(session, "SELECT ANY_MATCH(a, x -> (x LIKE CONCAT('a', b))) AS rejected_by_control_model, FILTER(a, x -> x LIKE CONCAT('a', b)) FROM ( SELECT ARRAY[CAST(random() AS VARCHAR)] a, CAST(1 AS VARCHAR) AS b )", "values (false, array[])");
        assertQuery(session, "SELECT ANY_MATCH(a, x -> regexp_like(x, CONCAT('a', b))) AS rejected_by_control_model, FILTER(a, x -> regexp_like(x, CONCAT('a', b))) FROM ( SELECT ARRAY[CAST(random() AS VARCHAR)] a, CAST(1 AS VARCHAR) AS b )", "values (false, array[])");
        assertQuery(session, "SELECT ANY_MATCH(a, x -> (x LIKE CONCAT('a', b))) AS rejected_by_control_model, FILTER(a, x -> x LIKE CONCAT('a', b) ESCAPE '#') FROM ( SELECT ARRAY[CAST(random() AS VARCHAR)] a, CAST(random() AS VARCHAR) AS b )", "values (false, array[])");
    }

    @Test
    public void testRewriteContainsToIn()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(REWRITE_CONSTANT_ARRAY_CONTAINS_TO_IN_EXPRESSION, "true")
                .build();
        assertQuery(session, "select k from (values 1, 2, 3, 4, 5) t(k) where contains(array[1, 3, 4], k)", "values 1, 3, 4");
        assertQuery(session, "select filter(arr, x -> contains(array[1,5], x)) from (values array[1, 2, 3, 4], array[1,3,5,7]) t(arr)", "values array[1], array[1, 5]");
        assertQuery(session, "select x, contains(array[null], x) from (values 1, 2, 3, 4, null) t(x)", "values (1, null), (2, null), (3, null), (4, null), (null, null)");
        assertQuery(session, "select x, contains(array[null, 1], x) from (values 1, 2, 3, 4, null) t(x)", "values (1, true), (2, null), (3, null), (4, null), (null, null)");
        assertQuery(session, "select x, contains(array[], x) from (values 1, 2, 3, 4, null) t(x)", "values (1, false), (2, false), (3, false), (4, false), (null, null)");
        assertQuery(session, "select x, contains(cast(null as array<bigint>), x) from (values 1, 2, 3, 4, null) t(x)", "values (1, null), (2, null), (3, null), (4, null), (null, null)");
    }

    @Test
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "true")
                .build();
        // Trigger optimization
        assertQuery(session, "select * from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        assertQuery(session, "select o.orderkey, c.name from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        assertQuery(session, "select *, cast(o.custkey as varchar), cast(c.custkey as varchar) from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        assertQuery(session, "select r.custkey, r.orderkey, r.name, n.nationkey from (select o.custkey, o.orderkey, c.name from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)) r, nation n");
        // Do not trigger optimization
        assertQuery(session, "select * from customer c join orders o on cast(acctbal as varchar) = cast(totalprice as varchar)");
    }

    @Test
    public void testPreserveAssignmentsInJoin()
    {
        // The following two timestamps represent the same point in time but with different time zones
        String timestampLosAngeles = "2001-08-22 03:04:05.321 America/Los_Angeles";
        String timestampNewYork = "2001-08-22 06:04:05.321 America/New_York";
        Set<List<Object>> rows = computeActual("WITH source AS (" +
                "SELECT * FROM (" +
                "    VALUES" +
                "        (TIMESTAMP '" + timestampLosAngeles + "')," +
                "        (TIMESTAMP '" + timestampNewYork + "')" +
                ") AS tbl (tstz)" +
                ")" +
                "SELECT * FROM source a JOIN source b ON a.tstz = b.tstz").getMaterializedRows().stream()
                .map(MaterializedRow::getFields)
                .collect(toImmutableSet());
        assertEquals(rows,
                ImmutableSet.of(
                        ImmutableList.of(zonedDateTime(timestampLosAngeles), zonedDateTime(timestampLosAngeles)),
                        ImmutableList.of(zonedDateTime(timestampLosAngeles), zonedDateTime(timestampNewYork)),
                        ImmutableList.of(zonedDateTime(timestampNewYork), zonedDateTime(timestampLosAngeles)),
                        ImmutableList.of(zonedDateTime(timestampNewYork), zonedDateTime(timestampNewYork))));
    }

    @Test
    public void testGenerateDomainFilters()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(GENERATE_DOMAIN_FILTERS, "true")
                .build();

        //Inner Join, domain predicates for both sides of Join inferred
        assertQuery(session, "select s.acctbal, n.name from supplier s inner join nation n on s.nationkey = n.nationkey " +
                "where (n.name = 'INDIA' AND s.acctbal BETWEEN 10 AND 5000) OR" +
                " (n.name = 'CANADA' AND s.acctbal BETWEEN 5001 AND 8000) OR" +
                " (n.name = 'ARGENTINA' AND s.acctbal > 8001)");

        //Outer Join, domain predicates for both sides of Join inferred since NULL is not inferred for inner side of join
        assertQuery(session, "select s.acctbal, n.name from supplier s left join nation n on s.nationkey = n.nationkey " +
                "where (n.name = 'INDIA' AND s.acctbal BETWEEN 10 AND 5000) OR" +
                " (n.name = 'CANADA' AND s.acctbal BETWEEN 5001 AND 8000) OR" +
                " (n.name = 'ARGENTINA' AND s.acctbal > 8001)");

        //Outer Join, domain predicate inferred only for outer side of join since NULL domain is inferred for `n.name`
        assertQuery(session, "select s.acctbal, n.name from supplier s left join nation n on s.nationkey = n.nationkey " +
                "where (n.name = 'INDIA' AND s.acctbal BETWEEN 10 AND 5000) OR" +
                " (n.name = 'CANADA' AND s.acctbal BETWEEN 5001 AND 8000) OR" +
                " (n.name IS NULL AND s.acctbal > 8001)");
    }

    @Test
    public void testMapBlockBug()
    {
        assertQueryFails(" VALUES(MAP_AGG(12345,123))", ".*Cannot evaluate non-scalar function.*");
    }

    @Test
    public void testReplaceConstantVariableReferencesWithConstants()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(REWRITE_EXPRESSION_WITH_CONSTANT_EXPRESSION, "true")
                .build();

        String sql = "select orderkey, orderpriority, avg(totalprice) from orders where orderpriority='3-MEDIUM' group by orderkey, orderpriority";
        assertQuery(enableOptimization, sql);

        sql = "select orderkey, orderpriority, idx from orders cross join unnest(array[1, 2]) t(idx) where orderpriority='3-MEDIUM'";
        assertQuery(enableOptimization, sql);

        sql = "select o.orderkey, o.orderpriority, l.tax from lineitem l join orders o on o.orderkey = l.orderkey where o.orderpriority='3-MEDIUM'";
        assertQuery(enableOptimization, sql);

        sql = "select orderpriority, avg(totalprice) from orders where orderpriority='3-MEDIUM' group by orderpriority";
        assertQuery(enableOptimization, sql);

        sql = "select o.orderkey, o.orderpriority, sum(l.quantity) from lineitem l join orders o on o.orderkey = l.orderkey where o.orderpriority='3-MEDIUM' group by o.orderkey, o.orderpriority";
        assertQuery(enableOptimization, sql);

        sql = "select orderpriority, orderkey from orders where orderpriority='3-MEDIUM' and orderkey in (select orderkey from lineitem)";
        assertQuery(enableOptimization, sql);

        sql = "SELECT orderkey FROM orders WHERE 3 = (SELECT orderkey)";
        assertQuery(enableOptimization, sql);

        sql = "select orderkey, orderpriority, avg(totalprice) from orders where orderpriority='3-MEDIUM' group by orderkey, orderpriority";
        assertQuery(enableOptimization, sql);

        sql = "select orderkey, orderpriority, idx from orders cross join unnest(array[1, 2]) t(idx) where orderpriority='3-MEDIUM'";
        assertQuery(enableOptimization, sql);

        sql = "select o.orderkey, o.orderpriority, l.tax from lineitem l join orders o on o.orderkey = l.orderkey where o.orderpriority='3-MEDIUM'";
        assertQuery(enableOptimization, sql);

        sql = "select o.orderkey, o.orderpriority, l.tax from lineitem l left join (select orderkey, orderpriority from orders where orderpriority='3-MEDIUM') o on o.orderkey = l.orderkey";
        assertQuery(enableOptimization, sql);

        sql = "select o.orderkey, l.linestatus, l.tax from (select tax, linestatus, orderkey from lineitem where linestatus ='O') l left join orders o on o.orderkey = l.orderkey";
        assertQuery(enableOptimization, sql);

        sql = "select orderpriority, orderkey from orders where orderpriority='3-MEDIUM' and orderkey in (select orderkey from lineitem)";
        assertQuery(enableOptimization, sql);

        sql = "select orderkey, orderpriority from orders where orderpriority='3-MEDIUM'";
        assertQuery(enableOptimization, sql);

        sql = "select orderkey, orderpriority from orders where orderpriority='3-MEDIUM' and orderpriority='5-HIGH'";
        assertQuery(enableOptimization, sql);

        sql = "with t1 as (select orderkey, orderstatus from orders where orderkey = 10) select l.orderkey, partkey, orderstatus from t1 join lineitem l on t1.orderkey = l.orderkey where partkey in (select suppkey from lineitem)";
        assertQuery(enableOptimization, sql);

        sql = "select orderkey+1 as nk from lineitem where orderkey=1";
        assertQuery(enableOptimization, sql);

        sql = "select orderkey, orderpriority from orders where orderpriority='3-MEDIUM' order by orderpriority";
        assertQuery(enableOptimization, sql);

        sql = "select orderkey, orderpriority from orders where orderpriority='3-MEDIUM' order by orderpriority, orderkey";
        assertQuery(enableOptimization, sql);

        sql = "select orderkey, orderpriority from orders where orderpriority='3-MEDIUM' order by orderpriority, orderkey limit 10";
        assertQuery(enableOptimization, sql);

        sql = "select orderkey, orderpriority from orders where orderpriority='3-MEDIUM' order by orderpriority, orderkey limit 10";
        assertQuery(enableOptimization, sql);

        sql = "select orderkey, price, count(*) from (select orderkey, extendedprice as price from lineitem where orderkey=5 union all select orderkey, totalprice as price from orders where orderkey=5) group by orderkey, price";
        assertQuery(enableOptimization, sql);

        sql = "select orderkey, price, count(*) from (select orderkey, extendedprice as price from lineitem where orderkey=5 union all select orderkey, totalprice as price from orders where orderkey=2) group by orderkey, price";
        assertQuery(enableOptimization, sql);

        sql = "select orderkey, price, count(*) from (select orderkey, extendedprice as price from lineitem where orderkey=5 union all select orderkey, totalprice as price from orders) group by orderkey, price";
        assertQuery(enableOptimization, sql);
    }

    @Test
    public void testMergeKHyperLogLog()
    {
        assertQueryWithSameQueryRunner("select k1, cardinality(merge(khll)), uniqueness_distribution(merge(khll)) from (select k1, k2, khyperloglog_agg(v1, v2) khll from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), " +
                        "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2) group by k1",
                "select k1, cardinality(khyperloglog_agg(v1, v2)), uniqueness_distribution(khyperloglog_agg(v1, v2)) from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), (2, 1, 11, 30), (2, 1, 11, 11), " +
                        "(2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1");

        assertQueryWithSameQueryRunner("select cardinality(merge(khll)), uniqueness_distribution(merge(khll)) from (select k1, k2, khyperloglog_agg(v1, v2) khll from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), " +
                        "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2)",
                "select cardinality(khyperloglog_agg(v1, v2)), uniqueness_distribution(khyperloglog_agg(v1, v2)) from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), (2, 1, 11, 30), (2, 1, 11, 11), " +
                        "(2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2)");
    }

    @Test
    public void testRepeat()
    {
        assertQuery("select repeat(k1, k2), repeat(k1, 5), repeat(3, k2) from (values (3, 2), (5, 4), (2, 4))t(k1, k2)",
                "values (array[3, 3], array[3,3,3,3,3], array[3, 3]), (array[5, 5, 5, 5], array[5, 5, 5, 5, 5], array[3, 3, 3, 3]), (array[2, 2, 2, 2], array[2, 2, 2, 2, 2], array[3, 3, 3, 3])");
    }

    @Test
    public void testRemoveMapCast()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(REMOVE_MAP_CAST, "true")
                .build();
        assertQuery(enableOptimization, "select feature[key] from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 4)) t(feature,  key)",
                "values 0.5, 0.1");
        assertQuery(enableOptimization, "select element_at(feature, key) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 4)) t(feature,  key)",
                "values 0.5, 0.1");
        assertQuery(enableOptimization, "select element_at(feature, key) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)",
                "values 0.5, null");
        assertQuery(enableOptimization, "select feature[key] from (values (map(array[cast(1 as varchar), '2', '3', '4'], array[0.3, 0.5, 0.9, 0.1]), cast('2' as varchar)), (map(array[cast(1 as varchar), '2', '3', '4'], array[0.3, 0.5, 0.9, 0.1]), '4')) t(feature,  key)",
                "values 0.5, 0.1");

        Session disableOptimization = Session.builder(getSession())
                .setSystemProperty(REMOVE_MAP_CAST, "false")
                .build();
        assertQueryWithSameQueryRunner(enableOptimization, "select map_subset(feature, array[key]) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 4)) t(feature,  key)", disableOptimization);
        assertQueryWithSameQueryRunner(enableOptimization, "select map_subset(feature, array[1, cast(3 as bigint)]) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 4)) t(feature,  key)", disableOptimization);
        assertQueryWithSameQueryRunner(enableOptimization, "select map_subset(feature, array[1, 3, key]) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 4)) t(feature,  key)", disableOptimization);
        assertQueryWithSameQueryRunner(enableOptimization, "select map_subset(feature, array[key]) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)", disableOptimization);
        assertQueryWithSameQueryRunner(enableOptimization, "select map_subset(feature, array[1, 400000000000]) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)", disableOptimization);
        assertQueryWithSameQueryRunner(enableOptimization, "select map_subset(feature, array[key, 2, 400000000000]) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)", disableOptimization);
    }

    @Test
    public void testRemoveMapCastFailure()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(REMOVE_MAP_CAST, "true")
                .build();
        assertQueryFails(enableOptimization, "select feature[key] from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)",
                ".*Out of range for integer.*");
    }

    // Test to guardrail problems in constraint framework mentioned in https://github.com/prestodb/presto/pull/22171
    @Test
    public void testGuardConstraintFramework()
    {
        assertQuery("with t as (select orderkey, count(1) cnt from (select * from (select * from orders where 1=0) left join (select partkey, suppkey from lineitem where 1=0) on partkey=10 where suppkey is not null) group by rollup(orderkey)) select t1.orderkey, t1.cnt from t t1 cross join t t2",
                "values (null, 0)");
        assertQuery("select orderkey from (select * from (select * from orders where 1=0)) group by rollup(orderkey)",
                "values (null)");
    }

    @Test
    public void testLambdaInAggregation()
    {
        assertQuery("SELECT id, reduce_agg(value, 0, (a, b) -> a + b+0, (a, b) -> a + b) FROM ( VALUES (1, 2), (1, 3), (1, 4), (2, 20), (2, 30), (2, 40) ) AS t(id, value) GROUP BY id", "values (1, 9), (2, 90)");
        assertQuery("SELECT id, reduce_agg(value, 's', (a, b) -> concat(a, b, 's'), (a, b) -> concat(a, b, 's')) FROM ( VALUES (1, '2'), (1, '3'), (1, '4'), (2, '20'), (2, '30'), (2, '40') ) AS t(id, value) GROUP BY id",
                "values (1, 's2s3s4s'), (2, 's20s30s40s')");
        assertQueryFails("SELECT id, reduce_agg(value, array[id, value], (a, b) -> a || b, (a, b) -> a || b) FROM ( VALUES (1, 2), (1, 3), (1, 4), (2, 20), (2, 30), (2, 40) ) AS t(id, value) GROUP BY id",
                ".*REDUCE_AGG only supports non-NULL literal as the initial value.*");
    }

    @Test
    public void testJoinPrefilter()
    {
        {
            // Orig
            String testQuery = "SELECT 1 from region join nation using(regionkey)";
            MaterializedResult result = computeActual("explain(type distributed) " + testQuery);
            assertEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
            result = computeActual(testQuery);
            assertEquals(result.getRowCount(), 25);

            // With feature
            Session session = Session.builder(getSession())
                    .setSystemProperty(JOIN_PREFILTER_BUILD_SIDE, String.valueOf(true))
                    .build();
            result = computeActual(session, "explain(type distributed) " + testQuery);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
            result = computeActual(session, testQuery);
            assertEquals(result.getRowCount(), 25);
        }

        {
            // Orig
            @Language("SQL") String testQuery = "SELECT 1 from region r join nation n on cast(r.regionkey as varchar) = cast(n.regionkey as varchar)";
            MaterializedResult result = computeActual("explain(type distributed) " + testQuery);
            assertEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
            result = computeActual(testQuery);
            assertEquals(result.getRowCount(), 25);

            // With feature
            Session session = Session.builder(getSession())
                    .setSystemProperty(JOIN_PREFILTER_BUILD_SIDE, String.valueOf(true))
                    .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, String.valueOf(false))
                    .build();
            result = computeActual(session, "explain(type distributed) " + testQuery);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("XX_HASH_64"), -1);
            result = computeActual(session, testQuery);
            assertEquals(result.getRowCount(), 25);
        }

        {
            // Orig
            String testQuery = "SELECT 1 from lineitem l join orders o on l.orderkey = o.orderkey and l.suppkey = o.custkey";
            MaterializedResult result = computeActual("explain(type distributed) " + testQuery);
            assertEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
            result = computeActual(testQuery);
            assertEquals(result.getRowCount(), 37);

            // With feature
            Session session = Session.builder(getSession())
                    .setSystemProperty(JOIN_PREFILTER_BUILD_SIDE, String.valueOf(true))
                    .build();
            result = computeActual(session, "explain(type distributed) " + testQuery);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("XX_HASH_64"), -1);
            result = computeActual(session, testQuery);
            assertEquals(result.getRowCount(), 37);
        }
    }

    @Test
    public void testRemoveCrossJoinWithSingleRowConstantInput()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(REMOVE_CROSS_JOIN_WITH_CONSTANT_SINGLE_ROW_INPUT, "true")
                .build();
        assertQuery(enableOptimization, "SELECT * FROM (SELECT EXTRACT(DAY FROM DATE '2017-01-01')) t CROSS JOIN (VALUES 1)",
                "values (1, 1)");
        assertQuery(enableOptimization, "SELECT * FROM (SELECT * FROM (VALUES 1) t(a)) t, region r WHERE r.regionkey = t.a");
        assertQuery(enableOptimization, "WITH t(msg) AS (SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) SELECT b.msg.x FROM t a, t b WHERE a.msg.y = b.msg.y",
                "values 1");
        assertQuery(enableOptimization, "WITH t(msg) AS (SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) SELECT a.msg.y, b.msg.x from t a cross join t b where a.msg.x = 7 or is_finite(b.msg.y)",
                "values (2.0, 1)");
        assertQuery(enableOptimization, "WITH t(msg) AS (SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) SELECT b.msg.x FROM t a, t b WHERE a.msg.y = b.msg.y limit 100",
                "values 1");
        assertQuery(enableOptimization, "select orderkey, col1 from orders cross join (select cast(col as varchar) col1 from (values 1) t(col))");
    }

    @Parameters("storageFormat")
    @Test
    public void testAddDistinctForSemiJoinBuild(@Optional("PARQUET") String storageFormat)
    {
        Session enabled = Session.builder(getSession())
                .setSystemProperty(ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD, "true")
                .build();
        Session disabled = Session.builder(getSession())
                .setSystemProperty(ADD_DISTINCT_BELOW_SEMI_JOIN_BUILD, "false")
                .build();

        // DWRF does not support date type.
        String format = (System.getProperty("storageFormat") == null) ? storageFormat : System.getProperty("storageFormat");
        String orderdate = getDateExpression(format, "o.orderdate");

        @Language("SQL") String sql = format("SELECT * FROM customer c WHERE custkey in ( SELECT custkey FROM orders o WHERE %s > date('1995-01-01'))", orderdate);
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
        sql = format("SELECT * FROM customer c WHERE custkey in ( SELECT distinct custkey FROM orders o WHERE %s > date('1995-01-01'))", orderdate);
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
        sql = "SELECT *\n" +
                "FROM customer c\n" +
                "WHERE c.custkey IN (\n" +
                "  SELECT o.custkey\n" +
                "  FROM orders o\n" +
                "  WHERE o.totalprice > 1000\n" +
                ")";
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
        sql = "SELECT c.name\n" +
                "FROM customer c\n" +
                "WHERE c.custkey IN (\n" +
                "    SELECT o.custkey\n" +
                "    FROM orders o\n" +
                ")";
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
        sql = "SELECT s.name\n" +
                "FROM supplier s\n" +
                "WHERE s.suppkey IN (\n" +
                "    SELECT l.suppkey\n" +
                "    FROM lineitem l\n" +
                ")";
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
        sql = "SELECT c.name FROM customer c WHERE c.custkey IN ( SELECT o.custkey FROM orders o JOIN lineitem l ON o.orderkey = l.orderkey WHERE l.partkey > 1235 )";
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
        sql = "SELECT p.name\n" +
                "FROM part p\n" +
                "WHERE p.partkey IN (\n" +
                "    SELECT l.partkey\n" +
                "    FROM lineitem l\n" +
                "    JOIN orders o ON l.orderkey = o.orderkey\n" +
                "    JOIN customer c ON o.custkey = c.custkey\n" +
                "    JOIN nation n ON c.nationkey = n.nationkey\n" +
                "    WHERE n.name = 'UNITED STATES'\n" +
                ")";
        assertQueryWithSameQueryRunner(enabled, sql, disabled);
    }

    /**
     * When optimize_hash_generation is enabled, the "hash_code" operator is used for
     * hashing join/group by values. When it is disabled Type.hash() is used.
     * We want to test both code paths.
     */
    @DataProvider(name = "optimize_hash_generation")
    public Object[][] optimizeHashGeneration()
    {
        return new Object[][] {{"true"}, {"false"}};
    }

    @Test(dataProvider = "optimize_hash_generation")
    public void testDoubleJoinPositiveAndNegativeZero(String optimizeHashGeneration)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, optimizeHashGeneration)
                .build();
        assertQuery(session, "WITH t AS ( SELECT * FROM (VALUES(DOUBLE '0.0'), (DOUBLE '-0.0'))_t(x)) SELECT * FROM t t1 JOIN t t2 on t1.x = t2.x",
                "SELECT * FROM (VALUES (0.0, 0.0), (0.0, -0.0), (-0.0, 0.0), (-0.0, -0.0))");
    }

    @Test(dataProvider = "optimize_hash_generation")
    public void testRealJoinPositiveAndNegativeZero(String optimizeHashGeneration)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, optimizeHashGeneration)
                .build();
        assertQuery(session, "WITH t AS ( SELECT * FROM (VALUES(REAL '0.0'), (REAL '-0.0'))_t(x)) SELECT * FROM t t1 JOIN t t2 on t1.x = t2.x",
                "SELECT * FROM (VALUES " +
                        "(CAST (0.0 AS REAL), CAST (0.0 AS REAL)), " +
                        "(CAST (0.0 AS REAL),  CAST(-0.0 AS REAL)), " +
                        "(CAST (-0.0 AS REAL), CAST(0.0 AS REAL)), " +
                        "(CAST (-0.0 AS REAL), CAST(-0.0 AS REAL)))");
    }

    @Test(dataProvider = "optimize_hash_generation")
    public void testDoubleDistinctPositiveAndNegativeZero(String optimizeHashGeneration)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, optimizeHashGeneration)
                .build();
        assertQuery(session, "SELECT DISTINCT x FROM (VALUES (DOUBLE '0.0'), (DOUBLE '-0.0')) t(x)", "SELECT 0.0");
    }

    @Test(dataProvider = "optimize_hash_generation")
    public void testRealDistinctPositiveAndNegativeZero(String optimizeHashGeneration)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, optimizeHashGeneration)
                .build();
        assertQuery(session, "SELECT DISTINCT x FROM (VALUES (REAL '0.0'), (REAL '-0.0')) t(x)", "SELECT  CAST(0.0 AS REAL)");
    }

    @Test
    public void testEvaluateProjectOnValues()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(INLINE_PROJECTIONS_ON_VALUES, "true")
                .build();
        assertQuery(session,
                "SELECT MAP(ARRAY[1,2,3],ARRAY['one','two','three'])[x] from (values 1,2) as t(x)",
                "SELECT * FROM (VALUES 'one','two')");
        assertQuery(session,
                "SELECT CASE WHEN x<y THEN x ELSE y END from (values (1,2),(3,4)) as t(x,y)",
                "SELECT * FROM (VALUES 1,3)");
        assertQuery(session,
                "SELECT MAP(ARRAY[ARRAY[1,1],ARRAY[2,2]],ARRAY[1,2])[x] from (values ARRAY[1,1]) as t(x)",
                "SELECT * FROM (VALUES 1)");
        assertQuery(session,
                "SELECT SUBSTR(Y,1,1) FROM (SELECT SUBSTR(X, 1,2) FROM (VALUES 'abcd', 'efgh') AS T(X)) AS T(Y)",
                "SELECT * FROM (VALUES 'a','e')");
        assertQuery(session,
                "SELECT a * 2, a * 4 from (VALUES 2, 4) t(a)",
                "SELECT * FROM (VALUES (4,8),(8,16))");
        assertQuery(session,
                "SELECT a + 1 FROM (VALUES (1, 2), (3, 4)) t(a, b)",
                "SELECT * FROM (VALUES 2, 4)");
        assertQuery(session,
                "WITH t1 as (SELECT a + 1 as a FROM (VALUES 6) as t(a)) SELECT a * 2, a - 1 FROM t1",
                "SELECT * FROM (VALUES (14, 6))");
        assertQuery(session,
                "SELECT a * 2, a - 1 FROM (SELECT x * 2 as a FROM (VALUES 15) t(x))",
                "SELECT * FROM (VALUES (60, 29))");
    }

    @Test
    public void testMinMaxByToWindowFunction()
    {
        Session enabled = Session.builder(getSession())
                .setSystemProperty(REWRITE_MIN_MAX_BY_TO_TOP_N, "true")
                .build();
        Session disabled = Session.builder(getSession())
                .setSystemProperty(REWRITE_MIN_MAX_BY_TO_TOP_N, "false")
                .build();
        @Language("SQL") String sql = "with t as (SELECT * FROM ( VALUES (3, '2025-01-08', MAP(ARRAY[2, 1], ARRAY[0.34, 0.92])), (1, '2025-01-02', MAP(ARRAY[1, 3], ARRAY[0.23, 0.5])), " +
                "(7, '2025-01-17', MAP(ARRAY[6, 8], ARRAY[0.60, 0.70])), (2, '2025-01-06', MAP(ARRAY[2, 3, 5, 7], ARRAY[0.75, 0.32, 0.19, 0.46])), " +
                "(5, '2025-01-14', MAP(ARRAY[8, 4, 6], ARRAY[0.88, 0.99, 0.00])), (4, '2025-01-12', MAP(ARRAY[7, 3, 2], ARRAY[0.33, 0.44, 0.55])), " +
                "(8, '2025-01-20', MAP(ARRAY[1, 7, 6], ARRAY[0.35, 0.45, 0.55])), (6, '2025-01-16', MAP(ARRAY[9, 1, 3], ARRAY[0.30, 0.40, 0.50])), " +
                "(2, '2025-01-05', MAP(ARRAY[3, 4], ARRAY[0.98, 0.21])), (1, '2025-01-04', MAP(ARRAY[1, 2], ARRAY[0.45, 0.67])), (7, '2025-01-18', MAP(ARRAY[4, 2, 9], ARRAY[0.80, 0.90, 0.10])), " +
                "(3, '2025-01-10', MAP(ARRAY[4, 1, 8, 6], ARRAY[0.85, 0.13, 0.42, 0.91])), (8, '2025-01-19', MAP(ARRAY[3, 5], ARRAY[0.15, 0.25])), " +
                "(4, '2025-01-11', MAP(ARRAY[5, 6], ARRAY[0.11, 0.22])), (5, '2025-01-13', MAP(ARRAY[1, 9], ARRAY[0.66, 0.77])), (6, '2025-01-15', MAP(ARRAY[2, 5], ARRAY[0.10, 0.20])) ) " +
                "t(id, ds, feature)) select id, max_by(feature, ds), max(ds) from t group by id";

        MaterializedResult result = computeActual(enabled, "explain(type distributed) " + sql);
        assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("TopNRowNumber"), -1);

        assertQueryWithSameQueryRunner(enabled, sql, disabled);

        sql = "with t as (SELECT * FROM ( VALUES (3, '2025-01-08', MAP(ARRAY[2, 1], ARRAY[0.34, 0.92]), MAP(ARRAY['a', 'b'], ARRAY[0.12, 0.88])), " +
                "(1, '2025-01-02', MAP(ARRAY[1, 3], ARRAY[0.23, 0.5]), MAP(ARRAY['x', 'y'], ARRAY[0.45, 0.55])), (7, '2025-01-17', MAP(ARRAY[6, 8], ARRAY[0.60, 0.70]), MAP(ARRAY['m', 'n'], ARRAY[0.21, 0.79])), " +
                "(2, '2025-01-06', MAP(ARRAY[2, 3, 5, 7], ARRAY[0.75, 0.32, 0.19, 0.46]), MAP(ARRAY['p', 'q', 'r'], ARRAY[0.11, 0.22, 0.67])), (5, '2025-01-14', MAP(ARRAY[8, 4, 6], ARRAY[0.88, 0.99, 0.00]), MAP(ARRAY['s', 't', 'u'], ARRAY[0.33, 0.44, 0.23])), " +
                "(4, '2025-01-12', MAP(ARRAY[7, 3, 2], ARRAY[0.33, 0.44, 0.55]), MAP(ARRAY['v', 'w'], ARRAY[0.66, 0.34])), (8, '2025-01-20', MAP(ARRAY[1, 7, 6], ARRAY[0.35, 0.45, 0.55]), MAP(ARRAY['i', 'j', 'k'], ARRAY[0.78, 0.89, 0.12])), " +
                "(6, '2025-01-16', MAP(ARRAY[9, 1, 3], ARRAY[0.30, 0.40, 0.50]), MAP(ARRAY['c', 'd'], ARRAY[0.90, 0.10])), (2, '2025-01-05', MAP(ARRAY[3, 4], ARRAY[0.98, 0.21]), MAP(ARRAY['e', 'f'], ARRAY[0.56, 0.44])), " +
                "(1, '2025-01-04', MAP(ARRAY[1, 2], ARRAY[0.45, 0.67]), MAP(ARRAY['g', 'h'], ARRAY[0.23, 0.77])) ) t(id, ds, feature, extra_feature)) " +
                "select id, max(ds), max_by(feature, ds), max_by(extra_feature, ds) from t group by id";

        result = computeActual(enabled, "explain(type distributed) " + sql);
        assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("TopNRowNumber"), -1);

        assertQueryWithSameQueryRunner(enabled, sql, disabled);

        sql = "with t as (SELECT * FROM ( VALUES (3, '2025-01-08', MAP(ARRAY[2, 1], ARRAY[0.34, 0.92])), (1, '2025-01-02', MAP(ARRAY[1, 3], ARRAY[0.23, 0.5])), " +
                "(7, '2025-01-17', MAP(ARRAY[6, 8], ARRAY[0.60, 0.70])), (2, '2025-01-06', MAP(ARRAY[2, 3, 5, 7], ARRAY[0.75, 0.32, 0.19, 0.46])), " +
                "(5, '2025-01-14', MAP(ARRAY[8, 4, 6], ARRAY[0.88, 0.99, 0.00])), (4, '2025-01-12', MAP(ARRAY[7, 3, 2], ARRAY[0.33, 0.44, 0.55])), " +
                "(8, '2025-01-20', MAP(ARRAY[1, 7, 6], ARRAY[0.35, 0.45, 0.55])), (6, '2025-01-16', MAP(ARRAY[9, 1, 3], ARRAY[0.30, 0.40, 0.50])), " +
                "(2, '2025-01-05', MAP(ARRAY[3, 4], ARRAY[0.98, 0.21])), (1, '2025-01-04', MAP(ARRAY[1, 2], ARRAY[0.45, 0.67])), (7, '2025-01-18', MAP(ARRAY[4, 2, 9], ARRAY[0.80, 0.90, 0.10])), " +
                "(3, '2025-01-10', MAP(ARRAY[4, 1, 8, 6], ARRAY[0.85, 0.13, 0.42, 0.91])), (8, '2025-01-19', MAP(ARRAY[3, 5], ARRAY[0.15, 0.25])), " +
                "(4, '2025-01-11', MAP(ARRAY[5, 6], ARRAY[0.11, 0.22])), (5, '2025-01-13', MAP(ARRAY[1, 9], ARRAY[0.66, 0.77])), (6, '2025-01-15', MAP(ARRAY[2, 5], ARRAY[0.10, 0.20])) ) " +
                "t(id, ds, feature)) select id, min_by(feature, ds), min(ds) from t group by id";

        result = computeActual(enabled, "explain(type distributed) " + sql);
        assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("TopNRowNumber"), -1);

        assertQueryWithSameQueryRunner(enabled, sql, disabled);

        sql = "with t as (SELECT * FROM ( VALUES (3, '2025-01-08', MAP(ARRAY[2, 1], ARRAY[0.34, 0.92]), MAP(ARRAY['a', 'b'], ARRAY[0.12, 0.88])), " +
                "(1, '2025-01-02', MAP(ARRAY[1, 3], ARRAY[0.23, 0.5]), MAP(ARRAY['x', 'y'], ARRAY[0.45, 0.55])), (7, '2025-01-17', MAP(ARRAY[6, 8], ARRAY[0.60, 0.70]), MAP(ARRAY['m', 'n'], ARRAY[0.21, 0.79])), " +
                "(2, '2025-01-06', MAP(ARRAY[2, 3, 5, 7], ARRAY[0.75, 0.32, 0.19, 0.46]), MAP(ARRAY['p', 'q', 'r'], ARRAY[0.11, 0.22, 0.67])), (5, '2025-01-14', MAP(ARRAY[8, 4, 6], ARRAY[0.88, 0.99, 0.00]), MAP(ARRAY['s', 't', 'u'], ARRAY[0.33, 0.44, 0.23])), " +
                "(4, '2025-01-12', MAP(ARRAY[7, 3, 2], ARRAY[0.33, 0.44, 0.55]), MAP(ARRAY['v', 'w'], ARRAY[0.66, 0.34])), (8, '2025-01-20', MAP(ARRAY[1, 7, 6], ARRAY[0.35, 0.45, 0.55]), MAP(ARRAY['i', 'j', 'k'], ARRAY[0.78, 0.89, 0.12])), " +
                "(6, '2025-01-16', MAP(ARRAY[9, 1, 3], ARRAY[0.30, 0.40, 0.50]), MAP(ARRAY['c', 'd'], ARRAY[0.90, 0.10])), (2, '2025-01-05', MAP(ARRAY[3, 4], ARRAY[0.98, 0.21]), MAP(ARRAY['e', 'f'], ARRAY[0.56, 0.44])), " +
                "(1, '2025-01-04', MAP(ARRAY[1, 2], ARRAY[0.45, 0.67]), MAP(ARRAY['g', 'h'], ARRAY[0.23, 0.77])) ) t(id, ds, feature, extra_feature)) " +
                "select id, min(ds), min_by(feature, ds), min_by(extra_feature, ds) from t group by id";

        result = computeActual(enabled, "explain(type distributed) " + sql);
        assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("TopNRowNumber"), -1);

        assertQueryWithSameQueryRunner(enabled, sql, disabled);

        sql = "with t as (SELECT * FROM ( VALUES (3, 100, '2025-01-08', MAP(ARRAY[2, 1], ARRAY[0.34, 0.92]), MAP(ARRAY['a', 'b'], ARRAY[0.12, 0.88])), " +
                "(1, 20, '2025-01-02', MAP(ARRAY[1, 3], ARRAY[0.23, 0.5]), MAP(ARRAY['x', 'y'], ARRAY[0.45, 0.55])), (7, 90, '2025-01-17', MAP(ARRAY[6, 8], ARRAY[0.60, 0.70]), MAP(ARRAY['m', 'n'], ARRAY[0.21, 0.79])), " +
                "(2, 10, '2025-01-06', MAP(ARRAY[2, 3, 5, 7], ARRAY[0.75, 0.32, 0.19, 0.46]), MAP(ARRAY['p', 'q', 'r'], ARRAY[0.11, 0.22, 0.67])), (5, 65, '2025-01-14', MAP(ARRAY[8, 4, 6], ARRAY[0.88, 0.99, 0.00]), MAP(ARRAY['s', 't', 'u'], ARRAY[0.33, 0.44, 0.23])), " +
                "(4, 40, '2025-01-12', MAP(ARRAY[7, 3, 2], ARRAY[0.33, 0.44, 0.55]), MAP(ARRAY['v', 'w'], ARRAY[0.66, 0.34])), (8, 68, '2025-01-20', MAP(ARRAY[1, 7, 6], ARRAY[0.35, 0.45, 0.55]), MAP(ARRAY['i', 'j', 'k'], ARRAY[0.78, 0.89, 0.12])), " +
                "(6, 101, '2025-01-16', MAP(ARRAY[9, 1, 3], ARRAY[0.30, 0.40, 0.50]), MAP(ARRAY['c', 'd'], ARRAY[0.90, 0.10])), (2, 35, '2025-01-05', MAP(ARRAY[3, 4], ARRAY[0.98, 0.21]), MAP(ARRAY['e', 'f'], ARRAY[0.56, 0.44])), " +
                "(1, 25, '2025-01-04', MAP(ARRAY[1, 2], ARRAY[0.45, 0.67]), MAP(ARRAY['g', 'h'], ARRAY[0.23, 0.77])) ) t(id, key, ds, feature, extra_feature)) " +
                "select id, min(ds), min_by(feature, ds), min_by(extra_feature, ds), min_by(key, ds) from t group by id";

        result = computeActual(enabled, "explain(type distributed) " + sql);
        assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("TopNRowNumber"), -1);

        assertQueryWithSameQueryRunner(enabled, sql, disabled);
    }

    private List<MaterializedRow> getNativeWorkerSessionProperties(List<MaterializedRow> inputRows, String sessionPropertyName)
    {
        return inputRows.stream()
                .filter(row -> Pattern.matches(sessionPropertyName, row.getFields().get(4).toString()))
                .collect(toList());
    }

    /**
     * Returns a date expression, casting to DATE if storageFormat is DWRF.
     */
    protected String getDateExpression(String storageFormat, String columnExpression)
    {
        // DWRF does not support date type.
        return storageFormat.equals("DWRF") ? "cast(" + columnExpression + " as DATE)" : columnExpression;
    }
}
