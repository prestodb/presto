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

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.operator.scalar.TestingRowConstructor;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.util.DateTimeZoneIndex;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import io.airlift.tpch.TpchTable;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.tree.ExplainType.Type.DISTRIBUTED;
import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.tableNameGetter;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestQueries
        extends AbstractTestQueryFramework
{
    // We can just use the default type registry, since we don't use any parametric types
    protected static final List<ParametricFunction> CUSTOM_FUNCTIONS = new FunctionListBuilder(new TypeRegistry())
            .aggregate(CustomSum.class)
            .window("custom_rank", BIGINT, ImmutableList.<Type>of(), CustomRank.class)
            .scalar(CustomAdd.class)
            .scalar(CreateHll.class)
            .scalar(TestingRowConstructor.class)
            .getFunctions();

    protected AbstractTestQueries(QueryRunner queryRunner)
    {
        super(queryRunner);
    }

    @Test
    public void selectNull()
            throws Exception
    {
        assertQuery("SELECT NULL", "SELECT NULL FROM (SELECT * FROM ORDERS LIMIT 1)");
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
    public void testVarbinary()
            throws Exception
    {
        assertQuery("SELECT LENGTH(x) FROM (SELECT from_base64('gw==') as x)", "SELECT 1");
        assertQuery("SELECT LENGTH(from_base64('gw=='))", "SELECT 1");
    }

    @Test
    public void testRowFieldAccessor()
            throws Exception
    {
        assertQuery("SELECT a.col0 FROM (VALUES ROW (test_row(1, 2))) AS t (a)", "SELECT 1");
        assertQuery("SELECT a.col0 FROM (VALUES ROW (test_row(1.0, 2.0))) AS t (a)", "SELECT 1.0");
        assertQuery("SELECT a.col0 FROM (VALUES ROW (test_row(TRUE, FALSE))) AS t (a)", "SELECT TRUE");
        assertQuery("SELECT a.col1 FROM (VALUES ROW (test_row(1.0, 'kittens'))) AS t (a)", "SELECT 'kittens'");
    }

    @Test
    public void testUnnest()
            throws Exception
    {
        assertQuery("SELECT 1 FROM (VALUES (ARRAY[1])) AS t (a) CROSS JOIN UNNEST(a)", "SELECT 1");
        assertQuery("SELECT x[1] FROM UNNEST(ARRAY[ARRAY[2, 2, 3]]) t(x)", "SELECT 2");
        assertQuery("SELECT x[1][2] FROM UNNEST(ARRAY[ARRAY[ARRAY[2, 2, 3]]]) t(x)", "SELECT 2");
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
    }

    @Test
    public void testArrays()
            throws Exception
    {
        assertQuery("SELECT a[1] FROM (SELECT ARRAY[orderkey] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey FROM orders");
        assertQuery("SELECT a[1 + cast(round(rand()) AS BIGINT)] FROM (SELECT ARRAY[orderkey, orderkey] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey FROM orders");
        assertQuery("SELECT a[1] + 1 FROM (SELECT ARRAY[orderkey] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey + 1 FROM orders");
        assertQuery("SELECT a[1] FROM (SELECT ARRAY[orderkey + 1] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey + 1 FROM orders");
        assertQuery("SELECT a[1][1] FROM (SELECT ARRAY[ARRAY[orderkey + 1]] AS a FROM orders ORDER BY orderkey) t", "SELECT orderkey + 1 FROM orders");
        assertQuery("SELECT CARDINALITY(a) FROM (SELECT ARRAY[orderkey, orderkey + 1] AS a FROM orders ORDER BY orderkey) t", "SELECT 2 FROM orders");
    }

    @Test
    public void testMaps()
            throws Exception
    {
        assertQuery("SELECT m[max_key] FROM (SELECT map_agg(orderkey, orderkey) m, max(orderkey) max_key FROM orders)", "SELECT max(orderkey) FROM orders");
    }

    @Test
    public void testValues()
            throws Exception
    {
        assertQuery("VALUES 1, 2, 3, 4");
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
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT nan(), infinity(), -infinity()");
        MaterializedRow row = Iterables.getOnlyElement(actual.getMaterializedRows());
        assertEquals(row.getField(0), Double.NaN);
        assertEquals(row.getField(1), Double.POSITIVE_INFINITY);
        assertEquals(row.getField(2), Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testMaxMinStringWithNulls()
            throws Exception
    {
        assertQuery("SELECT custkey, MAX(NULLIF(orderstatus, 'O')), MIN(NULLIF(orderstatus, 'O')) FROM orders GROUP BY custkey");
    }

    @Test
    public void testGroupByPartitioningColumn()
            throws Exception
    {
        assertQuery("SELECT orderkey, count(*) FROM orders GROUP BY orderkey");
    }

    @Test
    public void testJoinPartitionedTable()
            throws Exception
    {
        assertQuery("SELECT * FROM orders a JOIN orders b ON a.orderkey = b.orderkey");
    }

    @Test
    public void testApproxPercentile()
            throws Exception
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
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT sum(orderkey), row_number() OVER (ORDER BY orderkey)\n" +
                "FROM orders\n" +
                "WHERE orderkey <= 10\n" +
                "GROUP BY orderkey\n" +
                "HAVING sum(orderkey) >= 3\n" +
                "ORDER BY orderkey DESC\n" +
                "LIMIT 3");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(7, 5)
                .row(6, 4)
                .row(5, 3)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testWhereNull()
            throws Exception
    {
        // This query is has this strange shape to force the compiler to leave a true on the stack
        // with the null flag set so if the filter method is not handling nulls correctly, this
        // query will fail
        assertQuery("SELECT custkey FROM orders WHERE custkey = custkey AND cast(nullif(custkey, custkey) as boolean) AND cast(nullif(custkey, custkey) as boolean)");
    }

    @Test
    public void testSumOfNulls()
            throws Exception
    {
        assertQuery("SELECT orderstatus, sum(CAST(NULL AS BIGINT)) FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testApproximateCountDistinct()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT approx_distinct(custkey) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(996)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproximateCountDistinctWithStandardError()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT approx_distinct(custkey, 0.023) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(996)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproximateCountDistinctGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderstatus, approx_distinct(custkey) FROM orders GROUP BY orderstatus");
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 995)
                .row("F", 993)
                .row("P", 303)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproximateCountDistinctGroupByWithStandardError()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderstatus, approx_distinct(custkey, 0.023) FROM orders GROUP BY orderstatus");
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 995)
                .row("F", 993)
                .row("P", 303)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testCountBoolean()
            throws Exception
    {
        assertQuery("SELECT COUNT(true) FROM orders");
    }

    @Test
    public void testJoinWithMultiFieldGroupBy()
            throws Exception
    {
        assertQuery("SELECT orderstatus FROM lineitem JOIN (SELECT DISTINCT orderkey, orderstatus FROM ORDERS) T on lineitem.orderkey = T.orderkey");
    }

    @Test
    public void testGroupByRepeatedField()
            throws Exception
    {
        assertQuery("SELECT sum(custkey) FROM orders GROUP BY orderstatus, orderstatus");
    }

    @Test
    public void testGroupByRepeatedField2()
            throws Exception
    {
        assertQuery("SELECT count(*) FROM (select orderstatus a, orderstatus b FROM orders) GROUP BY a, b");
    }

    @Test
    public void testGroupByMultipleFieldsWithPredicateOnAggregationArgument()
            throws Exception
    {
        assertQuery("SELECT custkey, orderstatus, MAX(orderkey) FROM ORDERS WHERE orderkey = 1 GROUP BY custkey, orderstatus");
    }

    @Test
    public void testReorderOutputsOfGroupByAggregation()
            throws Exception
    {
        assertQuery(
                "SELECT orderstatus, a, custkey, b FROM (SELECT custkey, orderstatus, -COUNT(*) a, MAX(orderkey) b FROM ORDERS WHERE orderkey = 1 GROUP BY custkey, orderstatus) T");
    }

    @Test
    public void testGroupAggregationOverNestedGroupByAggregation()
            throws Exception
    {
        assertQuery("SELECT sum(custkey), max(orderstatus), min(c) FROM (SELECT orderstatus, custkey, COUNT(*) c FROM ORDERS GROUP BY orderstatus, custkey) T");
    }

    @Test
    public void test15WayGroupBy()
            throws Exception
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
            throws Exception
    {
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM ORDERS");
    }

    @Test
    public void testDistinctJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT b.quantity), a.orderstatus " +
                "FROM orders a " +
                "JOIN lineitem b " +
                "ON a.orderkey = b.orderkey " +
                "GROUP BY a.orderstatus");
    }

    @Test
    public void testArithmeticNegation()
            throws Exception
    {
        assertQuery("SELECT -custkey FROM orders");
    }

    @Test
    public void testDistinct()
            throws Exception
    {
        assertQuery("SELECT DISTINCT custkey FROM orders");
    }

    @Test
    public void testDistinctGroupBy()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count, orderdate");
    }

    @Test
    public void testSingleDistinctOptimizer()
            throws Exception
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
    public void testDistinctHaving()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) AS count " +
                "FROM orders " +
                "GROUP BY orderdate " +
                "HAVING COUNT(DISTINCT clerk) > 1");
    }

    @Test
    public void testDistinctWindow()
            throws Exception
    {
        MaterializedResult actual = computeActual(
                "SELECT RANK() OVER (PARTITION BY orderdate ORDER BY COUNT(DISTINCT clerk)) rnk " +
                "FROM orders " +
                "GROUP BY orderdate, custkey " +
                "ORDER BY rnk " +
                "LIMIT 1");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT).row(1).build();
        assertEquals(actual, expected);
    }

    @Test
    public void testDistinctWhere()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT clerk) FROM orders WHERE LENGTH(clerk) > 5");
    }

    @Test
    public void testMultipleDifferentDistinct()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT orderstatus), SUM(DISTINCT custkey) FROM orders");
    }

    @Test
    public void testMultipleDistinct()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(DISTINCT custkey), SUM(DISTINCT custkey) FROM orders",
                "SELECT COUNT(*), SUM(custkey) FROM (SELECT DISTINCT custkey FROM orders) t");
    }

    @Test
    public void testComplexDistinct()
            throws Exception
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
            throws Exception
    {
        assertQuery("" +
                "SELECT DISTINCT orderstatus, custkey " +
                "FROM (SELECT orderstatus, custkey FROM orders ORDER BY orderkey LIMIT 10) " +
                "LIMIT 10");
        assertQuery("SELECT COUNT(*) FROM (SELECT DISTINCT orderstatus, custkey FROM orders LIMIT 10)");
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM orders WHERE custkey = 1268 LIMIT 2");
    }

    @Test
    public void testCountDistinct()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT custkey + 1) FROM orders", "SELECT COUNT(*) FROM (SELECT DISTINCT custkey + 1 FROM orders) t");
    }

    @Test
    public void testDistinctWithOrderBy()
            throws Exception
    {
        assertQueryOrdered("SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 10");
    }

    @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = "For SELECT DISTINCT, ORDER BY expressions must appear in select list")
    public void testDistinctWithOrderByNotInSelect()
            throws Exception
    {
        assertQueryOrdered("SELECT DISTINCT custkey FROM orders ORDER BY orderkey LIMIT 10");
    }

    @Test
    public void testOrderByLimit()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey DESC LIMIT 10");
    }

    @Test
    public void testOrderByExpressionWithLimit()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM ORDERS ORDER BY orderkey + 1 DESC LIMIT 10");
    }

    @Test
    public void testGroupByOrderByLimit()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey ORDER BY SUM(totalprice) DESC LIMIT 10");
    }

    @Test
    public void testLimitZero()
            throws Exception
    {
        assertQuery("SELECT custkey, totalprice FROM orders LIMIT 0");
    }

    @Test
    public void testOrderByLimitZero()
            throws Exception
    {
        assertQuery("SELECT custkey, totalprice FROM orders ORDER BY orderkey LIMIT 0");
    }

    @Test
    public void testRepeatedAggregations()
            throws Exception
    {
        assertQuery("SELECT SUM(orderkey), SUM(orderkey) FROM ORDERS");
    }

    @Test
    public void testRepeatedOutputs()
            throws Exception
    {
        assertQuery("SELECT orderkey a, orderkey b FROM ORDERS WHERE orderstatus = 'F'");
    }

    @Test
    public void testRepeatedOutputs2()
            throws Exception
    {
        // this test exposed a bug that wasn't caught by other tests that resulted in the execution engine
        // trying to read orderkey as the second field, causing a type mismatch
        assertQuery("SELECT orderdate, orderdate, orderkey FROM orders");
    }

    @Test
    public void testLimit()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderkey FROM ORDERS LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertTrue(all.getMaterializedRows().containsAll(actual.getMaterializedRows()));
    }

    @Test
    public void testAggregationWithLimit()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey LIMIT 10");
        MaterializedResult all = computeExpected("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertTrue(all.getMaterializedRows().containsAll(actual.getMaterializedRows()));
    }

    @Test
    public void testLimitInInlineView()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderkey FROM (SELECT orderkey FROM ORDERS LIMIT 100) T LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertTrue(all.getMaterializedRows().containsAll(actual.getMaterializedRows()));
    }

    @Test
    public void testCountAll()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM ORDERS");
        assertQuery("SELECT COUNT(42) FROM ORDERS", "SELECT COUNT(*) FROM ORDERS");
        assertQuery("SELECT COUNT(42 + 42) FROM ORDERS", "SELECT COUNT(*) FROM ORDERS");
        assertQuery("SELECT COUNT(null) FROM ORDERS", "SELECT 0");
    }

    @Test
    public void testCountColumn()
            throws Exception
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
            throws Exception
    {
        assertQuery("SELECT * FROM ORDERS");
    }

    @Test
    public void testMultipleWildcards()
            throws Exception
    {
        assertQuery("SELECT *, 123, * FROM ORDERS");
    }

    @Test
    public void testMixedWildcards()
            throws Exception
    {
        assertQuery("SELECT *, orders.*, orderkey FROM orders");
    }

    @Test
    public void testQualifiedWildcardFromAlias()
            throws Exception
    {
        assertQuery("SELECT T.* FROM ORDERS T");
    }

    @Test
    public void testQualifiedWildcardFromInlineView()
            throws Exception
    {
        assertQuery("SELECT T.* FROM (SELECT orderkey + custkey FROM ORDERS) T");
    }

    @Test
    public void testQualifiedWildcard()
            throws Exception
    {
        assertQuery("SELECT ORDERS.* FROM ORDERS");
    }

    @Test
    public void testAverageAll()
            throws Exception
    {
        assertQuery("SELECT AVG(totalprice) FROM ORDERS");
    }

    @Test
    public void testVariance()
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM ORDERS WHERE orderstatus = 'F'");
    }

    @Test
    public void testGroupByArray()
            throws Exception
    {
        assertQuery("SELECT col[1], count FROM (SELECT ARRAY[custkey] col, COUNT(*) count FROM ORDERS GROUP BY 1 ORDER BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey ORDER BY custkey");
    }

    @Test
    public void testGroupByMap()
            throws Exception
    {
        assertQuery("SELECT col[1], count FROM (SELECT MAP(ARRAY[1], ARRAY[custkey]) col, COUNT(*) count FROM ORDERS GROUP BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testGroupByRow()
            throws Exception
    {
        assertQuery("SELECT col.col1, count FROM (SELECT test_row(custkey, custkey) col, COUNT(*) count FROM ORDERS GROUP BY 1)", "SELECT custkey, COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testJoinCoercion()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM orders t join (SELECT * FROM orders LIMIT 1) t2 ON sin(t2.custkey) = 0");
    }

    @Test
    public void testGroupByNoAggregations()
            throws Exception
    {
        assertQuery("SELECT custkey FROM ORDERS GROUP BY custkey");
    }

    @Test
    public void testGroupByCount()
            throws Exception
    {
        assertQuery(
                "SELECT orderstatus, COUNT(*) FROM ORDERS GROUP BY orderstatus",
                "SELECT orderstatus, CAST(COUNT(*) AS INTEGER) FROM orders GROUP BY orderstatus"
        );
    }

    @Test
    public void testGroupByMultipleFields()
            throws Exception
    {
        assertQuery("SELECT custkey, orderstatus, COUNT(*) FROM ORDERS GROUP BY custkey, orderstatus");
    }

    @Test
    public void testGroupByWithAlias()
            throws Exception
    {
        assertQuery(
                "SELECT orderdate x, COUNT(*) FROM orders GROUP BY orderdate",
                "SELECT orderdate x, CAST(COUNT(*) AS INTEGER) FROM orders GROUP BY orderdate"
        );
    }

    @Test
    public void testGroupBySum()
            throws Exception
    {
        assertQuery("SELECT orderstatus, SUM(totalprice) FROM ORDERS GROUP BY orderstatus");
    }

    @Test
    public void testGroupByWithWildcard()
            throws Exception
    {
        assertQuery("SELECT * FROM (SELECT orderkey FROM orders) t GROUP BY orderkey");
    }

    @Test
    public void testCountAllWithComparison()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < discount");
    }

    @Test
    public void testSelectWithComparison()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM lineitem WHERE tax < discount");
    }

    @Test
    public void testCountWithNotPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE NOT tax < discount");
    }

    @Test
    public void testCountWithNullPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE NULL");
    }

    @Test
    public void testCountWithIsNullPredicate()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') IS NULL",
                "SELECT COUNT(*) FROM orders WHERE orderstatus = 'F' "
        );
    }

    @Test
    public void testCountWithIsNotNullPredicate()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') IS NOT NULL",
                "SELECT COUNT(*) FROM orders WHERE orderstatus <> 'F' "
        );
    }

    @Test
    public void testCountWithNullIfPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM orders WHERE NULLIF(orderstatus, 'F') = orderstatus ");
    }

    @Test
    public void testCountWithCoalescePredicate()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM orders WHERE COALESCE(NULLIF(orderstatus, 'F'), 'bar') = 'bar'",
                "SELECT COUNT(*) FROM orders WHERE orderstatus = 'F'"
        );
    }

    @Test
    public void testCountWithAndPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < discount AND tax > 0.01 AND discount < 0.05");
    }

    @Test
    public void testCountWithOrPredicate()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE tax < 0.01 OR discount > 0.05");
    }

    @Test
    public void testCountWithInlineView()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT orderkey FROM lineitem) x");
    }

    @Test
    public void testNestedCount()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT orderkey, COUNT(*) FROM lineitem GROUP BY orderkey) x");
    }

    @Test
    public void testAggregationWithProjection()
            throws Exception
    {
        assertQuery("SELECT sum(totalprice * 2) - sum(totalprice) FROM orders");
    }

    @Test
    public void testAggregationWithProjection2()
            throws Exception
    {
        assertQuery("SELECT sum(totalprice * 2) + sum(totalprice * 2) FROM orders");
    }

    @Test
    public void testGroupByOnSupersetOfPartitioning()
            throws Exception
    {
        assertQuery("SELECT orderdate, c, count(*) FROM (SELECT orderdate, count(*) c FROM orders GROUP BY orderdate) GROUP BY orderdate, c");
    }

    @Test
    public void testInlineView()
            throws Exception
    {
        assertQuery("SELECT orderkey, custkey FROM (SELECT orderkey, custkey FROM ORDERS) U");
    }

    @Test
    public void testAliasedInInlineView()
            throws Exception
    {
        assertQuery("SELECT x, y FROM (SELECT orderkey x, custkey y FROM ORDERS) U");
    }

    @Test
    public void testInlineViewWithProjections()
            throws Exception
    {
        assertQuery("SELECT x + 1, y FROM (SELECT orderkey * 10 x, custkey y FROM ORDERS) u");
    }

    @Test
    public void testGroupByWithoutAggregation()
            throws Exception
    {
        assertQuery("SELECT orderstatus FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testNestedGroupByWithSameKey()
            throws Exception
    {
        assertQuery("SELECT custkey, sum(t) FROM (SELECT custkey, count(*) t FROM orders GROUP BY custkey) GROUP BY custkey");
    }
    @Test
    public void testGroupByWithNulls()
            throws Exception
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
            throws Exception
    {
        assertQuery("SELECT lines, COUNT(*) FROM (SELECT orderkey, COUNT(*) lines FROM lineitem GROUP BY orderkey) U GROUP BY lines");
    }

    @Test
    public void testSimpleJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = 2");
    }

    @Test
    public void testJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.orderkey = 2");
    }

    @Test
    public void testSimpleJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2");
    }

    @Test
    public void testJoinDoubleClauseWithLeftOverlap()
            throws Exception
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testJoinDoubleClauseWithRightOverlap()
            throws Exception
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey");
    }

    @Test
    public void testJoinWithAlias()
            throws Exception
    {
        assertQuery("SELECT * FROM (lineitem JOIN orders ON lineitem.orderkey = orders.orderkey) x");
    }

    @Test
    public void testJoinWithConstantExpression()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND 123 = 123");
    }

    @Test
    public void testJoinUsing()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM lineitem join orders using (orderkey)",
                "SELECT COUNT(*) FROM lineitem join orders on lineitem.orderkey = orders.orderkey"
        );
    }

    @Test
    public void testJoinWithReversedComparison()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.orderkey = lineitem.orderkey");
    }

    @Test
    public void testJoinWithComplexExpressions()
            throws Exception
    {
        assertQuery("SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = CAST(orders.orderkey AS BIGINT)");
    }

    @Test
    public void testJoinWithComplexExpressions2()
            throws Exception
    {
        assertQuery(
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = CASE WHEN orders.custkey = 1 and orders.orderstatus = 'F' THEN orders.orderkey ELSE NULL END");
    }

    @Test
    public void testJoinWithComplexExpressions3()
            throws Exception
    {
        assertQuery(
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey + 1 = orders.orderkey + 1",
                "SELECT SUM(custkey) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey "
                // H2 takes a million years because it can't join efficiently on a non-indexed field/expression
        );
    }

    @Test
    public void testSelfJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM orders a JOIN orders b on a.orderkey = b.orderkey");
    }

    @Test
    public void testWildcardFromJoin()
            throws Exception
    {
        assertQuery(
                "SELECT * FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b using (orderkey)",
                "SELECT * FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b on a.orderkey = b.orderkey"
        );
    }

    @Test
    public void testQualifiedWildcardFromJoin()
            throws Exception
    {
        assertQuery(
                "SELECT a.*, b.* FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b using (orderkey)",
                "SELECT a.*, b.* FROM (select orderkey, partkey from lineitem) a join (select orderkey, custkey from orders) b on a.orderkey = b.orderkey"
        );
    }

    @Test
    public void testJoinAggregations()
            throws Exception
    {
        assertQuery(
                "SELECT x + y FROM (" +
                        "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN (" +
                        "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b ON a.orderdate = b.orderdate");
    }

    @Test
    public void testNonEqualityJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity <= 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity != 2");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate > orders.orderdate");
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderdate < lineitem.shipdate");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Non-equi.*")
    public void testNonEqualityLeftJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 2");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Non-equi.*")
    public void testNonEqualityRightJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 2");
    }

    @Test
    public void testJoinOnMultipleFields()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate = orders.orderdate");
    }

    @Test
    public void testJoinUsingMultipleFields()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM lineitem JOIN (SELECT orderkey, orderdate shipdate FROM ORDERS) T USING (orderkey, shipdate)",
                "SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.shipdate = orders.orderdate"
        );
    }

    @Test
    public void testJoinWithNonJoinExpression()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey = 1");
    }

    @Test
    public void testJoinWithNullValues()
            throws Exception
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
    public void testLeftFilteredJoin()
            throws Exception
    {
        // Test predicate move around
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testRightFilteredJoin()
            throws Exception
    {
        // Test predicate move around
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM lineitem JOIN (SELECT *  FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testJoinWithFullyPushedDownJoinClause()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON orders.custkey = 1 AND lineitem.orderkey = 1");
    }

    @Test
    public void testJoinPredicateMoveAround()
            throws Exception
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8 AND lineitem.linenumber % 2 = 0\n" +
                "WHERE orders.custkey % 8 < 7 AND orders.custkey % 8 = lineitem.orderkey % 8 AND lineitem.suppkey % 7 > orders.custkey % 7");
    }

    @Test
    public void testSimpleFullJoin()
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
    public void testSimpleLeftJoin()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey");
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testLeftJoinNormalizedToInner()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey WHERE orders.orderkey IS NOT NULL");
    }

    @Test
    public void testLeftJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testLeftJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN orders ON orders.orderkey = 1024");
    }

    @Test
    public void testSimpleLeftJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleLeftJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2");
    }

    @Test
    public void testDoubleFilteredLeftJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredLeftJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testLeftJoinDoubleClauseWithLeftOverlap()
            throws Exception
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testLeftJoinDoubleClauseWithRightOverlap()
            throws Exception
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey");
    }

    @Test
    public void testBuildFilteredLeftJoin()
            throws Exception
    {
        assertQuery("SELECT * FROM lineitem LEFT JOIN (SELECT * FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testProbeFilteredLeftJoin()
            throws Exception
    {
        assertQuery("SELECT * FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a LEFT JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testLeftJoinPredicateMoveAround()
            throws Exception
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "LEFT JOIN (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8\n" +
                "WHERE (orders.custkey % 8 < 7 OR orders.custkey % 8 IS NULL) AND orders.custkey % 8 = lineitem.orderkey % 8");
    }

    @Test
    public void testLeftJoinEqualityInference()
            throws Exception
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
            throws Exception
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
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey");

        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.custkey");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testRightJoinNormalizedToInner()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey WHERE lineitem.orderkey IS NOT NULL");
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.custkey WHERE lineitem.orderkey IS NOT NULL");
    }

    @Test
    public void testRightJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testRightJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredRightJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON orders.orderkey = 1024");
    }

    @Test
    public void testDoubleFilteredRightJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM (SELECT * FROM lineitem WHERE orderkey % 1024 = 0) lineitem RIGHT JOIN (SELECT * FROM orders WHERE orderkey % 1024 = 0) orders ON lineitem.orderkey = 1024");
    }

    @Test
    public void testSimpleRightJoinWithLeftConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = 2");
    }

    @Test
    public void testSimpleRightJoinWithRightConstantEquality()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = 2");
    }

    @Test
    public void testRightJoinDoubleClauseWithLeftOverlap()
            throws Exception
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.orderkey = orders.custkey");
    }

    @Test
    public void testRightJoinDoubleClauseWithRightOverlap()
            throws Exception
    {
        // Checks to make sure that we properly handle duplicate field references in join clauses
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.orderkey = lineitem.partkey");
    }

    @Test
    public void testBuildFilteredRightJoin()
            throws Exception
    {
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM (SELECT * FROM lineitem WHERE orderkey % 2 = 0) a RIGHT JOIN orders ON a.orderkey = orders.orderkey");
    }

    @Test
    public void testProbeFilteredRightJoin()
            throws Exception
    {
        assertQuery("SELECT custkey, linestatus, tax, totalprice, orderstatus FROM lineitem RIGHT JOIN (SELECT *  FROM orders WHERE orderkey % 2 = 0) a ON lineitem.orderkey = a.orderkey");
    }

    @Test
    public void testRightJoinPredicateMoveAround()
            throws Exception
    {
        assertQuery("SELECT COUNT(*)\n" +
                "FROM (SELECT * FROM orders WHERE orderkey % 16 = 0 AND custkey % 2 = 0) orders\n" +
                "RIGHT JOIN (SELECT * FROM lineitem WHERE orderkey % 16 = 0 AND partkey % 2 = 0) lineitem\n" +
                "ON lineitem.orderkey % 8 = orders.orderkey % 8\n" +
                "WHERE (orders.custkey % 8 < 7 OR orders.custkey % 8 IS NULL) AND orders.custkey % 8 = lineitem.orderkey % 8");
    }

    @Test
    public void testRightJoinEqualityInference()
            throws Exception
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
            throws Exception
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
    public void testOrderBy()
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus FROM orders ORDER BY orderstatus");
    }

    @Test
    public void testOrderBy2()
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus FROM orders ORDER BY orderkey DESC");
    }

    @Test
    public void testOrderByMultipleFields()
            throws Exception
    {
        assertQueryOrdered("SELECT custkey, orderstatus FROM orders ORDER BY custkey DESC, orderstatus");
    }

    @Test
    public void testOrderByWithNulls()
            throws Exception
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
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus x FROM orders ORDER BY x ASC");
    }

    @Test
    public void testOrderByAliasWithSameNameAsUnselectedColumn()
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus orderdate FROM orders ORDER BY orderdate ASC");
    }

    @Test
    public void testOrderByOrdinal()
            throws Exception
    {
        assertQueryOrdered("SELECT orderstatus, orderdate FROM orders ORDER BY 2, 1");
    }

    @Test
    public void testOrderByOrdinalWithWildcard()
            throws Exception
    {
        assertQueryOrdered("SELECT * FROM orders ORDER BY 1");
    }

    @Test
    public void testGroupByOrdinal()
            throws Exception
    {
        assertQuery(
                "SELECT orderstatus, sum(totalprice) FROM orders GROUP BY 1",
                "SELECT orderstatus, sum(totalprice) FROM orders GROUP BY orderstatus");
    }

    @Test
    public void testGroupBySearchedCase()
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
    public void testMaxBy()
            throws Exception
    {
        assertQuery("SELECT MAX_BY(orderkey, totalprice) FROM orders", "SELECT orderkey FROM orders ORDER BY totalprice DESC LIMIT 1");
    }

    @Test
    public void testMinBy()
            throws Exception
    {
        assertQuery("SELECT MIN_BY(orderkey, totalprice) FROM orders", "SELECT orderkey FROM orders ORDER BY totalprice ASC LIMIT 1");
    }

    @Test
    public void testGroupByBetween()
            throws Exception
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
            throws Exception
    {
        assertQuery("SELECT 1.0 / COUNT(*) FROM orders");
        assertQuery("SELECT custkey, 1.0 / COUNT(*) FROM orders GROUP BY custkey");
    }

    @Test
    public void testWindowImplicitCoercion()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderkey, 1.0 / row_number() OVER (ORDER BY orderkey) FROM orders LIMIT 2");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, DOUBLE)
                .row(1, 1.0)
                .row(2, 0.5)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testHaving()
            throws Exception
    {
        assertQuery("SELECT orderstatus, sum(totalprice) FROM orders GROUP BY orderstatus HAVING orderstatus = 'O'");
    }

    @Test
    public void testHaving2()
            throws Exception
    {
        assertQuery("SELECT custkey, sum(orderkey) FROM orders GROUP BY custkey HAVING sum(orderkey) > 400000");
    }

    @Test
    public void testHaving3()
            throws Exception
    {
        assertQuery("SELECT custkey, sum(totalprice) * 2 FROM orders GROUP BY custkey");
        assertQuery("SELECT custkey, avg(totalprice + 5) FROM orders GROUP BY custkey");
        assertQuery("SELECT custkey, sum(totalprice) * 2 FROM orders GROUP BY custkey HAVING avg(totalprice + 5) > 10");
    }

    @Test
    public void testHavingWithoutGroupBy()
            throws Exception
    {
        assertQuery("SELECT sum(orderkey) FROM orders HAVING sum(orderkey) > 400000");
    }

    @Test
    public void testGroupByAsJoinProbe()
            throws Exception
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
            throws Exception
    {
        assertQuery("" +
                "SELECT * FROM orders a " +
                "   JOIN (SELECT * FROM orders WHERE orderkey IS NULL) b " +
                "   ON a.orderkey = b.orderkey");
    }

    @Test
    public void testColumnAliases()
            throws Exception
    {
        assertQuery(
                "SELECT x, T.y, z + 1 FROM (SELECT custkey, orderstatus, totalprice FROM orders) T (x, y, z)",
                "SELECT custkey, orderstatus, totalprice + 1 FROM orders");
    }

    @Test
    public void testSameInputToAggregates()
            throws Exception
    {
        assertQuery("SELECT max(a), max(b) FROM (SELECT custkey a, custkey b FROM orders) x");
    }

    @Test
    public void testWindowFunctionWithImplicitCoercion()
            throws Exception
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
                .row(1, "O", (1 * 10) + 100)
                .row(2, "O", (2 * 9) + 100)
                .row(3, "F", (3 * 8) + 100)
                .row(4, "O", (4 * 7) + 100)
                .row(5, "F", (5 * 6) + 100)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testWindowFunctionsFromAggregate()
            throws Exception
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
                .row("F", "Clerk#000000090", 2784836.61, 1)
                .row("F", "Clerk#000000084", 2674447.15, 2)
                .row("O", "Clerk#000000500", 2569878.29, 1)
                .row("O", "Clerk#000000050", 2500162.92, 2)
                .row("P", "Clerk#000000071", 841820.99, 1)
                .row("P", "Clerk#000001000", 643679.49, 2)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testOrderByWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, row_number() OVER (ORDER BY orderkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY 2 DESC\n" +
                "LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(34, 10)
                .row(33, 9)
                .row(32, 8)
                .row(7, 7)
                .row(6, 6)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testRowNumberNoOptimization()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE NOT rn <= 10\n");
        MaterializedResult all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());
        assertEquals(actual.getMaterializedRows().size(), all.getMaterializedRows().size() - 10);
        assertTrue(all.getMaterializedRows().containsAll(actual.getMaterializedRows()));

        actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn - 5 <= 10\n");
        all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());
        assertEquals(actual.getMaterializedRows().size(), 15);
        assertTrue(all.getMaterializedRows().containsAll(actual.getMaterializedRows()));
    }

    @Test
    public void testRowNumberUnpartitionedLimit()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER () rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 5\n");
        MaterializedResult all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 5);
        assertTrue(all.getMaterializedRows().containsAll(actual.getMaterializedRows()));
    }

    @Test
    public void testRowNumberPartitionedLimit()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, orderstatus FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 5\n");
        MaterializedResult all = computeExpected("SELECT orderkey, orderstatus FROM ORDERS", actual.getTypes());

        // there are 3 distinct orderstatus, so expect 15 rows.
        assertEquals(actual.getMaterializedRows().size(), 15);
        assertTrue(all.getMaterializedRows().containsAll(actual.getMaterializedRows()));

        // Test for unreferenced outputs
        actual = computeActual("" +
                "SELECT orderkey FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus) rn, orderkey\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 5\n");
        all = computeExpected("SELECT orderkey FROM ORDERS", actual.getTypes());

        // there are 3 distinct orderstatus, so expect 15 rows.
        assertEquals(actual.getMaterializedRows().size(), 15);
        assertTrue(all.getMaterializedRows().containsAll(actual.getMaterializedRows()));
    }

    @Test(timeOut = 60000)
    public void testRowNumberWithLimit() throws Exception
    {
        assertQuery("SELECT row_number() over() as rn FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey where orders.orderkey = 10000 limit 20");
    }

    @Test
    public void testTopNUnpartitionedWindow()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 5\n");
        String sql = "SELECT row_number() OVER (), orderkey, orderstatus FROM orders ORDER BY orderkey LIMIT 5";
        MaterializedResult expected = computeExpected(sql, actual.getTypes());
        assertEquals(actual, expected);
    }

    @Test
    public void testTopNPartitionedWindow()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 2\n");
        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR)
                .row(1, 1, "O")
                .row(2, 2, "O")
                .row(1, 3, "F")
                .row(2, 5, "F")
                .row(1, 65, "P")
                .row(2, 197, "P")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        // Test for unreferenced outputs
        actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderkey\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 2\n");
        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1, 1)
                .row(2, 2)
                .row(1, 3)
                .row(2, 5)
                .row(1, 65)
                .row(2, 197)
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());

        actual = computeActual("" +
                "SELECT * FROM (\n" +
                "   SELECT row_number() OVER (PARTITION BY orderstatus ORDER BY orderkey) rn, orderstatus\n" +
                "   FROM orders\n" +
                ") WHERE rn <= 2\n");
        expected = resultBuilder(getSession(), BIGINT, VARCHAR)
                .row(1, "O")
                .row(2, "O")
                .row(1, "F")
                .row(2, "F")
                .row(1, "P")
                .row(2, "P")
                .build();
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testWindowFunctionWithGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT *, rank() OVER (PARTITION BY x)\n" +
                "FROM (SELECT 'foo' x)\n" +
                "GROUP BY 1");

        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, BIGINT)
                .row("foo", 1)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testPartialPrePartitionedWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, COUNT(*) OVER (PARTITION BY orderkey, custkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1, 1)
                .row(2, 1)
                .row(3, 1)
                .row(4, 1)
                .row(5, 1)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testFullPrePartitionedWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, COUNT(*) OVER (PARTITION BY orderkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1, 1)
                .row(2, 1)
                .row(3, 1)
                .row(4, 1)
                .row(5, 1)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testPartialPreSortedWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, COUNT(*) OVER (ORDER BY orderkey, custkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1, 1)
                .row(2, 2)
                .row(3, 3)
                .row(4, 4)
                .row(5, 5)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testFullPreSortedWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, COUNT(*) OVER (ORDER BY orderkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(1, 1)
                .row(2, 2)
                .row(3, 3)
                .row(4, 4)
                .row(5, 5)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testFullyPartitionedAndPartiallySortedWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, custkey, orderPriority, COUNT(*) OVER (PARTITION BY orderkey ORDER BY custkey, orderPriority)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey, custkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR, BIGINT)
                .row(1, 370, "5-LOW", 1)
                .row(2, 781, "1-URGENT", 1)
                .row(3, 1234, "5-LOW", 1)
                .row(4, 1369, "5-LOW", 1)
                .row(5, 445, "5-LOW", 1)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testFullyPartitionedAndFullySortedWindowFunction()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderkey, custkey, COUNT(*) OVER (PARTITION BY orderkey ORDER BY custkey)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey, custkey LIMIT 10)\n" +
                "ORDER BY orderkey LIMIT 5");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, BIGINT, VARCHAR, BIGINT)
                .row(1, 370, 1)
                .row(2, 781, 1)
                .row(3, 1234, 1)
                .row(4, 1369, 1)
                .row(5, 445, 1)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testOrderByWindowFunctionWithNulls()
            throws Exception
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
                .row(3, 1)
                .row(1, 2)
                .row(2, 3)
                .row(4, 4)
                .row(5, 5)
                .build();

        assertEquals(actual, expected);

        // Nulls last
        actual = computeActual("" +
                "SELECT orderkey, row_number() OVER (ORDER BY nullif(orderkey, 3) NULLS LAST)\n" +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10)\n" +
                "ORDER BY 2 DESC\n" +
                "LIMIT 5");

        expected = resultBuilder(getSession(), BIGINT, BIGINT)
                .row(3, 10)
                .row(34, 9)
                .row(33, 8)
                .row(32, 7)
                .row(7, 6)
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
                .row(1, "O", 1001, 1002)
                .row(2, "O", 1001, 1002)
                .row(3, "F", 1003, 1005)
                .row(4, "O", 1001, 1002)
                .row(5, "F", 1003, 1005)
                .build();

        assertEquals(actual, expected);
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
                .row(1, "O", 1001)
                .row(2, "O", 3007)
                .row(3, "F", 3014)
                .row(4, "O", 4045)
                .row(5, "F", 2008)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testWindowNoChannels()
    {
        MaterializedResult actual = computeActual("SELECT rank() OVER ()\n" +
                "FROM (SELECT * FROM orders LIMIT 10)\n" +
                "LIMIT 3");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT, VARCHAR, BIGINT)
                .row(1)
                .row(1)
                .row(1)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testScalarFunction()
            throws Exception
    {
        assertQuery("SELECT SUBSTR('Quadratically', 5, 6) FROM orders LIMIT 1");
    }

    @Test
    public void testCast()
            throws Exception
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
    }

    @Test
    public void testConcatOperator()
            throws Exception
    {
        assertQuery("SELECT '12' || '34' FROM orders LIMIT 1");
    }

    @Test
    public void testQuotedIdentifiers()
            throws Exception
    {
        assertQuery("SELECT \"TOTALPRICE\" \"my price\" FROM \"ORDERS\"");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*orderkey_1.*")
    public void testInvalidColumn()
            throws Exception
    {
        computeActual("select * from lineitem l join (select orderkey_1, custkey from orders) o on l.orderkey = o.orderkey_1");
    }

    @Test
    public void testUnaliasedSubqueries()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM (SELECT orderkey FROM orders)");
    }

    @Test
    public void testUnaliasedSubqueries1()
            throws Exception
    {
        assertQuery("SELECT a FROM (SELECT orderkey a FROM orders)");
    }

    @Test
    public void testJoinUnaliasedSubqueries()
            throws Exception
    {
        assertQuery(
                "SELECT COUNT(*) FROM (SELECT * FROM lineitem) join (SELECT * FROM orders) using (orderkey)",
                "SELECT COUNT(*) FROM lineitem join orders on lineitem.orderkey = orders.orderkey"
        );
    }

    @Test
    public void testWith()
            throws Exception
    {
        assertQuery("" +
                "WITH a AS (SELECT * FROM orders) " +
                "SELECT * FROM a",
                "SELECT * FROM orders");
    }

    @Test
    public void testWithQualifiedPrefix()
            throws Exception
    {
        assertQuery("" +
                "WITH a AS (SELECT 123 FROM orders LIMIT 1)" +
                "SELECT a.* FROM a",
                "SELECT 123 FROM orders LIMIT 1");
    }

    @Test
    public void testWithAliased()
            throws Exception
    {
        assertQuery("" +
                "WITH a AS (SELECT * FROM orders) " +
                "SELECT * FROM a x",
                "SELECT * FROM orders");
    }

    @Test
    public void testReferenceToWithQueryInFromClause()
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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

    @Test(enabled = false)
    public void testWithColumnAliasing()
            throws Exception
    {
        assertQuery(
                "WITH a (id) AS (SELECT 123 FROM orders LIMIT 1) SELECT * FROM a",
                "SELECT 123 FROM orders LIMIT 1");
    }

    @Test
    public void testWithHiding()
            throws Exception
    {
        assertQuery("" +
                "WITH a AS (SELECT custkey FROM orders), " +
                "     b AS (" +
                "         WITH a AS (SELECT orderkey FROM orders)" +
                "         SELECT * FROM a" + // should refer to inner 'a'
                "    )" +
                "SELECT * FROM b",
                "SELECT orderkey FROM orders"
        );
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Recursive WITH queries are not supported")
    public void testWithRecursive()
            throws Exception
    {
        computeActual("WITH RECURSIVE a AS (SELECT 123) SELECT * FROM a");
    }

    @Test
    public void testCaseNoElse()
            throws Exception
    {
        assertQuery("SELECT orderkey, CASE orderstatus WHEN 'O' THEN 'a' END FROM orders");
    }

    @Test
    public void testIfExpression()
            throws Exception
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
    }

    @Test
    public void testIn()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1, 2, 3)");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1.5, 2.3)");
        assertQuery("SELECT orderkey FROM orders WHERE totalprice IN (1, 2, 3)");
    }

    @Test
    public void testGroupByIf()
            throws Exception
    {
        assertQuery(
                "SELECT IF(orderkey between 1 and 5, 'orders', 'others'), sum(totalprice) FROM orders GROUP BY 1",
                "SELECT CASE WHEN orderkey BETWEEN 1 AND 5 THEN 'orders' ELSE 'others' END, sum(totalprice)\n" +
                        "FROM orders\n" +
                        "GROUP BY CASE WHEN orderkey BETWEEN 1 AND 5 THEN 'orders' ELSE 'others' END");
    }

    @Test
    public void testDuplicateFields()
            throws Exception
    {
        assertQuery(
                "SELECT * FROM (SELECT orderkey, orderkey FROM orders)",
                "SELECT orderkey, orderkey FROM orders");
    }

    @Test
    public void testWildcardFromSubquery()
            throws Exception
    {
        assertQuery("SELECT * FROM (SELECT orderkey X FROM orders)");
    }

    @Test
    public void testCaseInsensitiveOutputAliasInOrderBy()
            throws Exception
    {
        assertQueryOrdered("SELECT orderkey X FROM orders ORDER BY x");
    }

    @Test
    public void testCaseInsensitiveAttribute()
            throws Exception
    {
        assertQuery("SELECT x FROM (SELECT orderkey X FROM orders)");
    }

    @Test
    public void testCaseInsensitiveAliasedRelation()
            throws Exception
    {
        assertQuery("SELECT A.* FROM orders a");
    }

    @Test
    public void testSubqueryBody()
            throws Exception
    {
        assertQuery("(SELECT orderkey, custkey FROM ORDERS)");
    }

    @Test
    public void testSubqueryBodyOrderLimit()
            throws Exception
    {
        assertQueryOrdered("(SELECT orderkey AS a, custkey AS b FROM ORDERS) ORDER BY a LIMIT 1");
    }

    @Test
    public void testSubqueryBodyProjectedOrderby()
            throws Exception
    {
        assertQueryOrdered("(SELECT orderkey, custkey FROM ORDERS) ORDER BY orderkey * -1");
    }

    @Test
    public void testSubqueryBodyDoubleOrderby()
            throws Exception
    {
        assertQueryOrdered("(SELECT orderkey, custkey FROM ORDERS ORDER BY custkey) ORDER BY orderkey");
    }

    @Test
    public void testNodeRoster()
            throws Exception
    {
        List<MaterializedRow> result = computeActual("SELECT * FROM system.runtime.nodes").getMaterializedRows();
        assertEquals(result.size(), getNodeCount());
    }

    @Test
    public void testCountOnInternalTables()
            throws Exception
    {
        List<MaterializedRow> rows = computeActual("SELECT count(*) FROM system.runtime.nodes").getMaterializedRows();
        assertEquals(((Long) rows.get(0).getField(0)).longValue(), getNodeCount());
    }

    @Test
    public void testDefaultExplainTextFormat()
    {
        String query = "SELECT 123";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(actual, getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testDefaultExplainGraphvizFormat()
    {
        String query = "SELECT 123";
        MaterializedResult result = computeActual("EXPLAIN (FORMAT GRAPHVIZ) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(actual, getGraphvizExplainPlan(query, LOGICAL));
    }

    @Test
    public void testLogicalExplain()
    {
        String query = "SELECT 123";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(actual, getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testLogicalExplainTextFormat()
    {
        String query = "SELECT 123";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT TEXT) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(actual, getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testLogicalExplainGraphvizFormat()
    {
        String query = "SELECT 123";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT GRAPHVIZ) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(actual, getGraphvizExplainPlan(query, LOGICAL));
    }

    @Test
    public void testDistributedExplain()
    {
        String query = "SELECT 123";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(actual, getExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testDistributedExplainTextFormat()
    {
        String query = "SELECT 123";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED, FORMAT TEXT) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(actual, getExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testDistributedExplainGraphvizFormat()
    {
        String query = "SELECT 123";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED, FORMAT GRAPHVIZ) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(actual, getGraphvizExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testExplainOfExplain()
    {
        String query = "EXPLAIN SELECT 123";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(actual, getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testShowCatalogs()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW CATALOGS");
        Set<String> catalogNames = ImmutableSet.copyOf(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertTrue(catalogNames.contains(getSession().getCatalog()));
    }

    @Test
    public void testShowSchemas()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS");
        ImmutableSet<String> schemaNames = ImmutableSet.copyOf(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertTrue(schemaNames.containsAll(ImmutableSet.of(getSession().getSchema(), INFORMATION_SCHEMA)));
    }

    @Test
    public void testShowSchemasFrom()
            throws Exception
    {
        MaterializedResult result = computeActual(format("SHOW SCHEMAS FROM %s", getSession().getCatalog()));
        ImmutableSet<String> schemaNames = ImmutableSet.copyOf(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertTrue(schemaNames.containsAll(ImmutableSet.of(getSession().getSchema(), INFORMATION_SCHEMA)));
    }

    @Test
    public void testShowTables()
            throws Exception
    {
        Set<String> expectedTables = ImmutableSet.copyOf(transform(TpchTable.getTables(), tableNameGetter()));

        MaterializedResult result = computeActual("SHOW TABLES");
        Set<String> tableNames = ImmutableSet.copyOf(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(tableNames, expectedTables);
    }

    @Test
    public void testShowTablesFrom()
            throws Exception
    {
        Set<String> expectedTables = ImmutableSet.copyOf(transform(TpchTable.getTables(), tableNameGetter()));

        MaterializedResult result = computeActual("SHOW TABLES FROM " + getSession().getSchema());
        Set<String> tableNames = ImmutableSet.copyOf(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(tableNames, expectedTables);

        result = computeActual("SHOW TABLES FROM " + getSession().getCatalog() + "." + getSession().getSchema());
        tableNames = ImmutableSet.copyOf(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(tableNames, expectedTables);

        result = computeActual("SHOW TABLES FROM UNKNOWN");
        tableNames = ImmutableSet.copyOf(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(tableNames, ImmutableSet.of());
    }

    @Test
    public void testShowTablesLike()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW TABLES LIKE 'or%'");
        ImmutableSet<String> tableNames = ImmutableSet.copyOf(transform(result.getMaterializedRows(), onlyColumnGetter()));
        assertEquals(tableNames, ImmutableSet.of(ORDERS.getTableName()));
    }

    @Test
    public void testShowColumns()
            throws Exception
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, BOOLEAN, BOOLEAN, VARCHAR)
                .row("orderkey", "bigint", true, false, "")
                .row("custkey", "bigint", true, false, "")
                .row("orderstatus", "varchar", true, false, "")
                .row("totalprice", "double", true, false, "")
                .row("orderdate", "date", true, false, "")
                .row("orderpriority", "varchar", true, false, "")
                .row("clerk", "varchar", true, false, "")
                .row("shippriority", "bigint", true, false, "")
                .row("comment", "varchar", true, false, "")
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testShowPartitions()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW PARTITIONS FROM orders");
        // table is not partitioned
        // TODO: add a partitioned table for tests and test where/order/limit
        assertEquals(result.getMaterializedRows().size(), 0);
    }

    @Test
    public void testShowFunctions()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW FUNCTIONS");
        ImmutableMultimap<String, MaterializedRow> functions = Multimaps.index(result.getMaterializedRows(), new Function<MaterializedRow, String>()
        {
            @Override
            public String apply(MaterializedRow input)
            {
                assertEquals(input.getFieldCount(), 6);
                return (String) input.getField(0);
            }
        });

        assertTrue(functions.containsKey("avg"), "Expected function names " + functions + " to contain 'avg'");
        assertEquals(functions.get("avg").asList().size(), 2);
        assertEquals(functions.get("avg").asList().get(0).getField(1), "double");
        assertEquals(functions.get("avg").asList().get(0).getField(2), "bigint");
        assertEquals(functions.get("avg").asList().get(0).getField(3), "aggregate");
        assertEquals(functions.get("avg").asList().get(1).getField(1), "double");
        assertEquals(functions.get("avg").asList().get(1).getField(2), "double");
        assertEquals(functions.get("avg").asList().get(0).getField(3), "aggregate");

        assertTrue(functions.containsKey("abs"), "Expected function names " + functions + " to contain 'abs'");
        assertEquals(functions.get("abs").asList().get(0).getField(3), "scalar");
        assertEquals(functions.get("abs").asList().get(0).getField(4), true);

        assertTrue(functions.containsKey("rand"), "Expected function names " + functions + " to contain 'rand'");
        assertEquals(functions.get("rand").asList().get(0).getField(3), "scalar");
        assertEquals(functions.get("rand").asList().get(0).getField(4), false);

        assertTrue(functions.containsKey("rank"), "Expected function names " + functions + " to contain 'rank'");
        assertEquals(functions.get("rank").asList().get(0).getField(3), "window");

        assertTrue(functions.containsKey("rank"), "Expected function names " + functions + " to contain 'split_part'");
        assertEquals(functions.get("split_part").asList().get(0).getField(1), "varchar");
        assertEquals(functions.get("split_part").asList().get(0).getField(2), "varchar, varchar, bigint");
        assertEquals(functions.get("split_part").asList().get(0).getField(3), "scalar");

        assertFalse(functions.containsKey("like"), "Expected function names " + functions + " not to contain 'like'");
    }

    @Test
    public void testInformationSchemaFiltering()
            throws Exception
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
    }

    @Test
    public void testSelectColumnOfNulls()
            throws Exception
    {
        // Currently nulls can confuse the local planner, so select some
        assertQueryOrdered("SELECT \n" +
                " CAST(NULL AS VARCHAR),\n" +
                " CAST(NULL AS BIGINT)\n" +
                "FROM ORDERS\n" +
                " ORDER BY 1\n");
    }

    @Test
    public void testShowSession()
            throws Exception
    {
        assertQueryOrdered(
                getSession()
                        .withCatalogProperty("connector", "cheese", "burger")
                        .withSystemProperty("foo", "bar")
                        .withSystemProperty("apple", "pie"),
                "SHOW SESSION",
                "SELECT * FROM VALUES ('apple', 'pie'), ('foo', 'bar'), ('connector.cheese', 'burger')");
    }

    @Test
    public void testNoFrom()
            throws Exception
    {
        assertQuery("SELECT 1 + 2, 3 + 4", "SELECT 1 + 2, 3 + 4 FROM orders LIMIT 1");
    }

    @Test
    public void testTopNByMultipleFields()
            throws Exception
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
    public void testUnion()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM orders UNION SELECT custkey FROM orders");
        assertQuery("SELECT 123 UNION DISTINCT SELECT 123 UNION ALL SELECT 123");

        // mixed single-node vs fixed vs source-distributed
        assertQuery("SELECT orderkey FROM orders UNION ALL SELECT 123 UNION ALL (SELECT custkey FROM orders GROUP BY custkey)");
    }

    @Test
    public void testUnionDistinct()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM orders UNION DISTINCT SELECT custkey FROM orders");
    }

    @Test
    public void testUnionAll()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM orders UNION ALL SELECT custkey FROM orders");
    }

    @Test
    public void testUnionArray()
            throws Exception
    {
        assertQuery("SELECT a[1] FROM (SELECT ARRAY[1] UNION ALL SELECT ARRAY[1]) t(a) LIMIT 1", "SELECT 1");
    }

    @Test
    public void testChainedUnionsWithOrder()
            throws Exception
    {
        assertQueryOrdered(
                "SELECT orderkey FROM orders UNION (SELECT custkey FROM orders UNION SELECT linenumber FROM lineitem) UNION ALL SELECT orderkey FROM lineitem ORDER BY orderkey");
    }

    @Test
    public void testUnionWithJoin()
            throws Exception
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
            throws Exception
    {
        assertQuery(
                "SELECT ds, count(*) FROM (" +
                        "   SELECT orderdate ds, orderkey FROM orders " +
                        "   UNION ALL " +
                        "   SELECT shipdate ds, orderkey FROM lineitem) a " +
                        "GROUP BY ds");
    }

    @Test
    public void testUnionWithAggregationAndJoin()
            throws Exception
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
            throws Exception
    {
        assertQuery("SELECT *\n" +
                "FROM (SELECT orderdate ds, orderkey\n" +
                "      FROM orders\n" +
                "      UNION ALL\n" +
                "      SELECT shipdate ds, orderkey\n" +
                "      FROM lineitem) a\n" +
                "JOIN orders o\n" +
                "ON (substr(cast(a.ds AS VARCHAR), 6, 2) = substr(cast(o.orderdate AS VARCHAR), 6, 2) AND a.orderkey = o.orderkey)\n");
    }

    @Test
    public void testSubqueryUnion()
            throws Exception
    {
        assertQueryOrdered("SELECT * FROM (SELECT orderkey FROM orders UNION SELECT custkey FROM orders UNION SELECT orderkey FROM orders) ORDER BY orderkey LIMIT 1000");
    }

    @Test
    public void testSelectOnlyUnion()
            throws Exception
    {
        assertQuery("SELECT 123, 'foo' UNION ALL SELECT 999, 'bar'");
    }

    @Test
    public void testMultiColumnUnionAll()
            throws Exception
    {
        assertQuery("SELECT * FROM orders UNION ALL SELECT * FROM orders");
    }

    @Test
    public void testTableQuery()
            throws Exception
    {
        assertQuery("TABLE orders", "SELECT * FROM orders");
    }

    @Test
    public void testTableQueryOrderLimit()
            throws Exception
    {
        assertQueryOrdered("TABLE orders ORDER BY orderkey LIMIT 10", "SELECT * FROM orders ORDER BY orderkey LIMIT 10");
    }

    @Test
    public void testTableQueryInUnion()
            throws Exception
    {
        assertQuery("(SELECT * FROM orders ORDER BY orderkey LIMIT 10) UNION ALL TABLE orders", "(SELECT * FROM orders ORDER BY orderkey LIMIT 10) UNION ALL SELECT * FROM orders");
    }

    @Test
    public void testTableAsSubquery()
            throws Exception
    {
        assertQueryOrdered("(TABLE orders) ORDER BY orderkey", "(SELECT * FROM orders) ORDER BY orderkey");
    }

    @Test
    public void testLimitPushDown()
            throws Exception
    {
        MaterializedResult actual = computeActual(
                "(TABLE orders ORDER BY orderkey) UNION ALL " +
                        "SELECT * FROM orders WHERE orderstatus = 'F' UNION ALL " +
                        "(TABLE orders ORDER BY orderkey LIMIT 20) UNION ALL " +
                        "(TABLE orders LIMIT 5) UNION ALL " +
                        "TABLE orders LIMIT 10");
        MaterializedResult all = computeExpected("SELECT * FROM ORDERS", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertTrue(all.getMaterializedRows().containsAll(actual.getMaterializedRows()));
    }

    @Test
    public void testOrderLimitCompaction()
            throws Exception
    {
        assertQueryOrdered("SELECT * FROM (SELECT * FROM orders ORDER BY orderkey) LIMIT 10");
    }

    @Test
    public void testUnaliasSymbolReferencesWithUnion()
            throws Exception
    {
        assertQuery("SELECT 1, 1, 'a', 'a' UNION ALL SELECT 1, 2, 'a', 'b'");
    }

    @Test
    public void testRandCrossJoins()
            throws Exception
    {
        assertQuery("" +
                "SELECT COUNT(*) " +
                "FROM (SELECT * FROM orders ORDER BY rand() LIMIT 5) a " +
                "CROSS JOIN (SELECT * FROM lineitem ORDER BY rand() LIMIT 5) b");
    }

    @Test
    public void testCrossJoins()
            throws Exception
    {
        assertQuery("" +
                "SELECT a.custkey, b.orderkey " +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a " +
                "CROSS JOIN (SELECT * FROM lineitem ORDER BY orderkey LIMIT 5) b");
    }

    @Test
    public void testSimpleCrossJoins()
            throws Exception
    {
        assertQuery("SELECT * FROM (SELECT 1 a) x CROSS JOIN (SELECT 2 b) y");
    }

    @Test
    public void testImplicitCrossJoin()
            throws Exception
    {
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 5) b, " +
                "(SELECT * FROM orders ORDER BY orderkey LIMIT 5) c ");
    }

    @Test
    public void testJoinOnConstantExpression()
            throws Exception
    {
        assertQuery("" +
                "SELECT * FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) a " +
                "   JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) b " +
                "   ON 123 = 123");
    }

    @Test
    public void testSemiJoin()
            throws Exception
    {
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
    public void testAntiJoin()
            throws Exception
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
            throws Exception
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

    @Test
    public void testSemiJoinNullHandling()
            throws Exception
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
    public void testPredicatePushdown()
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
    public void testRightJoinAsInnerPredicatePushdown()
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
            throws Exception
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
        MaterializedRow row = Iterables.getOnlyElement(materializedResult.getMaterializedRows());
        assertEquals(row.getFieldCount(), 1);
        long count = (Long) row.getField(0);
        // Technically non-deterministic unit test but has essentially a next to impossible chance of a false positive
        assertTrue(count > 0 && count < 1000000);
    }

    @Test
    public void testTrivialNonDeterministicPredicatePushdown()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) WHERE rand() >= 0");
    }

    @Test
    public void testNonDeterministicTableScanPredicatePushdown()
            throws Exception
    {
        MaterializedResult materializedResult = computeActual("" +
                "SELECT COUNT(*)\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM lineitem\n" +
                "  LIMIT 1000\n" +
                ")\n" +
                "WHERE rand() > 0.5");
        MaterializedRow row = Iterables.getOnlyElement(materializedResult.getMaterializedRows());
        assertEquals(row.getFieldCount(), 1);
        long count = (Long) row.getField(0);
        // Technically non-deterministic unit test but has essentially a next to impossible chance of a false positive
        assertTrue(count > 0 && count < 1000);
    }

    @Test
    public void testNonDeterministicAggregationPredicatePushdown()
            throws Exception
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
        MaterializedRow row = Iterables.getOnlyElement(materializedResult.getMaterializedRows());
        assertEquals(row.getFieldCount(), 1);
        long count = (Long) row.getField(0);
        // Technically non-deterministic unit test but has essentially a next to impossible chance of a false positive
        assertTrue(count > 0 && count < 1000);
    }

    @Test
    public void testSemiJoinPredicateMoveAround()
            throws Exception
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
            throws Exception
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
            throws Exception
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE BERNOULLI (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey FROM orders TABLESAMPLE BERNOULLI (0)");
        MaterializedResult all = computeExpected("SELECT orderkey FROM orders", fullSample.getTypes());

        assertTrue(all.getMaterializedRows().containsAll(fullSample.getMaterializedRows()));
        assertEquals(emptySample.getMaterializedRows().size(), 0);
    }

    @Test
    public void testTableSampleBernoulli()
            throws Exception
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
    public void testTableSamplePoissonized()
            throws Exception
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        long total = (long) computeExpected("SELECT COUNT(*) FROM orders", ImmutableList.of(BIGINT)).getMaterializedRows().get(0).getField(0);

        for (int i = 0; i < 100; i++) {
            String value = (String) computeActual("SELECT COUNT(*) FROM orders TABLESAMPLE POISSONIZED (50) APPROXIMATE AT 95 CONFIDENCE").getMaterializedRows().get(0).getField(0);
            stats.addValue(Long.parseLong(value.split(" ")[0]) * 1.0 / total);
        }

        double mean = stats.getGeometricMean();
        assertTrue(mean > 0.45 && mean < 0.55, format("Expected mean sampling rate to be ~0.5, but was %s", mean));
    }

    @Test
    public void testTableSamplePoissonizedRescaled()
            throws Exception
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        long total = (long) computeExpected("SELECT COUNT(*) FROM orders", ImmutableList.of(BIGINT)).getMaterializedRows().get(0).getField(0);

        for (int i = 0; i < 100; i++) {
            String value = (String) computeActual("SELECT COUNT(*) FROM orders TABLESAMPLE POISSONIZED (50) RESCALED APPROXIMATE AT 95 CONFIDENCE").getMaterializedRows().get(0).getField(0);
            stats.addValue(Long.parseLong(value.split(" ")[0]) * 1.0 / total);
        }

        double mean = stats.getGeometricMean();
        assertTrue(mean > 0.90 && mean < 1.1, format("Expected sample to be rescaled to ~1.0, but was %s", mean));
        assertTrue(stats.getVariance() > 0, "Samples all had the exact same size");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "\\QUnexpected parameters (bigint) for function length. Expected:\\E.*")
    public void testFunctionNotRegistered()
    {
        computeActual("SELECT length(1)");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "\\QUnexpected parameters (color) for function greatest. Expected: greatest(E) E:orderable\\E.*")
    public void testFunctionArgumentTypeConstraint()
    {
        computeActual("SELECT greatest(rgb(255, 0, 0))");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "\\QOperator NOT_EQUAL(bigint, varchar) not registered\\E")
    public void testTypeMismatch()
    {
        computeActual("SELECT 1 <> 'x'");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "\\QUnknown type: ARRAY<FOO>\\E")
    public void testInvalidType()
    {
        computeActual("select cast(null as array<foo>)");
    }

    @Test
    public void testTimeLiterals()
            throws Exception
    {
        MaterializedResult.Builder builder = MaterializedResult.resultBuilder(getSession(), DATE, TIME, TIME_WITH_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE);

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
    public void testNonReservedTimeWords()
            throws Exception
    {
        assertQuery("" +
                "SELECT TIME, TIMESTAMP, DATE, INTERVAL\n" +
                "FROM (SELECT 1 TIME, 2 TIMESTAMP, 3 DATE, 4 INTERVAL)");
    }

    @Test
    public void testCustomAdd()
            throws Exception
    {
        assertQuery(
                "SELECT custom_add(orderkey, custkey) FROM orders",
                "SELECT orderkey + custkey FROM orders");
    }

    @Test
    public void testCustomSum()
            throws Exception
    {
        @Language("SQL") String sql = "SELECT orderstatus, custom_sum(orderkey) FROM orders GROUP BY orderstatus";
        assertQuery(sql, sql.replace("custom_sum", "sum"));
    }

    @Test
    public void testCustomRank()
            throws Exception
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
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(custkey)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1002)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetVarchar()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(CAST(custkey AS VARCHAR))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1024)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetDouble()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(CAST(custkey AS DOUBLE))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1014)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetBigintGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(custkey)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1001)
                .row("F", 998)
                .row("P", 304)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetVarcharGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(CAST(custkey AS VARCHAR))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1021)
                .row("F", 1019)
                .row("P", 304)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetDoubleGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(CAST(custkey AS DOUBLE))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1011)
                .row("F", 1011)
                .row("P", 304)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetWithNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(IF(orderstatus = 'O', custkey))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(1001)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetOnlyNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(null)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(new Object[] { null })
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetGroupByWithOnlyNullsInOneGroup()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(IF(orderstatus != 'O', custkey))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", null)
                .row("F", 998)
                .row("P", 304)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testApproxSetGroupByWithNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(IF(custkey % 2 <> 0, custkey))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 499)
                .row("F", 496)
                .row("P", 153)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLog()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(create_hll(custkey))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1002)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLogGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(merge(create_hll(custkey))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1001)
                .row("F", 998)
                .row("P", 304)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLogWithNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(create_hll(IF(orderstatus = 'O', custkey)))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1001)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLogGroupByWithNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(merge(create_hll(IF(orderstatus != 'O', custkey)))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", null)
                .row("F", 998)
                .row("P", 304)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testMergeHyperLogLogOnlyNulls()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(null)) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(new Object[] { null })
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testValuesWithNonTrivialType()
            throws Exception
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
            throws Exception
    {
        MaterializedResult actual = computeActual("VALUES (current_timestamp, now())");

        List<MaterializedRow> rows = actual.getMaterializedRows();
        assertEquals(rows.size(), 1);

        MaterializedRow row = rows.get(0);
        assertEquals(row.getField(0), row.getField(1));
    }
}
