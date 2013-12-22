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
package com.facebook.presto;

import com.facebook.presto.connector.dual.DualMetadata;
import com.facebook.presto.connector.dual.DualSplitManager;
import com.facebook.presto.importer.MockPeriodicImportManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanOptimizersFactory;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.storage.MockStorageManager;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.MaterializedResult;
import com.facebook.presto.util.MaterializedTuple;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.PreparedBatchPart;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_CATALOG;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_SCHEMA;
import static com.facebook.presto.sql.tree.ExplainType.Type.DISTRIBUTED;
import static com.facebook.presto.sql.tree.ExplainType.Type.LOGICAL;
import static com.facebook.presto.tpch.TpchMetadata.TPCH_LINEITEM_METADATA;
import static com.facebook.presto.tpch.TpchMetadata.TPCH_LINEITEM_NAME;
import static com.facebook.presto.tpch.TpchMetadata.TPCH_ORDERS_METADATA;
import static com.facebook.presto.tpch.TpchMetadata.TPCH_ORDERS_NAME;
import static com.facebook.presto.tpch.TpchMetadata.TPCH_SCHEMA_NAME;
import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.InMemoryTpchBlocksProvider.readTpchRecords;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestQueries
{
    private Handle handle;

    @Test void testSpecialFloatingPointValues()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT nan(), infinity(), -infinity()");
        MaterializedTuple tuple = Iterables.getOnlyElement(actual.getMaterializedTuples());
        assertEquals(tuple.getField(0), Double.NaN);
        assertEquals(tuple.getField(1), Double.POSITIVE_INFINITY);
        assertEquals(tuple.getField(2), Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testMaxMinStringWithNulls()
            throws Exception
    {
        assertQuery("SELECT custkey, MAX(NULLIF(orderstatus, 'O')), MIN(NULLIF(orderstatus, 'O')) FROM orders GROUP BY custkey");
    }

    @Test
    public void testApproxPercentile()
            throws Exception
    {
        MaterializedResult raw = computeActual("SELECT orderstatus, orderkey, totalprice FROM ORDERS");

        Multimap<String, Long> orderKeyByStatus = ArrayListMultimap.create();
        Multimap<String, Double> totalPriceByStatus = ArrayListMultimap.create();
        for (MaterializedTuple tuple : raw.getMaterializedTuples()) {
            orderKeyByStatus.put((String) tuple.getField(0), (Long) tuple.getField(1));
            totalPriceByStatus.put((String) tuple.getField(0), (Double) tuple.getField(2));
        }

        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, " +
                "   approx_percentile(orderkey, 0.5), " +
                "   approx_percentile(totalprice, 0.5)," +
                "   approx_percentile(orderkey, 2, 0.5)," +
                "   approx_percentile(totalprice, 2, 0.5)\n" +
                "FROM ORDERS\n" +
                "GROUP BY orderstatus");

        for (MaterializedTuple tuple : actual.getMaterializedTuples()) {
            String status = (String) tuple.getField(0);
            Long orderKey = (Long) tuple.getField(1);
            Double totalPrice = (Double) tuple.getField(2);
            Long orderKeyWeighted = (Long) tuple.getField(3);
            Double totalPriceWeighted = (Double) tuple.getField(4);

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

        MaterializedResult expected = resultBuilder(FIXED_INT_64, FIXED_INT_64)
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

        MaterializedResult expected = resultBuilder(FIXED_INT_64)
                .row(971)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedTuples(), expected.getMaterializedTuples());
    }

    @Test
    public void testApproximateCountDistinctGroupBy()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderstatus, approx_distinct(custkey) FROM orders GROUP BY orderstatus");
        MaterializedResult expected = resultBuilder(actual.getTupleInfo())
                .row("O", 969)
                .row("F", 964)
                .row("P", 301)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedTuples(), expected.getMaterializedTuples());
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
    public void testDistinctMultipleFields()
            throws Exception
    {
        assertQuery("SELECT DISTINCT custkey, orderstatus FROM ORDERS");
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

    @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = "All DISTINCT argument lists used in aggregations must match")
    public void testCountMultipleDifferentDistinct()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT orderstatus), SUM(DISTINCT custkey) FROM orders");
    }

    @Test
    public void testCountMultipleDistinct()
            throws Exception
    {
        assertQuery("SELECT COUNT(DISTINCT custkey), SUM(DISTINCT custkey) FROM orders", "SELECT COUNT(*), SUM(custkey) FROM (SELECT DISTINCT custkey FROM orders) t");
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
    public void testLimit()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderkey FROM ORDERS LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM ORDERS", actual.getTupleInfo());

        assertEquals(actual.getMaterializedTuples().size(), 10);
        assertTrue(all.getMaterializedTuples().containsAll(actual.getMaterializedTuples()));
    }

    @Test
    public void testAggregationWithLimit()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey LIMIT 10");
        MaterializedResult all = computeExpected("SELECT custkey, SUM(totalprice) FROM ORDERS GROUP BY custkey", actual.getTupleInfo());

        assertEquals(actual.getMaterializedTuples().size(), 10);
        assertTrue(all.getMaterializedTuples().containsAll(actual.getMaterializedTuples()));
    }

    @Test
    public void testLimitInInlineView()
            throws Exception
    {
        MaterializedResult actual = computeActual("SELECT orderkey FROM (SELECT orderkey FROM ORDERS LIMIT 100) T LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM ORDERS", actual.getTupleInfo());

        assertEquals(actual.getMaterializedTuples().size(), 10);
        assertTrue(all.getMaterializedTuples().containsAll(actual.getMaterializedTuples()));
    }

    @Test
    public void testCountAll()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM ORDERS");
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

    @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = ".*not supported.*")
    public void testJoinOnConstantExpression()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON 123 = 123");
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
    }

    @Test
    public void testRightJoinNormalizedToInner()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM lineitem RIGHT JOIN orders ON lineitem.orderkey = orders.orderkey WHERE lineitem.orderkey IS NOT NULL");
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
        assertQuery("SELECT custkey, sum(totalprice) * 2 FROM orders GROUP BY custkey HAVING avg(totalprice + 5) > 10");
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

        MaterializedResult expected = resultBuilder(FIXED_INT_64, VARIABLE_BINARY, FIXED_INT_64)
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

        MaterializedResult expected = resultBuilder(VARIABLE_BINARY, VARIABLE_BINARY, DOUBLE, FIXED_INT_64)
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

        MaterializedResult expected = resultBuilder(FIXED_INT_64, FIXED_INT_64)
                .row(34, 10)
                .row(33, 9)
                .row(32, 8)
                .row(7, 7)
                .row(6, 6)
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

    @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = ".*orderkey_1.*")
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
        computeActual("WITH RECURSIVE a AS (SELECT 123 FROM dual) SELECT * FROM a");
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
        List<MaterializedTuple> result = computeActual("SELECT * FROM sys.node").getMaterializedTuples();
        assertEquals(result.size(), getNodeCount());
    }

    @Test
    public void testDual()
            throws Exception
    {
        MaterializedResult result = computeActual("SELECT * FROM dual");
        List<MaterializedTuple> tuples = result.getMaterializedTuples();
        assertEquals(tuples.size(), 1);
    }

    @Test
    public void testDefaultExplainTextFormat()
    {
        String query = "SELECT 123 FROM dual";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(actual, getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testDefaultExplainGraphvizFormat()
    {
        String query = "SELECT 123 FROM dual";
        MaterializedResult result = computeActual("EXPLAIN (FORMAT GRAPHVIZ) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(actual, getGraphvizExplainPlan(query, LOGICAL));
    }

    @Test
    public void testLogicalExplain()
    {
        String query = "SELECT 123 FROM dual";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(actual, getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testLogicalExplainTextFormat()
    {
        String query = "SELECT 123 FROM dual";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT TEXT) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(actual, getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testLogicalExplainGraphvizFormat()
    {
        String query = "SELECT 123 FROM dual";
        MaterializedResult result = computeActual("EXPLAIN (TYPE LOGICAL, FORMAT GRAPHVIZ) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(actual, getGraphvizExplainPlan(query, LOGICAL));
    }

    @Test
    public void testDistributedExplain()
    {
        String query = "SELECT 123 FROM dual";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(actual, getExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testDistributedExplainTextFormat()
    {
        String query = "SELECT 123 FROM dual";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED, FORMAT TEXT) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(actual, getExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testDistributedExplainGraphvizFormat()
    {
        String query = "SELECT 123 FROM dual";
        MaterializedResult result = computeActual("EXPLAIN (TYPE DISTRIBUTED, FORMAT GRAPHVIZ) " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(actual, getGraphvizExplainPlan(query, DISTRIBUTED));
    }

    @Test
    public void testExplainOfExplain()
    {
        String query = "EXPLAIN SELECT 123 FROM dual";
        MaterializedResult result = computeActual("EXPLAIN " + query);
        String actual = Iterables.getOnlyElement(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(actual, getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testShowSchemas()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS");
        ImmutableSet<String> schemaNames = ImmutableSet.copyOf(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(schemaNames, ImmutableSet.of(TPCH_SCHEMA_NAME, INFORMATION_SCHEMA, "sys"));
    }

    @Test
    public void testShowSchemasFrom()
            throws Exception
    {
        MaterializedResult result = computeActual(String.format("SHOW SCHEMAS FROM %s", TpchMetadata.TPCH_CATALOG_NAME));
        ImmutableSet<String> schemaNames = ImmutableSet.copyOf(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(schemaNames, ImmutableSet.of(TPCH_SCHEMA_NAME, INFORMATION_SCHEMA, "sys"));
    }

    @Test
    public void testShowTables()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW TABLES");
        ImmutableSet<String> tableNames = ImmutableSet.copyOf(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(tableNames, ImmutableSet.of(TPCH_ORDERS_NAME, TPCH_LINEITEM_NAME));
    }

    @Test
    public void testShowTablesFrom()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW TABLES FROM DEFAULT");
        ImmutableSet<String> tableNames = ImmutableSet.copyOf(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(tableNames, ImmutableSet.of(TPCH_ORDERS_NAME, TPCH_LINEITEM_NAME));

        result = computeActual("SHOW TABLES FROM TPCH.DEFAULT");
        tableNames = ImmutableSet.copyOf(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(tableNames, ImmutableSet.of(TPCH_ORDERS_NAME, TPCH_LINEITEM_NAME));

        result = computeActual("SHOW TABLES FROM UNKNOWN");
        tableNames = ImmutableSet.copyOf(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(tableNames, ImmutableSet.of());
    }

    @Test
    public void testShowTablesLike()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW TABLES LIKE 'or%'");
        ImmutableSet<String> tableNames = ImmutableSet.copyOf(transform(result.getMaterializedTuples(), onlyColumnGetter()));
        assertEquals(tableNames, ImmutableSet.of(TPCH_ORDERS_NAME));
    }

    @Test
    public void testShowColumns()
            throws Exception
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expected = resultBuilder(VARIABLE_BINARY, VARIABLE_BINARY, BOOLEAN, BOOLEAN)
                .row("orderkey", "bigint", true, false)
                .row("custkey", "bigint", true, false)
                .row("orderstatus", "varchar", true, false)
                .row("totalprice", "double", true, false)
                .row("orderdate", "varchar", true, false)
                .row("orderpriority", "varchar", true, false)
                .row("clerk", "varchar", true, false)
                .row("shippriority", "bigint", true, false)
                .row("comment", "varchar", true, false)
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
        assertEquals(result.getMaterializedTuples().size(), 0);
    }

    @Test
    public void testShowFunctions()
            throws Exception
    {
        MaterializedResult result = computeActual("SHOW FUNCTIONS");
        ImmutableMultimap<String, MaterializedTuple> functions = Multimaps.index(result.getMaterializedTuples(), new Function<MaterializedTuple, String>()
        {
            @Override
            public String apply(MaterializedTuple input)
            {
                assertEquals(input.getFieldCount(), 5);
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

        assertTrue(functions.containsKey("rand"), "Expected function names " + functions + " to contain 'rand'");
        assertEquals(functions.get("rand").asList().get(0).getField(3), "scalar (non-deterministic)");

        assertTrue(functions.containsKey("rank"), "Expected function names " + functions + " to contain 'rank'");
        assertEquals(functions.get("rank").asList().get(0).getField(3), "window");

        assertTrue(functions.containsKey("rank"), "Expected function names " + functions + " to contain 'split_part'");
        assertEquals(functions.get("split_part").asList().get(0).getField(1), "varchar");
        assertEquals(functions.get("split_part").asList().get(0).getField(2), "varchar, varchar, bigint");
        assertEquals(functions.get("split_part").asList().get(0).getField(3), "scalar");
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
    }

    @Test
    public void testUnion()
            throws Exception
    {
        assertQuery("SELECT orderkey FROM orders UNION SELECT custkey FROM orders");
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
    public void testChainedUnionsWithOrder()
            throws Exception
    {
        assertQueryOrdered("SELECT orderkey FROM orders UNION (SELECT custkey FROM orders UNION SELECT linenumber FROM lineitem) UNION ALL SELECT orderkey FROM lineitem ORDER BY orderkey");
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
        assertQuery("TABLE orders ORDER BY orderkey LIMIT 10", "SELECT * FROM orders ORDER BY orderkey LIMIT 10", true);
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
        assertQuery("(TABLE orders) ORDER BY orderkey", "(SELECT * FROM orders) ORDER BY orderkey", true);
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
        MaterializedResult all = computeExpected("SELECT * FROM ORDERS", actual.getTupleInfo());

        assertEquals(actual.getMaterializedTuples().size(), 10);
        assertTrue(all.getMaterializedTuples().containsAll(actual.getMaterializedTuples()));
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
        MaterializedTuple tuple = Iterables.getOnlyElement(materializedResult.getMaterializedTuples());
        Assert.assertEquals(tuple.getFieldCount(), 1);
        long count = (Long) tuple.getField(0);
        // Technically non-deterministic unit test but has essentially a next to impossible chance of a false positive
        Assert.assertTrue(count > 0 && count < 1000000);
    }

    @Test
    public void testTrivialNonDeterministicPredicatePushdown()
            throws Exception
    {
        assertQuery("SELECT COUNT(*) FROM dual WHERE rand() >= 0");
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
        MaterializedTuple tuple = Iterables.getOnlyElement(materializedResult.getMaterializedTuples());
        Assert.assertEquals(tuple.getFieldCount(), 1);
        long count = (Long) tuple.getField(0);
        // Technically non-deterministic unit test but has essentially a next to impossible chance of a false positive
        Assert.assertTrue(count > 0 && count < 1000);
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
        MaterializedTuple tuple = Iterables.getOnlyElement(materializedResult.getMaterializedTuples());
        Assert.assertEquals(tuple.getFieldCount(), 1);
        long count = (Long) tuple.getField(0);
        // Technically non-deterministic unit test but has essentially a next to impossible chance of a false positive
        Assert.assertTrue(count > 0 && count < 1000);
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
    public void testTableSampleBernoulliBoundaryValues()
            throws Exception
    {

        MaterializedResult fullSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE BERNOULLI (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey FROM orders TABLESAMPLE BERNOULLI (0)");
        MaterializedResult all = computeExpected("SELECT orderkey FROM orders", fullSample.getTupleInfo());

        assertTrue(all.getMaterializedTuples().containsAll(fullSample.getMaterializedTuples()));
        assertEquals(emptySample.getMaterializedTuples().size(), 0);
    }

    @Test
    public void testTableSampleBernoulli()
            throws Exception
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        int total = computeExpected("SELECT orderkey FROM orders", TupleInfo.SINGLE_LONG).getMaterializedTuples().size();

        for (int i = 0; i < 100; i++) {
            List<MaterializedTuple> values = computeActual("SELECT orderkey FROM ORDERS TABLESAMPLE BERNOULLI (50)").getMaterializedTuples();

            assertEquals(values.size(), ImmutableSet.copyOf(values).size(), "TABLESAMPLE produced duplicate rows");
            stats.addValue(values.size() * 1.0 / total);
        }

        double mean = stats.getGeometricMean();
        assertTrue(mean > 0.45 && mean < 0.55, String.format("Expected mean sampling rate to be ~0.5, but was %s", mean));
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "\\QUnexpected parameters (bigint) for function length. Expected: length(varchar)\\E")
    public void testFunctionNotRegistered()
    {
        computeActual("SELECT length(1)");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Types are not comparable with '<>': bigint vs varchar")
    public void testTypeMismatch()
    {
        computeActual("SELECT 1 <> 'x'");
    }

    @Test
    public void testTimeLiterals()
            throws Exception
    {
        assertQuery(
                "SELECT TIME '3:04', TIMESTAMP '1960-01-22 3:04', DATE '2013-03-22', INTERVAL '123' DAY\n",
                "SELECT " +
                        MILLISECONDS.toSeconds(new DateTime(1970, 1, 1, 3, 4, 0, 0, DateTimeZone.UTC).getMillis()) + ",  " +
                        MILLISECONDS.toSeconds(new DateTime(1960, 1, 22, 3, 4, 0, 0, DateTimeZone.UTC).getMillis()) + ",  " +
                        MILLISECONDS.toSeconds(new DateTime(2013, 3, 22, 0, 0, 0, 0, DateTimeZone.UTC).getMillis()) + ",  " +
                        String.valueOf(TimeUnit.DAYS.toSeconds(123)));
    }

    @Test
    public void testNonReservedTimeWords()
            throws Exception
    {
        assertQuery("" +
                "SELECT TIME, TIMESTAMP, DATE, INTERVAL\n" +
                "FROM (SELECT 1 TIME, 2 TIMESTAMP, 3 DATE, 4 INTERVAL)");
    }

    @BeforeClass(alwaysRun = true)
    public void setupDatabase()
            throws Exception
    {
        Logging.initialize();

        handle = DBI.open("jdbc:h2:mem:test" + System.nanoTime());

        RecordSet ordersRecords = readTpchRecords(TPCH_ORDERS_METADATA);
        handle.execute("CREATE TABLE orders (\n" +
                "  orderkey BIGINT PRIMARY KEY,\n" +
                "  custkey BIGINT NOT NULL,\n" +
                "  orderstatus CHAR(1) NOT NULL,\n" +
                "  totalprice DOUBLE NOT NULL,\n" +
                "  orderdate CHAR(10) NOT NULL,\n" +
                "  orderpriority CHAR(15) NOT NULL,\n" +
                "  clerk CHAR(15) NOT NULL,\n" +
                "  shippriority BIGINT NOT NULL,\n" +
                "  comment VARCHAR(79) NOT NULL\n" +
                ")");
        insertRows(TPCH_ORDERS_METADATA, handle, ordersRecords);

        RecordSet lineItemRecords = readTpchRecords(TPCH_LINEITEM_METADATA);
        handle.execute("CREATE TABLE lineitem (\n" +
                "  orderkey BIGINT,\n" +
                "  partkey BIGINT NOT NULL,\n" +
                "  suppkey BIGINT NOT NULL,\n" +
                "  linenumber BIGINT,\n" +
                "  quantity BIGINT NOT NULL,\n" +
                "  extendedprice DOUBLE NOT NULL,\n" +
                "  discount DOUBLE NOT NULL,\n" +
                "  tax DOUBLE NOT NULL,\n" +
                "  returnflag CHAR(1) NOT NULL,\n" +
                "  linestatus CHAR(1) NOT NULL,\n" +
                "  shipdate CHAR(10) NOT NULL,\n" +
                "  commitdate CHAR(10) NOT NULL,\n" +
                "  receiptdate CHAR(10) NOT NULL,\n" +
                "  shipinstruct VARCHAR(25) NOT NULL,\n" +
                "  shipmode VARCHAR(10) NOT NULL,\n" +
                "  comment VARCHAR(44) NOT NULL,\n" +
                "  PRIMARY KEY (orderkey, linenumber)" +
                ")");
        insertRows(TPCH_LINEITEM_METADATA, handle, lineItemRecords);

        setUpQueryFramework(TpchMetadata.TPCH_CATALOG_NAME, TpchMetadata.TPCH_SCHEMA_NAME);
    }

    @AfterClass(alwaysRun = true)
    public void cleanupDatabase()
            throws Exception
    {
        tearDownQueryFramework();
        handle.close();
    }

    protected abstract int getNodeCount();

    protected abstract void setUpQueryFramework(String catalog, String schema)
            throws Exception;

    protected void tearDownQueryFramework()
            throws Exception
    {
    }

    protected abstract MaterializedResult computeActual(@Language("SQL") String sql);

    protected void assertQuery(@Language("SQL") String sql)
            throws Exception
    {
        assertQuery(sql, sql, false);
    }

    private void assertQueryOrdered(@Language("SQL") String sql)
            throws Exception
    {
        assertQuery(sql, sql, true);
    }

    protected void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected)
            throws Exception
    {
        assertQuery(actual, expected, false);
    }

    private static final Logger log = Logger.get(AbstractTestQueries.class);

    private void assertQuery(@Language("SQL") String actual, @Language("SQL") String expected, boolean ensureOrdering)
            throws Exception
    {
        long start = System.nanoTime();
        MaterializedResult actualResults = computeActual(actual);
        log.info("FINISHED in %s", Duration.nanosSince(start));

        MaterializedResult expectedResults = computeExpected(expected, actualResults.getTupleInfo());

        if (ensureOrdering) {
            assertEquals(actualResults.getMaterializedTuples(), expectedResults.getMaterializedTuples());
        }
        else {
            assertEqualsIgnoreOrder(actualResults.getMaterializedTuples(), expectedResults.getMaterializedTuples());
        }
    }

    public static void assertEqualsIgnoreOrder(Iterable<?> actual, Iterable<?> expected)
    {
        assertNotNull(actual, "actual is null");
        assertNotNull(expected, "expected is null");

        ImmutableMultiset<?> actualSet = ImmutableMultiset.copyOf(actual);
        ImmutableMultiset<?> expectedSet = ImmutableMultiset.copyOf(expected);
        if (!actualSet.equals(expectedSet)) {
            fail(format("not equal\nActual %s rows:\n    %s\nExpected %s rows:\n    %s\n",
                    actualSet.size(),
                    Joiner.on("\n    ").join(Iterables.limit(actualSet, 100)),
                    expectedSet.size(),
                    Joiner.on("\n    ").join(Iterables.limit(expectedSet, 100))));
        }
    }

    private MaterializedResult computeExpected(@Language("SQL") final String sql, TupleInfo resultTupleInfo)
    {
        return new MaterializedResult(
                handle.createQuery(sql)
                        .map(tupleMapper(resultTupleInfo))
                        .list(),
                resultTupleInfo
        );
    }

    private static ResultSetMapper<Tuple> tupleMapper(final TupleInfo tupleInfo)
    {
        return new ResultSetMapper<Tuple>()
        {
            @Override
            public Tuple map(int index, ResultSet resultSet, StatementContext ctx)
                    throws SQLException
            {
                List<TupleInfo.Type> types = tupleInfo.getTypes();
                int count = resultSet.getMetaData().getColumnCount();
                checkArgument(types.size() == count, "tuple info does not match result");
                TupleInfo.Builder builder = tupleInfo.builder();
                for (int i = 1; i <= count; i++) {
                    TupleInfo.Type type = types.get(i - 1);
                    switch (type) {
                        case BOOLEAN:
                            boolean booleanValue = resultSet.getBoolean(i);
                            if (resultSet.wasNull()) {
                                builder.appendNull();
                            }
                            else {
                                builder.append(booleanValue);
                            }
                            break;
                        case FIXED_INT_64:
                            long longValue = resultSet.getLong(i);
                            if (resultSet.wasNull()) {
                                builder.appendNull();
                            }
                            else {
                                builder.append(longValue);
                            }
                            break;
                        case DOUBLE:
                            double doubleValue = resultSet.getDouble(i);
                            if (resultSet.wasNull()) {
                                builder.appendNull();
                            }
                            else {
                                builder.append(doubleValue);
                            }
                            break;
                        case VARIABLE_BINARY:
                            String value = resultSet.getString(i);
                            if (resultSet.wasNull()) {
                                builder.appendNull();
                            }
                            else {
                                builder.append(Slices.wrappedBuffer(value.getBytes(UTF_8)));
                            }
                            break;
                        default:
                            throw new AssertionError("unhandled type: " + type);
                    }
                }
                return builder.build();
            }
        };
    }

    private static void insertRows(ConnectorTableMetadata tableMetadata, Handle handle, RecordSet data)
    {
        String vars = Joiner.on(',').join(nCopies(tableMetadata.getColumns().size(), "?"));
        String sql = format("INSERT INTO %s VALUES (%s)", tableMetadata.getTable().getTableName(), vars);

        RecordCursor cursor = data.cursor();
        while (true) {
            // insert 1000 rows at a time
            PreparedBatch batch = handle.prepareBatch(sql);
            for (int row = 0; row < 1000; row++) {
                if (!cursor.advanceNextPosition()) {
                    batch.execute();
                    return;
                }
                PreparedBatchPart part = batch.add();
                for (int column = 0; column < tableMetadata.getColumns().size(); column++) {
                    ColumnMetadata columnMetadata = tableMetadata.getColumns().get(column);
                    switch (columnMetadata.getType()) {
                        case BOOLEAN:
                            part.bind(column, cursor.getBoolean(column));
                            break;
                        case LONG:
                            part.bind(column, cursor.getLong(column));
                            break;
                        case DOUBLE:
                            part.bind(column, cursor.getDouble(column));
                            break;
                        case STRING:
                            part.bind(column, new String(cursor.getString(column), UTF_8));
                            break;
                    }
                }
            }
            batch.execute();
        }
    }

    protected Function<MaterializedTuple, String> onlyColumnGetter()
    {
        return new Function<MaterializedTuple, String>()
        {
            @Override
            public String apply(MaterializedTuple input)
            {
                assertEquals(input.getFieldCount(), 1);
                return (String) input.getField(0);
            }
        };
    }

    private static String getExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return explainer.getPlan(SqlParser.createStatement(query), planType);
    }

    private static String getGraphvizExplainPlan(String query, ExplainType.Type planType)
    {
        QueryExplainer explainer = getQueryExplainer();
        return explainer.getGraphvizPlan(SqlParser.createStatement(query), planType);
    }

    private static QueryExplainer getQueryExplainer()
    {
        Session session = new Session("user", "test", DEFAULT_CATALOG, DEFAULT_SCHEMA, null, null);
        MetadataManager metadata = new MetadataManager();
        metadata.addInternalSchemaMetadata(MetadataManager.INTERNAL_CONNECTOR_ID, new DualMetadata());
        SplitManager splitManager = new SplitManager(ImmutableSet.<ConnectorSplitManager>of(new DualSplitManager(new InMemoryNodeManager())));
        List<PlanOptimizer> optimizers = new PlanOptimizersFactory(metadata, splitManager).get();
        return new QueryExplainer(session, optimizers, metadata, new MockPeriodicImportManager(), new MockStorageManager());
    }
}
