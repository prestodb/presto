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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_DISTINCT_AGGREGATIONS;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersHll;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestNativeAggregations
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createOrders(queryRunner);
        createOrdersHll(queryRunner);
        createOrdersEx(queryRunner);
        createNation(queryRunner);
        createRegion(queryRunner);
    }

    @Test
    public void testAggregations()
    {
        assertQuery("SELECT count(*) FROM nation");
        assertQuery("SELECT regionkey, count(*) FROM nation GROUP BY regionkey");

        assertQuery("SELECT avg(discount), avg(quantity) FROM lineitem");
        assertQuery(
                "SELECT linenumber, avg(discount), avg(quantity) FROM lineitem GROUP BY linenumber");

        assertQuery("SELECT sum(totalprice) FROM orders");
        assertQuery("SELECT orderpriority, sum(totalprice) FROM orders GROUP BY orderpriority");

        assertQuery("SELECT custkey, min(totalprice), max(orderkey) FROM orders GROUP BY custkey");

        assertQuery("SELECT bitwise_and_agg(orderkey), bitwise_and_agg(suppkey), bitwise_or_agg(partkey), bitwise_or_agg(linenumber) FROM lineitem");
        assertQuery("SELECT orderkey, bitwise_and_agg(orderkey), bitwise_and_agg(suppkey) FROM lineitem GROUP BY orderkey");
        assertQuery("SELECT bitwise_and_agg(custkey), bitwise_or_agg(orderkey) FROM orders");
        assertQuery("SELECT shippriority, bitwise_and_agg(orderkey), bitwise_or_agg(custkey) FROM orders GROUP BY shippriority");

        assertQuery("SELECT sum(custkey), clerk FROM orders GROUP BY clerk HAVING sum(custkey) > 10000");

        assertQuery("SELECT orderkey, array_sort(array_agg(linenumber)) FROM lineitem GROUP BY 1");
        assertQuery("SELECT orderkey, map_agg(linenumber, discount) FROM lineitem GROUP BY 1");

        assertQuery("SELECT array_agg(nationkey ORDER BY name) FROM nation");
        assertQuery("SELECT orderkey, array_agg(quantity ORDER BY linenumber DESC) FROM lineitem GROUP BY 1");

        assertQuery("SELECT array_sort(map_keys(map_union(quantity_by_linenumber))) FROM orders_ex");

        assertQuery("SELECT orderkey, count_if(linenumber % 2 > 0) FROM lineitem GROUP BY 1");
        assertQuery("SELECT orderkey, bool_and(linenumber % 2 = 1) FROM lineitem GROUP BY 1");
        assertQuery("SELECT orderkey, bool_or(linenumber % 2 = 0) FROM lineitem GROUP BY 1");

        assertQuery("SELECT linenumber = 2 AND quantity > 10, sum(quantity / 7) FROM lineitem GROUP BY 1");

        assertQuerySucceeds("SELECT approx_percentile(totalprice, 0.25) FROM orders");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, orderkey, 0.25) FROM orders");
        assertQuerySucceeds("SELECT clerk, approx_percentile(totalprice, 0.25) FROM orders GROUP BY 1");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, 0.25, 0.005) FROM orders");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, orderkey, 0.25, 0.005) FROM orders");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, 0.25), approx_percentile(totalprice, 0.5) FROM orders");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, orderkey, 0.25), approx_percentile(totalprice, orderkey, 0.5) FROM orders");
        assertQuerySucceeds("SELECT clerk, approx_percentile(totalprice, 0.25), approx_percentile(totalprice, 0.5) FROM orders GROUP BY 1");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, 0.25, 0.005), approx_percentile(totalprice, 0.5, 0.005) FROM orders");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, orderkey, 0.25, 0.005), approx_percentile(totalprice, orderkey, 0.5, 0.005) FROM orders");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, ARRAY[0.25, 0.5]) FROM orders");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, orderkey, ARRAY[0.25, 0.5]) FROM orders");
        assertQuerySucceeds("SELECT clerk, approx_percentile(totalprice, ARRAY[0.25, 0.5]) FROM orders GROUP BY 1");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, ARRAY[0.25, 0.5], 0.005) FROM orders");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, orderkey, ARRAY[0.25, 0.5], 0.005) FROM orders");

        // count is not using any channel or mask.
        // sum1 and sum3 are using different channels, but the same mask.
        // sum2 and sum1 are using the same channel, but different masks.
        assertQuery("SELECT count(1), sum(IF(linenumber = 7, partkey)), sum(IF(linenumber = 5, partkey)), sum(IF(linenumber = 7, orderkey)) FROM lineitem");
        assertQuery("SELECT count(1), sum(partkey) FILTER (where linenumber = 7), sum(partkey) FILTER (where linenumber = 5), sum(orderkey) FILTER (where linenumber = 7) FROM lineitem");
        assertQuery("SELECT shipmode, count(1), sum(IF(linenumber = 7, partkey)), sum(IF(linenumber = 5, partkey)), sum(IF(linenumber = 7, orderkey)) FROM lineitem group by 1");
        assertQuery("SELECT shipmode, count(1), sum(partkey) FILTER (where linenumber = 7), sum(partkey) FILTER (where linenumber = 5), sum(orderkey) FILTER (where linenumber = 7) FROM lineitem group by 1");

        // distinct limit
        assertQueryResultCount("SELECT orderkey FROM lineitem GROUP BY 1 LIMIT 17", 17);

        // aggregation with no grouping keys and no aggregates
        assertQuery("with a as (select sum(nationkey) from nation) select x from a, unnest(array[1, 2,3]) as t(x)");
    }

    @Test
    public void testGroupingSets()
    {
        assertQuery("SELECT orderstatus, orderpriority, count(1), min(orderkey) FROM orders GROUP BY GROUPING SETS ((orderstatus), (orderpriority))");
        assertQuery("SELECT orderstatus, orderpriority, count(1), min(orderkey) FROM orders GROUP BY CUBE (orderstatus, orderpriority)");
        assertQuery("SELECT orderstatus, orderpriority, count(1), min(orderkey) FROM orders GROUP BY ROLLUP (orderstatus, orderpriority)");

        // With grouping expression.
        assertQuery("SELECT orderstatus, orderpriority, grouping(orderstatus), grouping(orderpriority), grouping(orderstatus, orderpriority), count(1), min(orderkey) FROM orders GROUP BY GROUPING SETS ((orderstatus), (orderpriority))");
        assertQuery("SELECT orderstatus, orderpriority, grouping(orderstatus), grouping(orderpriority), grouping(orderstatus, orderpriority), count(1), min(orderkey) FROM orders GROUP BY CUBE (orderstatus, orderpriority)");
        assertQuery("SELECT orderstatus, orderpriority, grouping(orderstatus), grouping(orderpriority), grouping(orderstatus, orderpriority), count(1), min(orderkey) FROM orders GROUP BY ROLLUP (orderstatus, orderpriority)");

        // With aliased columns.
        assertQuery("SELECT lna, lnb, SUM(quantity) FROM (SELECT linenumber lna, linenumber lnb, CAST(quantity AS BIGINT) quantity FROM lineitem) GROUP BY GROUPING SETS ((lna, lnb), (lna), (lnb), ())");
    }

    @Test
    public void testMixedDistinctAggregations()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_DISTINCT_AGGREGATIONS, "true")
                .build();
        assertQuery(session, "SELECT count(orderkey), count(DISTINCT orderkey) FROM orders");
        assertQuery(session, "SELECT max(orderstatus), COUNT(orderkey), sum(DISTINCT orderkey) FROM orders");
    }

    @Test
    public void testEmptyGroupingSets()
    {
        // Returns  a single row with the global aggregation.
        assertQuery("SELECT count(orderkey) FROM orders WHERE orderkey < 0 GROUP BY GROUPING SETS (())");

        // Returns 2 rows with global aggregation for the global grouping sets.
        assertQuery("SELECT count(orderkey) FROM orders WHERE orderkey < 0 GROUP BY GROUPING SETS ((), ())");

        // Returns a single row with the global aggregation. There are no rows for the orderkey group.
        assertQuery("SELECT count(orderkey) FROM orders WHERE orderkey < 0 GROUP BY GROUPING SETS ((orderkey), ())");

        // This is a shorthand for the above query. Returns a single row with the global aggregation.
        assertQuery("SELECT count(orderkey) FROM orders WHERE orderkey < 0 GROUP BY CUBE (orderkey)");

        assertQuery("SELECT count(orderkey) FROM orders WHERE orderkey < 0 GROUP BY ROLLUP (orderkey)");

        // Returns a single row with NULL orderkey.
        assertQuery("SELECT orderkey FROM orders WHERE orderkey < 0 GROUP BY CUBE (orderkey)");

        assertQuery("SELECT orderkey FROM orders WHERE orderkey < 0 GROUP BY ROLLUP (orderkey)");
    }

    @Test
    public void testStreamingAggregation()
    {
        assertQuery("SELECT name, (SELECT max(name) FROM region WHERE regionkey = nation.regionkey AND length(name) > length(nation.name)) FROM nation");
    }

    @Test
    public void testApproxDistinct()
    {
        // low cardinality -> expect exact results
        assertQuery("SELECT approx_distinct(linenumber) FROM lineitem");
        assertQuery("SELECT orderkey, approx_distinct(linenumber) FROM lineitem GROUP BY 1");

        // high cardinality -> results may not be exact
        assertQuerySucceeds("SELECT approx_distinct(orderkey) FROM lineitem");
        assertQuerySucceeds("SELECT linenumber, approx_distinct(orderkey) FROM lineitem GROUP BY 1");

        // approx_set + cardinality
        assertQuery("SELECT cardinality(approx_set(linenumber)) FROM lineitem");
        assertQuery("SELECT orderkey, cardinality(approx_set(linenumber)) FROM lineitem GROUP BY 1");

        // Verify that Velox can read HLL binaries written by Java Presto.
        assertQuery("SELECT cardinality(cast(hll as hyperloglog)) FROM orders_hll");
        assertQuery("SELECT cardinality(merge(cast(hll as hyperloglog))) FROM orders_hll");
    }

    @Test
    public void testApproxMostFrequent()
    {
        assertQuery("SELECT approx_most_frequent(3, linenumber, 1000) FROM lineitem");
        assertQuerySucceeds("SELECT orderkey, approx_most_frequent(3, linenumber, 10) FROM lineitem GROUP BY 1");
        assertQuerySucceeds("SELECT approx_most_frequent(3, orderkey, 1000) FROM lineitem");
        assertQuerySucceeds("SELECT linenumber, approx_most_frequent(3, orderkey, 10) FROM lineitem GROUP BY 1");
    }

    @Test
    public void testSum()
    {
        // tinyint
        assertQuery("SELECT sum(cast(linenumber as tinyint)), sum(cast(linenumber as tinyint)) FROM lineitem");
        // smallint
        assertQuery("SELECT sum(cast(linenumber as smallint)), sum(cast(linenumber as smallint)) FROM lineitem");
        // integer
        assertQuery("SELECT sum(linenumber), sum(linenumber) FROM lineitem");
        // bigint
        assertQuery("SELECT sum(orderkey), sum(orderkey) FROM lineitem");
        // real
        assertQuery("SELECT sum(tax_as_real), sum(tax_as_real) FROM lineitem");
        // double
        assertQuery("SELECT sum(quantity), sum(quantity) FROM lineitem");
    }

    @Test
    public void testMinMax()
    {
        // tinyint
        assertQuery("SELECT min(cast(linenumber as tinyint)), max(cast(linenumber as tinyint)) FROM lineitem");
        assertQuery("SELECT min(cast(linenumber as tinyint), 2), max(cast(linenumber as tinyint), 3) FROM lineitem");
        // smallint
        assertQuery("SELECT min(cast(linenumber as smallint)), max(cast(linenumber as smallint)) FROM lineitem");
        assertQuery("SELECT min(cast(linenumber as smallint), 2), max(cast(linenumber as smallint), 3) FROM lineitem");
        // integer
        assertQuery("SELECT min(linenumber), max(linenumber) FROM lineitem");
        assertQuery("SELECT min(linenumber, 3), max(linenumber, 2) FROM lineitem");
        // bigint
        assertQuery("SELECT min(orderkey), max(orderkey) FROM lineitem");
        assertQuery("SELECT min(orderkey, 10), max(orderkey, 100) FROM lineitem");
        // real
        assertQuery("SELECT min(cast(quantity as real)), max(cast(quantity as real)) FROM lineitem");
        assertQuery("SELECT min(cast(quantity as real), 7), max(cast(quantity as real), 5) FROM lineitem");
        // double
        assertQuery("SELECT min(quantity), max(quantity) FROM lineitem");
        assertQuery("SELECT min(quantity, 8), max(quantity, 6) FROM lineitem");
        // timestamp
        assertQuery("SELECT min(from_unixtime(orderkey)), max(from_unixtime(orderkey)) FROM lineitem");
        assertQueryFails("SELECT min(from_unixtime(orderkey), 2), max(from_unixtime(orderkey), 3) FROM lineitem",
                ".*Aggregate function signature is not supported.*");
        // Commitdate is cast to date here since the original commitdate column read from lineitem in dwrf format is
        // of type char. The cast to date can be removed for Parquet which has date support.
        assertQuery("SELECT min(cast(commitdate as date)), max(cast(commitdate as date)) FROM lineitem");
        assertQueryFails("SELECT min(cast(commitdate as date), 2), max(cast(commitdate as date), 3) FROM lineitem",
                ".*Aggregate function signature is not supported.*");
    }

    @Test
    public void testMinMaxBy()
    {
        // We use filters to make queries deterministic.
        assertQuery("SELECT max_by(partkey, orderkey), max_by(quantity, orderkey), max_by(tax_as_real, orderkey) FROM lineitem where shipmode='MAIL'");
        assertQuery("SELECT min_by(partkey, orderkey), min_by(quantity, orderkey), min_by(tax_as_real, orderkey) FROM lineitem where shipmode='MAIL'");

        assertQuery("SELECT max_by(orderkey, extendedprice), max_by(orderkey, cast(extendedprice as REAL)) FROM lineitem");
        assertQuery("SELECT min_by(orderkey, extendedprice), min_by(orderkey, cast(extendedprice as REAL)) FROM lineitem where shipmode='MAIL'");

        // 3 argument variant of max_by, min_by
        assertQuery("SELECT max_by(orderkey, linenumber, 5), min_by(orderkey, linenumber, 5) FROM lineitem GROUP BY orderkey");

        // Non-numeric arguments
        assertQuery("SELECT max_by(row(orderkey, custkey), orderkey, 5), min_by(row(orderkey, custkey), orderkey, 5) FROM orders");
        assertQuery("SELECT max_by(row(orderkey, linenumber), linenumber, 5), min_by(row(orderkey, linenumber), linenumber, 5) FROM lineitem GROUP BY orderkey");
        assertQuery("SELECT orderkey, MAX_BY(v, c, 5), MIN_BY(v, c, 5) FROM " +
                "(SELECT orderkey, 'This is a long line ' || CAST(orderkey AS VARCHAR) AS v, 'This is also a really long line ' || CAST(linenumber AS VARCHAR) AS c FROM lineitem) " +
                "GROUP BY 1");
    }

    @Test
    public void testStdDev()
    {
        // tinyint
        assertQuery("SELECT stddev(linenumber_as_tinyint), stddev_pop(linenumber_as_tinyint), stddev_samp(linenumber_as_tinyint) FROM lineitem");
        // smallint
        assertQuery("SELECT stddev(linenumber_as_smallint), stddev_pop(linenumber_as_smallint), stddev_samp(linenumber_as_smallint) FROM lineitem");
        // integer
        assertQuery("SELECT stddev(linenumber), stddev_pop(linenumber), stddev_samp(linenumber) FROM lineitem");
        // bigint
        assertQuery("SELECT stddev(orderkey), stddev_pop(orderkey), stddev_samp(orderkey) FROM lineitem");
        // real
        assertQuery("SELECT stddev(tax_as_real), stddev_pop(tax_as_real), stddev_samp(tax_as_real) FROM lineitem");
        // double
        assertQuery("SELECT stddev(tax), stddev_pop(tax), stddev_samp(tax) FROM lineitem");
    }

    @Test
    public void testVariance()
    {
        // tinyint
        assertQuery("SELECT variance(linenumber_as_tinyint), var_pop(linenumber_as_tinyint), var_samp(linenumber_as_tinyint) FROM lineitem");
        // smallint
        assertQuery("SELECT variance(linenumber_as_smallint), var_pop(linenumber_as_smallint), var_samp(linenumber_as_smallint) FROM lineitem");
        // integer
        assertQuery("SELECT variance(linenumber), var_pop(linenumber), var_samp(linenumber) FROM lineitem");
        // bigint
        assertQuery("SELECT variance(orderkey), var_pop(orderkey), var_samp(orderkey) FROM lineitem");
        // real
        assertQuery("SELECT variance(tax_as_real), var_pop(tax_as_real), var_samp(tax_as_real) FROM lineitem");
        // double
        assertQuery("SELECT variance(tax), var_pop(tax), var_samp(tax) FROM lineitem");
    }

    @Test
    public void testCovariance()
    {
        // real
        assertQuery("SELECT corr(tax_as_real, discount_as_real), covar_pop(tax_as_real, discount_as_real), covar_samp(tax_as_real, discount_as_real) FROM lineitem");
        assertQuery("SELECT linenumber, corr(tax_as_real, discount_as_real), covar_pop(tax_as_real, discount_as_real), covar_samp(tax_as_real, discount_as_real) FROM lineitem GROUP BY 1");
        assertQuery("SELECT orderkey, round(corr(tax_as_real, discount_as_real), 1), round(covar_pop(tax_as_real, discount_as_real), 1), round(covar_samp(tax_as_real, discount_as_real), 1) FROM lineitem GROUP BY 1");

        // double
        assertQuery("SELECT corr(tax, extendedprice), covar_pop(tax, extendedprice), covar_samp(tax, extendedprice) FROM lineitem");
        assertQuery("SELECT linenumber, corr(tax, extendedprice), covar_pop(tax, extendedprice), covar_samp(tax, extendedprice) FROM lineitem GROUP BY 1");
        assertQuery("SELECT orderkey, round(corr(tax, extendedprice), 1), round(covar_pop(tax, extendedprice), 1), round(covar_samp(tax, extendedprice), 1) FROM lineitem GROUP BY 1");
    }

    @Test
    public void testChecksum()
    {
        assertQuery("SELECT checksum(v) FROM (VALUES 1.0, 3.0, 5.0, NULL ) as t (v)");
        assertQuery("SELECT checksum(orderkey) FROM lineitem WHERE orderkey < 2");
        assertQuery("SELECT checksum(orderkey) FROM lineitem WHERE orderkey  = -1");
        assertQuery("SELECT checksum(orderkey) FROM lineitem");
        assertQuery("SELECT checksum(extendedprice) FROM lineitem where orderkey < 20");
        assertQuery("SELECT checksum(shipdate) FROM lineitem");
        assertQuery("SELECT checksum(comment) FROM lineitem");
        assertQuery("SELECT checksum(quantities) FROM orders_ex");
        assertQuery("SELECT checksum(quantity_by_linenumber) FROM orders_ex");
        assertQuery("SELECT shipmode, checksum(extendedprice) FROM lineitem GROUP BY shipmode");
        assertQuery("SELECT checksum(from_unixtime(orderkey, '+01:00')) FROM lineitem WHERE orderkey < 20");

        // test DECIMAL data
        assertQuery("SELECT checksum(a), checksum(b) FROM (VALUES (DECIMAL '1.234', DECIMAL '611180549424.4633133')) AS t(a, b)");
        assertQuery("SELECT checksum(a), checksum(b) FROM (VALUES (DECIMAL '1.234', DECIMAL '611180549424.4633133'), (NULL, NULL)) AS t(a, b)");
        assertQuery("SELECT checksum(a), checksum(b) FROM (VALUES (DECIMAL '1.234', CAST('2343331593029422743' AS DECIMAL(38, 0))), (CAST('999999999999999999' AS DECIMAL(18, 0)), CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0)))) AS t(a, b)");
        assertQuery("SELECT checksum(a), checksum(b) FROM (VALUES (CAST('999999999999999999' AS DECIMAL(18, 0)), CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), (CAST('-999999999999999999' as DECIMAL(18, 0)), CAST('-99999999999999999999999999999999999999' AS DECIMAL(38, 0)))) AS t(a, b)");
    }

    @Test
    public void testArbitrary()
    {
        // Non-deterministic queries
        assertQuerySucceeds("SELECT orderkey, arbitrary(comment) FROM lineitem GROUP BY 1");
        assertQuerySucceeds("SELECT orderkey, arbitrary(discount) FROM lineitem GROUP BY 1");
        assertQuerySucceeds("SELECT orderkey, arbitrary(linenumber) FROM lineitem GROUP BY 1");
        assertQuerySucceeds("SELECT orderkey, arbitrary(linenumber_as_smallint) FROM lineitem GROUP BY 1");
        assertQuerySucceeds("SELECT orderkey, arbitrary(linenumber_as_tinyint) FROM lineitem GROUP BY 1");
        assertQuerySucceeds("SELECT orderkey, arbitrary(tax_as_real) FROM lineitem GROUP BY 1");
    }

    @Test
    public void testMultiMapAgg()
    {
        assertQuery("SELECT orderkey, multimap_agg(linenumber % 3, discount) FROM lineitem GROUP BY 1");
    }

    @Test
    public void testMarkDistinct()
    {
        assertQuery("SELECT count(distinct orderkey), count(distinct linenumber) FROM lineitem");
        assertQuery("SELECT orderkey, count(distinct comment), sum(distinct linenumber) FROM lineitem GROUP BY 1");
    }

    @Test
    public void testDistinct()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("use_mark_distinct", "falze")
                .build();
        assertQuery(session, "SELECT count(distinct orderkey), count(distinct linenumber) FROM lineitem");
        assertQuery(session, "SELECT count(distinct orderkey), sum(distinct linenumber), array_sort(array_agg(distinct linenumber)) FROM lineitem");
        assertQueryFails(session, "SELECT count(distinct orderkey), array_agg(distinct linenumber ORDER BY linenumber) FROM lineitem",
                ".*Aggregations over sorted unique values are not supported yet");
    }

    @Test
    public void testReduceAgg()
    {
        assertQuery("SELECT reduce_agg(orderkey, 0, (x, y) -> x + y, (x, y) -> x + y) FROM orders");
        assertQuery("SELECT orderkey, reduce_agg(linenumber, 0, (x, y) -> x + y, (x, y) -> x + y) FROM lineitem GROUP BY orderkey");
        assertQuery("SELECT orderkey, array_sort(reduce_agg(linenumber, array[], (s, x) -> s || x, (s, s2) -> s || s2)) FROM lineitem GROUP BY orderkey");
    }

    private void assertQueryResultCount(String sql, int expectedResultCount)
    {
        assertEquals(getQueryRunner().execute(sql).getRowCount(), expectedResultCount);
    }
}
