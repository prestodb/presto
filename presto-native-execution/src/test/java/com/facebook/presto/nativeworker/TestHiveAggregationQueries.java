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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestHiveAggregationQueries
        extends AbstractTestHiveQueries
{
    public TestHiveAggregationQueries()
    {
        super(true);
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

        assertQuery("SELECT sum(totalprice), clerk FROM orders GROUP BY clerk HAVING sum(totalprice) > 1532544.84");

        // non-deterministic query
        assertQuerySucceeds("SELECT orderkey, arbitrary(comment) FROM lineitem GROUP BY 1");

        assertQuery("SELECT orderkey, array_agg(linenumber) FROM lineitem GROUP BY 1");
        assertQuery("SELECT orderkey, map_agg(linenumber, discount) FROM lineitem GROUP BY 1");
        assertQuery("SELECT orderkey, count_if(linenumber % 2 > 0) FROM lineitem GROUP BY 1");
        assertQuery("SELECT orderkey, bool_and(linenumber % 2 = 1) FROM lineitem GROUP BY 1");
        assertQuery("SELECT orderkey, bool_or(linenumber % 2 = 0) FROM lineitem GROUP BY 1");

        assertQuery("SELECT linenumber = 2 AND quantity > 10, sum(quantity / 7) FROM lineitem GROUP BY 1");

        // TODO Add queries with arbitrary(integer/smallint/tinyint), min_by and max_by when these get fixed.

        assertQuerySucceeds("SELECT approx_percentile(totalprice, 0.25), approx_percentile(totalprice, 0.5) FROM orders");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, orderkey, 0.25), approx_percentile(totalprice, orderkey, 0.5) FROM orders");
        assertQuerySucceeds("SELECT clerk, approx_percentile(totalprice, 0.25), approx_percentile(totalprice, 0.5) FROM orders GROUP BY 1");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, 0.25, 0.005), approx_percentile(totalprice, 0.5, 0.005) FROM orders");
        assertQuerySucceeds("SELECT approx_percentile(totalprice, orderkey, 0.25, 0.005), approx_percentile(totalprice, orderkey, 0.5, 0.005) FROM orders");

        // count is not using any channel or mask.
        // sum1 and sum3 are using different channels, but the same mask.
        // sum2 and sum1 are using the same channel, but different masks.
        assertQuery("SELECT count(1), sum(IF(linenumber = 7, partkey)), sum(IF(linenumber = 5, partkey)), sum(IF(linenumber = 7, orderkey)) FROM lineitem");
        assertQuery("SELECT count(1), sum(partkey) FILTER (where linenumber = 7), sum(partkey) FILTER (where linenumber = 5), sum(orderkey) FILTER (where linenumber = 7) FROM lineitem");
        assertQuery("SELECT shipmode, count(1), sum(IF(linenumber = 7, partkey)), sum(IF(linenumber = 5, partkey)), sum(IF(linenumber = 7, orderkey)) FROM lineitem group by 1");
        assertQuery("SELECT shipmode, count(1), sum(partkey) FILTER (where linenumber = 7), sum(partkey) FILTER (where linenumber = 5), sum(orderkey) FILTER (where linenumber = 7) FROM lineitem group by 1");

        // distinct limit
        assertQueryResultCount("SELECT orderkey FROM lineitem GROUP BY 1 LIMIT 17", 17);
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
        // smallint
        assertQuery("SELECT min(cast(linenumber as smallint)), max(cast(linenumber as smallint)) FROM lineitem");
        // integer
        assertQuery("SELECT min(linenumber), max(linenumber) FROM lineitem");
        // bigint
        assertQuery("SELECT min(orderkey), max(orderkey) FROM lineitem");
        // real
        assertQuery("SELECT min(cast(quantity as real)), max(cast(quantity as real)) FROM lineitem");
        // double
        assertQuery("SELECT min(quantity), max(quantity) FROM lineitem");
        // timestamp
        assertQuery("SELECT min(from_unixtime(orderkey)), max(from_unixtime(orderkey)) FROM lineitem");
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
    }

    private void assertQueryResultCount(String sql, int expectedResultCount)
    {
        assertEquals(getQueryRunner().execute(sql).getRowCount(), expectedResultCount);
    }
}
