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
package com.facebook.presto.nativetests;

import com.facebook.presto.tests.AbstractTestAggregations;
import org.testng.annotations.Test;

import static java.lang.String.format;

public abstract class AbstractTestAggregationsNative
        extends AbstractTestAggregations
{
    private static final String approxDistributionTypesUnsupportedError = ".*Failed to parse type.*digest.*";

    @Override
    @Test
    public void testApproximateCountDistinct()
    {
        String timeTypeUnsupportedError = "Failed to parse type.*time";
        String charTypeUnsupportedError = "Failed to parse type.*char";
        String signatureUnsupportedError = ".*Aggregate function signature is not supported.*";

        // test NULL
        assertQuery("SELECT approx_distinct(NULL)", "SELECT 0");
        assertQuery("SELECT approx_distinct(NULL, 0.023)", "SELECT 0");

        // test date
        assertQuery("SELECT approx_distinct(orderdate) FROM orders", "SELECT 2372");
        assertQuery("SELECT approx_distinct(orderdate, 0.023) FROM orders", "SELECT 2372");

        // test timestamp
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP)) FROM orders", "SELECT 2347");
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP), 0.023) FROM orders", "SELECT 2347");

        // test timestamp with time zone
        assertQueryFails("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP WITH TIME ZONE)) FROM orders",
                signatureUnsupportedError, true);
        assertQueryFails("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP WITH TIME ZONE), 0.023) FROM orders",
                signatureUnsupportedError, true);

        // test time
        assertQueryFails("SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME)) FROM orders", timeTypeUnsupportedError, true);
        assertQueryFails("SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME), 0.023) FROM orders", timeTypeUnsupportedError, true);

        // test time with time zone
        assertQueryFails("SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME WITH TIME ZONE)) FROM orders", timeTypeUnsupportedError, true);
        assertQueryFails("SELECT approx_distinct(CAST(from_unixtime(custkey) AS TIME WITH TIME ZONE), 0.023) FROM orders", timeTypeUnsupportedError, true);

        // test short decimal
        assertQuery("SELECT approx_distinct(CAST(custkey AS DECIMAL(18, 0))) FROM orders", "SELECT 990");
        assertQuery("SELECT approx_distinct(CAST(custkey AS DECIMAL(18, 0)), 0.023) FROM orders", "SELECT 990");

        // test long decimal
        assertQuery("SELECT approx_distinct(CAST(custkey AS DECIMAL(25, 20))) FROM orders", "SELECT 1013");
        assertQuery("SELECT approx_distinct(CAST(custkey AS DECIMAL(25, 20)), 0.023) FROM orders", "SELECT 1013");

        // test real
        assertQuery("SELECT approx_distinct(CAST(custkey AS REAL)) FROM orders", "SELECT 982");
        assertQuery("SELECT approx_distinct(CAST(custkey AS REAL), 0.023) FROM orders", "SELECT 982");

        // test bigint
        assertQuery("SELECT approx_distinct(custkey) FROM orders", "SELECT 990");
        assertQuery("SELECT approx_distinct(custkey, 0.023) FROM orders", "SELECT 990");

        // test integer
        assertQuery("SELECT approx_distinct(CAST(custkey AS INTEGER)) FROM orders", "SELECT 1028");
        assertQuery("SELECT approx_distinct(CAST(custkey AS INTEGER), 0.023) FROM orders", "SELECT 1028");

        // test smallint
        assertQuery("SELECT approx_distinct(CAST(custkey AS SMALLINT)) FROM orders", "SELECT 1023");
        assertQuery("SELECT approx_distinct(CAST(custkey AS SMALLINT), 0.023) FROM orders", "SELECT 1023");

        // test tinyint
        assertQuery("SELECT approx_distinct(CAST((custkey % 128) AS TINYINT)) FROM orders", "SELECT 128");
        assertQuery("SELECT approx_distinct(CAST((custkey % 128) AS TINYINT), 0.023) FROM orders", "SELECT 128");

        // test double
        assertQuery("SELECT approx_distinct(CAST(custkey AS DOUBLE)) FROM orders", "SELECT 1014");
        assertQuery("SELECT approx_distinct(CAST(custkey AS DOUBLE), 0.023) FROM orders", "SELECT 1014");

        // test varchar
        assertQuery("SELECT approx_distinct(CAST(custkey AS VARCHAR)) FROM orders", "SELECT 1036");
        assertQuery("SELECT approx_distinct(CAST(custkey AS VARCHAR), 0.023) FROM orders", "SELECT 1036");

        // test char
        assertQueryFails("SELECT approx_distinct(CAST(CAST(custkey AS VARCHAR) AS CHAR(20))) FROM orders", charTypeUnsupportedError, true);
        assertQueryFails("SELECT approx_distinct(CAST(CAST(custkey AS VARCHAR) AS CHAR(20)), 0.023) FROM orders", charTypeUnsupportedError, true);

        // test varbinary
        assertQuery("SELECT approx_distinct(to_utf8(CAST(custkey AS VARCHAR))) FROM orders", "SELECT 1036");
        assertQuery("SELECT approx_distinct(to_utf8(CAST(custkey AS VARCHAR)), 0.023) FROM orders", "SELECT 1036");
    }

    @Override
    @Test(dataProvider = "getType")
    public void testStatisticalDigest(String type)
    {
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE)), 0.5E0) > 0 FROM lineitem", type), approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE)), 0.5E0) > 0 FROM lineitem", type), approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE)), 0.5E0) > 0 FROM lineitem", type), approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE), 2), 0.5E0) > 0 FROM lineitem", type), approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 3), 0.5E0) > 0 FROM lineitem", type), approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 4), 0.5E0) > 0 FROM lineitem", type), approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE), 2, 0.0001E0), 0.5E0) > 0 FROM lineitem", type), approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 3, 0.0001E0), 0.5E0) > 0 FROM lineitem", type), approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 4, 0.0001E0), 0.5E0) > 0 FROM lineitem", type), approxDistributionTypesUnsupportedError, true);
    }

    /**
     * Comprehensive correctness testing is done in the TestQuantileDigestAggregationFunction and TestTDigestAggregationFunction
     */
    @Override
    @Test(dataProvider = "getType")
    public void testStatisticalDigestGroupBy(String type)
    {
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE)), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE)), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE)), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE), 2), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 3), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 4), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE), 2, 0.0001E0), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 3, 0.0001E0), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                approxDistributionTypesUnsupportedError, true);
        assertQueryFails(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 4, 0.0001E0), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                approxDistributionTypesUnsupportedError, true);
    }

    /**
     * Comprehensive correctness testing is done in the TestMergeQuantileDigestFunction and TestMergeTDigestFunction
     */
    @Override
    @Test(dataProvider = "getType")
    public void testStatisticalDigestMerge(String type)
    {
        assertQueryFails(format("SELECT value_at_quantile(merge(%s), 0.5E0) > 0 FROM (SELECT partkey, %s_agg(CAST(orderkey AS DOUBLE)) as %s FROM lineitem GROUP BY partkey)",
                        type,
                        type,
                        type),
                approxDistributionTypesUnsupportedError, true);
    }

    /**
     * Comprehensive correctness testing is done in the TestMergeQuantileDigestFunction and TestMergeTDigestFunction
     */
    @Override
    @Test(dataProvider = "getType")
    public void testStatisticalDigestMergeGroupBy(String type)
    {
        assertQueryFails(format("SELECT partkey, value_at_quantile(merge(%s), 0.5E0) > 0 " +
                                "FROM (SELECT partkey, suppkey, %s_agg(CAST(orderkey AS DOUBLE)) as %s FROM lineitem GROUP BY partkey, suppkey)" +
                                "GROUP BY partkey",
                        type,
                        type,
                        type),
                approxDistributionTypesUnsupportedError, true);
    }

    /// See issue for details: https://github.com/prestodb/presto/issues/20909
    @Override
    @Test
    public void testSumDataSizeForStats()
    {
        // varchar
        assertQuery("SELECT \"sum_data_size_for_stats\"(comment) FROM orders", "SELECT 787364");

        // char
        // Presto removes trailing whitespaces when casting to CHAR.
        // Hard code the expected data size since there is no easy to way to compute it in H2.
        assertQueryFails("SELECT \"sum_data_size_for_stats\"(CAST(comment AS CHAR(1000))) FROM orders",
                "Failed to parse type \\[char\\(1000\\)]", true);

        // varbinary
        assertQuery("SELECT \"sum_data_size_for_stats\"(CAST(comment AS VARBINARY)) FROM orders", "SELECT 787364");

        // array
        assertQuery("SELECT \"sum_data_size_for_stats\"(ARRAY[comment]) FROM orders", "SELECT 847364");
        assertQuery("SELECT \"sum_data_size_for_stats\"(ARRAY[comment, comment]) FROM orders", "SELECT 1634728");

        // map
        assertQuery("SELECT \"sum_data_size_for_stats\"(map(ARRAY[1], ARRAY[comment])) FROM orders", "SELECT 907364");
        assertQuery("SELECT \"sum_data_size_for_stats\"(map(ARRAY[1, 2], ARRAY[comment, comment])) FROM orders", "SELECT 1754728");

        // row
        assertQuery("SELECT \"sum_data_size_for_stats\"(ROW(comment)) FROM orders", "SELECT 847364");
        assertQuery("SELECT \"sum_data_size_for_stats\"(ROW(comment, comment)) FROM orders", "SELECT 1634728");
    }

    /// See issue for details: https://github.com/prestodb/presto/issues/20909
    @Override
    @Test
    public void testMaxDataSizeForStats()
    {
        // varchar
        assertQuery("SELECT \"max_data_size_for_stats\"(comment) FROM orders", "select 82");

        // char
        assertQueryFails("SELECT \"max_data_size_for_stats\"(CAST(comment AS CHAR(1000))) FROM orders",
                "Failed to parse type \\[char\\(1000\\)]", true);

        // varbinary
        assertQuery("SELECT \"max_data_size_for_stats\"(CAST(comment AS VARBINARY)) FROM orders", "select 82");

        // max_data_size_for_stats is not needed for array, map and row
    }
}
