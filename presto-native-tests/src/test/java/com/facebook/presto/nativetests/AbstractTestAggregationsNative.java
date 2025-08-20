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
    private static final String QDIGEST_TYPE = "qdigest";

    private String storageFormat;
    private boolean implicitCastCharNToVarchar;
    private String approxDistinctUnsupportedSignatureError;
    private String charTypeUnsupportedError;
    private String timeTypeUnsupportedError;

    public void init(String storageFormat, boolean charNToVarcharImplicitCast, boolean sidecarEnabled)
    {
        this.storageFormat = storageFormat;
        this.implicitCastCharNToVarchar = charNToVarcharImplicitCast;
        if (sidecarEnabled) {
            charTypeUnsupportedError = ".*Unknown type: char.*";
            timeTypeUnsupportedError = ".*Unknown type: time.*";
            approxDistinctUnsupportedSignatureError = ".*Unexpected parameters \\(timestamp with time zone.*\\) for function.*";
        }
        else {
            charTypeUnsupportedError = "Failed to parse type.*char";
            timeTypeUnsupportedError = "Failed to parse type.*time";
            approxDistinctUnsupportedSignatureError = ".*Aggregate function signature is not supported.*";
        }
    }

    /// `approx_distinct` aggregate function returns a different value for certain datatypes in Presto C++, see this
    /// issue for more details: https://github.com/facebookincubator/velox/issues/9761.
    /// `approx_distinct` does not support arguments of type `TIMESTAMP WITH TIME ZONE` in Presto C++, see this issue
    /// for more details: https://github.com/prestodb/presto/issues/24815.
    /// Presto C++ does not support datatypes `CHAR` and `TIME`, see:
    /// https://github.com/prestodb/presto/blob/master/presto-docs/src/main/sphinx/presto_cpp/limitations.rst.
    @Override
    @Test
    public void testApproximateCountDistinct()
    {
        // test NULL
        assertQuery("SELECT approx_distinct(NULL)", "SELECT 0");
        assertQuery("SELECT approx_distinct(NULL, 0.023)", "SELECT 0");

        // test date
        String orderdate = storageFormat.equals("DWRF") ? "cast(orderdate as DATE)" : "orderdate";
        assertQuery(format("SELECT approx_distinct(%s) FROM orders", orderdate), "SELECT 2372");
        assertQuery(format("SELECT approx_distinct(%s, 0.023) FROM orders", orderdate), "SELECT 2372");

        // test timestamp
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP)) FROM orders", "SELECT 2347");
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP), 0.023) FROM orders", "SELECT 2347");

        // test timestamp with time zone
        assertQueryFails("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP WITH TIME ZONE)) FROM orders",
                approxDistinctUnsupportedSignatureError, true);
        assertQueryFails("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP WITH TIME ZONE), 0.023) FROM orders",
                approxDistinctUnsupportedSignatureError, true);

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
        String charQuery = "SELECT approx_distinct(CAST(CAST(custkey AS VARCHAR) AS CHAR(20))) FROM orders";
        String charWithErrorQuery = "SELECT approx_distinct(CAST(CAST(custkey AS VARCHAR) AS CHAR(20)), 0.023) FROM orders";
        if (implicitCastCharNToVarchar) {
            assertQuery(charQuery, "SELECT 1036");
            assertQuery(charWithErrorQuery, "SELECT 1036");
        }
        else {
            assertQueryFails(charQuery, charTypeUnsupportedError, true);
            assertQueryFails(charWithErrorQuery, charTypeUnsupportedError, true);
        }

        // test varbinary
        assertQuery("SELECT approx_distinct(to_utf8(CAST(custkey AS VARCHAR))) FROM orders", "SELECT 1036");
        assertQuery("SELECT approx_distinct(to_utf8(CAST(custkey AS VARCHAR)), 0.023) FROM orders", "SELECT 1036");
    }

    /// `sum_data_size_for_stats` returns a different value for `Varchar` and `Varbinary` datatypes in Presto C++, see:
    /// https://github.com/prestodb/presto/issues/20909. `CHAR` datatype is not supported in Presto C++, see issue:
    /// https://github.com/prestodb/presto/issues/21332.
    @Override
    @Test
    public void testSumDataSizeForStats()
    {
        // varchar
        assertQuery("SELECT \"sum_data_size_for_stats\"(comment) FROM orders", "SELECT 787364");

        // char
        String charQuery = "SELECT \"sum_data_size_for_stats\"(CAST(comment AS CHAR(1000))) FROM orders";
        if (implicitCastCharNToVarchar) {
            assertQuery(charQuery, "SELECT 787364");
        }
        else {
            assertQueryFails(charQuery, charTypeUnsupportedError, true);
        }

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

    /// `max_data_size_for_stats` returns a different value for `Varchar` and `Varbinary` datatypes in Presto C++, see:
    /// https://github.com/prestodb/presto/issues/20909. `CHAR` datatype is not supported in Presto C++, see issue:
    /// https://github.com/prestodb/presto/issues/21332.
    @Override
    @Test
    public void testMaxDataSizeForStats()
    {
        // varchar
        assertQuery("SELECT \"max_data_size_for_stats\"(comment) FROM orders", "select 82");

        // char
        String charQuery = "SELECT \"max_data_size_for_stats\"(CAST(comment AS CHAR(1000))) FROM orders";
        if (implicitCastCharNToVarchar) {
            assertQuery(charQuery, "SELECT 82");
        }
        else {
            assertQueryFails(charQuery, charTypeUnsupportedError, true);
        }

        // varbinary
        assertQuery("SELECT \"max_data_size_for_stats\"(CAST(comment AS VARBINARY)) FROM orders", "select 82");

        // max_data_size_for_stats is not needed for array, map and row
    }

    @Override
    @Test(dataProvider = "getType")
    public void testStatisticalDigest(String type)
    {
        assertQuery(format("SELECT value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE)), 0.5E0) > 0 FROM lineitem", type), "SELECT true");
        assertQuery(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE)), 0.5E0) > 0 FROM lineitem", type), "SELECT true");
        assertQuery(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE)), 0.5E0) > 0 FROM lineitem", type), "SELECT true");
        assertQuery(format("SELECT value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE), 2), 0.5E0) > 0 FROM lineitem", type), "SELECT true");
        assertQuery(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 3), 0.5E0) > 0 FROM lineitem", type), "SELECT true");
        assertQuery(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 4), 0.5E0) > 0 FROM lineitem", type), "SELECT true");
        assertQuery(format("SELECT value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE), 2, 0.0001E0), 0.5E0) > 0 FROM lineitem", type), "SELECT true");
        assertQuery(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 3, 0.0001E0), 0.5E0) > 0 FROM lineitem", type), "SELECT true");
        assertQuery(format("SELECT value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 4, 0.0001E0), 0.5E0) > 0 FROM lineitem", type), "SELECT true");
    }

    /// Function `tdigest_agg` is not supported in Presto C++, see: https://github.com/prestodb/presto/issues/24811.
    @Override
    @Test(dataProvider = "getType")
    public void testStatisticalDigestGroupBy(String type)
    {
        assertQuery(format("SELECT partkey, value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE)), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                    "SELECT partkey, true FROM lineitem GROUP BY partkey");
        assertQuery(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE)), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                    "SELECT partkey, true FROM lineitem GROUP BY partkey");
        assertQuery(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE)), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                    "SELECT partkey, true FROM lineitem GROUP BY partkey");
        assertQuery(format("SELECT partkey, value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE), 2), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                    "SELECT partkey, true FROM lineitem GROUP BY partkey");
        assertQuery(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 3), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                    "SELECT partkey, true FROM lineitem GROUP BY partkey");
        assertQuery(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 4), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                    "SELECT partkey, true FROM lineitem GROUP BY partkey");
        assertQuery(format("SELECT partkey, value_at_quantile(%s_agg(CAST(orderkey AS DOUBLE), 2, 0.0001E0), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                    "SELECT partkey, true FROM lineitem GROUP BY partkey");
        assertQuery(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 3, 0.0001E0), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                    "SELECT partkey, true FROM lineitem GROUP BY partkey");
        assertQuery(format("SELECT partkey, value_at_quantile(%s_agg(CAST(quantity AS DOUBLE), 4, 0.0001E0), 0.5E0) > 0 FROM lineitem GROUP BY partkey", type),
                    "SELECT partkey, true FROM lineitem GROUP BY partkey");
    }

    @Override
    @Test(dataProvider = "getType", enabled = false)
    public void testStatisticalDigestMerge(String type)
    {
        assertQuery(format("SELECT value_at_quantile(merge(%s), 0.5E0) > 0 FROM (SELECT partkey, %s_agg(CAST(orderkey AS DOUBLE)) as %s FROM lineitem GROUP BY partkey)",
                        type,
                        type,
                        type),
                "SELECT true");
    }

    @Override
    @Test(dataProvider = "getType", enabled = false)
    public void testStatisticalDigestMergeGroupBy(String type)
    {
        assertQuery(format("SELECT partkey, value_at_quantile(merge(%s), 0.5E0) > 0 " +
                                "FROM (SELECT partkey, suppkey, %s_agg(CAST(orderkey AS DOUBLE)) as %s FROM lineitem GROUP BY partkey, suppkey)" +
                                "GROUP BY partkey",
                        type,
                        type,
                        type),
                "SELECT partkey, true FROM lineitem GROUP BY partkey");
    }
}
