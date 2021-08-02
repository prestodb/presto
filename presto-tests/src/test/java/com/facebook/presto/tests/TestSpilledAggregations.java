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
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT;
import static com.facebook.presto.SystemSessionProperties.DISTINCT_AGGREGATION_SPILL_ENABLED;
import static com.facebook.presto.SystemSessionProperties.ORDER_BY_AGGREGATION_SPILL_ENABLED;

public class TestSpilledAggregations
        extends AbstractTestAggregations
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TestDistributedSpilledQueries.localCreateQueryRunner();
    }

    @Test
    public void testOrderBySpillingBasic()
    {
        assertQuery("SELECT orderpriority, custkey, array_agg(orderstatus ORDER BY orderstatus) FROM orders GROUP BY orderpriority, custkey ORDER BY 1, 2");
    }

    @Test
    public void testDoesNotSpillOrderByWhenDisabled()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(ORDER_BY_AGGREGATION_SPILL_ENABLED, "false")
                // set this low so that if we ran with spill the query would fail
                .setSystemProperty(AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT, "1B")
                .build();
        assertQuery(session,
                "SELECT orderpriority, custkey, array_agg(orderstatus ORDER BY orderstatus) FROM orders GROUP BY orderpriority, custkey ORDER BY 1, 2");
    }

    @Test
    public void testOrderBySpillingGroupingSets()
    {
        assertQuery(
                "SELECT orderpriority, custkey, array_agg(orderstatus ORDER BY orderstatus) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) " +
                        "GROUP BY GROUPING SETS ((), (orderpriority), (orderpriority, custkey))",
                "SELECT NULL, NULL, array_agg(orderstatus ORDER BY orderstatus) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) UNION ALL " +
                        "SELECT orderpriority, NULL, array_agg(orderstatus ORDER BY orderstatus) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) GROUP BY orderpriority UNION ALL " +
                        "SELECT orderpriority, custkey, array_agg(orderstatus ORDER BY orderstatus) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) GROUP BY orderpriority, custkey");
    }

    @Test
    public void testDistinctSpillingBasic()
    {
        // the sum() is necessary so that the aggregation isn't optimized into multiple aggregation nodes
        assertQuery("SELECT custkey, sum(custkey), count(DISTINCT orderpriority) FILTER(WHERE orderkey > 5) FROM orders GROUP BY custkey ORDER BY 1");
    }

    @Test
    public void testDoesNotSpillDistinctWhenDisabled()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(DISTINCT_AGGREGATION_SPILL_ENABLED, "false")
                // set this low so that if we ran with spill the query would fail
                .setSystemProperty(AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT, "1B")
                .build();
        // the sum() is necessary so that the aggregation isn't optimized into multiple aggregation nodes
        assertQuery(session,
                "SELECT custkey, sum(custkey), count(DISTINCT orderpriority) FILTER(WHERE orderkey > 5) FROM orders GROUP BY custkey ORDER BY 1");
    }

    @Test
    public void testDistinctAndOrderBySpillingBasic()
    {
        assertQuery("SELECT custkey, orderpriority, sum(custkey), array_agg(DISTINCT orderpriority ORDER BY orderpriority) FROM orders GROUP BY custkey, orderpriority ORDER BY 1, 2");
    }

    @Test
    public void testDistinctSpillingCount()
    {
        assertQuery("SELECT orderpriority, custkey, sum(custkey), count(DISTINCT totalprice) FROM orders GROUP BY orderpriority, custkey ORDER BY 1, 2");
    }

    @Test
    public void testDistinctSpillingGroupingSets()
    {
        assertQuery(
                "SELECT custkey, count(DISTINCT orderpriority) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) " +
                        "GROUP BY GROUPING SETS ((), (custkey))",
                "SELECT NULL, count(DISTINCT orderpriority) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) UNION ALL " +
                        "SELECT custkey, count(DISTINCT orderpriority) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) GROUP BY custkey");
    }

    @Test
    public void testNonGroupedOrderBySpill()
    {
        assertQuery("SELECT array_agg(orderstatus ORDER BY orderstatus) FROM orders");
    }

    @Test
    public void testMultipleDistinctAggregations()
    {
        assertQuery("SELECT custkey, count(DISTINCT orderpriority), count(DISTINCT orderstatus), count(DISTINCT totalprice), count(DISTINCT clerk) FROM orders GROUP BY custkey");
    }
}
