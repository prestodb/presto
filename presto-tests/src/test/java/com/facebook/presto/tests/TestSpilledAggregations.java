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

import org.testng.annotations.Test;

public class TestSpilledAggregations
        extends AbstractTestAggregations
{
    public TestSpilledAggregations()
    {
        this(TestDistributedSpilledQueries::createQueryRunner);
    }

    protected TestSpilledAggregations(QueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    @Test
    public void OrderBySpillingBasic()
    {
        assertQuery("SELECT orderpriority, custkey, array_agg(orderstatus ORDER BY orderstatus) FROM orders GROUP BY orderpriority, custkey ORDER BY 1, 2");
    }

    @Test
    public void OrderBySpillingGroupingSets()
    {
        assertQuery(
                "SELECT orderpriority, custkey, array_agg(orderstatus ORDER BY orderstatus) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) " +
                        "GROUP BY GROUPING SETS ((), (orderpriority), (orderpriority, custkey))",
                "SELECT NULL, NULL, array_agg(orderstatus ORDER BY orderstatus) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) UNION ALL " +
                        "SELECT orderpriority, NULL, array_agg(orderstatus ORDER BY orderstatus) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) GROUP BY orderpriority UNION ALL " +
                        "SELECT orderpriority, custkey, array_agg(orderstatus ORDER BY orderstatus) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) GROUP BY orderpriority, custkey");
    }

    @Test
    public void DistinctSpillingBasic()
    {
        // the sum() is necessary so that the aggregation isn't optimized into multiple aggregation nodes
        assertQuery("SELECT custkey, sum(custkey), count(DISTINCT orderpriority) FILTER(WHERE orderkey > 5) FROM orders GROUP BY custkey ORDER BY 1");
    }

    @Test
    public void DistinctAndOrderBySpillingBasic()
    {
        assertQuery("SELECT custkey, orderpriority, sum(custkey), array_agg(DISTINCT orderpriority ORDER BY orderpriority) FROM orders GROUP BY custkey, orderpriority ORDER BY 1, 2");
    }

    @Test
    public void DistinctSpillingCount()
    {
        assertQuery("SELECT orderpriority, custkey, sum(custkey), count(DISTINCT totalprice) FROM orders GROUP BY orderpriority, custkey ORDER BY 1, 2");
    }

    @Test
    public void DistinctSpillingGroupingSets()
    {
        assertQuery(
                "SELECT custkey, count(DISTINCT orderpriority) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) " +
                        "GROUP BY GROUPING SETS ((), (custkey))",
                "SELECT NULL, count(DISTINCT orderpriority) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) UNION ALL " +
                        "SELECT custkey, count(DISTINCT orderpriority) FROM orders WHERE orderkey IN (1, 2, 3, 4, 5) GROUP BY custkey");
    }

    @Test
    public void TestNonGroupedOrderBySpill()
    {
        assertQuery("SELECT array_agg(orderstatus ORDER BY orderstatus) FROM orders");
    }
}
