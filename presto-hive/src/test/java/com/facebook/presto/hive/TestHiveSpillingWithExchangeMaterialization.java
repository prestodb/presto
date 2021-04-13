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
package com.facebook.presto.hive;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hive.HiveQueryRunner.createMaterializingAndSpillingQueryRunner;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.tpch.TpchTable.getTables;

public class TestHiveSpillingWithExchangeMaterialization
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createMaterializingAndSpillingQueryRunner(getTables());
    }

    @Test
    public void testJoin()
    {
        assertQuery("SELECT orderstatus FROM lineitem JOIN orders on lineitem.orderkey = orders.orderkey");
    }

    @Test
    public void testOrderBy()
    {
        assertQueryOrdered("SELECT orderstatus FROM orders ORDER BY orderstatus");
    }

    @Test
    public void testAggregation()
    {
        assertQuery("SELECT custkey, orderpriority, sum(custkey), array_agg(DISTINCT orderpriority ORDER BY orderpriority) FROM orders GROUP BY custkey, orderpriority ORDER BY 1, 2");
    }

    @Test
    public void testWindow()
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
}
