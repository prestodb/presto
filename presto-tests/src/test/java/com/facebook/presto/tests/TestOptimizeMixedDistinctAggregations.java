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

import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

public class TestOptimizeMixedDistinctAggregations
        extends AbstractTestAggregations
{
    public TestOptimizeMixedDistinctAggregations()
    {
        super(() -> TpchQueryRunnerBuilder.builder()
                .setSingleCoordinatorProperty("optimizer.optimize-mixed-distinct-aggregations", "true")
                .build());
    }

    @Override
    public void testCountDistinct()
    {
        assertQuery("SELECT COUNT(DISTINCT custkey + 1) FROM orders", "SELECT COUNT(*) FROM (SELECT DISTINCT custkey + 1 FROM orders) t");
        assertQuery("SELECT COUNT(DISTINCT linenumber), COUNT(*) from lineitem where linenumber < 0");
    }

    // TODO add dedicated test cases and remove `extends AbstractTestAggregation`

    @Test
    public void testMultiColumnsCountDistinct()
    {
        assertQuery("SELECT COUNT(DISTINCT orderkey), COUNT(DISTINCT custkey) from orders");
        assertQuery("SELECT COUNT(DISTINCT orderkey), COUNT(DISTINCT custkey) from orders group by orderstatus");
        assertQuery("SELECT orderstatus, COUNT(DISTINCT orderkey), COUNT(DISTINCT custkey) from orders group by orderstatus");
        assertQuery("SELECT orderstatus, COUNT(DISTINCT orderkey), SUM(totalprice) from orders group by orderstatus");
        assertQuery("SELECT orderstatus, COUNT(DISTINCT orderkey), COUNT(DISTINCT custkey), SUM(totalprice) from orders group by orderstatus");
        assertQuery("SELECT orderstatus, COUNT(DISTINCT orderkey), COUNT(DISTINCT custkey), COUNT(DISTINCT totalprice), COUNT(custkey), SUM(totalprice) from orders group by orderstatus");
        assertQuery("SELECT orderstatus, COUNT(DISTINCT orderkey), COUNT(DISTINCT totalprice), SUM(totalprice) from orders group by orderstatus");
        assertQuery("SELECT orderstatus, orderpriority, COUNT(orderstatus), COUNT(DISTINCT orderpriority)," +
                " COUNT(DISTINCT orderkey), COUNT(DISTINCT totalprice), SUM(totalprice), MAX(custkey) from orders group by orderstatus, orderpriority");
    }

    @Test
    public void testMultiInputs()
    {
        assertQuery(
                "SELECT corr(DISTINCT x, y) FROM " +
                        "(VALUES " +
                        "   (1, 1)," +
                        "   (2, 2)," +
                        "   (2, 2)," +
                        "   (3, 3)" +
                        ") t(x, y)",
                "VALUES (1.0)");

        assertQuery(
                "SELECT corr(DISTINCT x, y), corr(DISTINCT y, x) FROM " +
                        "(VALUES " +
                        "   (1, 1)," +
                        "   (2, 2)," +
                        "   (2, 2)," +
                        "   (3, 3)" +
                        ") t(x, y)",
                "VALUES (1.0, 1.0)");

        assertQuery(
                "SELECT corr(DISTINCT x, y), corr(DISTINCT y, x), count(*) FROM " +
                        "(VALUES " +
                        "   (1, 1)," +
                        "   (2, 2)," +
                        "   (2, 2)," +
                        "   (3, 3)" +
                        ") t(x, y)",
                "VALUES (1.0, 1.0, 4)");

        assertQuery(
                "SELECT corr(DISTINCT x, y), corr(DISTINCT y, x), count(DISTINCT x) FROM " +
                        "(VALUES " +
                        "   (1, 1)," +
                        "   (2, 2)," +
                        "   (2, 2)," +
                        "   (3, 3)" +
                        ") t(x, y)",
                "VALUES (1.0, 1.0, 3)");
    }
}
