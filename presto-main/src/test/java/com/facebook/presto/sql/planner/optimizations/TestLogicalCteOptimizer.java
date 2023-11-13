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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.sql.planner.SqlPlannerContext.NestedCteStack.delimiter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.cteConsumer;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.cteProducer;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sequence;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestLogicalCteOptimizer
        extends BasePlanTest
{
    @Test
    public void testConvertSimpleCte()
    {
        assertUnitPlan("WITH  temp as (SELECT orderkey FROM ORDERS) " +
                        "SELECT * FROM temp t1 ",
                anyTree(
                        sequence(cteProducer("temp", anyTree(tableScan("orders"))),
                                anyTree(cteConsumer("temp")))));
    }

    @Test
    public void testSimpleRedefinedCteWithSameName()
    {
        assertUnitPlan("WITH  temp as " +
                        "( with temp as (SELECT orderkey FROM ORDERS) SELECT * FROM temp) " +
                        "SELECT * FROM temp",
                anyTree(
                        sequence(
                                cteProducer("temp", anyTree(cteConsumer("temp" + delimiter + "temp"))),
                                cteProducer("temp" + delimiter + "temp", anyTree(tableScan("orders"))),
                                anyTree(cteConsumer("temp")))));
    }

    @Test
    public void testComplexRedefinedNestedCtes()
    {
        assertUnitPlan(
                "WITH " +
                        "cte1 AS ( " +
                        "   SELECT orderkey, totalprice FROM ORDERS WHERE orderkey < 100 " +
                        "), " +
                        "cte2 AS ( " +
                        "   WITH cte3 AS ( WITH cte4 AS (SELECT orderkey, totalprice FROM cte1 WHERE totalprice > 1000) SELECT * FROM cte4) " +
                        "   SELECT cte3.orderkey FROM cte3 " +
                        "), " +
                        "cte3 AS ( " +
                        "   SELECT * FROM customer WHERE custkey < 50 " +
                        ") " +
                        "SELECT cte3.*, cte2.orderkey FROM cte3 JOIN cte2 ON cte3.custkey = cte2.orderkey",
                anyTree(
                        sequence(
                                cteProducer("cte3", anyTree(tableScan("customer"))),
                                cteProducer("cte2", anyTree(cteConsumer("cte2" + delimiter + "cte3"))),
                                cteProducer("cte2" + delimiter + "cte3", anyTree(cteConsumer("cte2" + delimiter + "cte3" + delimiter + "cte4"))),
                                cteProducer("cte2" + delimiter + "cte3" + delimiter + "cte4", anyTree(cteConsumer("cte1"))),
                                cteProducer("cte1", anyTree(tableScan("orders"))),
                                anyTree(
                                        join(
                                                anyTree(cteConsumer("cte3")),
                                                anyTree(cteConsumer("cte2")))))));
    }

    @Test
    public void testRedefinedCtesInDifferentScope()
    {
        assertUnitPlan("WITH  cte1 AS ( WITH cte2 as (SELECT orderkey FROM ORDERS WHERE orderkey < 100)" +
                        "SELECT * FROM cte2), " +
                        " cte2 AS (SELECT * FROM customer WHERE custkey < 50) " +
                        "SELECT * FROM cte2  JOIN cte1 ON true",
                anyTree(
                        sequence(
                                cteProducer("cte2", anyTree(tableScan("customer"))),
                                cteProducer("cte1", anyTree(cteConsumer("cte1" + delimiter + "cte2"))),
                                cteProducer("cte1" + delimiter + "cte2", anyTree(tableScan("orders"))),
                                anyTree(join(anyTree(cteConsumer("cte2")), anyTree(cteConsumer("cte1")))))));
    }

    @Test
    public void testNestedCte()
    {
        assertUnitPlan("WITH  temp1 as (SELECT orderkey FROM ORDERS), " +
                        " temp2 as (SELECT * FROM temp1) " +
                        "SELECT * FROM temp2",
                anyTree(
                        sequence(cteProducer("temp2", anyTree(cteConsumer("temp1"))),
                                cteProducer("temp1", anyTree(tableScan("orders"))),
                                anyTree(cteConsumer("temp2")))));
    }

    @Test
    public void testMultipleIndependentCtes()
    {
        assertUnitPlan("WITH  temp1 as (SELECT orderkey FROM ORDERS), " +
                        " temp2 as (SELECT custkey FROM CUSTOMER) " +
                        "SELECT * FROM temp1, temp2",
                anyTree(
                        sequence(cteProducer("temp1", anyTree(tableScan("orders"))),
                                cteProducer("temp2", anyTree(tableScan("customer"))),
                                anyTree(join(anyTree(cteConsumer("temp1")), anyTree(cteConsumer("temp2")))))));
    }

    @Test
    public void testDependentCtes()
    {
        assertUnitPlan("WITH  temp1 as (SELECT orderkey FROM ORDERS), " +
                        " temp2 as (SELECT orderkey FROM temp1) " +
                        "SELECT * FROM temp2 , temp1",
                anyTree(
                        sequence(cteProducer("temp2", anyTree(cteConsumer("temp1"))),
                                cteProducer("temp1", anyTree(tableScan("orders"))),
                                anyTree(join(anyTree(cteConsumer("temp2")), anyTree(cteConsumer("temp1")))))));
    }

    @Test
    public void testComplexCteWithJoins()
    {
        assertUnitPlan(
                "WITH  cte_orders AS (SELECT orderkey, custkey FROM ORDERS), " +
                        " cte_line_item AS (SELECT l.orderkey, l.suppkey FROM lineitem l JOIN cte_orders o ON l.orderkey = o.orderkey) " +
                        "SELECT li.orderkey, s.suppkey, s.name FROM cte_line_item li JOIN SUPPLIER s ON li.suppkey = s.suppkey",
                anyTree(
                        sequence(
                                cteProducer("cte_line_item",
                                        anyTree(
                                                join(
                                                        anyTree(tableScan("lineitem")),
                                                        anyTree(cteConsumer("cte_orders"))))),
                                cteProducer("cte_orders", anyTree(tableScan("orders"))),
                                anyTree(
                                        join(
                                                anyTree(cteConsumer("cte_line_item")),
                                                anyTree(tableScan("supplier")))))));
    }

    @Test
    public void tesNoPersistentCteOnlyWithRowType()
    {
        assertUnitPlan("WITH temp AS " +
                        "( SELECT CAST(ROW('example_status', 100) AS ROW(status VARCHAR, amount INTEGER)) AS order_details" +
                        " FROM (VALUES (1))" +
                        ") SELECT * FROM temp",
                anyTree(values("1")));
    }

    @Test
    public void testSimplePersistentCteWithRowTypeAndNonRowType()
    {
        assertUnitPlan("WITH temp AS (" +
                        "  SELECT * FROM (VALUES " +
                        "    (CAST(ROW('example_status', 100) AS ROW(status VARCHAR, amount INTEGER)), 1)," +
                        "    (CAST(ROW('another_status', 200) AS ROW(status VARCHAR, amount INTEGER)), 2)" +
                        "  ) AS t (order_details, orderkey)" +
                        ") SELECT * FROM temp",
                anyTree(
                        sequence(
                                cteProducer("temp", anyTree(values("status", "amount"))),
                                anyTree(cteConsumer("temp")))));
    }

    @Test
    public void testNoPersistentCteWithZeroLengthVarcharType()
    {
        assertUnitPlan("WITH temp AS (" +
                        "  SELECT * FROM (VALUES " +
                        "    (CAST('' AS VARCHAR(0)), 9)" +
                        "  ) AS t (text_column, number_column)" +
                        ") SELECT * FROM temp",
                anyTree(values("text_column", "number_column")));
    }

    private void assertUnitPlan(String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new LogicalCteOptimizer(getQueryRunner().getMetadata()));
        assertPlan(sql, getSession(), Optimizer.PlanStage.OPTIMIZED, pattern, optimizers);
    }

    private Session getSession()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .build();
    }
}
