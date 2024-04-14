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
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.CteReferenceNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Consumer;

import static com.facebook.presto.SystemSessionProperties.CTE_HEURISTIC_REPLICATION_THRESHOLD;
import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.sql.planner.SqlPlannerContext.CteInfo.delimiter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.cteConsumer;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.cteProducer;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.lateral;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sequence;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static org.testng.Assert.assertFalse;

public class TestLogicalCteOptimizer
        extends BasePlanTest
{
    private static final List<Class<? extends PlanNode>> CTE_PLAN_NODES = ImmutableList.of(CteReferenceNode.class, CteConsumerNode.class, CteProducerNode.class, SequenceNode.class);

    @Test
    public void testConvertSimpleCte()
    {
        assertUnitPlan("WITH  temp as (SELECT orderkey FROM ORDERS) " +
                        "SELECT * FROM temp t JOIN temp t2 ON true ",
                anyTree(
                        sequence(cteProducer(addQueryScopeDelimiter("temp", 0), anyTree(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("temp", 0))))));
    }

    @Test
    public void testSimpleRedefinedCteWithSameNameDefinedAgain()
    {
        assertUnitPlan("WITH \n" +
                        "test_base AS (SELECT colB FROM (VALUES (1), (2)) AS TempTable(colB)),\n" +
                        "test AS (\n" +
                        "    \n" +
                        "    WITH test_base as (SELECT colA FROM (VALUES (1), (2)) AS TempTable(colA)),\n" +
                        "    test1 AS (\n" +
                        "        WITH test2 AS(\n" +
                        "          SELECT * FROM test_base\n" +
                        "        )\n" +
                        "        SELECT * FROM test2\n" +
                        "    )\n" +
                        "    SELECT * FROM test1\n" +
                        ")\n" +
                        "\n" +
                        "SELECT * FROM test",
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("test", 3), anyTree(cteConsumer(addQueryScopeDelimiter("test1", 2)))),
                                cteProducer(addQueryScopeDelimiter("test1", 2), anyTree(cteConsumer(addQueryScopeDelimiter("test2", 1)))),
                                cteProducer(addQueryScopeDelimiter("test2", 1), anyTree(cteConsumer(addQueryScopeDelimiter("test_base", 0)))),
                                cteProducer(addQueryScopeDelimiter("test_base", 0), anyTree(values("colA"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("test", 3))))));
    }

    @Test
    public void testSimpleRedefinedCteWithSameName()
    {
        assertUnitPlan("WITH  temp as " +
                        "( with temp as (SELECT orderkey FROM ORDERS) SELECT * FROM temp) " +
                        "SELECT * FROM temp",
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("temp", 1), anyTree(cteConsumer(addQueryScopeDelimiter("temp", 0)))),
                                cteProducer(addQueryScopeDelimiter("temp", 0), anyTree(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("temp", 1))))));
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
                                cteProducer(addQueryScopeDelimiter("cte3", 0), anyTree(tableScan("customer"))),
                                cteProducer(addQueryScopeDelimiter("cte2", 4), anyTree(cteConsumer(addQueryScopeDelimiter("cte3", 3)))),
                                cteProducer(addQueryScopeDelimiter("cte3", 3), anyTree(cteConsumer(addQueryScopeDelimiter("cte4", 2)))),
                                cteProducer(addQueryScopeDelimiter("cte4", 2), anyTree(cteConsumer(addQueryScopeDelimiter("cte1", 1)))),
                                cteProducer(addQueryScopeDelimiter("cte1", 1), anyTree(tableScan("orders"))),
                                anyTree(
                                        join(
                                                anyTree(cteConsumer(addQueryScopeDelimiter("cte3", 0))),
                                                anyTree(cteConsumer(addQueryScopeDelimiter("cte2", 4))))))));
    }

    @Test
    public void testRedefinedCteConflictingNamesInDifferentScope()
    {
        assertUnitPlan("WITH test AS (SELECT colA FROM (VALUES (1), (2)) AS TempTable(colA)),\n" +
                        " _query AS (\n" +
                        "    with test AS (\n" +
                        "     SELECT * FROM test\n" +
                        "     )\n" +
                        "     SELECT * FROM test\n" +
                        "  )\n" +
                        "  SELECT * FROM _query",
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("_query", 2), anyTree(cteConsumer(addQueryScopeDelimiter("test", 1)))),
                                cteProducer(addQueryScopeDelimiter("test", 1), anyTree(cteConsumer(addQueryScopeDelimiter("test", 0)))),
                                cteProducer(addQueryScopeDelimiter("test", 0), anyTree(values("colA"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("_query", 2))))));
    }

    @Test
    public void testCtesDefinedInEntirelyDifferentScope()
    {
        // From clause is visited first
        assertUnitPlan("SELECT \n" +
                        "   *, (WITH T as (SELECT colA FROM (VALUES (1), (2)) AS TempTable(colA)) SELECT * FROM T)\n" +
                        "FROM (\n" +
                        "    WITH T AS ( \n" +
                        "      SELECT ColumnA, ColumnB FROM (\n" +
                        "            VALUES \n" +
                        "            (1, 'A'),\n" +
                        "            (2, 'B'),\n" +
                        "            (3, 'C'),\n" +
                        "            (4, 'D')\n" +
                        "        ) AS TempTable(ColumnA, ColumnB)\n" +
                        "    )\n" +
                        "    SELECT * FROM T JOIN T ON TRUE" +
                        ")",
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("T", 0), anyTree(values("ColumnA", "ColumnB"))),
                                cteProducer(addQueryScopeDelimiter("T", 1), anyTree(values("colA"))),
                                anyTree(lateral(ImmutableList.of(),
                                        anyTree(join(anyTree(cteConsumer(addQueryScopeDelimiter("T", 0))), anyTree(cteConsumer(addQueryScopeDelimiter("T", 0))))),
                                        anyTree(cteConsumer(addQueryScopeDelimiter("T", 1))))))));
    }

    @Test
    public void testNestedCtesReused()
    {
        assertUnitPlan("WITH  cte1 AS ( WITH cte2 as (SELECT orderkey FROM ORDERS WHERE orderkey < 100)" +
                        "SELECT * FROM cte2)" +
                        "SELECT * FROM  cte1  JOIN cte1 ON true",
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("cte1", 1), anyTree(cteConsumer(addQueryScopeDelimiter("cte2", 0)))),
                                cteProducer(addQueryScopeDelimiter("cte2", 0), anyTree(tableScan("orders"))),
                                anyTree(join(anyTree(cteConsumer(addQueryScopeDelimiter("cte1", 1))), anyTree(cteConsumer(addQueryScopeDelimiter("cte1", 1))))))));
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
                                cteProducer(addQueryScopeDelimiter("cte2", 0), anyTree(tableScan("customer"))),
                                cteProducer(addQueryScopeDelimiter("cte1", 2), anyTree(cteConsumer(addQueryScopeDelimiter("cte2", 1)))),
                                cteProducer(addQueryScopeDelimiter("cte2", 1), anyTree(tableScan("orders"))),
                                anyTree(join(anyTree(cteConsumer(addQueryScopeDelimiter("cte2", 0))), anyTree(cteConsumer(addQueryScopeDelimiter("cte1", 2))))))));
    }

    @Test
    public void testNestedCte()
    {
        assertUnitPlan("WITH  temp1 as (SELECT orderkey FROM ORDERS), " +
                        " temp2 as (SELECT * FROM temp1) " +
                        "SELECT * FROM temp2",
                anyTree(
                        sequence(cteProducer(addQueryScopeDelimiter("temp2", 1), anyTree(cteConsumer(addQueryScopeDelimiter("temp1", 0)))),
                                cteProducer(addQueryScopeDelimiter("temp1", 0), anyTree(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("temp2", 1))))));
    }

    @Test
    public void testMultipleIndependentCtes()
    {
        assertUnitPlan("WITH  temp1 as (SELECT orderkey FROM ORDERS), " +
                        " temp2 as (SELECT custkey FROM CUSTOMER) " +
                        "SELECT * FROM temp1, temp2",
                anyTree(
                        sequence(cteProducer(addQueryScopeDelimiter("temp1", 0), anyTree(tableScan("orders"))),
                                cteProducer(addQueryScopeDelimiter("temp2", 1), anyTree(tableScan("customer"))),
                                anyTree(join(anyTree(cteConsumer(addQueryScopeDelimiter("temp1", 0))), anyTree(cteConsumer(addQueryScopeDelimiter("temp2", 1))))))));
    }

    @Test
    public void testDependentCtes()
    {
        assertUnitPlan("WITH  temp1 as (SELECT orderkey FROM ORDERS), " +
                        " temp2 as (SELECT orderkey FROM temp1) " +
                        "SELECT * FROM temp2 , temp1",
                anyTree(
                        sequence(cteProducer(addQueryScopeDelimiter("temp2", 1), anyTree(cteConsumer(addQueryScopeDelimiter("temp1", 0)))),
                                cteProducer(addQueryScopeDelimiter("temp1", 0), anyTree(tableScan("orders"))),
                                anyTree(join(anyTree(cteConsumer(addQueryScopeDelimiter("temp2", 1))), anyTree(cteConsumer(addQueryScopeDelimiter("temp1", 0))))))));
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
                                cteProducer(addQueryScopeDelimiter("cte_line_item", 1),
                                        anyTree(
                                                join(
                                                        anyTree(tableScan("lineitem")),
                                                        anyTree(cteConsumer(addQueryScopeDelimiter("cte_orders", 0)))))),
                                cteProducer(addQueryScopeDelimiter("cte_orders", 0), anyTree(tableScan("orders"))),
                                anyTree(
                                        join(
                                                anyTree(cteConsumer(addQueryScopeDelimiter("cte_line_item", 1))),
                                                anyTree(tableScan("supplier")))))));
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
                                cteProducer(addQueryScopeDelimiter("temp", 0), anyTree(values("status", "amount"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("temp", 0))))));
    }

    // ToDo prestodb/21791: write with 0 length varchar fails in hive
    // See reference - While Presto supports Varchar of length 0 (as discussed in trinodb/trino#1136
    @Test(enabled = false)
    public void testNoPersistentCteWithZeroLengthVarcharType()
    {
        assertUnitPlan("WITH temp AS (" +
                        "  SELECT * FROM (VALUES " +
                        "    (CAST('' AS VARCHAR(0)), 9)" +
                        "  ) AS t (text_column, number_column)" +
                        ") SELECT * FROM temp",
                anyTree(values("text_column", "number_column")));
    }

    @Test
    public void testHeuristicComplexCteMaterialization()
    {
        assertUnitPlan(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC_COMPLEX_QUERIES_ONLY")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "4")
                        .build(),
                "WITH temp AS (" +
                        "    SELECT orderkey FROM ORDERS GROUP BY orderkey" +
                        ")" +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp",
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("temp", 0), anyTree(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("temp", 0))))));
    }

    @Test
    public void testHeuristicComplexCteMaterializationForInnerCtes()
    {
        assertUnitPlan(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC_COMPLEX_QUERIES_ONLY")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "4")
                        .build(),
                "WITH temp AS (" +
                        "With inner_temp AS(  " +
                        "  SELECT orderkey FROM ORDERS GROUP BY orderkey)" +
                        "SELECT * FROM inner_temp" +
                        ")" +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp",
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("inner_temp", 0), anyTree(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("inner_temp", 0))))));
    }

    @Test
    public void testNoHeuristicCteMaterializationForInnerCtes()
    {
        assertUnitPlan(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC_COMPLEX_QUERIES_ONLY")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "4")
                        .build(),
                "WITH temp AS (" +
                        "With inner_temp AS(  " +
                        "  SELECT orderkey  FROM ORDERS GROUP BY orderkey)" +
                        "SELECT * FROM inner_temp GROUP BY orderkey" +
                        ")" +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp",
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("temp", 1), anyTree(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("temp", 1))))));
    }

    @Test
    public void testHeuristicComplexCteMaterializationForBothCtes()
    {
        assertUnitPlan(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC_COMPLEX_QUERIES_ONLY")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "4")
                        .build(),
                "WITH temp AS (\n" +
                        "    WITH inner_temp AS (\n" +
                        "        SELECT orderkey\n" +
                        "        FROM ORDERS\n" +
                        "        GROUP BY orderkey\n" +
                        "    )\n" +
                        "    SELECT * FROM inner_temp GROUP BY orderkey \n" +
                        "    UNION\n" +
                        "    SELECT * FROM inner_temp GROUP BY orderkey\n" +
                        "    UNION\n" +
                        "    SELECT * FROM inner_temp GROUP BY orderkey\n" +
                        "    UNION\n" +
                        "    SELECT * FROM inner_temp GROUP BY orderkey\n" +
                        ")\n" +
                        "SELECT * FROM temp\n" +
                        "UNION\n" +
                        "SELECT * FROM temp\n" +
                        "UNION\n" +
                        "SELECT * FROM temp\n" +
                        "UNION\n" +
                        "SELECT * FROM temp",
                anyTree(
                        // both materialized
                        sequence(
                                cteProducer(addQueryScopeDelimiter("temp", 1), anyTree(cteConsumer(addQueryScopeDelimiter("inner_temp", 0)))),
                                cteProducer(addQueryScopeDelimiter("inner_temp", 0), anyTree(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("temp", 1))))));
    }

    @Test
    public void testHeuristicMaterializationWithMultipleNestedCtesAllMaterialized()
    {
        assertUnitPlan(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "2")
                        .build(),
                "WITH outer_temp AS (\n" +
                        "   WITH mid_temp AS (\n" +
                        "       WITH inner_temp AS (\n" +
                        "           SELECT orderkey, COUNT(*) as total_orders FROM ORDERS GROUP BY orderkey\n" +
                        "       )\n" +
                        "       SELECT orderkey FROM inner_temp WHERE total_orders > 1\n" +
                        "       UNION ALL \n" +
                        "       SELECT orderkey FROM inner_temp\n" +
                        "   )\n" +
                        "   SELECT orderkey FROM mid_temp\n" +
                        "   UNION ALL \n" +
                        "   SELECT orderkey FROM mid_temp\n" +
                        ")\n" +
                        "SELECT * FROM outer_temp \n" +
                        "UNION ALL " +
                        "SELECT * FROM outer_temp  \n" +
                        "UNION ALL " +
                        "SELECT * FROM outer_temp \n",
                anyTree(
                        // Expect specific materializations based on the threshold and complexity
                        sequence(
                                cteProducer(addQueryScopeDelimiter("outer_temp", 2), anyTree(cteConsumer(addQueryScopeDelimiter("mid_temp", 1)))),
                                cteProducer(addQueryScopeDelimiter("mid_temp", 1), anyTree(cteConsumer(addQueryScopeDelimiter("inner_temp", 0)))),
                                cteProducer(addQueryScopeDelimiter("inner_temp", 0), anyTree(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("outer_temp", 2))))));
    }

    @Test
    public void testHeuristicMaterializationWithMultipleNestedCtesWhereInnerNotMaterialized()
    {
        assertUnitPlan(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "3")
                        .build(),
                "WITH outer_temp AS (\n" +
                        "   WITH mid_temp AS (\n" +
                        "       WITH inner_temp AS (\n" +
                        "           SELECT orderkey, COUNT(*) as total_orders FROM ORDERS GROUP BY orderkey\n" +
                        "       )\n" +
                        "       SELECT orderkey FROM inner_temp WHERE total_orders > 1\n" +
                        "       UNION ALL \n" +
                        "       SELECT orderkey FROM inner_temp\n" +
                        "   )\n" +
                        "   SELECT orderkey FROM mid_temp\n" +
                        "   UNION ALL \n" +
                        "   SELECT orderkey FROM mid_temp\n" +
                        "   UNION ALL \n" +
                        "   SELECT orderkey FROM mid_temp\n" +
                        ")" +
                        "SELECT * FROM outer_temp \n" +
                        "UNION ALL " +
                        "SELECT * FROM outer_temp \n" +
                        "UNION ALL " +
                        "SELECT * FROM outer_temp \n",
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("outer_temp", 2), anyTree(cteConsumer(addQueryScopeDelimiter("mid_temp", 1)))),
                                cteProducer(addQueryScopeDelimiter("mid_temp", 1), anyTree(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("outer_temp", 2))))));
    }

    @Test
    public void testHeuristicMaterializationWithDifferentPaths()
    {
        assertUnitPlan(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "3")
                        .build(),
                "WITH first_cte AS (\n" +
                        "   SELECT orderkey FROM ORDERS\n" +
                        "), second_cte AS (\n" +
                        "   SELECT orderkey FROM first_cte\n" +
                        "), third_cte AS (\n" +
                        "   SELECT orderkey FROM second_cte\n" +
                        "   UNION ALL \n" +
                        "   SELECT orderkey FROM first_cte \n" +
                        ")\n" +
                        "SELECT * FROM third_cte \n" +
                        "UNION ALL\n" +
                        "SELECT * FROM second_cte \n" +
                        "UNION ALL\n" +
                        "SELECT * FROM first_cte \n",
                anyTree(
                        // Check the materialization decisions based on usage across different paths
                        sequence(
                                cteProducer(addQueryScopeDelimiter("first_cte", 0), anyTree(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("first_cte", 0))))));
    }

    @Test
    public void testHeuristicMaterializationWithDeepNestedCteUsage()
    {
        // Heuristic Threshold = 3
        //CTE b is used 2 times in a.
        //CTE a is used 2 times in y.
        //CTE y is used 4 times in x.
        //CTE x is used 3 times in the main query.
        //Hence we expect cte x,y and b to be materialized
        assertUnitPlan(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "3")
                        .build(),
                "WITH b AS (\n" +
                        "   SELECT orderkey FROM ORDERS\n" +
                        "),\n" +
                        "a AS (\n" +
                        "   SELECT orderkey FROM b\n" +
                        "   UNION ALL\n" +
                        "   SELECT orderkey FROM b\n" +
                        "),\n" +
                        "y AS (\n" +
                        "   SELECT orderkey FROM a\n" +
                        "   UNION ALL\n" +
                        "   SELECT orderkey FROM a\n" +
                        "),\n" +
                        "x AS (\n" +
                        "   SELECT orderkey FROM y\n" +
                        "   UNION ALL\n" +
                        "   SELECT orderkey FROM y\n" +
                        "   UNION ALL\n" +
                        "   SELECT orderkey FROM y\n" +
                        "   UNION ALL\n" +
                        "   SELECT orderkey FROM y\n" +
                        ")\n" +
                        "SELECT * FROM x\n" +
                        "UNION ALL\n" +
                        "SELECT * FROM x\n" +
                        "UNION ALL\n" +
                        "SELECT * FROM x\n",
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("x", 3), anyTree(cteConsumer(addQueryScopeDelimiter("y", 2)))),
                                cteProducer(addQueryScopeDelimiter("y", 2), anyTree(cteConsumer(addQueryScopeDelimiter("b", 0)))),
                                cteProducer(addQueryScopeDelimiter("b", 0), anyTree(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("x", 3))))));
    }

    @Test
    public void testHeuristicMaterializationWithDeepNestedCteUsage2()
    {
        // Heuristic Threshold = 5
        //CTE b is used 2 times in a.
        //CTE a is used 2 times in y.
        //CTE y is used 4 times in x.
        //CTE x is used 3 times in the main query.
        // Hence we expect cte y to be materialized
        assertUnitPlan(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "5")
                        .build(),
                "WITH b AS (\n" +
                        "   SELECT orderkey FROM ORDERS\n" +
                        "),\n" +
                        "a AS (\n" +
                        "   SELECT orderkey FROM b\n" +
                        "   UNION ALL\n" +
                        "   SELECT orderkey FROM b\n" +
                        "),\n" +
                        "y AS (\n" +
                        "   SELECT orderkey FROM a\n" +
                        "   UNION ALL\n" +
                        "   SELECT orderkey FROM a\n" +
                        "),\n" +
                        "x AS (\n" +
                        "   SELECT orderkey FROM y\n" +
                        "   UNION ALL\n" +
                        "   SELECT orderkey FROM y\n" +
                        "   UNION ALL\n" +
                        "   SELECT orderkey FROM y\n" +
                        "   UNION ALL\n" +
                        "   SELECT orderkey FROM y\n" +
                        ")\n" +
                        "SELECT * FROM x \n" +
                        "UNION ALL\n" +
                        "SELECT * FROM x\n" +
                        "UNION ALL\n" +
                        "SELECT * FROM x\n",
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("y", 2), anyTree(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("y", 2))))));
    }

    @Test
    public void testHeuristicMaterializationWithDeepNestedCteUsage3()
    {
        // Heuristic Threshold = 3
        //CTE b is used 1 times in a and 1 time in c.
        //CTE c is used 1 times in the main query.
        //CTE a is used 3 times in the main query.
        //Hence we expect: cte a to be materialized
        assertUnitPlan(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "3")
                        .build(),
                "WITH b AS (\n" +
                        "   SELECT orderkey FROM ORDERS\n" +
                        "),\n" +
                        "a AS (\n" +
                        "   SELECT orderkey FROM b\n" +
                        "),\n" +
                        "c as ( SELECT orderkey FROM b)\n" +
                        "SELECT * FROM c\n" +
                        "UNION ALL\n" +
                        "SELECT * FROM a\n" +
                        "UNION ALL\n" +
                        "SELECT * FROM a\n" +
                        "UNION ALL\n" +
                        "SELECT * FROM a\n",
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("a", 2), anyTree(tableScan("orders"))),
                                anyTree(PlanMatchPattern.union(
                                        PlanMatchPattern.union(
                                                PlanMatchPattern.union(
                                                        anyTree(tableScan("orders")), anyTree(cteConsumer(addQueryScopeDelimiter("a", 2)))),
                                                anyTree(cteConsumer(addQueryScopeDelimiter("a", 2)))),
                                        anyTree(cteConsumer(addQueryScopeDelimiter("a", 2))))))));
    }

    @Test
    public void testNoHeuristicComplexCteMaterializationWithoutComplexNodes()
    {
        assertUnitPlanWithValidator(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC_COMPLEX_QUERIES_ONLY")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "4")
                        .build(),
                "WITH temp AS (" +
                        "    SELECT orderkey FROM ORDERS" +
                        ")" +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp",
                anyTree(tableScan("orders")),
                plan ->
                        assertFalse(PlanNodeSearcher.searchFrom(plan.getRoot())
                                .where(planNode -> CTE_PLAN_NODES.stream().anyMatch(clazz -> clazz.isInstance(planNode)))
                                .matches()));
    }

    @Test
    public void testNoHeuristicComplexCteMaterializationWithoutDataNodes()
    {
        assertUnitPlanWithValidator(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC_COMPLEX_QUERIES_ONLY")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "0")
                        .build(),
                "WITH temp AS (" +
                        "    SELECT colB FROM (VALUES (1), (2)) AS TempTable(colB)" +
                        ")" +
                        "SELECT * FROM temp ",
                anyTree(values("colB")),
                plan ->
                        assertFalse(PlanNodeSearcher.searchFrom(plan.getRoot())
                                .where(planNode -> CTE_PLAN_NODES.stream().anyMatch(clazz -> clazz.isInstance(planNode)))
                                .matches()));
    }

    @Test
    public void testHeuristicCteMaterialization()
    {
        assertUnitPlan(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "4")
                        .build(),
                "WITH temp AS (" +
                        "    SELECT orderkey FROM ORDERS" +
                        ")" +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp",
                anyTree(
                        sequence(
                                cteProducer(addQueryScopeDelimiter("temp", 0), anyTree(tableScan("orders"))),
                                anyTree(cteConsumer(addQueryScopeDelimiter("temp", 0))))));
    }

    @Test
    public void testNoHeuristicCteMaterializationWithLesserReferences()
    {
        assertUnitPlanWithValidator(
                Session.builder(this.getQueryRunner().getDefaultSession())
                        .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "HEURISTIC")
                        .setSystemProperty(CTE_HEURISTIC_REPLICATION_THRESHOLD, "4")
                        .build(),
                "WITH temp AS (" +
                        "    SELECT orderkey FROM ORDERS" +
                        ")" +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp " +
                        "UNION " +
                        "SELECT * FROM temp ",
                anyTree(tableScan("orders")),
                plan ->
                        assertFalse(PlanNodeSearcher.searchFrom(plan.getRoot())
                                .where(planNode -> CTE_PLAN_NODES.stream().anyMatch(clazz -> clazz.isInstance(planNode)))
                                .matches()));
    }

    public static String addQueryScopeDelimiter(String cteName, int scope)
    {
        return String.valueOf(scope) + delimiter + cteName;
    }

    private void assertUnitPlan(String sql, PlanMatchPattern pattern)
    {
        assertUnitPlan(getSession(), sql, pattern);
    }

    private void assertUnitPlan(Session session, String sql, PlanMatchPattern pattern)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new LogicalCteOptimizer(getQueryRunner().getMetadata()));
        assertPlan(sql, session, Optimizer.PlanStage.OPTIMIZED, pattern, optimizers);
    }

    private void assertUnitPlanWithValidator(Session session, String sql, PlanMatchPattern pattern, Consumer<Plan> planValidator)
    {
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new LogicalCteOptimizer(getQueryRunner().getMetadata()));
        getQueryRunner().inTransaction(session, transactionSession -> {
            Plan actualPlan = getQueryRunner().createPlan(
                    transactionSession,
                    sql,
                    optimizers,
                    Optimizer.PlanStage.OPTIMIZED,
                    WarningCollector.NOOP);
            PlanAssert.assertPlan(transactionSession, getQueryRunner().getMetadata(), getQueryRunner().getStatsCalculator(), actualPlan, pattern);
            planValidator.accept(actualPlan);
            return null;
        });
    }

    private Session getSession()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .build();
    }
}
