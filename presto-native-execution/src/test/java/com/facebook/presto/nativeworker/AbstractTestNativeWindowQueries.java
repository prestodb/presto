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

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyNot;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.limit;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public abstract class AbstractTestNativeWindowQueries
        extends AbstractTestQueryFramework
{
    protected enum FunctionType {
        RANK, VALUE,
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createOrders(queryRunner);
        createLineitem(queryRunner);
    }

    private static final List<String> OVER_CLAUSES_WITH_ORDER_BY = Arrays.asList(
            "PARTITION BY orderkey ORDER BY totalprice",
            "PARTITION BY custkey, orderdate ORDER BY totalprice desc",
            "PARTITION BY orderdate, shippriority ORDER BY orderkey asc nulls first, totalprice desc nulls first",
            "ORDER BY orderdate desc, totalprice asc, shippriority desc nulls first");

    private static final List<String> OVER_CLAUSES_WITHOUT_ORDER_BY = Arrays.asList(
            "PARTITION BY custkey, orderkey",
            "PARTITION BY orderdate, orderkey");

    private static final List<String> FRAME_CLAUSES = Arrays.asList(
            // Frame clauses in RANGE mode.
            "RANGE UNBOUNDED PRECEDING",
            "RANGE CURRENT ROW",
            "RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING",

            // Frame clauses in ROWS mode.
            "ROWS UNBOUNDED PRECEDING",
            "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
            "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            "ROWS BETWEEN 5 PRECEDING AND CURRENT ROW",
            "ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING",
            "ROWS BETWEEN orderkey PRECEDING AND CURRENT ROW",
            "ROWS BETWEEN CURRENT ROW AND orderkey FOLLOWING",

            // Frame clauses with empty frames.
            "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING",
            "ROWS BETWEEN 1 PRECEDING AND 4 PRECEDING",
            "ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING",
            "ROWS BETWEEN 4 FOLLOWING AND 1 FOLLOWING");

    private static final List<String> RANGE_WINDOWS = Arrays.asList(
            "PARTITION BY orderkey ORDER BY totalprice RANGE BETWEEN 5 PRECEDING AND CURRENT ROW",
            "PARTITION BY orderkey ORDER BY totalprice RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING",
            "PARTITION BY orderkey ORDER BY totalprice RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING",
            "PARTITION BY orderkey ORDER BY totalprice RANGE BETWEEN 10 PRECEDING AND 5 PRECEDING",
            // All empty frames.
            "PARTITION BY orderkey ORDER BY totalprice RANGE BETWEEN 5 PRECEDING AND 10 PRECEDING",
            "PARTITION BY orderkey ORDER BY totalprice RANGE BETWEEN 5 FOLLOWING AND 10 FOLLOWING",
            "PARTITION BY orderkey ORDER BY totalprice RANGE BETWEEN 5 FOLLOWING AND 5 FOLLOWING",
            // All empty frames.
            "PARTITION BY orderkey ORDER BY totalprice RANGE BETWEEN 10 FOLLOWING AND 5 FOLLOWING");

    protected List<String> getQueries(String function, FunctionType functionType)
    {
        ImmutableList.Builder<String> queries = ImmutableList.builder();
        List<String> overClauses = new ArrayList<>(OVER_CLAUSES_WITH_ORDER_BY);
        List<String> frameClauses = FRAME_CLAUSES;
        if (functionType == FunctionType.VALUE) {
            overClauses.addAll(OVER_CLAUSES_WITHOUT_ORDER_BY);
        }
        List<String> windowClauseList = new ArrayList<>();
        String windowClause = new String();
        int count = 0;
        final int framesPerQuery = 5;

        for (String overClause : overClauses) {
            for (String frameClause : frameClauses) {
                count++;
                windowClause += String.format("%s OVER (%s %s)", function, overClause, frameClause);
                if (count == framesPerQuery) {
                    windowClauseList.add(windowClause);
                    count = 0;
                    windowClause = "";
                }
                else {
                    windowClause += ", ";
                }
            }
        }
        if (count != 0) {
            windowClause = windowClause.substring(0, windowClause.length() - 2);
            windowClauseList.add(windowClause);
        }

        for (String wClause : windowClauseList) {
            queries.add(String.format("SELECT %s FROM orders", wClause));
        }

        if (functionType == FunctionType.VALUE) {
            for (String rangeClause : RANGE_WINDOWS) {
                queries.add(String.format("SELECT %s OVER (%s) FROM orders", function, rangeClause));
            }
        }

        return queries.build();
    }

    protected void testWindowFunction(String functionName, FunctionType functionType)
    {
        List<String> queries = getQueries(functionName, functionType);
        for (String query : queries) {
            assertQuery(query);
        }
    }

    @Test
    public void testCumeDist()
    {
        testWindowFunction("cume_dist()", FunctionType.RANK);
    }

    @Test
    public void testDenseRank()
    {
        testWindowFunction("dense_rank()", FunctionType.RANK);
    }

    @Test
    public void testPercentRank()
    {
        testWindowFunction("percent_rank()", FunctionType.RANK);
    }

    @Test
    public void testRank()
    {
        testWindowFunction("rank()", FunctionType.RANK);
    }

    @Test
    public void testRowNumber()
    {
        // `row_number() over (partition by key1)` will use `RowNumberNode` which hasn't been implemented yet.
        testWindowFunction("row_number()", FunctionType.RANK);
    }

    @Test
    public void testRowNumberWithFilter()
    {
        assertQuery("SELECT sum(rn) FROM (SELECT row_number() over() rn, * from orders) WHERE rn = 10");
    }

    @Test
    public void testRowNumberWithFilter_2()
    {
        assertQuery("SELECT * FROM (SELECT row_number() over(partition by orderstatus order by orderkey) rn, * from orders) WHERE rn = 1");
    }

    private static final PlanMatchPattern topNForFilter = anyTree(
            anyNot(FilterNode.class,
                    node(TopNRowNumberNode.class,
                            anyTree(
                                    tableScan("orders")))));

    private static final PlanMatchPattern topNForLimit = anyTree(
            limit(10,
                    anyTree(
                    node(TopNRowNumberNode.class,
                            anyTree(
                                    tableScan("orders"))))));
    @Test
    public void testTopNRowNumber()
    {
        String sql = "SELECT sum(rn) FROM (SELECT row_number() over(PARTITION BY orderdate ORDER BY totalprice) rn, * from orders) WHERE rn <= 10";
        assertQuery(sql);
        assertPlan(sql, topNForFilter);

        // Cannot test results for this query as they are not guaranteed to be the same due to lack of ORDER BY in LIMIT.
        // But adding an ORDER BY would prevent the TopNRowNumber optimization from being applied.
        sql = "SELECT sum(rn) FROM (SELECT row_number() over(PARTITION BY orderdate ORDER BY totalprice) rn, * from orders limit 10)";
        assertPlan(sql, topNForLimit);
    }

    @Test
    public void testTopNRank()
    {
        String sql = "SELECT sum(rn) FROM (SELECT rank() over(PARTITION BY orderdate ORDER BY totalprice) rn, * from orders) WHERE rn <= 10";
        assertQuery(sql);

        if (SystemSessionProperties.isOptimizeTopNRank(getSession())) {
            assertPlan(sql, topNForFilter);
            // Cannot test results for this query as they are not guaranteed to be the same due to lack of ORDER BY in LIMIT.
            // But adding an ORDER BY would prevent the TopNRowNumber optimization from being applied.
            sql = "SELECT sum(rn) FROM (SELECT rank() over(PARTITION BY orderdate ORDER BY totalprice) rn, * from orders limit 10)";
            assertPlan(sql, topNForLimit);
        }
    }

    @Test
    public void testTopNDenseRank()
    {
        String sql = "SELECT sum(rn) FROM (SELECT dense_rank() over(PARTITION BY orderdate ORDER BY totalprice) rn, * from orders) WHERE rn <= 10";
        assertQuery(sql);
        if (SystemSessionProperties.isOptimizeTopNRank(getSession())) {
            assertPlan(sql, topNForFilter);

            // Cannot test results for this query as they are not guaranteed to be the same due to lack of ORDER BY in LIMIT.
            // But adding an ORDER BY would prevent the TopNRowNumber optimization from being applied.
            sql = "SELECT dense_rank() over(PARTITION BY orderdate ORDER BY totalprice) rn, * from orders limit 10";
            assertPlan(sql, topNForLimit);
        }
    }

    @Test
    public void testFirstValueOrderKey()
    {
        testWindowFunction("first_value(orderkey)", FunctionType.VALUE);
    }

    @Test
    public void testFirstValueOrderDate()
    {
        testWindowFunction("first_value(orderdate)", FunctionType.VALUE);
    }

    @Test
    public void testLastValueOrderKey()
    {
        testWindowFunction("last_value(orderkey)", FunctionType.VALUE);
    }

    @Test
    public void testLastValueOrderDate()
    {
        testWindowFunction("last_value(orderdate)", FunctionType.VALUE);
    }

    @Test
    public void testNthValueOrderKey()
    {
        testWindowFunction("nth_value(orderkey, 9)", FunctionType.VALUE);
    }

    @Test
    public void testNthValueOrderDate()
    {
        testWindowFunction("nth_value(orderdate, 5)", FunctionType.VALUE);
    }

    @Test
    public void testLeadOrderKey()
    {
        testWindowFunction("lead(orderkey, 5)", FunctionType.VALUE);
    }

    @Test
    public void testLeadOrderDate()
    {
        testWindowFunction("lead(orderdate)", FunctionType.VALUE);
    }

    @Test
    public void testLeadTotalPrice()
    {
        testWindowFunction("lead(totalprice, 2, -123.456)", FunctionType.VALUE);
    }

    @Test
    public void testLagOrderKey()
    {
        testWindowFunction("lag(orderkey, 5)", FunctionType.VALUE);
    }

    @Test
    public void testLagOrderDate()
    {
        testWindowFunction("lag(orderdate)", FunctionType.VALUE);
    }

    @Test
    public void testLagTotalPrice()
    {
        testWindowFunction("lag(totalprice, 2, -123.456)", FunctionType.VALUE);
    }

    @Test
    public void testOverlappingPartitionAndSortingKeys_1()
    {
        assertQuery("SELECT row_number() OVER (PARTITION BY orderdate ORDER BY orderdate) FROM orders");
    }

    @Test
    public void testOverlappingPartitionAndSortingKeys_2()
    {
        assertQuery("SELECT min(orderkey) OVER (PARTITION BY orderdate ORDER BY orderdate, totalprice) FROM orders");
    }

    @Test
    public void testOverlappingPartitionAndSortingKeys_3()
    {
        assertQuery("SELECT * FROM (SELECT row_number() over(partition by orderstatus order by orderkey, orderstatus) rn, * from orders) WHERE rn = 1");
    }

    @Test
    public void testOverlappingPartitionAndSortingKeys_4()
    {
        assertQuery("WITH t AS (SELECT linenumber, row_number() over (partition by linenumber order by linenumber) as rn FROM lineitem) SELECT * FROM t WHERE rn = 1");
    }
}
