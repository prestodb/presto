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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;

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
        assertQuery("SELECT * FROM (SELECT row_number() over(partition by orderstatus order by orderkey) rn, * from orders) WHERE rn = 1");
    }

    @Test
    public void testFirstValue()
    {
        testWindowFunction("first_value(orderkey)", FunctionType.VALUE);
        testWindowFunction("first_value(orderdate)", FunctionType.VALUE);
    }

    @Test
    public void testLastValue()
    {
        testWindowFunction("last_value(orderkey)", FunctionType.VALUE);
        testWindowFunction("last_value(orderdate)", FunctionType.VALUE);
    }

    @Test
    public void testNthValue()
    {
        testWindowFunction("nth_value(orderkey, 9)", FunctionType.VALUE);
        testWindowFunction("nth_value(orderdate, 5)", FunctionType.VALUE);
    }

    @Test
    public void testLead()
    {
        testWindowFunction("lead(orderdate)", FunctionType.VALUE);
        testWindowFunction("lead(orderkey, 5)", FunctionType.VALUE);
        testWindowFunction("lead(totalprice, 2, -123.456)", FunctionType.VALUE);
    }

    @Test
    public void testLag()
    {
        testWindowFunction("lag(orderdate)", FunctionType.VALUE);
        testWindowFunction("lag(orderkey, 5)", FunctionType.VALUE);
        testWindowFunction("lag(totalprice, 2, -123.456)", FunctionType.VALUE);
    }
}
