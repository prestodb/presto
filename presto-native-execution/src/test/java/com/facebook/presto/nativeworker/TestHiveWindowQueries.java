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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;
import org.testng.internal.collections.Pair;

import java.util.Arrays;
import java.util.List;

public class TestHiveWindowQueries
        extends AbstractTestHiveQueries
{
    private static final List<String> OVER_CLAUSES_WITH_ORDER_BY = Arrays.asList(
            "PARTITION BY orderkey ORDER BY totalprice",
            "PARTITION BY custkey, orderkey ORDER BY totalprice",
            "PARTITION BY orderdate ORDER BY orderkey asc, totalprice desc",
            "PARTITION BY orderkey, custkey ORDER BY orderdate asc nulls first, totalprice asc, shippriority desc",
            "PARTITION BY custkey, orderkey, shippriority ORDER BY orderdate, totalprice asc nulls first",
            "PARTITION BY orderkey, orderdate ORDER BY totalprice asc nulls first",
            "ORDER BY orderdate desc, totalprice asc, shippriority desc nulls first");

    private static final List<String> OVER_CLAUSES_WITHOUT_ORDER_BY = Arrays.asList(
            "PARTITION BY orderkey",
            "PARTITION BY custkey, orderkey",
            "PARTITION BY orderdate",
            "PARTITION BY orderkey, orderdate",
            "PARTITION BY custkey, orderkey, shippriority",
            "PARTITION BY orderkey, custkey");

    public TestHiveWindowQueries()
    {
        super(true);
    }

    protected List<String> getRankingQueries(String rankingFunction, boolean orderBy)
    {
        ImmutableList.Builder<String> queries = ImmutableList.builder();
        List<String> columnProjections = Arrays.asList("orderkey, orderdate, totalprice");
        List<String> overClauses = orderBy ? OVER_CLAUSES_WITH_ORDER_BY : OVER_CLAUSES_WITHOUT_ORDER_BY;

        for (String columnProjection : columnProjections) {
            for (String overClause : overClauses) {
                queries.add(String.format("SELECT %s, %s OVER (%s) AS rnk FROM orders", columnProjection, rankingFunction, overClause));
            }
        }
        return queries.build();
    }

    protected void testRankingFunction(String functionName, boolean orderBy)
    {
        List<String> queries = getRankingQueries(functionName, orderBy);
        for (String query : queries) {
            assertQuery(query);
        }
    }

    protected List<String> buildWindowTestQueriesWithFrame(List<Pair<String, String>> functionCalls)
    {
        ImmutableList.Builder<String> queries = ImmutableList.builder();

        // Window function over clauses with different datatypes in PARTITION BY and ORDER BY clauses.
        List<String> overClauses = Arrays.asList("PARTITION BY orderkey ORDER BY commitdate asc nulls first, extendedprice asc, shipinstruct desc",
                "PARTITION BY commitdate ORDER BY orderkey desc, extendedprice asc, shipinstruct desc nulls first",
                "PARTITION BY extendedprice ORDER BY commitdate asc nulls first, orderkey desc nulls first, shipinstruct asc nulls first",
                "PARTITION BY shipinstruct ORDER BY commitdate, extendedprice, orderkey",
                "ORDER BY commitdate desc, orderkey asc, extendedprice desc nulls first, shipinstruct asc nulls first",
                "ORDER BY orderkey desc, commitdate asc, extendedprice asc nulls first, shipinstruct desc nulls first",
                "ORDER BY extendedprice asc nulls first, shipinstruct desc nulls first, commitdate desc, orderkey asc",
                "ORDER BY shipinstruct desc, orderkey asc, commitdate desc nulls first, extendedprice asc nulls first");

        // Window function frame clauses with UNBOUNDED PRECEDING/FOLLOWING and CURRENT ROW bounds in RANGE and ROWS modes.
        List<String> frameClauses = Arrays.asList("RANGE UNBOUNDED PRECEDING",
                "RANGE CURRENT ROW",
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
                "RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
                "ROWS UNBOUNDED PRECEDING",
                "ROWS CURRENT ROW",
                "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
                "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING");

        for (String overClause : overClauses) {
            for (String frameClause : frameClauses) {
                ImmutableList.Builder<String> windowFunctionCalls = ImmutableList.builder();
                for (Pair<String, String> functionCall : functionCalls) {
                    // Value functions require a total ordering of input rows to produce deterministic results with window frames in ROWS mode. The argument column for the value
                    // function is therefore passed as a parameter in functionCall and included in the ORDER BY clause.
                    windowFunctionCalls.add(String.format("%s OVER (%s, %s %s)", functionCall.first(), overClause, functionCall.second(), frameClause));
                }
                String windowFunction = String.join(",", windowFunctionCalls.build());
                queries.add(String.format("SELECT %s FROM lineitem", windowFunction));
            }
        }
        return queries.build();
    }

    protected void testAggregateWindowFunction(String aggregateFunction, Boolean numericAggregate)
    {
        ImmutableList.Builder<Pair<String, String>> aggregateFunctionPairs = ImmutableList.builder();
        aggregateFunctionPairs.add(Pair.of(String.format("%s(partkey)", aggregateFunction), "partkey"));

        // Varchar and Date datatypes are tested with columns shipmode and receiptdate respectively for min, max and count aggregate functions.
        if (!numericAggregate) {
            aggregateFunctionPairs.add(Pair.of(String.format("%s(shipmode)", aggregateFunction), "shipmode"));
            aggregateFunctionPairs.add(Pair.of(String.format("%s(receiptdate)", aggregateFunction), "receiptdate"));
        }

        List<String> queries = buildWindowTestQueriesWithFrame(aggregateFunctionPairs.build());
        for (String query : queries) {
            assertQuery(query);
        }
    }

    @Test
    public void testCumeDist()
    {
        testRankingFunction("cume_dist()", true);
        testRankingFunction("cume_dist()", false);
    }

    @Test
    public void testDenseRank()
    {
        testRankingFunction("dense_rank()", true);
        testRankingFunction("dense_rank()", false);
    }

    @Test
    public void testPercentRank()
    {
        testRankingFunction("percent_rank()", true);
        testRankingFunction("percent_rank()", false);
    }

    @Test
    public void testRank()
    {
        testRankingFunction("rank()", true);
        testRankingFunction("rank()", false);
    }

    @Test
    public void testRowNumber()
    {
        // `row_number() over (partition by key1)` will use `RowNumberNode` which hasn't been implemented yet.
        testRankingFunction("row_number()", true);
    }

    @Test
    public void testNthValue()
    {
        List<Pair<String, String>> nthValueFunctionPairs = Arrays.asList(Pair.of("nth_value(receiptdate, 5)", "receiptdate"),
                Pair.of("nth_value(partkey, 5)", "partkey"),
                Pair.of("nth_value(shipmode, 5)", "shipmode"),
                Pair.of("nth_value(tax, orderkey + 1)", "tax"),
                Pair.of("nth_value(linenumber, orderkey + 1)", "linenumber"));

        List<String> queries = buildWindowTestQueriesWithFrame(nthValueFunctionPairs);
        for (String query : queries) {
            assertQuery(query);
        }
    }

    @Test
    public void testSum()
    {
        testAggregateWindowFunction("sum", true);
    }

    @Test
    public void testMin()
    {
        testAggregateWindowFunction("min", false);
    }

    @Test
    public void testMax()
    {
        testAggregateWindowFunction("max", false);
    }

    @Test
    public void testCount()
    {
        testAggregateWindowFunction("count", false);
    }

    @Test
    public void testAvg()
    {
        testAggregateWindowFunction("avg", true);
    }
}
