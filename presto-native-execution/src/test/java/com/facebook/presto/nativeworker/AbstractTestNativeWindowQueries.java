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
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;

public abstract class AbstractTestNativeWindowQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createOrders(queryRunner);
    }

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
    @Ignore
    public void testRowNumberWithFilter()
    {
        assertQuery("SELECT sum(rn) FROM (SELECT row_number() over() rn, * from orders) WHERE rn = 10");
        assertQuery("SELECT * FROM (SELECT row_number() over(partition by orderstatus order by orderkey) rn, * from orders) WHERE rn = 1");
    }
}
