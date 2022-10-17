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

import java.util.Arrays;
import java.util.List;

public class TestHiveWindowQueries
        extends AbstractTestHiveQueries
{
    public TestHiveWindowQueries()
    {
        super(true);
    }

    protected List<String> getRankingQueries(String rankingFunction)
    {
        ImmutableList.Builder<String> queries = ImmutableList.builder();
        List<String> columnProjections = Arrays.asList("orderkey, orderdate, totalprice");
        List<String> overClauses = Arrays.asList("PARTITION BY orderkey ORDER BY totalprice",
                "PARTITION BY custkey, orderkey ORDER BY totalprice",
                "PARTITION BY orderdate ORDER BY orderkey asc, totalprice desc",
                "PARTITION BY orderkey, orderdate ORDER BY totalprice asc nulls first",
                "PARTITION BY orderkey, custkey ORDER BY orderdate asc nulls first, totalprice asc, shippriority desc",
                "PARTITION BY custkey, orderkey, shippriority ORDER BY orderdate, totalprice asc nulls first",
                "ORDER BY orderdate desc, totalprice asc, shippriority desc nulls first");

        for (String columnProjection : columnProjections) {
            for (String overClause : overClauses) {
                queries.add(String.format("SELECT %s, %s OVER (%s) AS rnk FROM orders", columnProjection, rankingFunction, overClause));
            }
        }
        return queries.build();
    }

    protected void testRankingFunction(String functionName)
    {
        List<String> queries = getRankingQueries(functionName);
        for (String query : queries) {
            assertQuery(query);
        }
    }

    @Test
    public void testCumeDist()
    {
        testRankingFunction("cume_dist()");
    }

    @Test
    public void testDenseRank()
    {
        testRankingFunction("dense_rank()");
    }

    @Test
    public void testPercentRank()
    {
        testRankingFunction("percent_rank()");
    }

    @Test
    public void testRank()
    {
        testRankingFunction("rank()");
    }

    @Test
    public void testRowNumber()
    {
        testRankingFunction("row_number()");
    }
}
