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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.REWRITE_APPROX_DISTINCT_IF_TO_MASK;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

/**
 * The rewrite must not change results, so every case is asserted against the same query with the
 * rule disabled.
 */
public class TestRewriteApproxDistinctIfToMask
        extends BasePlanTest
{
    private static Session session(boolean enabled)
    {
        return testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1")
                .setSystemProperty(REWRITE_APPROX_DISTINCT_IF_TO_MASK, Boolean.toString(enabled))
                .build();
    }

    private void assertUnchanged(String sql)
    {
        assertEquals(
                getQueryRunner().execute(session(true), sql).getMaterializedRows(),
                getQueryRunner().execute(session(false), sql).getMaterializedRows(),
                "rewrite changed results: " + sql);
    }

    @Test
    public void testSeveralConditionalsOverOneColumn()
    {
        assertUnchanged("SELECT orderkey, approx_distinct(IF(returnflag = 'A', comment)), " +
                "approx_distinct(IF(returnflag = 'N', comment)), approx_distinct(IF(linestatus = 'F', comment)) " +
                "FROM lineitem GROUP BY orderkey ORDER BY orderkey");
    }

    @Test
    public void testGlobalAggregation()
    {
        assertUnchanged("SELECT approx_distinct(IF(returnflag = 'A', comment)) FROM lineitem");
    }

    @Test
    public void testGroupWhereNoRowMatches()
    {
        assertUnchanged("SELECT orderkey, approx_distinct(IF(returnflag = 'NOT_A_FLAG', comment)) " +
                "FROM lineitem GROUP BY orderkey ORDER BY orderkey");
    }

    @Test
    public void testNoInputRows()
    {
        assertUnchanged("SELECT approx_distinct(IF(returnflag = 'A', comment)) FROM lineitem WHERE orderkey < 0");
    }

    @Test
    public void testExplicitStandardErrorIsPreserved()
    {
        assertUnchanged("SELECT approx_distinct(IF(returnflag = 'A', comment), 0.01) FROM lineitem");
    }

    @Test
    public void testNullValuesInsideTheCondition()
    {
        assertUnchanged("SELECT approx_distinct(IF(x > 1, y)) FROM (VALUES (1, 'a'), (2, NULL), (3, 'b'), (2, 'b')) t(x, y)");
    }

    @Test
    public void testMixedWithAggregationsTheRuleLeavesAlone()
    {
        assertUnchanged("SELECT orderkey, approx_distinct(IF(returnflag = 'A', comment)), count(*), max(quantity) " +
                "FROM lineitem GROUP BY orderkey ORDER BY orderkey");
    }

    @Test
    public void testUnconditionalApproxDistinctIsUntouched()
    {
        assertUnchanged("SELECT orderkey, approx_distinct(comment) FROM lineitem GROUP BY orderkey ORDER BY orderkey");
    }

    /**
     * A window predicate around an inner per metric predicate, which SimplifyRowExpressions
     * flattens to a single IF before this rule runs.
     */
    @Test
    public void testNestedConditionals()
    {
        assertUnchanged("SELECT orderkey, " +
                "approx_distinct(IF(shipdate >= DATE '1995-01-01' AND shipdate < DATE '1995-02-01', " +
                "                   IF(BITWISE_AND(partkey, 1) > 0, shipdate))), " +
                "approx_distinct(IF(shipdate >= DATE '1995-01-01' AND shipdate < DATE '1995-02-01', " +
                "                   IF(BITWISE_AND(partkey, 2) > 0, shipdate))), " +
                "approx_distinct(IF(shipdate >= DATE '1995-01-01' AND shipdate < DATE '1995-02-01', shipdate)) " +
                "FROM lineitem GROUP BY orderkey ORDER BY orderkey");
    }

    @Test
    public void testIfWithElseBranchIsNotRewritten()
    {
        assertUnchanged("SELECT approx_distinct(IF(returnflag = 'A', comment, linestatus)) FROM lineitem");
    }
}
