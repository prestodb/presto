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
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.REWRITE_APPROX_DISTINCT_IF_TO_MASK;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRewriteApproxDistinctIfToMaskRule
        extends BasePlanTest
{
    private static final String SQL = "" +
            "SELECT orderkey, approx_distinct(IF(returnflag = 'A', comment)), " +
            "approx_distinct(IF(returnflag = 'N', comment)), approx_distinct(IF(linestatus = 'F', comment)) " +
            "FROM lineitem GROUP BY orderkey";

    private static Session session(boolean enabled)
    {
        return testSessionBuilder().setCatalog("local").setSchema("tiny")
                .setSystemProperty("task_concurrency", "1")
                .setSystemProperty(REWRITE_APPROX_DISTINCT_IF_TO_MASK, Boolean.toString(enabled)).build();
    }

    private String planText(boolean enabled)
    {
        Plan plan = plan(SQL, Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED, false, session(enabled));
        return PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), plan.getStatsAndCosts(),
                getQueryRunner().getMetadata().getFunctionAndTypeManager(), session(enabled), 0);
    }

    /**
     * Without the rule each aggregation reads its own projected copy of the value.
     */
    @Test
    public void testValueIsCopiedPerAggregationWhenDisabled()
    {
        String plan = planText(false);
        assertEquals(countOccurrences(plan, "IF((returnflag) = (VARCHAR'A'), comment, null)"), 1, plan);
        assertTrue(plan.contains("IF((linestatus) = (VARCHAR'F'), comment, null)"), plan);
        assertTrue(!plan.contains("(mask = "), "no masks expected when disabled:\n" + plan);
    }

    /**
     * With the rule every aggregation reads one shared value column and differs only by mask.
     */
    @Test
    public void testValueIsSharedAndConditionsBecomeMasks()
    {
        String plan = planText(true);
        assertEquals(countOccurrences(plan, "(mask = "), 3, plan);
        assertTrue(!plan.contains(", comment, null)"), "IF should be gone from the projection:\n" + plan);
        assertEquals(countOccurrences(plan, "\"presto.default.approx_distinct\"((comment))"), 3,
                "all three aggregations should read the same column:\n" + plan);
    }

    private static int countOccurrences(String haystack, String needle)
    {
        int count = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + 1)) {
            count++;
        }
        return count;
    }
}
