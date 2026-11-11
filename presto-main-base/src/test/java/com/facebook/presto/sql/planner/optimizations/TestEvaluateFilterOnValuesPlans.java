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
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.EVALUATE_FILTER_ON_VALUES;
import static com.facebook.presto.SystemSessionProperties.SIMPLIFY_PLAN_WITH_EMPTY_INPUT;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestEvaluateFilterOnValuesPlans
        extends BasePlanTest
{
    // A full outer join against a constant VALUES whose filter removes every row, grouped by the coalesced join key
    private static final String FULL_JOIN_WITH_FILTERED_VALUES = "SELECT coalesce(o.orderkey, v.k) AS k, count(*) " +
            "FROM orders o " +
            "FULL JOIN (SELECT k FROM (VALUES (BIGINT '1', 'a')) t(k, ds) WHERE ds = 'b') v ON o.orderkey = v.k " +
            "GROUP BY coalesce(o.orderkey, v.k)";

    @Test
    public void testFullJoinWithFilteredValuesIsRemoved()
    {
        assertPlanValidatorWithSession(FULL_JOIN_WITH_FILTERED_VALUES, session(true), false,
                plan -> assertFalse(searchFrom(plan.getRoot()).where(JoinNode.class::isInstance).matches(), "join should be removed"));
    }

    @Test
    public void testFullJoinWithFilteredValuesIsKeptWhenDisabled()
    {
        assertPlanValidatorWithSession(FULL_JOIN_WITH_FILTERED_VALUES, session(false), false,
                plan -> assertTrue(searchFrom(plan.getRoot()).where(JoinNode.class::isInstance).matches(), "join should be kept"));
    }

    @Test
    public void testFullJoinWithRetainedValuesRowIsKept()
    {
        String sql = "SELECT coalesce(o.orderkey, v.k) AS k, count(*) " +
                "FROM orders o " +
                "FULL JOIN (SELECT k FROM (VALUES (BIGINT '1', 'a'), (BIGINT '2', 'b')) t(k, ds) WHERE ds = 'b') v ON o.orderkey = v.k " +
                "GROUP BY coalesce(o.orderkey, v.k)";
        assertPlanValidatorWithSession(sql, session(true), false,
                plan -> assertTrue(searchFrom(plan.getRoot()).where(JoinNode.class::isInstance).matches(), "join should be kept"));
    }

    private Session session(boolean evaluateFilterOnValues)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(EVALUATE_FILTER_ON_VALUES, Boolean.toString(evaluateFilterOnValues))
                .setSystemProperty(SIMPLIFY_PLAN_WITH_EMPTY_INPUT, "true")
                .build();
    }
}
