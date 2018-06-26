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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CachingStatsProvider;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;

import static com.facebook.presto.cost.PlanNodeCostEstimate.UNKNOWN_COST;
import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textLogicalPlan;
import static java.lang.String.format;

public final class PlanAssert
{
    private PlanAssert() {}

    public static void assertPlan(Session session, Metadata metadata, StatsCalculator statsCalculator, Plan actual, PlanMatchPattern pattern)
    {
        assertPlan(session, metadata, statsCalculator, actual, noLookup(), pattern);
    }

    public static void assertPlan(Session session, Metadata metadata, StatsCalculator statsCalculator, Plan actual, Lookup lookup, PlanMatchPattern pattern)
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, actual.getTypes());
        assertPlan(session, metadata, statsProvider, actual, lookup, pattern);
    }

    public static void assertPlan(Session session, Metadata metadata, StatsProvider statsProvider, Plan actual, Lookup lookup, PlanMatchPattern pattern)
    {
        CostProvider costProvider = node -> UNKNOWN_COST;
        MatchResult matches = actual.getRoot().accept(new PlanMatchingVisitor(session, metadata, statsProvider, lookup), pattern);
        if (!matches.isMatch()) {
            String formattedPlan = textLogicalPlan(actual.getRoot(), actual.getTypes(), metadata.getFunctionRegistry(), statsProvider, costProvider, session, 0);
            PlanNode resolvedPlan = resolveGroupReferences(actual.getRoot(), lookup);
            String resolvedFormattedPlan = textLogicalPlan(resolvedPlan, actual.getTypes(), metadata.getFunctionRegistry(), statsProvider, costProvider, session, 0);
            throw new AssertionError(format(
                    "Plan does not match, expected [\n\n%s\n] but found [\n\n%s\n] which resolves to [\n\n%s\n]",
                    pattern,
                    formattedPlan,
                    resolvedFormattedPlan));
        }
    }
}
