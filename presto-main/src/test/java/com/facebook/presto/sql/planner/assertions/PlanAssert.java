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
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.iterative.Lookup;

import java.util.function.Function;

import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textLogicalPlan;
import static java.lang.String.format;

public final class PlanAssert
{
    private PlanAssert() {}

    public static void assertPlan(Session session, Metadata metadata, StatsCalculator statsCalculator, Plan actual, PlanMatchPattern pattern)
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, actual.getTypes());
        assertPlan(session, metadata, statsProvider, actual, noLookup(), pattern, Function.identity());
    }

    public static void assertPlan(Session session, Metadata metadata, StatsProvider statsProvider, Plan actual, Lookup lookup, PlanMatchPattern pattern, Function<PlanNode, PlanNode> planSanitizer)
    {
        MatchResult matches = actual.getRoot().accept(new PlanMatchingVisitor(session, metadata, statsProvider, lookup), pattern);
        // TODO (Issue #13231) add back printing unresolved plan once we have no need to translate OriginalExpression to RowExpression
        if (!matches.isMatch()) {
            PlanNode resolvedPlan = resolveGroupReferences(actual.getRoot(), lookup);
            String resolvedFormattedPlan = textLogicalPlan(planSanitizer.apply(resolvedPlan), actual.getTypes(), metadata.getFunctionAndTypeManager(), StatsAndCosts.empty(), session, 0);
            throw new AssertionError(format(
                    "Plan does not match, expected [\n\n%s\n] but found [\n\n%s\n]",
                    pattern,
                    resolvedFormattedPlan));
        }
    }
}
