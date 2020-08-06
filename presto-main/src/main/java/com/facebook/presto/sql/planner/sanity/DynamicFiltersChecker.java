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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.expressions.DynamicFilters.DynamicFilterPlaceholder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.expressions.DynamicFilters.extractDynamicFilters;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;

/**
 * When dynamic filter assignments are present on a Join node, they should be consumed by a Filter node on it's probe side
 */
public class DynamicFiltersChecker
        implements PlanChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
    {
        plan.accept(new InternalPlanVisitor<Set<String>, Void>()
        {
            @Override
            public Set<String> visitPlan(PlanNode node, Void context)
            {
                Set<String> consumed = new HashSet<>();
                for (PlanNode source : node.getSources()) {
                    consumed.addAll(source.accept(this, context));
                }
                return consumed;
            }

            @Override
            public Set<String> visitOutput(OutputNode node, Void context)
            {
                Set<String> unmatched = visitPlan(node, context);
                verify(unmatched.isEmpty(), "All consumed dynamic filters could not be matched with a join.");
                return unmatched;
            }

            @Override
            public Set<String> visitJoin(JoinNode node, Void context)
            {
                Set<String> currentJoinDynamicFilters = node.getDynamicFilters().keySet();
                Set<String> consumedProbeSide = node.getLeft().accept(this, context);
                verify(
                        difference(currentJoinDynamicFilters, consumedProbeSide).isEmpty(),
                        "Dynamic filters present in join were not fully consumed by it's probe side.");

                Set<String> consumedBuildSide = node.getRight().accept(this, context);
                verify(intersection(currentJoinDynamicFilters, consumedBuildSide).isEmpty());

                Set<String> unmatched = new HashSet<>(consumedBuildSide);
                unmatched.addAll(consumedProbeSide);
                unmatched.removeAll(currentJoinDynamicFilters);
                return ImmutableSet.copyOf(unmatched);
            }

            @Override
            public Set<String> visitFilter(FilterNode node, Void context)
            {
                ImmutableSet.Builder<String> consumed = ImmutableSet.builder();
                List<DynamicFilterPlaceholder> dynamicFilters = extractDynamicFilters(node.getPredicate()).getDynamicConjuncts();
                dynamicFilters.stream()
                        .map(DynamicFilterPlaceholder::getId)
                        .forEach(consumed::add);
                consumed.addAll(node.getSource().accept(this, context));
                return consumed.build();
            }
        }, null);
    }
}
