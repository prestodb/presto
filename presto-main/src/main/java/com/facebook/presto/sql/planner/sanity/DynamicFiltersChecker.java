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
import com.facebook.presto.expressions.DynamicFilters;
import com.facebook.presto.expressions.DynamicFilters.DynamicFilterPlaceholder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AbstractJoinNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.relational.Expressions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.intersection;
import static java.lang.String.format;

/**
 * When dynamic filter assignments are present on a Join node, they should be consumed by a Filter node on its probe side
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
                List<DynamicFilters.DynamicFilterPlaceholder> nonPushedDownFilters = node
                        .getFilter()
                        .map(DynamicFilters::extractDynamicFilters)
                        .map(DynamicFilters.DynamicFilterExtractResult::getDynamicConjuncts)
                        .orElse(ImmutableList.of());
                verify(nonPushedDownFilters.isEmpty(), "Dynamic filters %s present in join's filter predicate were not pushed down.", nonPushedDownFilters);

                return extractUnmatchedDynamicFilters(node, context);
            }

            @Override
            public Set<String> visitSemiJoin(SemiJoinNode node, Void context)
            {
                return extractUnmatchedDynamicFilters(node, context);
            }

            private Set<String> extractUnmatchedDynamicFilters(AbstractJoinNode node, Void context)
            {
                Set<String> currentJoinDynamicFilters = node.getDynamicFilters().keySet();
                Set<String> consumedProbeSide = node.getProbe().accept(this, context);
                Set<String> unconsumedByProbeSide = difference(currentJoinDynamicFilters, consumedProbeSide);
                verify(
                        unconsumedByProbeSide.isEmpty(),
                        "Dynamic filters %s present in join were not fully consumed by its probe side, currentJoinDynamicFilters is: %s, consumedProbeSide is: %s",
                        unconsumedByProbeSide,
                        currentJoinDynamicFilters,
                        consumedProbeSide);

                Set<String> consumedBuildSide = node.getBuild().accept(this, context);
                Set<String> unconsumedByBuildSide = intersection(currentJoinDynamicFilters, consumedBuildSide);
                verify(unconsumedByBuildSide.isEmpty(),
                        format(
                                "Dynamic filters %s present in join were consumed by its build side. consumedBuildSide %s, currentJoinDynamicFilters %s",
                                Arrays.toString(unconsumedByBuildSide.toArray()),
                                Arrays.toString(consumedBuildSide.toArray()),
                                Arrays.toString(currentJoinDynamicFilters.toArray())));

                Set<String> unmatched = new HashSet<>(consumedBuildSide);
                unmatched.addAll(consumedProbeSide);
                unmatched.removeAll(currentJoinDynamicFilters);
                return ImmutableSet.copyOf(unmatched);
            }

            @Override
            public Set<String> visitFilter(FilterNode node, Void context)
            {
                ImmutableSet.Builder<String> consumed = ImmutableSet.builder();
                extractDynamicPredicates(node.getPredicate()).stream()
                        .map(DynamicFilterPlaceholder::getId)
                        .forEach(consumed::add);
                consumed.addAll(node.getSource().accept(this, context));
                return consumed.build();
            }
        }, null);
    }

    private static List<DynamicFilterPlaceholder> extractDynamicPredicates(RowExpression expression)
    {
        return Expressions.uniqueSubExpressions(expression).stream()
            .map(DynamicFilters::getPlaceholder)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(toImmutableList());
    }
}
