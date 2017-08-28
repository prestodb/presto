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

package com.facebook.presto.sql.planner.iterative.trait;

import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.RuleSet;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import static com.facebook.presto.sql.planner.iterative.rule.SimpleRule.simpleRule;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.atMost;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.exactly;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.scalar;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTraitType.CARDINALITY;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.enforceSingleRow;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.limit;
import static com.facebook.presto.sql.planner.plan.Patterns.markDistinct;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.values;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.min;

public final class CardinalityTraitCalculationRuleSet
        implements RuleSet
{
    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                simpleCardinalityRule(enforceSingleRow(), (node, context) -> scalar()),
                simpleCardinalityRule(values(), (node, context) -> exactly(node.getRows().size())),
                simpleCardinalityRule(
                        aggregation(),
                        (node, context) -> node.getStep() == SINGLE && node.getGroupingSets().size() == 1 && node.hasEmptyGroupingSet(),
                        (node, context) -> scalar()),
                propagateTrait(project()),
                propagateTrait(markDistinct()),
                propagateTrait(exchange(), (node, context) -> node.getSources().size() == 1),
                simpleCardinalityRule(filter(), CardinalityTraitCalculationRuleSet::sourceHasKnownCardinality, (node, context) -> {
                    CardinalityTrait sourceCardinality = context.getLookup().resolveTrait(node.getSource(), CARDINALITY).get();
                    return atMost(sourceCardinality.getCardinalityRange().upperEndpoint());
                }),
                simpleCardinalityRule(
                        limit(),
                        (node, context) -> {
                            Optional<CardinalityTrait> sourceCardinality = context.getLookup().resolveTrait(node.getSource(), CARDINALITY);
                            long upper = node.getCount();
                            long lower = 0;
                            if (sourceCardinality.isPresent()) {
                                upper = min(sourceCardinality.get().getCardinalityRange().upperEndpoint(), upper);
                                lower = min(upper, sourceCardinality.get().getCardinalityRange().lowerEndpoint());
                            }
                            return new CardinalityTrait(Range.closed(lower, upper));
                        }));
    }

    private <T extends PlanNode> Rule<T> propagateTrait(Pattern<T> pattern)
    {
        return propagateTrait(pattern, alwaysTrue());
    }

    private <T extends PlanNode> Rule<T> propagateTrait(Pattern<T> pattern, CardinalityDerivePredicate<T> predicate)
    {
        return simpleCardinalityRule(
                pattern,
                (node, context) -> predicate.apply(node, context) && sourceHasKnownCardinality(node, context),
                (node, context) -> context.getLookup().resolveTrait(getOnlyElement(node.getSources()), CARDINALITY).get());
    }

    private static <T extends PlanNode> boolean sourceHasKnownCardinality(T node, Rule.Context context)
    {
        return context.getLookup().resolveTrait(getOnlyElement(node.getSources()), CARDINALITY).isPresent();
    }

    private static <T extends PlanNode> Rule<T> simpleCardinalityRule(Pattern<T> pattern, CardinalityDeriveFunction<T> cardinalityDeriveFunction)
    {
        return simpleCardinalityRule(pattern, alwaysTrue(), cardinalityDeriveFunction);
    }

    private static <T extends PlanNode> Rule<T> simpleCardinalityRule(
            Pattern<T> pattern,
            CardinalityDerivePredicate<T> condition,
            CardinalityDeriveFunction<T> cardinalityDeriveFunction)
    {
        return simpleRule(pattern, (node, captures, context) -> {
            if (context.getLookup().resolveTrait(node, CARDINALITY).isPresent()) {
                return Rule.Result.empty();
            }
            if (condition.apply(node, context)) {
                return Rule.Result.set(cardinalityDeriveFunction.apply(node, context));
            }
            return Rule.Result.empty();
        });
    }

    @FunctionalInterface
    private interface CardinalityDeriveFunction<T extends PlanNode>
            extends BiFunction<T, Rule.Context, CardinalityTrait>
    {}

    private static <T extends PlanNode> CardinalityDerivePredicate<T> alwaysTrue()
    {
        return (node, context) -> true;
    }

    @FunctionalInterface
    private interface CardinalityDerivePredicate<T extends PlanNode>
            extends BiFunction<T, Rule.Context, Boolean>
    {}
}
