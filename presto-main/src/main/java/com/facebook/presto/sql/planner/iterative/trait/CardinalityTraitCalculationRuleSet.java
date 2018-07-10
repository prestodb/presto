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
import com.facebook.presto.sql.planner.iterative.Rule.Result;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.facebook.presto.sql.planner.iterative.rule.SimpleRule.rule;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.CARDINALITY;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.atMost;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.exactly;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.scalar;
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

public final class CardinalityTraitCalculationRuleSet
{
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                getEnforceSingleRowCardinalityCalculationRule(),
                getValuesCardinalityCalculationRule(),
                getAggregationCardinalityCalculationRule(),
                getProjectCardinalityCalculationRule(),
                getMarkDistinctCardinalityCalculationRule(),
                getExchangeCardinalityCalculationRule(),
                getFilterCardinalityCalculationRule(),
                getLimitCardinalityCalculationRule());
    }

    public Rule<ProjectNode> getProjectCardinalityCalculationRule()
    {
        return propagateTrait(project());
    }

    public Rule<EnforceSingleRowNode> getEnforceSingleRowCardinalityCalculationRule()
    {
        return applyTraitRule(enforceSingleRow(), node -> scalar());
    }

    public Rule<ValuesNode> getValuesCardinalityCalculationRule()
    {
        return applyTraitRule(values(), node -> exactly(node.getRows().size()));
    }

    public Rule<AggregationNode> getAggregationCardinalityCalculationRule()
    {
        return calculateCardinalityRule(aggregation(), (node, context) -> {
            if (node.getStep() == SINGLE && node.getGroupingSets().size() == 1 && node.hasEmptyGroupingSet()) {
                return Result.setTrait(scalar());
            }
            return Result.empty();
        });
    }

    public Rule<MarkDistinctNode> getMarkDistinctCardinalityCalculationRule()
    {
        return propagateTrait(markDistinct());
    }

    public Rule<ExchangeNode> getExchangeCardinalityCalculationRule()
    {
        return calculateCardinalityRule(exchange(), (node, context) -> {
            if (node.getSources().size() != 1) {
                return Result.empty();
            }
            Optional<CardinalityTrait> sourceCardinality = getCardinality(context, getOnlyElement(node.getSources()));
            return sourceCardinality.map(Result::setTrait)
                    .orElseGet(Result::empty);
        });
    }

    public Rule<FilterNode> getFilterCardinalityCalculationRule()
    {
        return calculateCardinalityRule(filter(), (node, context) -> {
            Optional<CardinalityTrait> sourceCardinality = getCardinality(context, getOnlyElement(node.getSources()));
            return sourceCardinality.map(cardinalityTrait -> Result.setTrait(atMost(cardinalityTrait.getMaxCardinality())))
                    .orElseGet(Result::empty);
        });
    }

    public Rule<LimitNode> getLimitCardinalityCalculationRule()
    {
        // TODO consider source min cardinality, it requires possibility to updated trait with more specific
        return applyTraitRule(limit(), node -> atMost(node.getCount()));
    }

    private <T extends PlanNode> Rule<T> propagateTrait(Pattern<T> pattern)
    {
        return calculateCardinalityRule(pattern, (node, context) -> {
            Optional<CardinalityTrait> sourceCardinality = getCardinality(context, getOnlyElement(node.getSources()));
            return sourceCardinality.map(Result::setTrait)
                    .orElseGet(Result::empty);
        });
    }

    private static <T extends PlanNode> Rule<T> applyTraitRule(Pattern<T> pattern, Function<T, CardinalityTrait> function)
    {
        return calculateCardinalityRule(pattern, (node, context) -> Result.setTrait(function.apply(node)));
    }

    private static <T extends PlanNode> Rule<T> calculateCardinalityRule(Pattern<T> pattern, BiFunction<T, Rule.Context, Result> function)
    {
        return rule(pattern, (node, captures, traitSet, context) -> {
            if (traitSet.getTrait(CARDINALITY).isPresent()) {
                return Result.empty();
            }
            return function.apply(node, context);
        });
    }

    private static Optional<CardinalityTrait> getCardinality(Rule.Context context, PlanNode planNode)
    {
        return context.getLookup().resolveTrait(planNode, CARDINALITY);
    }
}
