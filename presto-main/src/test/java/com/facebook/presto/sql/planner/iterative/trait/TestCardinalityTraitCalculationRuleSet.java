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
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.testing.TestingMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.iterative.Rule.Result.empty;
import static com.facebook.presto.sql.planner.iterative.rule.SimpleRule.simpleRule;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.atMost;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.atMostScalar;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.exactly;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.scalar;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTraitType.CARDINALITY;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class TestCardinalityTraitCalculationRuleSet
        extends BaseRuleTest
{
    private static final Rule<PlanNode> NOOP_RULE = simpleRule(
            Pattern.typeOf(PlanNode.class),
            (node, captures, context) -> empty());

    @Test
    public void testEnforceSingleRow()
    {
        tester().assertThat(NOOP_RULE)
                .withBefore(new CardinalityTraitCalculationRuleSet().rules())
                .on(planBuilder -> planBuilder
                        .enforceSingleRow(
                                planBuilder.tableScan(emptyList(), emptyMap())))
                .satisfies(scalar());
    }

    @Test
    public void testLimit()
    {
        tester().assertThat(NOOP_RULE)
                .withBefore(new CardinalityTraitCalculationRuleSet().rules())
                .on(planBuilder -> planBuilder
                        .limit(
                                5,
                                planBuilder.tableScan(emptyList(), emptyMap())))
                .satisfies(atMost(5L)) // equal to actual cardinality range
                .satisfies(atMost(6L)) // actual trait is more specific than required
                .doesNotSatisfy(atMost(4L)) // required is more specific than actual trait
                .doesNotSatisfy(exactly(2L)); // required is more specific than actual trait
    }

    @Test
    public void testFilter()
    {
        tester().assertThat(NOOP_RULE)
                .withBefore(new CardinalityTraitCalculationRuleSet().rules())
                .on(planBuilder -> planBuilder
                        .filter(
                                TRUE_LITERAL,
                                planBuilder.enforceSingleRow(
                                        planBuilder.tableScan(emptyList(), emptyMap()))))
                .satisfies(atMostScalar()) // equal to actual cardinality range
                .doesNotSatisfy(scalar()) // required is more specific than actual trait
                .doesNotSatisfy(CardinalityTrait.exactly(0L)) // required is more specific than actual trait
                .doesNotSatisfy(CardinalityTrait.exactly(2L)) // does not intersect with actual
                .satisfies(atMost(2L)); // actual trait is more specific than required
    }

    @Test
    public void testAggregation()
    {
        tester().assertThat(NOOP_RULE)
                .withBefore(new CardinalityTraitCalculationRuleSet().rules())
                .on(planBuilder -> planBuilder
                        .aggregation(aggregationBuilder -> aggregationBuilder
                                .addGroupingSet(emptyList())
                                .source(planBuilder.tableScan(emptyList(), emptyMap()))))
                .satisfies(scalar());

        tester().assertThat(NOOP_RULE)
                .withBefore(new CardinalityTraitCalculationRuleSet().rules())
                .on(planBuilder -> planBuilder
                        .aggregation(aggregationBuilder -> {
                            Symbol a = planBuilder.symbol("a");
                            aggregationBuilder
                                    .addGroupingSet(emptyList())
                                    .addGroupingSet(ImmutableList.of(a))
                                    .source(planBuilder.tableScan(ImmutableList.of(a), ImmutableMap.of(a, new TestingMetadata.TestingColumnHandle("a"))));
                        }))
                .hasNo(CARDINALITY);
    }
}
