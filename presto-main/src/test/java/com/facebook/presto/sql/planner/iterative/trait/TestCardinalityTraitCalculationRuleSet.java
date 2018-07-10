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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.testing.TestingMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.atMost;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.atMostScalar;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.exactly;
import static com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait.scalar;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class TestCardinalityTraitCalculationRuleSet
        extends BaseRuleTest
{
    private static final CardinalityTraitCalculationRuleSet CARDINALITY_RULES = new CardinalityTraitCalculationRuleSet();

    @Test
    public void testEnforceSingleRow()
    {
        tester().assertThat(CARDINALITY_RULES.getEnforceSingleRowCardinalityCalculationRule())
                .on(planBuilder -> planBuilder
                        .enforceSingleRow(
                                planBuilder.tableScan(emptyList(), emptyMap())))
                .hasTrait(scalar());
    }

    @Test
    public void testLimit()
    {
        tester().assertThat(CARDINALITY_RULES.getLimitCardinalityCalculationRule())
                .on(planBuilder -> planBuilder
                        .limit(
                                5,
                                planBuilder.tableScan(emptyList(), emptyMap())))
                .hasTrait(atMost(5L));
    }

    @Test
    public void testFilter()
    {
        tester().assertThat(CARDINALITY_RULES.getFilterCardinalityCalculationRule())
                .on(planBuilder -> planBuilder
                        .filter(
                                TRUE_LITERAL,
                                planBuilder.nodeWithTrait(scalar())))
                .hasTrait(atMostScalar());
    }

    @Test
    public void testAggregation()
    {
        tester().assertThat(CARDINALITY_RULES.getAggregationCardinalityCalculationRule())
                .on(planBuilder -> planBuilder
                        .aggregation(aggregationBuilder -> aggregationBuilder
                                .addGroupingSet(emptyList())
                                .source(planBuilder.tableScan(emptyList(), emptyMap()))))
                .hasTrait(scalar());

        tester().assertThat(CARDINALITY_RULES.getAggregationCardinalityCalculationRule())
                .on(planBuilder -> planBuilder
                        .aggregation(aggregationBuilder -> {
                            Symbol a = planBuilder.symbol("a");
                            aggregationBuilder
                                    .addGroupingSet(emptyList())
                                    .addGroupingSet(ImmutableList.of(a))
                                    .source(planBuilder.tableScan(ImmutableList.of(a), ImmutableMap.of(a, new TestingMetadata.TestingColumnHandle("a"))));
                        }))
                .doesNotFire();
    }

    @Test
    public void testMarkDistinct()
    {
        tester().assertThat(CARDINALITY_RULES.getMarkDistinctCardinalityCalculationRule())
                .on(planBuilder -> planBuilder
                        .markDistinct(
                                planBuilder.symbol("markDistinct"),
                                ImmutableList.of(),
                                planBuilder.nodeWithTrait(CardinalityTrait.exactly(7))))
                .hasTrait(exactly(7));
    }

    @Test
    public void testValues()
    {
        tester().assertThat(CARDINALITY_RULES.getValuesCardinalityCalculationRule())
                .on(planBuilder -> planBuilder
                        .values(ImmutableList.of(), ImmutableList.of(ImmutableList.of(), ImmutableList.of())))
                .hasTrait(exactly(2));
    }
}
