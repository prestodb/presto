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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.AggregationNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.SINGLE;

public class TestPruneAggregationSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllInputsReferenced()
    {
        tester().assertThat(new PruneAggregationSourceColumns())
                .on(p -> buildAggregation(p, alwaysTrue()))
                .matches(
                        aggregation(
                                singleGroupingSet("key"),
                                ImmutableMap.of(
                                        Optional.of("avg"),
                                        functionCall("avg", ImmutableList.of("input"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                strictProject(
                                        ImmutableMap.of(
                                                "input", expression("input"),
                                                "key", expression("key"),
                                                "keyHash", expression("keyHash"),
                                                "mask", expression("mask")),
                                        values("input", "key", "keyHash", "mask", "unused"))));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneAggregationSourceColumns())
                .on(p -> buildAggregation(p, symbol -> !symbol.getName().equals("unused")))
                .doesNotFire();
    }

    private AggregationNode buildAggregation(PlanBuilder planBuilder, Predicate<Symbol> sourceSymbolFilter)
    {
        Symbol avg = planBuilder.symbol("avg");
        Symbol input = planBuilder.symbol("input");
        Symbol key = planBuilder.symbol("key");
        Symbol keyHash = planBuilder.symbol("keyHash");
        Symbol mask = planBuilder.symbol("mask");
        Symbol unused = planBuilder.symbol("unused");
        List<Symbol> sourceSymbols = ImmutableList.of(input, key, keyHash, mask, unused);
        return planBuilder.aggregation(aggregationBuilder -> aggregationBuilder
                .singleGroupingSet(key)
                .addAggregation(avg, planBuilder.expression("avg(input)"), ImmutableList.of(BIGINT), mask)
                .hashSymbol(keyHash)
                .source(
                        planBuilder.values(
                                sourceSymbols.stream()
                                        .filter(sourceSymbolFilter)
                                        .collect(toImmutableList()),
                                ImmutableList.of())));
    }
}
