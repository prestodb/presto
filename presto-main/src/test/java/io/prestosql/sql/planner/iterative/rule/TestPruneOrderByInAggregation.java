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
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.tree.SortItem;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.sort;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.SINGLE;

public class TestPruneOrderByInAggregation
        extends BaseRuleTest
{
    private static final FunctionRegistry functionRegistry = MetadataManager.createTestMetadataManager().getFunctionRegistry();

    @Test
    public void testBasics()
    {
        tester().assertThat(new PruneOrderByInAggregation(functionRegistry))
                .on(this::buildAggregation)
                .matches(
                        aggregation(
                                singleGroupingSet("key"),
                                ImmutableMap.of(
                                        Optional.of("avg"), functionCall("avg", ImmutableList.of("input")),
                                        Optional.of("array_agg"), functionCall(
                                                "array_agg",
                                                ImmutableList.of("input"),
                                                ImmutableList.of(sort("input", SortItem.Ordering.ASCENDING, SortItem.NullOrdering.UNDEFINED)))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                values("input", "key", "keyHash", "mask")));
    }

    private AggregationNode buildAggregation(PlanBuilder planBuilder)
    {
        Symbol avg = planBuilder.symbol("avg");
        Symbol arrayAgg = planBuilder.symbol("array_agg");
        Symbol input = planBuilder.symbol("input");
        Symbol key = planBuilder.symbol("key");
        Symbol keyHash = planBuilder.symbol("keyHash");
        Symbol mask = planBuilder.symbol("mask");
        List<Symbol> sourceSymbols = ImmutableList.of(input, key, keyHash, mask);
        return planBuilder.aggregation(aggregationBuilder -> aggregationBuilder
                .singleGroupingSet(key)
                .addAggregation(avg, planBuilder.expression("avg(input order by input)"), ImmutableList.of(BIGINT), mask)
                .addAggregation(arrayAgg, planBuilder.expression("array_agg(input order by input)"), ImmutableList.of(BIGINT), mask)
                .hashSymbol(keyHash)
                .source(planBuilder.values(sourceSymbols, ImmutableList.of())));
    }
}
