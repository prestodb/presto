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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.tree.SortItem;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneOrderByInAggregation
        extends BaseRuleTest
{
    private static final FunctionAndTypeManager FUNCTION_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    @Test
    public void testBasics()
    {
        tester().assertThat(new PruneOrderByInAggregation(FUNCTION_MANAGER))
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
                                ImmutableMap.of(
                                        new Symbol("avg"), new Symbol("mask"),
                                        new Symbol("array_agg"), new Symbol("mask")),
                                Optional.empty(),
                                SINGLE,
                                values("input", "key", "keyHash", "mask")));
    }

    private AggregationNode buildAggregation(PlanBuilder planBuilder)
    {
        VariableReferenceExpression avg = planBuilder.variable("avg");
        VariableReferenceExpression arrayAgg = planBuilder.variable("array_agg");
        VariableReferenceExpression input = planBuilder.variable("input");
        VariableReferenceExpression key = planBuilder.variable("key");
        VariableReferenceExpression keyHash = planBuilder.variable("keyHash");
        VariableReferenceExpression mask = planBuilder.variable("mask");
        List<VariableReferenceExpression> sourceVariables = ImmutableList.of(input, key, keyHash, mask);
        return planBuilder.aggregation(aggregationBuilder -> aggregationBuilder
                .singleGroupingSet(key)
                .addAggregation(avg, planBuilder.expression("avg(input order by input)"), ImmutableList.of(BIGINT), mask)
                .addAggregation(arrayAgg, planBuilder.expression("array_agg(input order by input)"), ImmutableList.of(BIGINT), mask)
                .hashVariable(keyHash)
                .source(planBuilder.values(sourceVariables, ImmutableList.of())));
    }
}
