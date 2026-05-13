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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestPushPartialAggregationThroughExchangeOrdering
        extends BaseRuleTest
{
    @Test
    public void testMixedTypeOutputVariableOrdering()
    {
        // Verifies that the split() method in PushPartialAggregationThroughExchange
        // preserves aggregation output variable ordering when creating PARTIAL and FINAL
        // AggregationNodes. HashMap would scramble "sum_x" and "min_y" causing
        // BIGINT vs VARCHAR type mismatch at PartitionedOutput in Velox.
        PlanNode result = tester().assertThat(new PushPartialAggregationThroughExchangeRuleSet(getFunctionManager(), false).withoutProjectionRule())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression y = p.variable("y", VARCHAR);

                    return p.aggregation(agg -> agg
                            .addAggregation(
                                    p.variable("sum_x", BIGINT),
                                    p.rowExpression("sum(x)"))
                            .addAggregation(
                                    p.variable("min_y", VARCHAR),
                                    p.rowExpression("min(y)"))
                            .singleGroupingSet(a)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.gatheringExchange(
                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                    p.values(a, x, y))));
                })
                .get();

        // The FINAL AggregationNode's output variables must preserve original order:
        // grouping key [a(BIGINT)] then aggregation outputs [sum_x(BIGINT), min_y(VARCHAR)]
        List<Type> expectedTypes = ImmutableList.of(BIGINT, BIGINT, VARCHAR);
        List<Type> actualTypes = result.getOutputVariables().stream()
                .map(VariableReferenceExpression::getType)
                .collect(Collectors.toList());
        assertEquals(actualTypes, expectedTypes,
                "Output variable types must preserve original aggregation ordering to avoid " +
                "type mismatch at PartitionedOutput in Velox native execution");
    }
}
