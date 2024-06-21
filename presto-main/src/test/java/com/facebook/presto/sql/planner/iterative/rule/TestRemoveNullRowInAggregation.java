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

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.REMOVE_NULL_ROW_IN_AGGREGATION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestRemoveNullRowInAggregation
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOnWithOnlyNullInputFunction()
    {
        tester().assertThat(new RemoveNullRowInAggregation(tester.getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_NULL_ROW_IN_AGGREGATION, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("a", BIGINT);
                    p.variable("b", BIGINT);
                    af.globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .addAggregation(p.variable("b"), p.rowExpression("count(*)"))
                            .source(p.values(p.variable("a")));
                })).doesNotFire();
    }

    @Test
    public void tesDoesNotFireOnWithOneNullInputFunction()
    {
        tester().assertThat(new RemoveNullRowInAggregation(tester.getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_NULL_ROW_IN_AGGREGATION, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("a", BIGINT);
                    p.variable("b", BIGINT);
                    p.variable("c", BIGINT);
                    af.globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .addAggregation(p.variable("c"), p.rowExpression("count(a)"))
                            .addAggregation(p.variable("b"), p.rowExpression("count(*)"))
                            .source(p.values(p.variable("a")));
                })).doesNotFire();
    }

    @Test
    public void testRemoveNullRowInAggregationPlan()
    {
        ExpectedValueProvider<FunctionCall> aggregationPattern = PlanMatchPattern.functionCall("count", false, ImmutableList.of(anySymbol()));
        tester().assertThat(new RemoveNullRowInAggregation(tester.getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(REMOVE_NULL_ROW_IN_AGGREGATION, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("a", BIGINT);
                    p.variable("b", BIGINT);
                    af.globalGrouping()
                            .step(AggregationNode.Step.SINGLE)
                            .addAggregation(p.variable("a"), p.rowExpression("count(b)"))
                            .source(p.values(p.variable("b")));
                })).matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(Optional.empty(), aggregationPattern),
                        ImmutableMap.of(),
                        Optional.empty(),
                        SINGLE,
                        node(FilterNode.class, values("b"))));
    }
}
