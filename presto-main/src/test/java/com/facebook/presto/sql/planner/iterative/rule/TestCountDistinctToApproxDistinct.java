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
import com.facebook.presto.sql.analyzer.FeaturesConfig.ApproxResultsOption;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.APPROX_RESULTS_OPTION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestCountDistinctToApproxDistinct
        extends BaseRuleTest
{
    @Test
    public void testBasic()
    {
        ExpectedValueProvider<FunctionCall> aggregationPattern = PlanMatchPattern.functionCall("approx_distinct", ImmutableList.of("a"));

        for (ApproxResultsOption option : ApproxResultsOption.values()) {
            if (CountDistinctToApproxDistinct.isApproxDistinct(option)) {
                tester().assertThat(new CountDistinctToApproxDistinct(tester().getMetadata()))
                        .setSystemProperty(APPROX_RESULTS_OPTION, option.name())
                        .on(p -> p.aggregation(af -> {
                            af.globalGrouping()
                                    .step(AggregationNode.Step.FINAL)
                                    .addAggregation(p.variable("b"), expression("count(distinct a)"), ImmutableList.of(BIGINT))
                                    .source(p.values(p.variable("a")));
                        }))
                        .matches(
                                aggregation(
                                        ImmutableMap.of("b", aggregationPattern),
                                        FINAL,
                                        values(ImmutableMap.of("a", 0))));
            }
        }
    }

    @Test
    public void testSessionDisable()
    {
        for (ApproxResultsOption option : ApproxResultsOption.values()) {
            if (!CountDistinctToApproxDistinct.isApproxDistinct(option)) {
                tester().assertThat(new CountDistinctToApproxDistinct(tester().getMetadata()))
                        .setSystemProperty(APPROX_RESULTS_OPTION, option.name())
                        .on(p -> p.aggregation(af -> {
                            af.globalGrouping()
                                    .step(AggregationNode.Step.FINAL)
                                    .addAggregation(p.variable("b"), expression("count(distinct a)"), ImmutableList.of(BIGINT))
                                    .source(p.values(p.variable("a")));
                        }))
                        .doesNotFire();
            }
        }
    }
}
