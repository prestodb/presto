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

import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.MERGE_DUPLICATE_AGGREGATIONS;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestMergeDuplicateAggregation
        extends BaseRuleTest
{
    @Test
    public void testMergeIdenticalAggregations()
    {
        tester().assertThat(new MergeDuplicateAggregation(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_DUPLICATE_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("col", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col)"))
                            .source(p.values(p.variable("col")));
                }))
                .matches(
                        project(
                                ImmutableMap.of("sum_2", expression("sum_1")),
                                aggregation(
                                        ImmutableMap.of("sum_1", PlanMatchPattern.functionCall("sum", ImmutableList.of("col"))),
                                        values("col"))));
    }

    @Test
    public void testNotMergeAggregationsDifferentColumn()
    {
        tester().assertThat(new MergeDuplicateAggregation(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_DUPLICATE_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .source(p.values(p.variable("col1"), p.variable("col2")));
                }))
                .doesNotFire();
    }

    @Test
    public void testNotMergeAggregationsDifferentFunction()
    {
        tester().assertThat(new MergeDuplicateAggregation(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_DUPLICATE_AGGREGATIONS, "true")
                .on(p -> p.aggregation(af -> {
                    p.variable("col", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum"), p.rowExpression("sum(col)"))
                            .addAggregation(p.variable("count"), p.rowExpression("count(col)"))
                            .source(p.values(p.variable("col")));
                }))
                .doesNotFire();
    }
}
