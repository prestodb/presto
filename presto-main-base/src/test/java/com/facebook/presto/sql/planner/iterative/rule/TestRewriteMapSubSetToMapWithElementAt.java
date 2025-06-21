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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.REWRITE_MAP_SUBSET_WITH_CONSTANT_WITH_ELEMENT_AT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.planner.PlannerUtils.createMapType;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestRewriteMapSubSetToMapWithElementAt
        extends BaseRuleTest
{
    @Test
    public void testFeatureMap()
    {
        tester().assertThat(
                        ImmutableSet.<Rule<?>>builder()
                                .addAll(new SimplifyRowExpressions(getMetadata(), getExpressionManager()).rules())
                                .addAll(new RewriteMapSubSetToMapWithElementAt(getFunctionManager()).rules()).build())
                .setSystemProperty(REWRITE_MAP_SUBSET_WITH_CONSTANT_WITH_ELEMENT_AT, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", createMapType(getFunctionManager(), INTEGER, DOUBLE));
                    VariableReferenceExpression feature = p.variable("feature", createMapType(getFunctionManager(), INTEGER, DOUBLE));
                    VariableReferenceExpression key = p.variable("key", BIGINT);
                    return p.project(
                            assignment(a, p.rowExpression("map_subset(feature, array[1, 2, 3])")),
                            p.values(feature, key));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", expression("map_filter(map(array[1, 2, 3], array[element_at(feature, 1), element_at(feature, 2), element_at(feature, 3)]), (expr, expr_0) -> expr_0 is not null)")),
                                values("feature", "key")));
    }
}
