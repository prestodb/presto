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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.DELEGATING_ROW_EXPRESSION_OPTIMIZER_ENABLED;
import static com.facebook.presto.SystemSessionProperties.REMOVE_MAP_CAST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.planner.PlannerUtils.createMapType;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

// TODO: add tests when the delegating optimizer is enabled
public class TestRemoveMapCastRule
        extends BaseRuleTest
{
    @DataProvider(name = "delegating-row-expression-optimizer-enabled")
    public Object[][] delegatingDataProvider()
    {
        return new Object[][] {
                {true},
                {false},
        };
    }
    @Test(dataProvider = "delegating-row-expression-optimizer-enabled")
    public void testSubscriptCast(boolean enableDelegatingRowExpressionOptimizer)
    {
        tester().assertThat(
                        ImmutableSet.<Rule<?>>builder().addAll(new SimplifyRowExpressions(getMetadata(), getExpressionManager()).rules()).addAll(new RemoveMapCastRule(getFunctionManager()).rules()).build())
                .setSystemProperty(REMOVE_MAP_CAST, "true")
                .setSystemProperty(DELEGATING_ROW_EXPRESSION_OPTIMIZER_ENABLED, Boolean.toString(enableDelegatingRowExpressionOptimizer))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", DOUBLE);
                    VariableReferenceExpression feature = p.variable("feature", createMapType(getFunctionManager(), INTEGER, DOUBLE));
                    VariableReferenceExpression key = p.variable("key", BIGINT);
                    return p.project(
                            assignment(a, p.rowExpression("cast(feature as map<bigint, double>)[key]")),
                            p.values(feature, key));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", expression("feature[cast(key as integer)]")),
                                values("feature", "key")));
    }

    @Test(dataProvider = "delegating-row-expression-optimizer-enabled")
    public void testElementAtCast(boolean enableDelegatingRowExpressionOptimizer)
    {
        tester().assertThat(
                        ImmutableSet.<Rule<?>>builder().addAll(new SimplifyRowExpressions(getMetadata(), getExpressionManager()).rules()).addAll(new RemoveMapCastRule(getFunctionManager()).rules()).build())
                .setSystemProperty(REMOVE_MAP_CAST, "true")
                .setSystemProperty(DELEGATING_ROW_EXPRESSION_OPTIMIZER_ENABLED, Boolean.toString(enableDelegatingRowExpressionOptimizer))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", DOUBLE);
                    VariableReferenceExpression feature = p.variable("feature", createMapType(getFunctionManager(), INTEGER, DOUBLE));
                    VariableReferenceExpression key = p.variable("key", BIGINT);
                    return p.project(
                            assignment(a, p.rowExpression("element_at(cast(feature as map<bigint, double>), key)")),
                            p.values(feature, key));
                })
                .matches(
                        project(
                                ImmutableMap.of("a", expression("element_at(feature, try_cast(key as integer))")),
                                values("feature", "key")));
    }
}
