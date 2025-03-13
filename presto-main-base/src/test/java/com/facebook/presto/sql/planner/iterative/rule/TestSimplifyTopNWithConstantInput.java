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
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.tree.SortItem;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.limit;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.topN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestSimplifyTopNWithConstantInput
        extends BaseRuleTest
{
    @Test
    public void testAllSortKeyConstant()
    {
        tester().assertThat(new SimplifyTopNWithConstantInput())
                .on(p -> {
                    VariableReferenceExpression key1 = p.variable("key1");
                    VariableReferenceExpression key2 = p.variable("key2");
                    VariableReferenceExpression key3 = p.variable("key3");
                    return p.topN(
                            10,
                            ImmutableList.of(key1, key2),
                            p.project(assignment(key1, p.rowExpression("1"), key2, p.rowExpression("2")),
                                    p.values(key3)));
                })
                .matches(
                        limit(
                                10,
                                project(
                                        ImmutableMap.of("key1", expression("1"), "key2", expression("2")),
                                        values("key3"))));
    }

    @Test
    public void testSomeSortKeyConstant()
    {
        tester().assertThat(new SimplifyTopNWithConstantInput())
                .on(p -> {
                    VariableReferenceExpression key1 = p.variable("key1");
                    VariableReferenceExpression key2 = p.variable("key2");
                    VariableReferenceExpression key3 = p.variable("key3");
                    return p.topN(
                            10,
                            ImmutableList.of(key1, key2),
                            p.project(assignment(key1, p.rowExpression("1"), key2, p.rowExpression("key3")),
                                    p.values(key3)));
                })
                .matches(
                        topN(
                                10,
                                ImmutableList.of(sort("key2", SortItem.Ordering.ASCENDING, SortItem.NullOrdering.FIRST)),
                                project(
                                        ImmutableMap.of("key1", expression("1"), "key2", expression("key3")),
                                        values("key3"))));
    }

    @Test
    public void testNoSortKeyConstant()
    {
        tester().assertThat(new SimplifyTopNWithConstantInput())
                .on(p -> {
                    VariableReferenceExpression key1 = p.variable("key1");
                    VariableReferenceExpression key2 = p.variable("key2");
                    VariableReferenceExpression key3 = p.variable("key3");
                    return p.topN(
                            10,
                            ImmutableList.of(key1, key2),
                            p.project(assignment(key1, p.rowExpression("key3"), key2, p.rowExpression("key3")),
                                    p.values(key3)));
                }).doesNotFire();
    }
}
