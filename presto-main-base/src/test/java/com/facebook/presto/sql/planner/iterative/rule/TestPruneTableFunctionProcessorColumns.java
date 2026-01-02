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

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.PassThroughColumn;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableFunctionProcessor;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneTableFunctionProcessorColumns
        extends BaseRuleTest
{
    @Test
    public void testDoNotPruneProperOutputs()
    {
        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> p.project(
                        Assignments.of(),
                        p.tableFunctionProcessor(
                                builder -> builder
                                        .name("test_function")
                                        .properOutputs(p.variable("p"))
                                        .source(p.values(p.variable("x"))))))
                .doesNotFire();
    }

    @Test
    public void testPrunePassThroughOutputs()
    {
        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    return p.project(
                            Assignments.of(),
                            p.tableFunctionProcessor(
                                    builder -> builder
                                            .name("test_function")
                                            .passThroughSpecifications(
                                                    new PassThroughSpecification(
                                                            true,
                                                            ImmutableList.of(
                                                                    new PassThroughColumn(a, true),
                                                                    new PassThroughColumn(b, false))))
                                            .source(p.values(a, b))));
                })
                .matches(project(
                        ImmutableMap.of(),
                        tableFunctionProcessor(builder -> builder
                                        .name("test_function")
                                        .passThroughSymbols(ImmutableList.of(ImmutableList.of())),
                                values("a", "b"))));

        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> {
                    VariableReferenceExpression proper = p.variable("proper");
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    return p.project(
                            Assignments.of(),
                            p.tableFunctionProcessor(
                                    builder -> builder
                                            .name("test_function")
                                            .properOutputs(proper)
                                            .passThroughSpecifications(
                                                    new PassThroughSpecification(
                                                            true,
                                                            ImmutableList.of(
                                                                    new PassThroughColumn(a, true),
                                                                    new PassThroughColumn(b, false))))
                                            .source(p.values(a, b))));
                })
                .matches(project(
                        ImmutableMap.of(),
                        tableFunctionProcessor(builder -> builder
                                        .name("test_function")
                                        .properOutputs(ImmutableList.of("proper"))
                                        .passThroughSymbols(ImmutableList.of(ImmutableList.of())),
                                values("a", "b"))));
    }

    @Test
    public void testReferencedPassThroughOutputs()
    {
        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x");
                    VariableReferenceExpression y = p.variable("y");
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    return p.project(
                            Assignments.builder().put(y, y).put(b, b).build(),
                            p.tableFunctionProcessor(
                                    builder -> builder
                                            .name("test_function")
                                            .properOutputs(x, y)
                                            .passThroughSpecifications(
                                                    new PassThroughSpecification(
                                                            true,
                                                            ImmutableList.of(
                                                                    new PassThroughColumn(a, true),
                                                                    new PassThroughColumn(b, false))))
                                            .source(p.values(a, b))));
                })
                .matches(project(
                        ImmutableMap.of("y", expression("y"), "b", expression("b")),
                        tableFunctionProcessor(builder -> builder
                                        .name("test_function")
                                        .properOutputs(ImmutableList.of("x", "y"))
                                        .passThroughSymbols(ImmutableList.of(ImmutableList.of("b"))),
                                values("a", "b"))));
    }

    @Test
    public void testAllPassThroughOutputsReferenced()
    {
        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    return p.project(
                            Assignments.builder().put(a, a).put(b, b).build(),
                            p.tableFunctionProcessor(
                                    builder -> builder
                                            .name("test_function")
                                            .passThroughSpecifications(
                                                    new PassThroughSpecification(
                                                            true,
                                                            ImmutableList.of(
                                                                    new PassThroughColumn(a, true),
                                                                    new PassThroughColumn(b, false))))
                                            .source(p.values(a, b))));
                })
                .doesNotFire();

        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> {
                    VariableReferenceExpression proper = p.variable("proper");
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    return p.project(
                            Assignments.builder().put(a, a).put(b, b).build(),
                            p.tableFunctionProcessor(
                                    builder -> builder
                                            .name("test_function")
                                            .properOutputs(proper)
                                            .passThroughSpecifications(
                                                    new PassThroughSpecification(
                                                            true,
                                                            ImmutableList.of(
                                                                    new PassThroughColumn(a, true),
                                                                    new PassThroughColumn(b, false))))
                                            .source(p.values(a, b))));
                })
                .doesNotFire();
    }

    @Test
    public void testNoSource()
    {
        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> p.project(
                        Assignments.of(),
                        p.tableFunctionProcessor(
                                builder -> builder
                                        .name("test_function")
                                        .properOutputs(p.variable("proper")))))
                .doesNotFire();
    }

    @Test
    public void testMultipleTableArguments()
    {
        // multiple pass-through specifications indicate that the table function has multiple table arguments
        tester().assertThat(new PruneTableFunctionProcessorColumns())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    return p.project(
                            Assignments.builder().put(b, b).build(),
                            p.tableFunctionProcessor(
                                    builder -> builder
                                            .name("test_function")
                                            .properOutputs(p.variable("proper"))
                                            .passThroughSpecifications(
                                                    new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(a, true))),
                                                    new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(b, true))),
                                                    new PassThroughSpecification(true, ImmutableList.of(new PassThroughColumn(c, false))))
                                            .source(p.values(a, b, c, d))));
                })
                .matches(project(
                        ImmutableMap.of("b", expression("b")),
                        tableFunctionProcessor(builder -> builder
                                        .name("test_function")
                                        .properOutputs(ImmutableList.of("proper"))
                                        .passThroughSymbols(ImmutableList.of(ImmutableList.of(), ImmutableList.of("b"), ImmutableList.of())),
                                values("a", "b", "c", "d"))));
    }
}
