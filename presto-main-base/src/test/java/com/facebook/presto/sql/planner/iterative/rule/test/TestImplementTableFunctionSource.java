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

import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.TableFunctionNode;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.PassThroughColumn;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.TableArgumentProperties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableFunctionProcessor;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestImplementTableFunctionSource
        extends BaseRuleTest
{
    @Test
    public void testNoSources()
    {
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> p.tableFunction(
                        "test_function",
                        ImmutableList.of(p.variable("a")),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of()))
                .matches(tableFunctionProcessor(builder -> builder
                        .name("test_function")
                        .properOutputs(ImmutableList.of("a"))));
    }

    @Test
    public void testSingleSourceWithRowSemantics()
    {
        // no pass-through columns
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(p.values(c)),
                            ImmutableList.of(new TableFunctionNode.TableArgumentProperties(
                                    "table_argument",
                                    true,
                                    true,
                                    new TableFunctionNode.PassThroughSpecification(false, ImmutableList.of()),
                                    ImmutableList.of(c),
                                    Optional.empty())),
                            ImmutableList.of());
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of())),
                        values("c")));

        // pass-through columns
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(p.values(c)),
                            ImmutableList.of(new TableFunctionNode.TableArgumentProperties(
                                    "table_argument",
                                    true,
                                    true,
                                    new TableFunctionNode.PassThroughSpecification(true, ImmutableList.of(new TableFunctionNode.PassThroughColumn(c, false))),
                                    ImmutableList.of(c),
                                    Optional.empty())),
                            ImmutableList.of());
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c"))),
                        values("c")));
    }

    @Test
    public void testSingleSourceWithSetSemantics()
    {
        // no pass-through columns, no partition by
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(p.values(c, d)),
                            ImmutableList.of(new TableArgumentProperties(
                                    "table_argument",
                                    false,
                                    false,
                                    new PassThroughSpecification(false, ImmutableList.of()),
                                    ImmutableList.of(c, d),
                                    Optional.of(new DataOrganizationSpecification(ImmutableList.of(), Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(d, ASC_NULLS_LAST)))))))),
                            ImmutableList.of());
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of()))
                                .specification(specification(ImmutableList.of(), ImmutableList.of("d"), ImmutableMap.of("d", ASC_NULLS_LAST))),
                        values("c", "d")));

        // no pass-through columns, partitioning column present
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(p.values(c, d)),
                            ImmutableList.of(new TableArgumentProperties(
                                    "table_argument",
                                    false,
                                    false,
                                    new PassThroughSpecification(false, ImmutableList.of(new TableFunctionNode.PassThroughColumn(c, true))),
                                    ImmutableList.of(c, d),
                                    Optional.of(new DataOrganizationSpecification(ImmutableList.of(c), Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(d, ASC_NULLS_LAST)))))))),
                            ImmutableList.of());
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c")))
                                .specification(specification(ImmutableList.of("c"), ImmutableList.of("d"), ImmutableMap.of("d", ASC_NULLS_LAST))),
                        values("c", "d")));

        // pass-through columns
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(p.values(c, d)),
                            ImmutableList.of(new TableArgumentProperties(
                                    "table_argument",
                                    false,
                                    false,
                                    new PassThroughSpecification(true, ImmutableList.of(new PassThroughColumn(c, true), new PassThroughColumn(d, false))),
                                    ImmutableList.of(d),
                                    Optional.of(new DataOrganizationSpecification(ImmutableList.of(c), Optional.empty())))),
                            ImmutableList.of());
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c", "d")))
                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of())),
                        values("c", "d")));
    }
}
