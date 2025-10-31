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
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.TableFunctionNode;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.TableArgumentProperties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.common.block.SortOrder.DESC_NULLS_FIRST;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableFunctionProcessor;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;
import static com.facebook.presto.sql.planner.plan.TableFunctionNode.PassThroughColumn;

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
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of()))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c"))),
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
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c"))),
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
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c", "d")))
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
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c", "d")))
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
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("d")))
                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of())),
                        values("c", "d")));
    }

    @Test
    public void testTwoSourcesWithSetSemantics()
    {
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    VariableReferenceExpression e = p.variable("e");
                    VariableReferenceExpression f = p.variable("f");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    p.values(c, d),
                                    p.values(e, f)),
                            ImmutableList.of(
                                    new TableArgumentProperties(
                                            "input_1",
                                            false,
                                            false,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            ImmutableList.of(d),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(c), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_2",
                                            false,
                                            false,
                                            new PassThroughSpecification(true, ImmutableList.of(new PassThroughColumn(e, false), new PassThroughColumn(f, false))),
                                            ImmutableList.of(f),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(), Optional.empty())))),
                            ImmutableList.of());
                })
                .matches(tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("e", "f")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("d"), ImmutableList.of("f")))
                                .markerSymbols(ImmutableMap.of(
                                        "c", "marker_1",
                                        "d", "marker_1",
                                        "e", "marker_2",
                                        "f", "marker_2"))
                                .specification(specification(ImmutableList.of("c"), ImmutableList.of("combined_row_number"), ImmutableMap.of("combined_row_number", ASC_NULLS_LAST))),
                        project(// append marker symbols
                                ImmutableMap.of(
                                        "marker_1", expression("IF(input_1_row_number = combined_row_number, input_1_row_number, null)"),
                                        "marker_2", expression("IF(input_2_row_number = combined_row_number, input_2_row_number, null)")),
                                project(// append helper symbols for joined nodes
                                        ImmutableMap.of(
                                                "combined_row_number", expression("IF(COALESCE(input_1_row_number, BIGINT '-1') > COALESCE(input_2_row_number, BIGINT '-1'), input_1_row_number, input_2_row_number)"),
                                                "combined_partition_size", expression("IF(COALESCE(input_1_partition_size, BIGINT '-1') > COALESCE(input_2_partition_size, BIGINT '-1'), input_1_partition_size, input_2_partition_size)")),
                                        join(// join nodes using helper symbols
                                                JoinType.FULL,
                                                ImmutableList.of(),
                                                Optional.of("input_1_row_number = input_2_row_number OR " +
                                                        "(input_1_row_number > input_2_partition_size AND input_2_row_number = BIGINT '1' OR " +
                                                        "input_2_row_number > input_1_partition_size AND input_1_row_number = BIGINT '1')"),
                                                window(// append helper symbols for source input_1
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_1_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_1
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_1_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("c", "d"))),
                                                window(// append helper symbols for source input_2
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of(), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_2_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_2
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of(), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_2_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("e", "f"))))))));
    }

    @Test
    public void testThreeSourcesWithSetSemantics()
    {
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    VariableReferenceExpression e = p.variable("e");
                    VariableReferenceExpression f = p.variable("f");
                    VariableReferenceExpression g = p.variable("g");
                    VariableReferenceExpression h = p.variable("h");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    p.values(c, d),
                                    p.values(e, f),
                                    p.values(g, h)),
                            ImmutableList.of(
                                    new TableArgumentProperties(
                                            "input_1",
                                            false,
                                            false,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            ImmutableList.of(d),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(c), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_2",
                                            false,
                                            false,
                                            new PassThroughSpecification(true, ImmutableList.of(new PassThroughColumn(e, false), new PassThroughColumn(f, false))),
                                            ImmutableList.of(f),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_3",
                                            false,
                                            false,
                                            new PassThroughSpecification(false, ImmutableList.of()),
                                            ImmutableList.of(h),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(), Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(h, DESC_NULLS_FIRST)))))))),
                            ImmutableList.of());
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("e", "f"), ImmutableList.of()))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("d"), ImmutableList.of("f"), ImmutableList.of("h")))
                                .markerSymbols(ImmutableMap.of(
                                        "c", "marker_1",
                                        "d", "marker_1",
                                        "e", "marker_2",
                                        "f", "marker_2",
                                        "g", "marker_3",
                                        "h", "marker_3"))
                                .specification(specification(ImmutableList.of("c"), ImmutableList.of("combined_row_number_1_2_3"), ImmutableMap.of("combined_row_number_1_2_3", ASC_NULLS_LAST))),
                        project(// append marker symbols
                                ImmutableMap.of(
                                        "marker_1", expression("IF(input_1_row_number = combined_row_number_1_2_3, input_1_row_number, null)"),
                                        "marker_2", expression("IF(input_2_row_number = combined_row_number_1_2_3, input_2_row_number, null)"),
                                        "marker_3", expression("IF(input_3_row_number = combined_row_number_1_2_3, input_3_row_number, null)")),
                                project(// append helper symbols for joined nodes
                                        ImmutableMap.of(
                                                "combined_row_number_1_2_3", expression("IF(COALESCE(combined_row_number_1_2, BIGINT '-1') > COALESCE(input_3_row_number, BIGINT '-1'), combined_row_number_1_2, input_3_row_number)"),
                                                "combined_partition_size_1_2_3", expression("IF(COALESCE(combined_partition_size_1_2, BIGINT '-1') > COALESCE(input_3_partition_size, BIGINT '-1'), combined_partition_size_1_2, input_3_partition_size)")),
                                        join(// join nodes using helper symbols
                                                JoinType.FULL,
                                                ImmutableList.of(),
                                                Optional.of("combined_row_number_1_2 = input_3_row_number OR " +
                                                        "(combined_row_number_1_2 > input_3_partition_size AND input_3_row_number = BIGINT '1' OR " +
                                                        "input_3_row_number > combined_partition_size_1_2 AND combined_row_number_1_2 = BIGINT '1')"),
                                                project(// append helper symbols for joined nodes
                                                        ImmutableMap.of(
                                                                "combined_row_number_1_2", expression("IF(COALESCE(input_1_row_number, BIGINT '-1') > COALESCE(input_2_row_number, BIGINT '-1'), input_1_row_number, input_2_row_number)"),
                                                                "combined_partition_size_1_2", expression("IF(COALESCE(input_1_partition_size, BIGINT '-1') > COALESCE(input_2_partition_size, BIGINT '-1'), input_1_partition_size, input_2_partition_size)")),
                                                        join(// join nodes using helper symbols
                                                                JoinType.FULL,
                                                                ImmutableList.of(),
                                                                Optional.of("input_1_row_number = input_2_row_number OR " +
                                                                        "(input_1_row_number > input_2_partition_size AND input_2_row_number = BIGINT '1' OR " +
                                                                        "input_2_row_number > input_1_partition_size AND input_1_row_number = BIGINT '1')"),
                                                                window(// append helper symbols for source input_1
                                                                        builder -> builder
                                                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                                                .addFunction("input_1_partition_size", functionCall("count", ImmutableList.of())),
                                                                        // input_1
                                                                        window(builder -> builder
                                                                                        .specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of())
                                                                                        .addFunction("input_1_row_number", functionCall("row_number", ImmutableList.of())),
                                                                                values("c", "d"))),
                                                                window(// append helper symbols for source input_2
                                                                        builder -> builder
                                                                                .specification(specification(ImmutableList.of(), ImmutableList.of(), ImmutableMap.of()))
                                                                                .addFunction("input_2_partition_size", functionCall("count", ImmutableList.of())),
                                                                        // input_2
                                                                        window(builder -> builder
                                                                                        .specification(ImmutableList.of(), ImmutableList.of(), ImmutableMap.of())
                                                                                        .addFunction("input_2_row_number", functionCall("row_number", ImmutableList.of())),
                                                                                values("e", "f"))))),
                                                window(// append helper symbols for source input_3
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of(), ImmutableList.of("h"), ImmutableMap.of("h", DESC_NULLS_FIRST)))
                                                                .addFunction("input_3_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_3
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of(), ImmutableList.of("h"), ImmutableMap.of("h", DESC_NULLS_FIRST))
                                                                        .addFunction("input_3_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("g", "h"))))))));
    }

    @Test
    public void testTwoCoPartitionedSources()
    {
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    VariableReferenceExpression e = p.variable("e");
                    VariableReferenceExpression f = p.variable("f");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    p.values(c, d),
                                    p.values(e, f)),
                            ImmutableList.of(
                                    new TableArgumentProperties(
                                            "input_1",
                                            false,
                                            true,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            ImmutableList.of(c, d),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(c), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_2",
                                            false,
                                            false,
                                            new PassThroughSpecification(true, ImmutableList.of(new PassThroughColumn(e, true), new PassThroughColumn(f, false))),
                                            ImmutableList.of(f),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(e), Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(f, DESC_NULLS_FIRST)))))))),
                            ImmutableList.of(ImmutableList.of("input_1", "input_2")));
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("e", "f")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c", "d"), ImmutableList.of("f")))
                                .markerSymbols(ImmutableMap.of(
                                        "c", "marker_1",
                                        "d", "marker_1",
                                        "e", "marker_2",
                                        "f", "marker_2"))
                                .specification(specification(ImmutableList.of("combined_partition_column"), ImmutableList.of("combined_row_number"), ImmutableMap.of("combined_row_number", ASC_NULLS_LAST))),
                        project(// append marker symbols
                                ImmutableMap.of(
                                        "marker_1", expression("IF(input_1_row_number = combined_row_number, input_1_row_number, null)"),
                                        "marker_2", expression("IF(input_2_row_number = combined_row_number, input_2_row_number, null)")),
                                project(// append helper and partitioning symbols for co-partitioned nodes
                                        ImmutableMap.of(
                                                "combined_row_number", expression("IF(COALESCE(input_1_row_number, BIGINT '-1') > COALESCE(input_2_row_number, BIGINT '-1'), input_1_row_number, input_2_row_number)"),
                                                "combined_partition_size", expression("IF(COALESCE(input_1_partition_size, BIGINT '-1') > COALESCE(input_2_partition_size, BIGINT '-1'), input_1_partition_size, input_2_partition_size)"),
                                                "combined_partition_column", expression("COALESCE(c, e)")),
                                        join(// co-partition nodes
                                                JoinType.LEFT,
                                                ImmutableList.of(),
                                                Optional.of("NOT (c IS DISTINCT FROM e) " +
                                                        "AND ( " +
                                                        "input_1_row_number = input_2_row_number OR " +
                                                        "(input_1_row_number > input_2_partition_size AND input_2_row_number = BIGINT '1' OR " +
                                                        "input_2_row_number > input_1_partition_size AND input_1_row_number = BIGINT '1')) "),
                                                window(// append helper symbols for source input_1
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_1_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_1
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_1_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("c", "d"))),
                                                window(// append helper symbols for source input_2
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("e"), ImmutableList.of("f"), ImmutableMap.of("f", DESC_NULLS_FIRST)))
                                                                .addFunction("input_2_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_2
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("e"), ImmutableList.of("f"), ImmutableMap.of("f", DESC_NULLS_FIRST))
                                                                        .addFunction("input_2_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("e", "f"))))))));
    }

    @Test
    public void testCoPartitionJoinTypes()
    {
        // both sources are prune when empty, so they are combined using inner join
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    p.values(c),
                                    p.values(d)),
                            ImmutableList.of(
                                    new TableArgumentProperties(
                                            "input_1",
                                            false,
                                            true,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            ImmutableList.of(c),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(c), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_2",
                                            false,
                                            true,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(d, true))),
                                            ImmutableList.of(d),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(d), Optional.empty())))),
                            ImmutableList.of(ImmutableList.of("input_1", "input_2")));
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d")))
                                .markerSymbols(ImmutableMap.of(
                                        "c", "marker_1",
                                        "d", "marker_2"))
                                .specification(specification(ImmutableList.of("combined_partition_column"), ImmutableList.of("combined_row_number"), ImmutableMap.of("combined_row_number", ASC_NULLS_LAST))),
                        project(// append marker symbols
                                ImmutableMap.of(
                                        "marker_1", expression("IF(input_1_row_number = combined_row_number, input_1_row_number, null)"),
                                        "marker_2", expression("IF(input_2_row_number = combined_row_number, input_2_row_number, null)")),
                                project(// append helper and partitioning symbols for co-partitioned nodes
                                        ImmutableMap.of(
                                                "combined_row_number", expression("IF(COALESCE(input_1_row_number, BIGINT '-1') > COALESCE(input_2_row_number, BIGINT '-1'), input_1_row_number, input_2_row_number)"),
                                                "combined_partition_size", expression("IF(COALESCE(input_1_partition_size, BIGINT '-1') > COALESCE(input_2_partition_size, BIGINT '-1'), input_1_partition_size, input_2_partition_size)"),
                                                "combined_partition_column", expression("COALESCE(c, d)")),
                                        join(// co-partition nodes
                                                JoinType.INNER,
                                                ImmutableList.of(),
                                                Optional.of("NOT (c IS DISTINCT FROM d) " +
                                                        "AND ( " +
                                                        "input_1_row_number = input_2_row_number OR " +
                                                        "(input_1_row_number > input_2_partition_size AND input_2_row_number = BIGINT '1' OR " +
                                                        "input_2_row_number > input_1_partition_size AND input_1_row_number = BIGINT '1')) "),
                                                window(// append helper symbols for source input_1
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_1_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_1
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_1_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("c"))),
                                                window(// append helper symbols for source input_2
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_2_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_2
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_2_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("d"))))))));

        // only the left source is prune when empty, so sources are combined using left join
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    p.values(c),
                                    p.values(d)),
                            ImmutableList.of(
                                    new TableArgumentProperties(
                                            "input_1",
                                            false,
                                            true,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            ImmutableList.of(c),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(c), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_2",
                                            false,
                                            false,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(d, true))),
                                            ImmutableList.of(d),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(d), Optional.empty())))),
                            ImmutableList.of(ImmutableList.of("input_1", "input_2")));
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d")))
                                .markerSymbols(ImmutableMap.of(
                                        "c", "marker_1",
                                        "d", "marker_2"))
                                .specification(specification(ImmutableList.of("combined_partition_column"), ImmutableList.of("combined_row_number"), ImmutableMap.of("combined_row_number", ASC_NULLS_LAST))),
                        project(// append marker symbols
                                ImmutableMap.of(
                                        "marker_1", expression("IF(input_1_row_number = combined_row_number, input_1_row_number, null)"),
                                        "marker_2", expression("IF(input_2_row_number = combined_row_number, input_2_row_number, null)")),
                                project(// append helper and partitioning symbols for co-partitioned nodes
                                        ImmutableMap.of(
                                                "combined_row_number", expression("IF(COALESCE(input_1_row_number, BIGINT '-1') > COALESCE(input_2_row_number, BIGINT '-1'), input_1_row_number, input_2_row_number)"),
                                                "combined_partition_size", expression("IF(COALESCE(input_1_partition_size, BIGINT '-1') > COALESCE(input_2_partition_size, BIGINT '-1'), input_1_partition_size, input_2_partition_size)"),
                                                "combined_partition_column", expression("COALESCE(c, d)")),
                                        join(// co-partition nodes
                                                JoinType.LEFT,
                                                ImmutableList.of(),
                                                Optional.of("NOT (c IS DISTINCT FROM d) " +
                                                        "AND ( " +
                                                        "     input_1_row_number = input_2_row_number OR " +
                                                        "     (input_1_row_number > input_2_partition_size AND input_2_row_number = BIGINT '1' OR " +
                                                        "     input_2_row_number > input_1_partition_size AND input_1_row_number = BIGINT '1'))"),
                                                window(// append helper symbols for source input_1
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_1_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_1
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_1_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("c"))),
                                                window(// append helper symbols for source input_2
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_2_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_2
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_2_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("d"))))))));

        // only the right source is prune when empty. the sources are reordered so that the prune when empty source is first. they are combined using left join
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    p.values(c),
                                    p.values(d)),
                            ImmutableList.of(
                                    new TableArgumentProperties(
                                            "input_1",
                                            false,
                                            false,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            ImmutableList.of(c),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(c), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_2",
                                            false,
                                            true,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(d, true))),
                                            ImmutableList.of(d),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(d), Optional.empty())))),
                            ImmutableList.of(ImmutableList.of("input_1", "input_2")));
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d")))
                                .markerSymbols(ImmutableMap.of(
                                        "c", "marker_1",
                                        "d", "marker_2"))
                                .specification(specification(ImmutableList.of("combined_partition_column"), ImmutableList.of("combined_row_number"), ImmutableMap.of("combined_row_number", ASC_NULLS_LAST))),
                        project(// append marker symbols
                                ImmutableMap.of(
                                        "marker_1", expression("IF(input_1_row_number = combined_row_number, input_1_row_number, null)"),
                                        "marker_2", expression("IF(input_2_row_number = combined_row_number, input_2_row_number, null)")),
                                project(// append helper and partitioning symbols for co-partitioned nodes
                                        ImmutableMap.of(
                                                "combined_row_number", expression("IF(COALESCE(input_2_row_number, BIGINT '-1') > COALESCE(input_1_row_number, BIGINT '-1'), input_2_row_number, input_1_row_number)"),
                                                "combined_partition_size", expression("IF(COALESCE(input_2_partition_size, BIGINT '-1') > COALESCE(input_1_partition_size, BIGINT '-1'), input_2_partition_size, input_1_partition_size)"),
                                                "combined_partition_column", expression("COALESCE(d, c)")),
                                        join(// co-partition nodes
                                                JoinType.LEFT,
                                                ImmutableList.of(),
                                                Optional.of("NOT (d IS DISTINCT FROM c) " +
                                                        "AND (" +
                                                        "   input_2_row_number = input_1_row_number OR" +
                                                        "   (input_2_row_number > input_1_partition_size AND input_1_row_number = BIGINT '1' OR" +
                                                        "   input_1_row_number > input_2_partition_size AND input_2_row_number = BIGINT '1'))"),
                                                window(// append helper symbols for source input_2
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_2_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_2
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_2_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("d"))),
                                                window(// append helper symbols for source input_1
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_1_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_1
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_1_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("c"))))))));

        // neither source is prune when empty, so sources are combined using full join
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    p.values(c),
                                    p.values(d)),
                            ImmutableList.of(
                                    new TableArgumentProperties(
                                            "input_1",
                                            false,
                                            false,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            ImmutableList.of(c),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(c), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_2",
                                            false,
                                            false,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(d, true))),
                                            ImmutableList.of(d),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(d), Optional.empty())))),
                            ImmutableList.of(ImmutableList.of("input_1", "input_2")));
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d")))
                                .markerSymbols(ImmutableMap.of(
                                        "c", "marker_1",
                                        "d", "marker_2"))
                                .specification(specification(ImmutableList.of("combined_partition_column"), ImmutableList.of("combined_row_number"), ImmutableMap.of("combined_row_number", ASC_NULLS_LAST))),
                        project(// append marker symbols
                                ImmutableMap.of(
                                        "marker_1", expression("IF(input_1_row_number = combined_row_number, input_1_row_number, null)"),
                                        "marker_2", expression("IF(input_2_row_number = combined_row_number, input_2_row_number, null)")),
                                project(// append helper and partitioning symbols for co-partitioned nodes
                                        ImmutableMap.of(
                                                "combined_row_number", expression("IF(COALESCE(input_1_row_number, BIGINT '-1') > COALESCE(input_2_row_number, BIGINT '-1'), input_1_row_number, input_2_row_number)"),
                                                "combined_partition_size", expression("IF(COALESCE(input_1_partition_size, BIGINT '-1') > COALESCE(input_2_partition_size, BIGINT '-1'), input_1_partition_size, input_2_partition_size)"),
                                                "combined_partition_column", expression("COALESCE(c, d)")),
                                        join(// co-partition nodes
                                                JoinType.FULL,
                                                ImmutableList.of(),
                                                Optional.of("NOT (c IS DISTINCT FROM d)" +
                                                        " AND (" +
                                                        "     input_1_row_number = input_2_row_number OR" +
                                                        "     (input_1_row_number > input_2_partition_size AND input_2_row_number = BIGINT '1' OR" +
                                                        "     input_2_row_number > input_1_partition_size AND input_1_row_number = BIGINT '1'))"),
                                                window(// append helper symbols for source input_1
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_1_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_1
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_1_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("c"))),
                                                window(// append helper symbols for source input_2
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_2_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_2
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_2_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("d"))))))));
    }

    @Test
    public void testThreeCoPartitionedSources()
    {
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    VariableReferenceExpression e = p.variable("e");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    p.values(c),
                                    p.values(d),
                                    p.values(e)),
                            ImmutableList.of(
                                    new TableArgumentProperties(
                                            "input_1",
                                            false,
                                            true,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            ImmutableList.of(c),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(c), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_2",
                                            false,
                                            true,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(d, true))),
                                            ImmutableList.of(d),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(d), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_3",
                                            false,
                                            false,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(e, true))),
                                            ImmutableList.of(e),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(e), Optional.empty())))),
                            ImmutableList.of(ImmutableList.of("input_1", "input_2", "input_3")));
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d"), ImmutableList.of("e")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d"), ImmutableList.of("e")))
                                .markerSymbols(ImmutableMap.of(
                                        "c", "marker_1",
                                        "d", "marker_2",
                                        "e", "marker_3"))
                                .specification(specification(ImmutableList.of("combined_partition_column_1_2_3"), ImmutableList.of("combined_row_number_1_2_3"), ImmutableMap.of("combined_row_number_1_2_3", ASC_NULLS_LAST))),
                        project(// append marker symbols
                                ImmutableMap.of(
                                        "marker_1", expression("IF(input_1_row_number = combined_row_number_1_2_3, input_1_row_number, null)"),
                                        "marker_2", expression("IF(input_2_row_number = combined_row_number_1_2_3, input_2_row_number, null)"),
                                        "marker_3", expression("IF(input_3_row_number = combined_row_number_1_2_3, input_3_row_number, null)")),
                                project(// append helper and partitioning symbols for co-partitioned nodes
                                        ImmutableMap.of(
                                                "combined_row_number_1_2_3", expression("IF(COALESCE(combined_row_number_1_2, BIGINT '-1') > COALESCE(input_3_row_number, BIGINT '-1'), combined_row_number_1_2, input_3_row_number)"),
                                                "combined_partition_size_1_2_3", expression("IF(COALESCE(combined_partition_size_1_2, BIGINT '-1') > COALESCE(input_3_partition_size, BIGINT '-1'), combined_partition_size_1_2, input_3_partition_size)"),
                                                "combined_partition_column_1_2_3", expression("COALESCE(combined_partition_column_1_2, e)")),
                                        join(// co-partition nodes
                                                JoinType.LEFT,
                                                ImmutableList.of(),
                                                Optional.of("NOT (combined_partition_column_1_2 IS DISTINCT FROM e) " +
                                                        "AND (" +
                                                        "     combined_row_number_1_2 = input_3_row_number OR" +
                                                        "     (combined_row_number_1_2 > input_3_partition_size AND input_3_row_number = BIGINT '1' OR" +
                                                        "     input_3_row_number > combined_partition_size_1_2 AND combined_row_number_1_2 = BIGINT '1'))"),
                                                project(// append helper and partitioning symbols for co-partitioned nodes
                                                        ImmutableMap.of(
                                                                "combined_row_number_1_2", expression("IF(COALESCE(input_1_row_number, BIGINT '-1') > COALESCE(input_2_row_number, BIGINT '-1'), input_1_row_number, input_2_row_number)"),
                                                                "combined_partition_size_1_2", expression("IF(COALESCE(input_1_partition_size, BIGINT '-1') > COALESCE(input_2_partition_size, BIGINT '-1'), input_1_partition_size, input_2_partition_size)"),
                                                                "combined_partition_column_1_2", expression("COALESCE(c, d)")),
                                                        join(// co-partition nodes
                                                                JoinType.INNER,
                                                                ImmutableList.of(),
                                                                Optional.of("NOT (c IS DISTINCT FROM d) " +
                                                                        "AND (" +
                                                                        "     input_1_row_number = input_2_row_number OR" +
                                                                        "     (input_1_row_number > input_2_partition_size AND input_2_row_number = BIGINT '1' OR" +
                                                                        "     input_2_row_number > input_1_partition_size AND input_1_row_number = BIGINT '1'))"),
                                                                window(// append helper symbols for source input_1
                                                                        builder -> builder
                                                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                                                .addFunction("input_1_partition_size", functionCall("count", ImmutableList.of())),
                                                                        // input_1
                                                                        window(builder -> builder
                                                                                        .specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of())
                                                                                        .addFunction("input_1_row_number", functionCall("row_number", ImmutableList.of())),
                                                                                values("c"))),
                                                                window(// append helper symbols for source input_2
                                                                        builder -> builder
                                                                                .specification(specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of()))
                                                                                .addFunction("input_2_partition_size", functionCall("count", ImmutableList.of())),
                                                                        // input_2
                                                                        window(builder -> builder
                                                                                        .specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of())
                                                                                        .addFunction("input_2_row_number", functionCall("row_number", ImmutableList.of())),
                                                                                values("d"))))),
                                                window(// append helper symbols for source input_3
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("e"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_3_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_3
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("e"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_3_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("e"))))))));
    }

    @Test
    public void testTwoCoPartitionLists()
    {
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    VariableReferenceExpression e = p.variable("e");
                    VariableReferenceExpression f = p.variable("f");
                    VariableReferenceExpression g = p.variable("g");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    p.values(c),
                                    p.values(d),
                                    p.values(e),
                                    p.values(f, g)),
                            ImmutableList.of(
                                    new TableArgumentProperties(
                                            "input_1",
                                            false,
                                            true,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            ImmutableList.of(c),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(c), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_2",
                                            false,
                                            true,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(d, true))),
                                            ImmutableList.of(d),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(d), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_3",
                                            false,
                                            false,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(e, true))),
                                            ImmutableList.of(e),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(e), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_4",
                                            false,
                                            false,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(f, true))),
                                            ImmutableList.of(g),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(f), Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(g, DESC_NULLS_FIRST)))))))),
                            ImmutableList.of(
                                    ImmutableList.of("input_1", "input_2"),
                                    ImmutableList.of("input_3", "input_4")));
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d"), ImmutableList.of("e"), ImmutableList.of("f")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d"), ImmutableList.of("e"), ImmutableList.of("g")))
                                .markerSymbols(ImmutableMap.of(
                                        "c", "marker_1",
                                        "d", "marker_2",
                                        "e", "marker_3",
                                        "f", "marker_4",
                                        "g", "marker_4"))
                                .specification(specification(ImmutableList.of("combined_partition_column_1_2", "combined_partition_column_3_4"), ImmutableList.of("combined_row_number_1_2_3_4"), ImmutableMap.of("combined_row_number_1_2_3_4", ASC_NULLS_LAST))),
                        project(// append marker symbols
                                ImmutableMap.of(
                                        "marker_1", expression("IF(input_1_row_number = combined_row_number_1_2_3_4, input_1_row_number, null)"),
                                        "marker_2", expression("IF(input_2_row_number = combined_row_number_1_2_3_4, input_2_row_number, null)"),
                                        "marker_3", expression("IF(input_3_row_number = combined_row_number_1_2_3_4, input_3_row_number, null)"),
                                        "marker_4", expression("IF(input_4_row_number = combined_row_number_1_2_3_4, input_4_row_number, null)")),
                                project(// append helper symbols for joined nodes
                                        ImmutableMap.of(
                                                "combined_row_number_1_2_3_4", expression("IF(COALESCE(combined_row_number_1_2, BIGINT '-1') > COALESCE(combined_row_number_3_4, BIGINT '-1'), combined_row_number_1_2, combined_row_number_3_4)"),
                                                "combined_partition_size_1_2_3_4", expression("IF(COALESCE(combined_partition_size_1_2, BIGINT '-1') > COALESCE(combined_partition_size_3_4, BIGINT '-1'), combined_partition_size_1_2, combined_partition_size_3_4)")),
                                        join(// join nodes using helper symbols
                                                JoinType.LEFT,
                                                ImmutableList.of(),
                                                Optional.of("combined_row_number_1_2 = combined_row_number_3_4 OR " +
                                                        "(combined_row_number_1_2 > combined_partition_size_3_4 AND combined_row_number_3_4 = BIGINT '1' OR " +
                                                        "combined_row_number_3_4 > combined_partition_size_1_2 AND combined_row_number_1_2 = BIGINT '1')"),
                                                project(// append helper and partitioning symbols for co-partitioned nodes
                                                        ImmutableMap.of(
                                                                "combined_row_number_1_2", expression("IF(COALESCE(input_1_row_number, BIGINT '-1') > COALESCE(input_2_row_number, BIGINT '-1'), input_1_row_number, input_2_row_number)"),
                                                                "combined_partition_size_1_2", expression("IF(COALESCE(input_1_partition_size, BIGINT '-1') > COALESCE(input_2_partition_size, BIGINT '-1'), input_1_partition_size, input_2_partition_size)"),
                                                                "combined_partition_column_1_2", expression("COALESCE(c, d)")),
                                                        join(// co-partition nodes
                                                                JoinType.INNER,
                                                                ImmutableList.of(),
                                                                Optional.of("NOT (c IS DISTINCT FROM d) " +
                                                                        "AND ( " +
                                                                        "input_1_row_number = input_2_row_number OR " +
                                                                        "(input_1_row_number > input_2_partition_size AND input_2_row_number = BIGINT '1' OR " +
                                                                        "input_2_row_number > input_1_partition_size AND input_1_row_number = BIGINT '1'))"),
                                                                window(// append helper symbols for source input_1
                                                                        builder -> builder
                                                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                                                .addFunction("input_1_partition_size", functionCall("count", ImmutableList.of())),
                                                                        // input_1
                                                                        window(builder -> builder
                                                                                        .specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of())
                                                                                        .addFunction("input_1_row_number", functionCall("row_number", ImmutableList.of())),
                                                                                values("c"))),
                                                                window(// append helper symbols for source input_2
                                                                        builder -> builder
                                                                                .specification(specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of()))
                                                                                .addFunction("input_2_partition_size", functionCall("count", ImmutableList.of())),
                                                                        // input_2
                                                                        window(builder -> builder
                                                                                        .specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of())
                                                                                        .addFunction("input_2_row_number", functionCall("row_number", ImmutableList.of())),
                                                                                values("d"))))),
                                                project(// append helper and partitioning symbols for co-partitioned nodes
                                                        ImmutableMap.of(
                                                                "combined_row_number_3_4", expression("IF(COALESCE(input_3_row_number, BIGINT '-1') > COALESCE(input_4_row_number, BIGINT '-1'), input_3_row_number, input_4_row_number)"),
                                                                "combined_partition_size_3_4", expression("IF(COALESCE(input_3_partition_size, BIGINT '-1') > COALESCE(input_4_partition_size, BIGINT '-1'), input_3_partition_size, input_4_partition_size)"),
                                                                "combined_partition_column_3_4", expression("COALESCE(e, f)")),
                                                        join(// co-partition nodes
                                                                JoinType.FULL,
                                                                ImmutableList.of(),
                                                                Optional.of("NOT (e IS DISTINCT FROM f) " +
                                                                        "AND ( " +
                                                                        "input_3_row_number = input_4_row_number OR " +
                                                                        "(input_3_row_number > input_4_partition_size AND input_4_row_number = BIGINT '1' OR " +
                                                                        "input_4_row_number > input_3_partition_size AND input_3_row_number = BIGINT '1')) "),
                                                                window(// append helper symbols for source input_3
                                                                        builder -> builder
                                                                                .specification(specification(ImmutableList.of("e"), ImmutableList.of(), ImmutableMap.of()))
                                                                                .addFunction("input_3_partition_size", functionCall("count", ImmutableList.of())),
                                                                        // input_3
                                                                        window(builder -> builder
                                                                                        .specification(ImmutableList.of("e"), ImmutableList.of(), ImmutableMap.of())
                                                                                        .addFunction("input_3_row_number", functionCall("row_number", ImmutableList.of())),
                                                                                values("e"))),
                                                                window(// append helper symbols for source input_4
                                                                        builder -> builder
                                                                                .specification(specification(ImmutableList.of("f"), ImmutableList.of("g"), ImmutableMap.of("g", DESC_NULLS_FIRST)))
                                                                                .addFunction("input_4_partition_size", functionCall("count", ImmutableList.of())),
                                                                        // input_4
                                                                        window(builder -> builder
                                                                                        .specification(ImmutableList.of("f"), ImmutableList.of("g"), ImmutableMap.of("g", DESC_NULLS_FIRST))
                                                                                        .addFunction("input_4_row_number", functionCall("row_number", ImmutableList.of())),
                                                                                values("f", "g"))))))))));
    }

    @Test
    public void testCoPartitionedAndNotCoPartitionedSources()
    {
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    VariableReferenceExpression e = p.variable("e");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    p.values(c),
                                    p.values(d),
                                    p.values(e)),
                            ImmutableList.of(
                                    new TableArgumentProperties(
                                            "input_1",
                                            false,
                                            true,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            ImmutableList.of(c),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(c), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_2",
                                            false,
                                            true,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(d, true))),
                                            ImmutableList.of(d),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(d), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_3",
                                            false,
                                            false,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(e, true))),
                                            ImmutableList.of(e),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(e), Optional.empty())))),
                            ImmutableList.of(ImmutableList.of("input_2", "input_3")));
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d"), ImmutableList.of("e")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("d"), ImmutableList.of("e")))
                                .markerSymbols(ImmutableMap.of(
                                        "c", "marker_1",
                                        "d", "marker_2",
                                        "e", "marker_3"))
                                .specification(specification(ImmutableList.of("combined_partition_column_2_3", "c"), ImmutableList.of("combined_row_number_2_3_1"), ImmutableMap.of("combined_row_number_2_3_1", ASC_NULLS_LAST))),
                        project(// append marker symbols
                                ImmutableMap.of(
                                        "marker_1", expression("IF(input_1_row_number = combined_row_number_2_3_1, input_1_row_number, null)"),
                                        "marker_2", expression("IF(input_2_row_number = combined_row_number_2_3_1, input_2_row_number, null)"),
                                        "marker_3", expression("IF(input_3_row_number = combined_row_number_2_3_1, input_3_row_number, null)")),
                                project(// append helper symbols for joined nodes
                                        ImmutableMap.of(
                                                "combined_row_number_2_3_1", expression("IF(COALESCE(combined_row_number_2_3, BIGINT '-1') > COALESCE(input_1_row_number, BIGINT '-1'), combined_row_number_2_3, input_1_row_number)"),
                                                "combined_partition_size_2_3_1", expression("IF(COALESCE(combined_partition_size_2_3, BIGINT '-1') > COALESCE(input_1_partition_size, BIGINT '-1'), combined_partition_size_2_3, input_1_partition_size)")),
                                        join(// join nodes using helper symbols
                                                JoinType.INNER,
                                                ImmutableList.of(),
                                                Optional.of("combined_row_number_2_3 = input_1_row_number OR " +
                                                        "(combined_row_number_2_3 > input_1_partition_size AND input_1_row_number = BIGINT '1' OR " +
                                                        "input_1_row_number > combined_partition_size_2_3 AND combined_row_number_2_3 = BIGINT '1')"),
                                                project(// append helper and partitioning symbols for co-partitioned nodes
                                                        ImmutableMap.of(
                                                                "combined_row_number_2_3", expression("IF(COALESCE(input_2_row_number, BIGINT '-1') > COALESCE(input_3_row_number, BIGINT '-1'), input_2_row_number, input_3_row_number)"),
                                                                "combined_partition_size_2_3", expression("IF(COALESCE(input_2_partition_size, BIGINT '-1') > COALESCE(input_3_partition_size, BIGINT '-1'), input_2_partition_size, input_3_partition_size)"),
                                                                "combined_partition_column_2_3", expression("COALESCE(d, e)")),
                                                        join(// co-partition nodes
                                                                JoinType.LEFT,
                                                                ImmutableList.of(),
                                                                Optional.of("NOT (d IS DISTINCT FROM e) " +
                                                                        "AND ( " +
                                                                        "input_2_row_number = input_3_row_number OR " +
                                                                        "(input_2_row_number > input_3_partition_size AND input_3_row_number = BIGINT '1' OR " +
                                                                        "input_3_row_number > input_2_partition_size AND input_2_row_number = BIGINT '1'))"),
                                                                window(// append helper symbols for source input_2
                                                                        builder -> builder
                                                                                .specification(specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of()))
                                                                                .addFunction("input_2_partition_size", functionCall("count", ImmutableList.of())),
                                                                        // input_2
                                                                        window(builder -> builder
                                                                                        .specification(ImmutableList.of("d"), ImmutableList.of(), ImmutableMap.of())
                                                                                        .addFunction("input_2_row_number", functionCall("row_number", ImmutableList.of())),
                                                                                values("d"))),
                                                                window(// append helper symbols for source input_3
                                                                        builder -> builder
                                                                                .specification(specification(ImmutableList.of("e"), ImmutableList.of(), ImmutableMap.of()))
                                                                                .addFunction("input_3_partition_size", functionCall("count", ImmutableList.of())),
                                                                        // input_3
                                                                        window(builder -> builder
                                                                                        .specification(ImmutableList.of("e"), ImmutableList.of(), ImmutableMap.of())
                                                                                        .addFunction("input_3_row_number", functionCall("row_number", ImmutableList.of())),
                                                                                values("e"))))),
                                                window(// append helper symbols for source input_1
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_1_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_1
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_1_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("c"))))))));
    }

    @Test
    public void testCoerceForCopartitioning()
    {
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c", TINYINT);
                    VariableReferenceExpression cCoerced = p.variable("c_coerced", INTEGER);
                    VariableReferenceExpression d = p.variable("d");
                    VariableReferenceExpression e = p.variable("e", INTEGER);
                    VariableReferenceExpression f = p.variable("f");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    // coerce column c for co-partitioning
                                    p.project(
                                            Assignments.builder()
                                                    .put(c, p.rowExpression("c"))
                                                    .put(d, p.rowExpression("d"))
                                                    .put(cCoerced, p.rowExpression("CAST(c AS INTEGER)"))
                                                    .build(),
                                            p.values(c, d)),
                                    p.values(e, f)),
                            ImmutableList.of(
                                    new TableArgumentProperties(
                                            "input_1",
                                            false,
                                            true,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            ImmutableList.of(c, d),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(cCoerced), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_2",
                                            false,
                                            false,
                                            new PassThroughSpecification(true, ImmutableList.of(new PassThroughColumn(e, true), new PassThroughColumn(f, false))),
                                            ImmutableList.of(f),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(e), Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(f, DESC_NULLS_FIRST)))))))),
                            ImmutableList.of(ImmutableList.of("input_1", "input_2")));
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("e", "f")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c", "d"), ImmutableList.of("f")))
                                .markerSymbols(ImmutableMap.of(
                                        "c", "marker_1",
                                        "c_coerced", "marker_1",
                                        "d", "marker_1",
                                        "e", "marker_2",
                                        "f", "marker_2"))
                                .specification(specification(ImmutableList.of("combined_partition_column"), ImmutableList.of("combined_row_number"), ImmutableMap.of("combined_row_number", ASC_NULLS_LAST))),
                        project(// append marker symbols
                                ImmutableMap.of(
                                        "marker_1", expression("IF(input_1_row_number = combined_row_number, input_1_row_number, null)"),
                                        "marker_2", expression("IF(input_2_row_number = combined_row_number, input_2_row_number, null)")),
                                project(// append helper and partitioning symbols for co-partitioned nodes
                                        ImmutableMap.of(
                                                "combined_row_number", expression("IF(COALESCE(input_1_row_number, BIGINT '-1') > COALESCE(input_2_row_number, BIGINT '-1'), input_1_row_number, input_2_row_number)"),
                                                "combined_partition_size", expression("IF(COALESCE(input_1_partition_size, BIGINT '-1') > COALESCE(input_2_partition_size, BIGINT '-1'), input_1_partition_size, input_2_partition_size)"),
                                                "combined_partition_column", expression("COALESCE(c_coerced, e)")),
                                        join(// co-partition nodes
                                                JoinType.LEFT,
                                                ImmutableList.of(),
                                                Optional.of("NOT (c_coerced IS DISTINCT FROM e) " +
                                                        "AND ( " +
                                                        "     input_1_row_number = input_2_row_number OR" +
                                                        "     (input_1_row_number > input_2_partition_size AND input_2_row_number = BIGINT '1' OR" +
                                                        "     input_2_row_number > input_1_partition_size AND input_1_row_number = BIGINT '1'))"),
                                                window(// append helper symbols for source input_1
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("c_coerced"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_1_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_1
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("c_coerced"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_1_row_number", functionCall("row_number", ImmutableList.of())),
                                                                project(
                                                                        ImmutableMap.of("c_coerced", expression("CAST(c AS INTEGER)")),
                                                                        values("c", "d")))),
                                                window(// append helper symbols for source input_2
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("e"), ImmutableList.of("f"), ImmutableMap.of("f", DESC_NULLS_FIRST)))
                                                                .addFunction("input_2_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_2
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("e"), ImmutableList.of("f"), ImmutableMap.of("f", DESC_NULLS_FIRST))
                                                                        .addFunction("input_2_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("e", "f"))))))));
    }

    @Test
    public void testTwoCoPartitioningColumns()
    {
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    VariableReferenceExpression e = p.variable("e");
                    VariableReferenceExpression f = p.variable("f");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    p.values(c, d),
                                    p.values(e, f)),
                            ImmutableList.of(
                                    new TableArgumentProperties(
                                            "input_1",
                                            false,
                                            true,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true), new PassThroughColumn(d, true))),
                                            ImmutableList.of(c),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(c, d), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_2",
                                            false,
                                            false,
                                            new PassThroughSpecification(true, ImmutableList.of(new PassThroughColumn(e, true), new PassThroughColumn(f, true))),
                                            ImmutableList.of(e),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(e, f), Optional.empty())))),
                            ImmutableList.of(ImmutableList.of("input_1", "input_2")));
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c", "d"), ImmutableList.of("e", "f")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("e")))
                                .markerSymbols(ImmutableMap.of(
                                        "c", "marker_1",
                                        "d", "marker_1",
                                        "e", "marker_2",
                                        "f", "marker_2"))
                                .specification(specification(ImmutableList.of("combined_partition_column_1", "combined_partition_column_2"), ImmutableList.of("combined_row_number"), ImmutableMap.of("combined_row_number", ASC_NULLS_LAST))),
                        project(// append marker symbols
                                ImmutableMap.of(
                                        "marker_1", expression("IF(input_1_row_number = combined_row_number, input_1_row_number, null)"),
                                        "marker_2", expression("IF(input_2_row_number = combined_row_number, input_2_row_number, null)")),
                                project(// append helper and partitioning symbols for co-partitioned nodes
                                        ImmutableMap.of(
                                                "combined_row_number", expression("IF(COALESCE(input_1_row_number, BIGINT '-1') > COALESCE(input_2_row_number, BIGINT '-1'), input_1_row_number, input_2_row_number)"),
                                                "combined_partition_size", expression("IF(COALESCE(input_1_partition_size, BIGINT '-1') > COALESCE(input_2_partition_size, BIGINT '-1'), input_1_partition_size, input_2_partition_size)"),
                                                "combined_partition_column_1", expression("COALESCE(c, e)"),
                                                "combined_partition_column_2", expression("COALESCE(d, f)")),
                                        join(// co-partition nodes
                                                JoinType.LEFT,
                                                ImmutableList.of(),
                                                Optional.of("NOT (c IS DISTINCT FROM e) " +
                                                        "AND NOT (d IS DISTINCT FROM f) " +
                                                        "AND ( " +
                                                        "     input_1_row_number = input_2_row_number OR" +
                                                        "     (input_1_row_number > input_2_partition_size AND input_2_row_number = BIGINT '1' OR" +
                                                        "     input_2_row_number > input_1_partition_size AND input_1_row_number = BIGINT '1'))"),
                                                window(// append helper symbols for source input_1
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("c", "d"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_1_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_1
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("c", "d"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_1_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("c", "d"))),
                                                window(// append helper symbols for source input_2
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("e", "f"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_2_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_2
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("e", "f"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_2_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("e", "f"))))))));
    }

    @Test
    public void testTwoSourcesWithRowAndSetSemantics()
    {
        tester().assertThat(new ImplementTableFunctionSource(tester().getMetadata()))
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    VariableReferenceExpression e = p.variable("e");
                    VariableReferenceExpression f = p.variable("f");
                    return p.tableFunction(
                            "test_function",
                            ImmutableList.of(a, b),
                            ImmutableList.of(
                                    p.values(c, d),
                                    p.values(e, f)),
                            ImmutableList.of(
                                    new TableArgumentProperties(
                                            "input_1",
                                            false,
                                            false,
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            ImmutableList.of(d),
                                            Optional.of(new DataOrganizationSpecification(ImmutableList.of(c), Optional.empty()))),
                                    new TableArgumentProperties(
                                            "input_2",
                                            true,
                                            false,
                                            new PassThroughSpecification(true, ImmutableList.of(new PassThroughColumn(e, false), new PassThroughColumn(f, false))),
                                            ImmutableList.of(e),
                                            Optional.empty())),
                            ImmutableList.of());
                })
                .matches(PlanMatchPattern.tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("a", "b"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("c"), ImmutableList.of("e", "f")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("d"), ImmutableList.of("e")))
                                .markerSymbols(ImmutableMap.of(
                                        "c", "marker_1",
                                        "d", "marker_1",
                                        "e", "marker_2",
                                        "f", "marker_2"))
                                .specification(specification(ImmutableList.of("c"), ImmutableList.of("combined_row_number"), ImmutableMap.of("combined_row_number", ASC_NULLS_LAST))),
                        project(// append marker symbols
                                ImmutableMap.of(
                                        "marker_1", expression("IF(input_1_row_number = combined_row_number, input_1_row_number, null)"),
                                        "marker_2", expression("IF(input_2_row_number = combined_row_number, input_2_row_number, null)")),
                                project(// append helper symbols for joined nodes
                                        ImmutableMap.of(
                                                "combined_row_number", expression("IF(COALESCE(input_1_row_number, BIGINT '-1') > COALESCE(input_2_row_number, BIGINT '-1'), input_1_row_number, input_2_row_number)"),
                                                "combined_partition_size", expression("IF(COALESCE(input_1_partition_size, BIGINT '-1') > COALESCE(input_2_partition_size, BIGINT '-1'), input_1_partition_size, input_2_partition_size)")),
                                        join(// join nodes using helper symbols
                                                JoinType.FULL,
                                                ImmutableList.of(),
                                                Optional.of("input_1_row_number = input_2_row_number OR " +
                                                        "(input_1_row_number > input_2_partition_size AND input_2_row_number = BIGINT '1' OR " +
                                                        "input_2_row_number > input_1_partition_size AND input_1_row_number = BIGINT '1')"),
                                                window(// append helper symbols for source input_1
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_1_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_1
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of("c"), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_1_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("c", "d"))),
                                                window(// append helper symbols for source input_2
                                                        builder -> builder
                                                                .specification(specification(ImmutableList.of(), ImmutableList.of(), ImmutableMap.of()))
                                                                .addFunction("input_2_partition_size", functionCall("count", ImmutableList.of())),
                                                        // input_2
                                                        window(builder -> builder
                                                                        .specification(ImmutableList.of(), ImmutableList.of(), ImmutableMap.of())
                                                                        .addFunction("input_2_row_number", functionCall("row_number", ImmutableList.of())),
                                                                values("e", "f"))))))));
    }
}
