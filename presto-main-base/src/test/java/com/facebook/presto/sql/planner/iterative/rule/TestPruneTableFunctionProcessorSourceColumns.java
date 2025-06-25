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
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.PassThroughColumn;
import com.facebook.presto.sql.planner.plan.TableFunctionNode.PassThroughSpecification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableFunctionProcessor;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneTableFunctionProcessorSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneUnreferencedSymbol()
    {
        // symbols 'a', 'b', 'c', 'd', 'hash', and 'marker' are used by the node.
        // symbol 'unreferenced' is pruned out. Also, the mapping for this symbol is removed from marker mappings
        tester().assertThat(new PruneTableFunctionProcessorSourceColumns())
                .on(p -> {
                    VariableReferenceExpression proper = p.variable("proper");
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    VariableReferenceExpression unreferenced = p.variable("unreferenced");
                    VariableReferenceExpression hash = p.variable("hash");
                    VariableReferenceExpression marker = p.variable("marker");
                    return p.tableFunctionProcessor(
                            builder -> builder
                                    .name("test_function")
                                    .properOutputs(proper)
                                    .passThroughSpecifications(new PassThroughSpecification(true, ImmutableList.of(new PassThroughColumn(a, false))))
                                    .requiredSymbols(ImmutableList.of(ImmutableList.of(b)))
                                    .markerSymbols(ImmutableMap.of(
                                            a, marker,
                                            b, marker,
                                            c, marker,
                                            d, marker,
                                            unreferenced, marker))
                                    .specification(new DataOrganizationSpecification(ImmutableList.of(c), Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(d, ASC_NULLS_FIRST))))))
                                    .hashSymbol(hash)
                                    .source(p.values(a, b, c, d, unreferenced, hash, marker)));
                })
                .matches(tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .properOutputs(ImmutableList.of("proper"))
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("a")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("b")))
                                .markerSymbols(ImmutableMap.of(
                                        "a", "marker",
                                        "b", "marker",
                                        "c", "marker",
                                        "d", "marker"))
                                .specification(specification(ImmutableList.of("c"), ImmutableList.of("d"), ImmutableMap.of("d", ASC_NULLS_FIRST)))
                                .hashSymbol("hash"),
                        project(
                                ImmutableMap.of(
                                        "a", expression("a"),
                                        "b", expression("b"),
                                        "c", expression("c"),
                                        "d", expression("d"),
                                        "hash", expression("hash"),
                                        "marker", expression("marker")),
                                values("a", "b", "c", "d", "unreferenced", "hash", "marker"))));
    }

    @Test
    public void testPruneUnusedMarkerSymbol()
    {
        // symbol 'unreferenced' is pruned out because the node does not use it.
        // also, the mapping for this symbol is removed from marker mappings.
        // because the marker symbol 'marker' is no longer used, it is pruned out too.
        // note: currently a marker symbol cannot become unused because the function
        // must use at least one symbol from each source. it might change in the future.
        tester().assertThat(new PruneTableFunctionProcessorSourceColumns())
                .on(p -> {
                    VariableReferenceExpression unreferenced = p.variable("unreferenced");
                    VariableReferenceExpression marker = p.variable("marker");
                    return p.tableFunctionProcessor(
                            builder -> builder
                                    .name("test_function")
                                    .markerSymbols(ImmutableMap.of(unreferenced, marker))
                                    .source(p.values(unreferenced, marker)));
                })
                .matches(tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .markerSymbols(ImmutableMap.of()),
                        project(
                                ImmutableMap.of(),
                                values("unreferenced", "marker"))));
    }

    @Test
    public void testMultipleSources()
    {
        // multiple pass-through specifications indicate that the table function has multiple table arguments
        // the third argument provides symbols 'e', 'f', and 'unreferenced'. those symbols are mapped to common marker symbol 'marker3'
        tester().assertThat(new PruneTableFunctionProcessorSourceColumns())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression d = p.variable("d");
                    VariableReferenceExpression e = p.variable("e");
                    VariableReferenceExpression f = p.variable("f");
                    VariableReferenceExpression marker1 = p.variable("marker1");
                    VariableReferenceExpression marker2 = p.variable("marker2");
                    VariableReferenceExpression marker3 = p.variable("marker3");
                    VariableReferenceExpression unreferenced = p.variable("unreferenced");
                    return p.tableFunctionProcessor(
                            builder -> builder
                                    .name("test_function")
                                    .passThroughSpecifications(
                                            new PassThroughSpecification(true, ImmutableList.of(new PassThroughColumn(a, false))),
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(c, true))),
                                            new PassThroughSpecification(false, ImmutableList.of(new PassThroughColumn(e, true))))
                                    .requiredSymbols(ImmutableList.of(
                                            ImmutableList.of(b),
                                            ImmutableList.of(d),
                                            ImmutableList.of(f)))
                                    .markerSymbols(ImmutableMap.of(
                                            a, marker1,
                                            b, marker1,
                                            c, marker2,
                                            d, marker2,
                                            e, marker3,
                                            f, marker3,
                                            unreferenced, marker3))
                                    .source(p.values(a, b, c, d, e, f, marker1, marker2, marker3, unreferenced)));
                })
                .matches(tableFunctionProcessor(builder -> builder
                                .name("test_function")
                                .passThroughSymbols(ImmutableList.of(ImmutableList.of("a"), ImmutableList.of("c"), ImmutableList.of("e")))
                                .requiredSymbols(ImmutableList.of(ImmutableList.of("b"), ImmutableList.of("d"), ImmutableList.of("f")))
                                .markerSymbols(ImmutableMap.of(
                                        "a", "marker1",
                                        "b", "marker1",
                                        "c", "marker2",
                                        "d", "marker2",
                                        "e", "marker3",
                                        "f", "marker3")),
                        project(
                                ImmutableMap.of(
                                        "a", expression("a"),
                                        "b", expression("b"),
                                        "c", expression("c"),
                                        "d", expression("d"),
                                        "e", expression("e"),
                                        "f", expression("f"),
                                        "marker1", expression("marker1"),
                                        "marker2", expression("marker2"),
                                        "marker3", expression("marker3")),
                                values("a", "b", "c", "d", "e", "f", "marker1", "marker2", "marker3", "unreferenced"))));
    }

    @Test
    public void allSymbolsReferenced()
    {
        tester().assertThat(new PruneTableFunctionProcessorSourceColumns())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression marker = p.variable("marker");
                    return p.tableFunctionProcessor(
                            builder -> builder
                                    .name("test_function")
                                    .requiredSymbols(ImmutableList.of(ImmutableList.of(a)))
                                    .markerSymbols(ImmutableMap.of(a, marker))
                                    .source(p.values(a, marker)));
                })
                .doesNotFire();
    }
}
