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

import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.Window;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.CURRENT_ROW;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType.RANGE;
import static com.facebook.presto.sql.relational.Expressions.call;

public class TestSwapAdjacentWindowsBySpecifications
        extends BaseRuleTest
{
    private WindowNode.Frame frame;
    private FunctionHandle functionHandle;

    public TestSwapAdjacentWindowsBySpecifications()
    {
        frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        functionHandle = createTestMetadataManager().getFunctionAndTypeManager().lookupFunction("avg", fromTypes(BIGINT));
    }

    @Test
    public void doesNotFireOnPlanWithoutWindowFunctions()
    {
        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p -> p.values(p.variable("a")))
                .doesNotFire();
    }

    @Test
    public void doesNotFireOnPlanWithSingleWindowNode()
    {
        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p -> p.window(new WindowNode.Specification(
                                ImmutableList.of(p.variable("a")),
                                Optional.empty()),
                        ImmutableMap.of(p.variable("avg_1"),
                                new WindowNode.Function(call("avg", functionHandle, DOUBLE, ImmutableList.of()), frame, false)),
                        p.values(p.variable("a"))))
                .doesNotFire();
    }

    @Test
    public void subsetComesFirst()
    {
        String columnAAlias = "ALIAS_A";
        String columnBAlias = "ALIAS_B";

        ExpectedValueProvider<WindowNode.Specification> specificationA = specification(ImmutableList.of(columnAAlias), ImmutableList.of(), ImmutableMap.of());
        ExpectedValueProvider<WindowNode.Specification> specificationAB = specification(ImmutableList.of(columnAAlias, columnBAlias), ImmutableList.of(), ImmutableMap.of());

        Optional<Window> windowAB = Optional.of(new Window(ImmutableList.of(new SymbolReference("a"), new SymbolReference("b")), Optional.empty(), Optional.empty()));
        Optional<Window> windowA = Optional.of(new Window(ImmutableList.of(new SymbolReference("a")), Optional.empty(), Optional.empty()));

        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p ->
                        p.window(new WindowNode.Specification(
                                        ImmutableList.of(p.variable("a")),
                                        Optional.empty()),
                                ImmutableMap.of(p.variable("avg_1", DOUBLE), newWindowNodeFunction(ImmutableList.of(new Symbol("a")))),
                                p.window(new WindowNode.Specification(
                                                ImmutableList.of(p.variable("a"), p.variable("b")),
                                                Optional.empty()),
                                        ImmutableMap.of(p.variable("avg_2", DOUBLE), newWindowNodeFunction(ImmutableList.of(new Symbol("b")))),
                                        p.values(p.variable("a"), p.variable("b")))))
                .matches(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationAB)
                                        .addFunction(functionCall("avg", Optional.empty(), ImmutableList.of(columnBAlias))),
                                window(windowMatcherBuilder -> windowMatcherBuilder
                                                .specification(specificationA)
                                                .addFunction(functionCall("avg", Optional.empty(), ImmutableList.of(columnAAlias))),
                                        values(ImmutableMap.of(columnAAlias, 0, columnBAlias, 1)))));
    }

    @Test
    public void dependentWindowsAreNotReordered()
    {
        Optional<Window> windowA = Optional.of(new Window(ImmutableList.of(new SymbolReference("a")), Optional.empty(), Optional.empty()));

        tester().assertThat(new GatherAndMergeWindows.SwapAdjacentWindowsBySpecifications(0))
                .on(p ->
                        p.window(new WindowNode.Specification(
                                        ImmutableList.of(p.variable("a")),
                                        Optional.empty()),
                                ImmutableMap.of(p.variable("avg_1"), newWindowNodeFunction(ImmutableList.of(new Symbol("avg_2")))),
                                p.window(new WindowNode.Specification(
                                                ImmutableList.of(p.variable("a"), p.variable("b")),
                                                Optional.empty()),
                                        ImmutableMap.of(p.variable("avg_2"), newWindowNodeFunction(ImmutableList.of(new Symbol("a")))),
                                        p.values(p.variable("a"), p.variable("b")))))
                .doesNotFire();
    }

    private WindowNode.Function newWindowNodeFunction(List<Symbol> symbols)
    {
        return new WindowNode.Function(
                call("avg", functionHandle, BIGINT, symbols.stream().map(symbol -> new VariableReferenceExpression(symbol.getName(), BIGINT)).collect(Collectors.toList())),
                frame,
                false);
    }
}
