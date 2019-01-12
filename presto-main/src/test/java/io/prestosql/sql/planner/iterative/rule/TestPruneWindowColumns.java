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

import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.OrderingScheme;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.windowFrame;
import static com.facebook.presto.sql.tree.FrameBound.Type.CURRENT_ROW;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestPruneWindowColumns
        extends BaseRuleTest
{
    private static final Signature signature = new Signature(
            "min",
            FunctionKind.WINDOW,
            ImmutableList.of(),
            ImmutableList.of(),
            BIGINT.getTypeSignature(),
            ImmutableList.of(BIGINT.getTypeSignature()),
            false);

    private static final List<String> inputSymbolNameList =
            ImmutableList.of("orderKey", "partitionKey", "hash", "startValue1", "startValue2", "endValue1", "endValue2", "input1", "input2", "unused");
    private static final Set<String> inputSymbolNameSet = ImmutableSet.copyOf(inputSymbolNameList);

    private static final ExpectedValueProvider<WindowNode.Frame> frameProvider1 = windowFrame(
            WindowFrame.Type.RANGE,
            UNBOUNDED_PRECEDING,
            Optional.of("startValue1"),
            CURRENT_ROW,
            Optional.of("endValue1"));

    private static final ExpectedValueProvider<WindowNode.Frame> frameProvider2 = windowFrame(
            WindowFrame.Type.RANGE,
            UNBOUNDED_PRECEDING,
            Optional.of("startValue2"),
            CURRENT_ROW,
            Optional.of("endValue2"));

    @Test
    public void testWindowNotNeeded()
    {
        tester().assertThat(new PruneWindowColumns())
                .on(p -> buildProjectedWindow(p, symbol -> inputSymbolNameSet.contains(symbol.getName()), alwaysTrue()))
                .matches(
                        strictProject(
                                Maps.asMap(inputSymbolNameSet, PlanMatchPattern::expression),
                                values(inputSymbolNameList)));
    }

    @Test
    public void testOneFunctionNotNeeded()
    {
        tester().assertThat(new PruneWindowColumns())
                .on(p -> buildProjectedWindow(p,
                        symbol -> symbol.getName().equals("output2") || symbol.getName().equals("unused"),
                        alwaysTrue()))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "output2", expression("output2"),
                                        "unused", expression("unused")),
                                window(windowBuilder -> windowBuilder
                                                .prePartitionedInputs(ImmutableSet.of())
                                                .specification(
                                                        ImmutableList.of("partitionKey"),
                                                        ImmutableList.of("orderKey"),
                                                        ImmutableMap.of("orderKey", SortOrder.ASC_NULLS_FIRST))
                                                .preSortedOrderPrefix(0)
                                                .addFunction(
                                                        "output2",
                                                        functionCall("min", ImmutableList.of("input2")),
                                                        signature,
                                                        frameProvider2)
                                                .hashSymbol("hash"),
                                        strictProject(
                                                Maps.asMap(
                                                        Sets.difference(inputSymbolNameSet, ImmutableSet.of("input1", "startValue1", "endValue1")),
                                                        PlanMatchPattern::expression),
                                                values(inputSymbolNameList)))));
    }

    @Test
    public void testAllColumnsNeeded()
    {
        tester().assertThat(new PruneWindowColumns())
                .on(p -> buildProjectedWindow(p, alwaysTrue(), alwaysTrue()))
                .doesNotFire();
    }

    @Test
    public void testUsedInputsNotNeeded()
    {
        // If the WindowNode needs all its inputs, we can't discard them from its child.
        tester().assertThat(new PruneWindowColumns())
                .on(p -> buildProjectedWindow(
                        p,
                        // only the window function outputs
                        symbol -> !inputSymbolNameSet.contains(symbol.getName()),
                        // only the used input symbols
                        symbol -> !symbol.getName().equals("unused")))
                .doesNotFire();
    }

    @Test
    public void testUnusedInputNotNeeded()
    {
        tester().assertThat(new PruneWindowColumns())
                .on(p -> buildProjectedWindow(
                        p,
                        // only the window function outputs
                        symbol -> !inputSymbolNameSet.contains(symbol.getName()),
                        alwaysTrue()))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "output1", expression("output1"),
                                        "output2", expression("output2")),
                                window(windowBuilder -> windowBuilder
                                                .prePartitionedInputs(ImmutableSet.of())
                                                .specification(
                                                        ImmutableList.of("partitionKey"),
                                                        ImmutableList.of("orderKey"),
                                                        ImmutableMap.of("orderKey", SortOrder.ASC_NULLS_FIRST))
                                                .preSortedOrderPrefix(0)
                                                .addFunction(
                                                        "output1",
                                                        functionCall("min", ImmutableList.of("input1")),
                                                        signature,
                                                        frameProvider1)
                                                .addFunction(
                                                        "output2",
                                                        functionCall("min", ImmutableList.of("input2")),
                                                        signature,
                                                        frameProvider2)
                                                .hashSymbol("hash"),
                                        strictProject(
                                                Maps.asMap(
                                                        Sets.filter(inputSymbolNameSet, symbolName -> !symbolName.equals("unused")),
                                                        PlanMatchPattern::expression),
                                                values(inputSymbolNameList)))));
    }

    private static PlanNode buildProjectedWindow(
            PlanBuilder p,
            Predicate<Symbol> projectionFilter,
            Predicate<Symbol> sourceFilter)
    {
        Symbol orderKey = p.symbol("orderKey");
        Symbol partitionKey = p.symbol("partitionKey");
        Symbol hash = p.symbol("hash");
        Symbol startValue1 = p.symbol("startValue1");
        Symbol startValue2 = p.symbol("startValue2");
        Symbol endValue1 = p.symbol("endValue1");
        Symbol endValue2 = p.symbol("endValue2");
        Symbol input1 = p.symbol("input1");
        Symbol input2 = p.symbol("input2");
        Symbol unused = p.symbol("unused");
        Symbol output1 = p.symbol("output1");
        Symbol output2 = p.symbol("output2");
        List<Symbol> inputs = ImmutableList.of(orderKey, partitionKey, hash, startValue1, startValue2, endValue1, endValue2, input1, input2, unused);
        List<Symbol> outputs = ImmutableList.<Symbol>builder().addAll(inputs).add(output1, output2).build();

        return p.project(
                Assignments.identity(
                        outputs.stream()
                                .filter(projectionFilter)
                                .collect(toImmutableList())),
                p.window(
                        new WindowNode.Specification(
                                ImmutableList.of(partitionKey),
                                Optional.of(new OrderingScheme(
                                        ImmutableList.of(orderKey),
                                        ImmutableMap.of(orderKey, SortOrder.ASC_NULLS_FIRST)))),
                        ImmutableMap.of(
                                output1,
                                new WindowNode.Function(
                                        new FunctionCall(QualifiedName.of("min"), ImmutableList.of(input1.toSymbolReference())),
                                        signature,
                                        new WindowNode.Frame(
                                                WindowFrame.Type.RANGE,
                                                UNBOUNDED_PRECEDING,
                                                Optional.of(startValue1),
                                                CURRENT_ROW,
                                                Optional.of(endValue1),
                                                Optional.of(startValue1.toSymbolReference()),
                                                Optional.of(endValue2.toSymbolReference()))),
                                output2,
                                new WindowNode.Function(
                                        new FunctionCall(QualifiedName.of("min"), ImmutableList.of(input2.toSymbolReference())),
                                        signature,
                                        new WindowNode.Frame(
                                                WindowFrame.Type.RANGE,
                                                UNBOUNDED_PRECEDING,
                                                Optional.of(startValue2),
                                                CURRENT_ROW,
                                                Optional.of(endValue2),
                                                Optional.of(startValue2.toSymbolReference()),
                                                Optional.of(endValue2.toSymbolReference())))),
                        hash,
                        p.values(
                                inputs.stream()
                                        .filter(sourceFilter)
                                        .collect(toImmutableList()),
                                ImmutableList.of())));
    }
}
