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
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;
import static com.facebook.presto.sql.tree.FrameBound.Type.CURRENT_ROW;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

public class TestMergeAdjacentWindows
{
    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    private static final WindowNode.Frame frame = new WindowNode.Frame(WindowFrame.Type.RANGE, UNBOUNDED_PRECEDING,
            Optional.empty(), CURRENT_ROW, Optional.empty());
    private static final Signature signature = new Signature(
            "avg",
            FunctionKind.WINDOW,
            ImmutableList.of(),
            ImmutableList.of(),
            DOUBLE.getTypeSignature(),
            ImmutableList.of(DOUBLE.getTypeSignature()),
            false);

    @Test
    public void testPlanWithoutWindowNode()
            throws Exception
    {
        tester.assertThat(new MergeAdjacentWindows())
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();
    }

    @Test
    public void testPlanWithSingleWindowNode()
            throws Exception
    {
        tester.assertThat(new MergeAdjacentWindows())
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1", BIGINT), newWindowNodeFunction("avg", "a")),
                                p.values(p.symbol("a", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testDistinctAdjacentWindowSpecifications()
    {
        tester.assertThat(new MergeAdjacentWindows())
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1", BIGINT), newWindowNodeFunction("avg", "a")),
                                p.window(
                                        newWindowNodeSpecification(p, "b"),
                                        ImmutableMap.of(p.symbol("sum_1", BIGINT), newWindowNodeFunction("sum", "b")),
                                        p.values(p.symbol("b", BIGINT))
                                )
                        ))
                .doesNotFire();
    }

    @Test
    public void testNonWindowIntermediateNode()
    {
        tester.assertThat(new MergeAdjacentWindows())
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("lag_1", BIGINT), newWindowNodeFunction("lag", "a", "ONE")),
                                p.project(
                                        Assignments.copyOf(ImmutableMap.of(p.symbol("ONE", BIGINT), p.expression("CAST(1 AS bigint)"))),
                                        p.window(
                                                newWindowNodeSpecification(p, "a"),
                                                ImmutableMap.of(p.symbol("avg_1", BIGINT), newWindowNodeFunction("avg", "a")),
                                                p.values(p.symbol("a", BIGINT))
                                        )
                                )
                        ))
                .doesNotFire();
    }

    @Test
    public void testDependentAdjacentWindowsIdenticalSpecifications()
            throws Exception
    {
        Optional<Window> windowA = Optional.of(new Window(ImmutableList.of(new SymbolReference("a")), Optional.empty(), Optional.empty()));

        tester.assertThat(new MergeAdjacentWindows())
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1", BIGINT), newWindowNodeFunction("avg", windowA, "avg_2")),
                                p.window(
                                        newWindowNodeSpecification(p, "a"),
                                        ImmutableMap.of(p.symbol("avg_2", BIGINT), newWindowNodeFunction("avg", windowA, "a")),
                                        p.values(p.symbol("a", BIGINT))
                                )
                        ))
                .doesNotFire();
    }

    @Test
    public void testDependentAdjacentWindowsDistinctSpecifications()
            throws Exception
    {
        Optional<Window> windowA = Optional.of(new Window(ImmutableList.of(new SymbolReference("a")), Optional.empty(), Optional.empty()));

        tester.assertThat(new MergeAdjacentWindows())
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1", BIGINT), newWindowNodeFunction("avg", windowA, "avg_2")),
                                p.window(
                                        newWindowNodeSpecification(p, "b"),
                                        ImmutableMap.of(p.symbol("avg_2", BIGINT), newWindowNodeFunction("avg", windowA, "a")),
                                        p.values(p.symbol("a", BIGINT), p.symbol("b", BIGINT))
                                )
                        ))
                .doesNotFire();
    }

    @Test
    public void testIdenticalAdjacentWindowSpecifications()
            throws Exception
    {
        String columnAAlias = "ALIAS_A";

        ExpectedValueProvider<WindowNode.Specification> specificationA = specification(ImmutableList.of(columnAAlias), ImmutableList.of(), ImmutableMap.of());

        Optional<Window> windowA = Optional.of(new Window(ImmutableList.of(new SymbolReference("a")), Optional.empty(), Optional.empty()));

        tester.assertThat(new MergeAdjacentWindows())
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1", BIGINT), newWindowNodeFunction("avg", windowA, "a")),
                                p.window(
                                        newWindowNodeSpecification(p, "a"),
                                        ImmutableMap.of(p.symbol("sum_1", BIGINT), newWindowNodeFunction("sum", windowA, "a")),
                                        p.values(p.symbol("a", BIGINT))
                                )
                        ))
                .matches(window(
                        specificationA,
                        ImmutableList.of(
                                functionCall("avg", Optional.empty(), ImmutableList.of(columnAAlias)),
                                functionCall("sum", Optional.empty(), ImmutableList.of(columnAAlias))),
                        values(ImmutableMap.of(columnAAlias, 0))));
    }

    private static WindowNode.Specification newWindowNodeSpecification(PlanBuilder planBuilder, String symbolName)
    {
        return new WindowNode.Specification(ImmutableList.of(planBuilder.symbol(symbolName, BIGINT)), ImmutableList.of(), ImmutableMap.of());
    }

    private WindowNode.Function newWindowNodeFunction(String functionName, String... symbols)
    {
        return new WindowNode.Function(
                new FunctionCall(
                        QualifiedName.of(functionName),
                        Arrays.stream(symbols).map(symbol -> new SymbolReference(symbol)).collect(Collectors.toList())),
                signature,
                frame);
    }

    private WindowNode.Function newWindowNodeFunction(String functionName, Optional<Window> window, String symbolName)
    {
        return new WindowNode.Function(
                new FunctionCall(QualifiedName.of(functionName), window, false, ImmutableList.of(new SymbolReference(symbolName))),
                signature,
                frame);
    }
}
