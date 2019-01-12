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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.Signature;
import io.prestosql.sql.planner.assertions.ExpectedValueProvider;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.Window;
import io.prestosql.sql.tree.WindowFrame;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.functionCall;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.specification;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.window;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.prestosql.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;

public class TestMergeAdjacentWindows
        extends BaseRuleTest
{
    private static final WindowNode.Frame frame = new WindowNode.Frame(
            WindowFrame.Type.RANGE,
            UNBOUNDED_PRECEDING,
            Optional.empty(),
            CURRENT_ROW,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private static final Signature signature = new Signature(
            "avg",
            FunctionKind.WINDOW,
            ImmutableList.of(),
            ImmutableList.of(),
            DOUBLE.getTypeSignature(),
            ImmutableList.of(DOUBLE.getTypeSignature()),
            false);
    private static final String columnAAlias = "ALIAS_A";
    private static final ExpectedValueProvider<WindowNode.Specification> specificationA =
            specification(ImmutableList.of(columnAAlias), ImmutableList.of(), ImmutableMap.of());
    private static final Optional<Window> windowA =
            Optional.of(new Window(ImmutableList.of(new SymbolReference("a")), Optional.empty(), Optional.empty()));

    @Test
    public void testPlanWithoutWindowNode()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p -> p.values(p.symbol("a")))
                .doesNotFire();
    }

    @Test
    public void testPlanWithSingleWindowNode()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1"), newWindowNodeFunction("avg", "a")),
                                p.values(p.symbol("a"))))
                .doesNotFire();
    }

    @Test
    public void testDistinctAdjacentWindowSpecifications()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1"), newWindowNodeFunction("avg", "a")),
                                p.window(
                                        newWindowNodeSpecification(p, "b"),
                                        ImmutableMap.of(p.symbol("sum_1"), newWindowNodeFunction("sum", "b")),
                                        p.values(p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testIntermediateNonProjectNode()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(1))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_2"), newWindowNodeFunction("avg", "a")),
                                p.filter(
                                        expression("a > 5"),
                                        p.window(
                                                newWindowNodeSpecification(p, "a"),
                                                ImmutableMap.of(p.symbol("avg_1"), newWindowNodeFunction("avg", "a")),
                                                p.values(p.symbol("a"))))))
                .doesNotFire();
    }

    @Test
    public void testDependentAdjacentWindowsIdenticalSpecifications()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1"), newWindowNodeFunction("avg", windowA, "avg_2")),
                                p.window(
                                        newWindowNodeSpecification(p, "a"),
                                        ImmutableMap.of(p.symbol("avg_2"), newWindowNodeFunction("avg", windowA, "a")),
                                        p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void testDependentAdjacentWindowsDistinctSpecifications()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1"), newWindowNodeFunction("avg", windowA, "avg_2")),
                                p.window(
                                        newWindowNodeSpecification(p, "b"),
                                        ImmutableMap.of(p.symbol("avg_2"), newWindowNodeFunction("avg", windowA, "a")),
                                        p.values(p.symbol("a"), p.symbol("b")))))
                .doesNotFire();
    }

    @Test
    public void testIdenticalAdjacentWindowSpecifications()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("avg_1"), newWindowNodeFunction("avg", windowA, "a")),
                                p.window(
                                        newWindowNodeSpecification(p, "a"),
                                        ImmutableMap.of(p.symbol("sum_1"), newWindowNodeFunction("sum", windowA, "a")),
                                        p.values(p.symbol("a")))))
                .matches(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specificationA)
                                        .addFunction(functionCall("avg", Optional.empty(), ImmutableList.of(columnAAlias)))
                                        .addFunction(functionCall("sum", Optional.empty(), ImmutableList.of(columnAAlias))),
                                values(ImmutableMap.of(columnAAlias, 0))));
    }

    @Test
    public void testIntermediateProjectNodes()
    {
        String oneAlias = "ALIAS_one";
        String unusedAlias = "ALIAS_unused";
        String lagOutputAlias = "ALIAS_lagOutput";
        String avgOutputAlias = "ALIAS_avgOutput";

        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(2))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.symbol("lagOutput"), newWindowNodeFunction("lag", windowA, "a", "one")),
                                p.project(
                                        Assignments.builder()
                                                .put(p.symbol("one"), expression("CAST(1 AS bigint)"))
                                                .putIdentities(ImmutableList.of(p.symbol("a"), p.symbol("avgOutput")))
                                                .build(),
                                        p.project(
                                                Assignments.identity(p.symbol("a"), p.symbol("avgOutput"), p.symbol("unused")),
                                                p.window(
                                                        newWindowNodeSpecification(p, "a"),
                                                        ImmutableMap.of(p.symbol("avgOutput"), newWindowNodeFunction("avg", windowA, "a")),
                                                        p.values(p.symbol("a"), p.symbol("unused")))))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        columnAAlias, PlanMatchPattern.expression(columnAAlias),
                                        oneAlias, PlanMatchPattern.expression(oneAlias),
                                        lagOutputAlias, PlanMatchPattern.expression(lagOutputAlias),
                                        avgOutputAlias, PlanMatchPattern.expression(avgOutputAlias)),
                                window(windowMatcherBuilder -> windowMatcherBuilder
                                                .specification(specificationA)
                                                .addFunction(lagOutputAlias, functionCall("lag", Optional.empty(), ImmutableList.of(columnAAlias, oneAlias)))
                                                .addFunction(avgOutputAlias, functionCall("avg", Optional.empty(), ImmutableList.of(columnAAlias))),
                                        strictProject(
                                                ImmutableMap.of(
                                                        oneAlias, PlanMatchPattern.expression("CAST(1 AS bigint)"),
                                                        columnAAlias, PlanMatchPattern.expression(columnAAlias),
                                                        unusedAlias, PlanMatchPattern.expression(unusedAlias)),
                                                strictProject(
                                                        ImmutableMap.of(
                                                                columnAAlias, PlanMatchPattern.expression(columnAAlias),
                                                                unusedAlias, PlanMatchPattern.expression(unusedAlias)),
                                                        values(columnAAlias, unusedAlias))))));
    }

    private static WindowNode.Specification newWindowNodeSpecification(PlanBuilder planBuilder, String symbolName)
    {
        return new WindowNode.Specification(ImmutableList.of(planBuilder.symbol(symbolName, BIGINT)), Optional.empty());
    }

    private WindowNode.Function newWindowNodeFunction(String functionName, String... symbols)
    {
        return new WindowNode.Function(
                new FunctionCall(
                        QualifiedName.of(functionName),
                        Arrays.stream(symbols).map(SymbolReference::new).collect(Collectors.toList())),
                signature,
                frame);
    }

    private WindowNode.Function newWindowNodeFunction(String functionName, Optional<Window> window, String... symbols)
    {
        return new WindowNode.Function(
                new FunctionCall(
                        QualifiedName.of(functionName),
                        window,
                        false,
                        Arrays.stream(symbols).map(SymbolReference::new).collect(Collectors.toList())),
                signature,
                frame);
    }
}
