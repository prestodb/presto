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
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.CURRENT_ROW;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType.RANGE;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;

public class TestMergeAdjacentWindows
        extends BaseRuleTest
{
    private static final WindowNode.Frame frame = new WindowNode.Frame(
            RANGE,
            UNBOUNDED_PRECEDING,
            Optional.empty(),
            CURRENT_ROW,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private static final FunctionHandle SUM_FUNCTION_HANDLE = createTestMetadataManager().getFunctionAndTypeManager().lookupFunction("sum", fromTypes(DOUBLE));
    private static final FunctionHandle AVG_FUNCTION_HANDLE = createTestMetadataManager().getFunctionAndTypeManager().lookupFunction("avg", fromTypes(DOUBLE));
    private static final FunctionHandle LAG_FUNCTION_HANDLE = createTestMetadataManager().getFunctionAndTypeManager().lookupFunction("lag", fromTypes(DOUBLE));
    private static final String columnAAlias = "ALIAS_A";
    private static final ExpectedValueProvider<WindowNode.Specification> specificationA =
            specification(ImmutableList.of(columnAAlias), ImmutableList.of(), ImmutableMap.of());

    @Test
    public void testPlanWithoutWindowNode()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p -> p.values(p.variable("a")))
                .doesNotFire();
    }

    @Test
    public void testPlanWithSingleWindowNode()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.variable("avg_1"), newWindowNodeFunction("avg", AVG_FUNCTION_HANDLE, "a")),
                                p.values(p.variable("a"))))
                .doesNotFire();
    }

    @Test
    public void testDistinctAdjacentWindowSpecifications()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.variable("avg_1"), newWindowNodeFunction("avg", AVG_FUNCTION_HANDLE, "a")),
                                p.window(
                                        newWindowNodeSpecification(p, "b"),
                                        ImmutableMap.of(p.variable("sum_1"), newWindowNodeFunction("sum", SUM_FUNCTION_HANDLE, "b")),
                                        p.values(p.variable("b")))))
                .doesNotFire();
    }

    @Test
    public void testIntermediateNonProjectNode()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(1))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.variable("avg_2"), newWindowNodeFunction("avg", AVG_FUNCTION_HANDLE, "a")),
                                p.filter(
                                        expression("a > 5"),
                                        p.window(
                                                newWindowNodeSpecification(p, "a"),
                                                ImmutableMap.of(p.variable("avg_1"), newWindowNodeFunction("avg", AVG_FUNCTION_HANDLE, "a")),
                                                p.values(p.variable("a"))))))
                .doesNotFire();
    }

    @Test
    public void testDependentAdjacentWindowsIdenticalSpecifications()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.variable("avg_1"), newWindowNodeFunction("avg", AVG_FUNCTION_HANDLE, "avg_2")),
                                p.window(
                                        newWindowNodeSpecification(p, "a"),
                                        ImmutableMap.of(p.variable("avg_2"), newWindowNodeFunction("avg", AVG_FUNCTION_HANDLE, "a")),
                                        p.values(p.variable("a")))))
                .doesNotFire();
    }

    @Test
    public void testDependentAdjacentWindowsDistinctSpecifications()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.variable("avg_1"), newWindowNodeFunction("avg", AVG_FUNCTION_HANDLE, "avg_2")),
                                p.window(
                                        newWindowNodeSpecification(p, "b"),
                                        ImmutableMap.of(p.variable("avg_2"), newWindowNodeFunction("avg", AVG_FUNCTION_HANDLE, "a")),
                                        p.values(p.variable("a"), p.variable("b")))))
                .doesNotFire();
    }

    @Test
    public void testIdenticalAdjacentWindowSpecifications()
    {
        tester().assertThat(new GatherAndMergeWindows.MergeAdjacentWindowsOverProjects(0))
                .on(p ->
                        p.window(
                                newWindowNodeSpecification(p, "a"),
                                ImmutableMap.of(p.variable("avg_1"), newWindowNodeFunction("avg", AVG_FUNCTION_HANDLE, "a")),
                                p.window(
                                        newWindowNodeSpecification(p, "a"),
                                        ImmutableMap.of(p.variable("sum_1"), newWindowNodeFunction("sum", SUM_FUNCTION_HANDLE, "a")),
                                        p.values(p.variable("a")))))
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
                                ImmutableMap.of(p.variable("lagOutput"), newWindowNodeFunction("lag", LAG_FUNCTION_HANDLE, "a", "one")),
                                p.project(
                                        Assignments.builder()
                                                .put(p.variable("one"), constant(1L, BIGINT))
                                                .putAll(identityAssignments(ImmutableList.of(p.variable("a"), p.variable("avgOutput"))))
                                                .build(),
                                        p.project(
                                                identityAssignments(p.variable("a"), p.variable("avgOutput"), p.variable("unused")),
                                                p.window(
                                                        newWindowNodeSpecification(p, "a"),
                                                        ImmutableMap.of(p.variable("avgOutput"), newWindowNodeFunction("avg", AVG_FUNCTION_HANDLE, "a")),
                                                        p.values(p.variable("a"), p.variable("unused")))))))
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
        return new WindowNode.Specification(ImmutableList.of(planBuilder.variable(symbolName, BIGINT)), Optional.empty());
    }

    private WindowNode.Function newWindowNodeFunction(String name, FunctionHandle functionHandle, String... symbols)
    {
        return new WindowNode.Function(
                call(
                        name,
                        functionHandle,
                        BIGINT,
                        Arrays.stream(symbols).map(symbol -> new VariableReferenceExpression(symbol, BIGINT)).collect(Collectors.toList())),
                frame,
                false);
    }
}
