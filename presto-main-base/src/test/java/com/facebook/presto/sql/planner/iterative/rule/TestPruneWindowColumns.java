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

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
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

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.plan.WindowNode.Frame.BoundType.CURRENT_ROW;
import static com.facebook.presto.spi.plan.WindowNode.Frame.BoundType.FOLLOWING;
import static com.facebook.presto.spi.plan.WindowNode.Frame.BoundType.PRECEDING;
import static com.facebook.presto.spi.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.spi.plan.WindowNode.Frame.WindowType.RANGE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.windowFrame;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestPruneWindowColumns
        extends BaseRuleTest
{
    private static final String FUNCTION_NAME = "min";
    private static final FunctionHandle FUNCTION_HANDLE = createTestMetadataManager().getFunctionAndTypeManager().lookupFunction(FUNCTION_NAME, fromTypes(BIGINT));

    private static final List<String> inputSymbolNameList =
            ImmutableList.of("orderKey", "partitionKey", "hash", "startValue1", "startValue2", "startValue3", "endValue1", "endValue2", "endValue3", "sortKeyForStartComparison3",
                    "sortKeyForEndComparison3", "input1", "input2", "input3", "unused");
    private static final Set<String> inputSymbolNameSet = ImmutableSet.copyOf(inputSymbolNameList);

    private static final ExpectedValueProvider<WindowNode.Frame> frameProvider1 = windowFrame(
            RANGE,
            UNBOUNDED_PRECEDING,
            Optional.of("startValue1"),
            CURRENT_ROW,
            Optional.of("endValue1"),
            Optional.of("orderKey"));

    private static final ExpectedValueProvider<WindowNode.Frame> frameProvider2 = windowFrame(
            RANGE,
            UNBOUNDED_PRECEDING,
            Optional.of("startValue2"),
            CURRENT_ROW,
            Optional.of("endValue2"),
            Optional.of("orderKey"));

    private static final ExpectedValueProvider<WindowNode.Frame> frameProvider3 = windowFrame(
            RANGE,
            PRECEDING,
            Optional.of("startValue3"),
            Optional.of(BIGINT),
            Optional.of("sortKeyForStartComparison3"),
            Optional.of(BIGINT),
            FOLLOWING,
            Optional.of("endValue3"),
            Optional.of(BIGINT),
            Optional.of("sortKeyForEndComparison3"),
            Optional.of(BIGINT));

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
                        symbol -> symbol.getName().equals("output2") || symbol.getName().equals("output3") || symbol.getName().equals("unused"),
                        alwaysTrue()))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "output2", expression("output2"),
                                        "output3", expression("output3"),
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
                                                        FUNCTION_HANDLE,
                                                        frameProvider2)
                                                .addFunction(
                                                        "output3",
                                                        functionCall("min", ImmutableList.of("input3")),
                                                        FUNCTION_HANDLE,
                                                        frameProvider3)
                                                .hashSymbol("hash"),
                                        strictProject(
                                                Maps.asMap(
                                                        Sets.difference(inputSymbolNameSet, ImmutableSet.of("input1", "startValue1", "endValue1")),
                                                        PlanMatchPattern::expression),
                                                values(inputSymbolNameList)))));
    }

    @Test
    public void testTwoFunctionsNotNeeded()
    {
        tester().assertThat(new PruneWindowColumns())
                .on(p -> buildProjectedWindow(p,
                        symbol -> symbol.getName().equals("output3") || symbol.getName().equals("unused"),
                        alwaysTrue()))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "output3", expression("output3"),
                                        "unused", expression("unused")),
                                window(windowBuilder -> windowBuilder
                                                .prePartitionedInputs(ImmutableSet.of())
                                                .specification(
                                                        ImmutableList.of("partitionKey"),
                                                        ImmutableList.of("orderKey"),
                                                        ImmutableMap.of("orderKey", SortOrder.ASC_NULLS_FIRST))
                                                .preSortedOrderPrefix(0)
                                                .addFunction(
                                                        "output3",
                                                        functionCall("min", ImmutableList.of("input3")),
                                                        FUNCTION_HANDLE,
                                                        frameProvider3)
                                                .hashSymbol("hash"),
                                        strictProject(
                                                Maps.asMap(
                                                        Sets.difference(inputSymbolNameSet, ImmutableSet.of("input1", "startValue1", "endValue1", "input2", "startValue2", "endValue2")),
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
                                        "output2", expression("output2"),
                                        "output3", expression("output3")),
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
                                                        FUNCTION_HANDLE,
                                                        frameProvider1)
                                                .addFunction(
                                                        "output2",
                                                        functionCall("min", ImmutableList.of("input2")),
                                                        FUNCTION_HANDLE,
                                                        frameProvider2)
                                                .addFunction(
                                                        "output3",
                                                        functionCall("min", ImmutableList.of("input3")),
                                                        FUNCTION_HANDLE,
                                                        frameProvider3)
                                                .hashSymbol("hash"),
                                        strictProject(
                                                Maps.asMap(
                                                        Sets.filter(inputSymbolNameSet, symbolName -> !symbolName.equals("unused")),
                                                        PlanMatchPattern::expression),
                                                values(inputSymbolNameList)))));
    }

    private static PlanNode buildProjectedWindow(
            PlanBuilder p,
            Predicate<VariableReferenceExpression> projectionFilter,
            Predicate<VariableReferenceExpression> sourceFilter)
    {
        VariableReferenceExpression orderKey = p.variable("orderKey");
        VariableReferenceExpression partitionKey = p.variable("partitionKey");
        VariableReferenceExpression hash = p.variable("hash");
        VariableReferenceExpression startValue1 = p.variable("startValue1");
        VariableReferenceExpression startValue2 = p.variable("startValue2");
        VariableReferenceExpression startValue3 = p.variable("startValue3");
        VariableReferenceExpression sortKeyForStartComparison3 = p.variable("sortKeyForStartComparison3");
        VariableReferenceExpression endValue1 = p.variable("endValue1");
        VariableReferenceExpression endValue2 = p.variable("endValue2");
        VariableReferenceExpression endValue3 = p.variable("endValue3");
        VariableReferenceExpression sortKeyForEndComparison3 = p.variable("sortKeyForEndComparison3");
        VariableReferenceExpression input1 = p.variable("input1");
        VariableReferenceExpression input2 = p.variable("input2");
        VariableReferenceExpression input3 = p.variable("input3");
        VariableReferenceExpression unused = p.variable("unused");
        VariableReferenceExpression output1 = p.variable("output1");
        VariableReferenceExpression output2 = p.variable("output2");
        VariableReferenceExpression output3 = p.variable("output3");
        List<VariableReferenceExpression> inputs = ImmutableList.of(orderKey, partitionKey, hash, startValue1, startValue2, startValue3, endValue1, endValue2, endValue3,
                sortKeyForStartComparison3, sortKeyForEndComparison3, input1, input2, input3, unused);
        List<VariableReferenceExpression> outputs = ImmutableList.<VariableReferenceExpression>builder().addAll(inputs).add(output1, output2, output3).build();

        List<VariableReferenceExpression> filteredInputs = inputs.stream().filter(sourceFilter).collect(toImmutableList());

        return p.project(
                identityAssignments(
                        outputs.stream()
                                .filter(projectionFilter)
                                .collect(toImmutableList())),
                p.window(
                        new DataOrganizationSpecification(
                                ImmutableList.of(partitionKey),
                                Optional.of(new OrderingScheme(
                                        ImmutableList.of(new Ordering(orderKey, SortOrder.ASC_NULLS_FIRST))))),
                        ImmutableMap.of(
                                output1,
                                new WindowNode.Function(
                                        call(FUNCTION_NAME, FUNCTION_HANDLE, BIGINT, input1),
                                        new WindowNode.Frame(
                                                RANGE,
                                                UNBOUNDED_PRECEDING,
                                                Optional.of(startValue1),
                                                Optional.of(orderKey),
                                                CURRENT_ROW,
                                                Optional.of(endValue1),
                                                Optional.of(orderKey),
                                                Optional.of(new SymbolReference(startValue1.getName())).map(Expression::toString),
                                                Optional.of(new SymbolReference(endValue2.getName())).map(Expression::toString)),
                                        false),
                                output2,
                                new WindowNode.Function(
                                        call(FUNCTION_NAME, FUNCTION_HANDLE, BIGINT, input2),
                                        new WindowNode.Frame(
                                                RANGE,
                                                UNBOUNDED_PRECEDING,
                                                Optional.of(startValue2),
                                                Optional.of(orderKey),
                                                CURRENT_ROW,
                                                Optional.of(endValue2),
                                                Optional.of(orderKey),
                                                Optional.of(new SymbolReference(startValue2.getName())).map(Expression::toString),
                                                Optional.of(new SymbolReference(endValue2.getName())).map(Expression::toString)),
                                        false),
                                output3,
                                new WindowNode.Function(
                                        call(FUNCTION_NAME, FUNCTION_HANDLE, BIGINT, input3),
                                        new WindowNode.Frame(
                                                RANGE,
                                                PRECEDING,
                                                Optional.of(startValue3),
                                                Optional.of(sortKeyForStartComparison3),
                                                FOLLOWING,
                                                Optional.of(endValue3),
                                                Optional.of(sortKeyForEndComparison3),
                                                Optional.of(new SymbolReference(startValue3.getName())).map(Expression::toString),
                                                Optional.of(new SymbolReference(endValue3.getName())).map(Expression::toString)),
                                        false)),
                        hash,
                        p.values(
                                filteredInputs,
                                ImmutableList.of())));
    }
}
