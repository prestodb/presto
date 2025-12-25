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

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionCallRewriter;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.ENABLE_FUNCTION_CALL_REWRITER;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestFunctionCallRewriting
        extends BaseRuleTest
{
    private FunctionAndTypeManager functionAndTypeManager;
    private FunctionResolution functionResolution;

    private static RowExpression constant(long value)
    {
        return new com.facebook.presto.spi.relation.ConstantExpression(value, BIGINT);
    }

    private static VariableReferenceExpression variable(String name)
    {
        return new VariableReferenceExpression(Optional.empty(), name, BIGINT);
    }

    @BeforeClass
    @Override
    public void setUp()
    {
        super.setUp();
        functionAndTypeManager = getMetadata().getFunctionAndTypeManager();
        functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Test
    public void testRuleDisabledByDefault()
    {
        FunctionCallRewriter rewriter = createSimpleRewriter();

        FunctionHandle absHandle = functionAndTypeManager.lookupFunction("abs", fromTypes(BIGINT));
        CallExpression absCall = new CallExpression("abs", absHandle, BIGINT, ImmutableList.of(variable("a")));

        tester().assertThat(
                        new FunctionCallRewriting(
                                () -> ImmutableSet.of(rewriter),
                                functionResolution).projectRowExpressionRewriteRule())
                .on(p -> p.project(
                        assignment(p.variable("result", BIGINT), absCall),
                        p.values(p.variable("a", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testRuleEnabledWithNoRewriters()
    {
        FunctionHandle absHandle = functionAndTypeManager.lookupFunction("abs", fromTypes(BIGINT));
        CallExpression absCall = new CallExpression("abs", absHandle, BIGINT, ImmutableList.of(variable("a")));

        tester().assertThat(
                        new FunctionCallRewriting(
                                ImmutableSet::of,
                                functionResolution).projectRowExpressionRewriteRule())
                .setSystemProperty(ENABLE_FUNCTION_CALL_REWRITER, "true")
                .on(p -> p.project(
                        assignment(p.variable("result", BIGINT), absCall),
                        p.values(p.variable("a", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testRewriteProjectNode()
    {
        FunctionCallRewriter absRewriter = createAbsToAddOneRewriter();

        FunctionHandle absHandle = functionAndTypeManager.lookupFunction("abs", fromTypes(BIGINT));
        CallExpression absCall = new CallExpression("abs", absHandle, BIGINT, ImmutableList.of(variable("a")));

        tester().assertThat(
                        new FunctionCallRewriting(
                                () -> ImmutableSet.of(absRewriter),
                                functionResolution).projectRowExpressionRewriteRule())
                .setSystemProperty(ENABLE_FUNCTION_CALL_REWRITER, "true")
                .on(p -> p.project(
                        assignment(p.variable("result", BIGINT), absCall),
                        p.values(p.variable("a", BIGINT))))
                .matches(
                        project(ImmutableMap.of("result", expression("a + BIGINT '1'")),
                                values("a")));
    }

    @Test
    public void testRewriteFilterNode()
    {
        FunctionCallRewriter comparisonRewriter = createComparisonRewriter();

        FunctionHandle gtHandle = functionAndTypeManager.lookupFunction("$operator$GREATER_THAN", fromTypes(BIGINT, BIGINT));
        CallExpression gtCall = new CallExpression(
                "$operator$GREATER_THAN",
                gtHandle,
                BOOLEAN,
                ImmutableList.of(variable("a"), constant(5L)));

        tester().assertThat(
                        new FunctionCallRewriting(
                                () -> ImmutableSet.of(comparisonRewriter),
                                functionResolution).filterRowExpressionRewriteRule())
                .setSystemProperty(ENABLE_FUNCTION_CALL_REWRITER, "true")
                .on(p -> p.filter(gtCall, p.values(p.variable("a", BIGINT))))
                .matches(
                        filter("(BIGINT '5') < a",
                                values("a")));
    }

    @Test
    public void testNoMatchingRewriter()
    {
        FunctionCallRewriter customRewriter = (call, connectorSession, variableAllocator, idAllocator, funcResolution) -> {
            if (call.getDisplayName().equals("custom_function")) {
                return Optional.of(TRUE_CONSTANT);
            }
            return Optional.empty();
        };

        FunctionHandle absHandle = functionAndTypeManager.lookupFunction("abs", fromTypes(BIGINT));
        CallExpression absCall = new CallExpression("abs", absHandle, BIGINT, ImmutableList.of(variable("a")));

        tester().assertThat(
                        new FunctionCallRewriting(
                                () -> ImmutableSet.of(customRewriter),
                                functionResolution).projectRowExpressionRewriteRule())
                .setSystemProperty(ENABLE_FUNCTION_CALL_REWRITER, "true")
                .on(p -> p.project(
                        assignment(p.variable("result", BIGINT), absCall),
                        p.values(p.variable("a", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testMultipleRewriters()
    {
        FunctionCallRewriter rewriter1 = (call, connectorSession, variableAllocator, idAllocator, funcResolution) -> {
            if (call.getDisplayName().equals("custom_a")) {
                return Optional.of(TRUE_CONSTANT);
            }
            return Optional.empty();
        };

        FunctionCallRewriter absRewriter = createAbsToAddOneRewriter();

        FunctionHandle absHandle = functionAndTypeManager.lookupFunction("abs", fromTypes(BIGINT));
        CallExpression absCall = new CallExpression("abs", absHandle, BIGINT, ImmutableList.of(variable("a")));

        tester().assertThat(
                        new FunctionCallRewriting(
                                () -> ImmutableSet.of(rewriter1, absRewriter),
                                functionResolution).projectRowExpressionRewriteRule())
                .setSystemProperty(ENABLE_FUNCTION_CALL_REWRITER, "true")
                .on(p -> p.project(
                        assignment(p.variable("result", BIGINT), absCall),
                        p.values(p.variable("a", BIGINT))))
                .matches(
                        project(ImmutableMap.of("result", expression("a + BIGINT '1'")),
                                values("a")));
    }

    private FunctionCallRewriter createSimpleRewriter()
    {
        return (call, session, variableAllocator, idAllocator, funcResolution) -> {
            if (call.getDisplayName().equals("custom_rewrite_target")) {
                return Optional.of(TRUE_CONSTANT);
            }
            return Optional.empty();
        };
    }

    private FunctionCallRewriter createAbsToAddOneRewriter()
    {
        return (call, session, variableAllocator, idAllocator, funcResolution) -> {
            if (call.getDisplayName().equals("abs") && call.getArguments().size() == 1) {
                FunctionHandle addHandle = funcResolution.arithmeticFunction(
                        OperatorType.ADD,
                        BIGINT,
                        BIGINT);
                return Optional.of(new CallExpression(
                        "add",
                        addHandle,
                        BIGINT,
                        ImmutableList.of(call.getArguments().get(0), constant(1L))));
            }
            return Optional.empty();
        };
    }

    private FunctionCallRewriter createComparisonRewriter()
    {
        return (call, session, variableAllocator, idAllocator, funcResolution) -> {
            if (call.getDisplayName().contains("GREATER_THAN") && call.getArguments().size() == 2) {
                FunctionHandle ltHandle = functionAndTypeManager.lookupFunction(
                        "$operator$LESS_THAN",
                        fromTypes(BIGINT, BIGINT));
                return Optional.of(new CallExpression(
                        "$operator$LESS_THAN",
                        ltHandle,
                        BOOLEAN,
                        ImmutableList.of(call.getArguments().get(1), call.getArguments().get(0))));
            }
            return Optional.empty();
        };
    }
}
