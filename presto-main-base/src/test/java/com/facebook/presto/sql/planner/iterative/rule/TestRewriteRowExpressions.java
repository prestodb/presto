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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.expressions.JsonCodecRowExpressionSerde;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.SystemSessionProperties.EXPRESSION_OPTIMIZER_IN_ROW_EXPRESSION_REWRITE;
import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.expressions.ExpressionOptimizerManager.DEFAULT_EXPRESSION_OPTIMIZER_NAME;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestRewriteRowExpressions
        extends BaseRuleTest
{
    private static final MetadataManager METADATA = createTestMetadataManager();

    private FunctionAndTypeManager functionAndTypeManager;
    private ExpressionOptimizerManager expressionOptimizerManager;
    private RewriteRowExpressions optimizer;
    private RuleTester ruleTesterWithOptimizer;

    private static RowExpression ifExpression(RowExpression condition, long trueValue, long falseValue)
    {
        return new SpecialFormExpression(IF, BIGINT, ImmutableList.of(condition, constant(trueValue, BIGINT), constant(falseValue, BIGINT)));
    }

    @BeforeClass
    @Override
    public void setUp()
    {
        super.setUp();
        functionAndTypeManager = createTestFunctionAndTypeManager();
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        expressionOptimizerManager = new ExpressionOptimizerManager(
                new PluginNodeManager(nodeManager),
                METADATA.getFunctionAndTypeManager(),
                new JsonCodecRowExpressionSerde(jsonCodec(RowExpression.class)));
        optimizer = new RewriteRowExpressions(expressionOptimizerManager);

        // Create a RuleTester with the session property enabled
        ruleTesterWithOptimizer = new RuleTester(
                ImmutableList.of(),
                ImmutableMap.of(EXPRESSION_OPTIMIZER_IN_ROW_EXPRESSION_REWRITE, DEFAULT_EXPRESSION_OPTIMIZER_NAME),
                Optional.empty());
    }

    @AfterClass(alwaysRun = true)
    public void cleanUp()
    {
        expressionOptimizerManager = null;
        optimizer = null;
        if (ruleTesterWithOptimizer != null) {
            ruleTesterWithOptimizer.close();
            ruleTesterWithOptimizer = null;
        }
    }

    @Test
    public void testIsRewriterEnabledWithEmptyOptimizerName()
    {
        Session sessionWithEmptyOptimizer = testSessionBuilder()
                .setSystemProperty(EXPRESSION_OPTIMIZER_IN_ROW_EXPRESSION_REWRITE, "")
                .build();
        assertFalse(optimizer.isRewriterEnabled(sessionWithEmptyOptimizer));
    }

    @Test
    public void testIsRewriterEnabledWithValidOptimizerName()
    {
        Session sessionWithOptimizer = testSessionBuilder()
                .setSystemProperty(EXPRESSION_OPTIMIZER_IN_ROW_EXPRESSION_REWRITE, DEFAULT_EXPRESSION_OPTIMIZER_NAME)
                .build();
        assertTrue(optimizer.isRewriterEnabled(sessionWithOptimizer));
    }

    @Test
    public void testRewriteWithDefaultOptimizer()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(EXPRESSION_OPTIMIZER_IN_ROW_EXPRESSION_REWRITE, DEFAULT_EXPRESSION_OPTIMIZER_NAME)
                .build();

        RowExpression expression = constant(1L, BIGINT);
        RowExpression rewritten = RewriteRowExpressions.rewrite(expression, session, expressionOptimizerManager);
        assertEquals(rewritten, expression);
    }

    @Test
    public void testRewriteIfConstantOptimization()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(EXPRESSION_OPTIMIZER_IN_ROW_EXPRESSION_REWRITE, DEFAULT_EXPRESSION_OPTIMIZER_NAME)
                .build();

        RowExpression ifExpressionTrue = ifExpression(constant(true, BOOLEAN), 1L, 2L);
        RowExpression rewrittenTrue = RewriteRowExpressions.rewrite(ifExpressionTrue, session, expressionOptimizerManager);
        assertEquals(rewrittenTrue, constant(1L, BIGINT));

        RowExpression ifExpressionFalse = ifExpression(constant(false, BOOLEAN), 1L, 2L);
        RowExpression rewrittenFalse = RewriteRowExpressions.rewrite(ifExpressionFalse, session, expressionOptimizerManager);
        assertEquals(rewrittenFalse, constant(2L, BIGINT));

        RowExpression ifExpressionNull = ifExpression(constant(null, BOOLEAN), 1L, 2L);
        RowExpression rewrittenNull = RewriteRowExpressions.rewrite(ifExpressionNull, session, expressionOptimizerManager);
        assertEquals(rewrittenNull, constant(2L, BIGINT));
    }

    @Test
    public void testRewriteWithArithmeticExpression()
    {
        Session session = testSessionBuilder()
                .setSystemProperty(EXPRESSION_OPTIMIZER_IN_ROW_EXPRESSION_REWRITE, DEFAULT_EXPRESSION_OPTIMIZER_NAME)
                .build();

        FunctionHandle addHandle = functionAndTypeManager.resolveOperator(ADD, fromTypes(BIGINT, BIGINT));
        RowExpression addExpression = new com.facebook.presto.spi.relation.CallExpression(
                ADD.name(),
                addHandle,
                BIGINT,
                ImmutableList.of(constant(1L, BIGINT), constant(2L, BIGINT)));

        RowExpression rewritten = RewriteRowExpressions.rewrite(addExpression, session, expressionOptimizerManager);
        assertEquals(rewritten, constant(3L, BIGINT));
    }

    @Test
    public void testFilterRuleDoesNotFireWhenDisabled()
    {
        tester().assertThat(optimizer.filterRowExpressionRewriteRule())
                .on(p -> p.filter(p.rowExpression("1 + 1 = 2"), p.values()))
                .doesNotFire();
    }

    @Test
    public void testProjectRuleDoesNotFireWhenDisabled()
    {
        tester().assertThat(optimizer.projectRowExpressionRewriteRule())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.project(
                            assignment(a, p.rowExpression("1 + 2")),
                            p.values());
                })
                .doesNotFire();
    }

    @Test
    public void testFilterRuleRewritesConstantExpression()
    {
        ruleTesterWithOptimizer.assertThat(
                        new RewriteRowExpressions(ruleTesterWithOptimizer.getExpressionManager()).filterRowExpressionRewriteRule())
                .on(p -> p.filter(p.rowExpression("1 + 1 = 2"), p.values()))
                .matches(
                        filter("true",
                                values()));
    }

    @Test
    public void testProjectRuleRewritesConstantArithmetic()
    {
        ruleTesterWithOptimizer.assertThat(
                        new RewriteRowExpressions(ruleTesterWithOptimizer.getExpressionManager()).projectRowExpressionRewriteRule())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.project(
                            assignment(a, p.rowExpression("bigint '1' + 2")),
                            p.values());
                })
                .matches(
                        project(ImmutableMap.of("a", expression("INTEGER'3'")),
                                values()));
    }

    @Test
    public void testRuleSetReturnsAllRules()
    {
        Set<com.facebook.presto.sql.planner.iterative.Rule<?>> rules = optimizer.rules();
        assertNotNull(rules);
        // RowExpressionRewriteRuleSet returns 10 rules
        assertEquals(rules.size(), 10);
    }

    @Test
    public void testFilterRuleRewritesIfExpression()
    {
        ruleTesterWithOptimizer.assertThat(
                        new RewriteRowExpressions(ruleTesterWithOptimizer.getExpressionManager()).filterRowExpressionRewriteRule())
                .on(p -> p.filter(p.rowExpression("IF(true, true, false)"), p.values()))
                .matches(
                        filter("true",
                                values()));
    }

    @Test
    public void testProjectRuleRewritesIfExpression()
    {
        ruleTesterWithOptimizer.assertThat(
                        new RewriteRowExpressions(ruleTesterWithOptimizer.getExpressionManager()).projectRowExpressionRewriteRule())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", BIGINT);
                    return p.project(
                            assignment(a, p.rowExpression("IF(true, BIGINT '1', BIGINT '2')")),
                            p.values());
                })
                .matches(
                        project(ImmutableMap.of("a", expression("BIGINT'1'")),
                                values()));
    }
}
