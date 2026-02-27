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
package com.facebook.presto.sidecar.expressions;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.sidecar.NativeSidecarPluginQueryRunner;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.function.Function;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.createSidecarTestModule;
import static com.facebook.presto.sql.expressions.AbstractTestExpressionInterpreter.SYMBOL_TYPES;
import static com.facebook.presto.sql.expressions.AbstractTestExpressionInterpreter.assertRowExpressionEvaluationEquals;

public class TestNativeExpressionOptimizer
{
    private final DistributedQueryRunner queryRunner;
    private final MetadataManager metadata;
    private final TestingRowExpressionTranslator translator;
    private final NativeExpressionOptimizer expressionOptimizer;

    public TestNativeExpressionOptimizer()
            throws Exception
    {
        this.queryRunner = NativeSidecarPluginQueryRunner.getQueryRunner();
        FunctionAndTypeManager functionAndTypeManager = queryRunner.getCoordinator().getFunctionAndTypeManager();
        NodeManager nodeManager = queryRunner.getCoordinator().getPluginNodeManager();
        this.metadata = createTestMetadataManager(functionAndTypeManager);
        this.translator = new TestingRowExpressionTranslator(metadata);
        this.expressionOptimizer = getNativeExpressionOptimizer(functionAndTypeManager, nodeManager);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(queryRunner);
    }

    @Test
    public void testLambdaBodyConstantFolding()
    {
        // Simple lambda constant folding.
        assertOptimizedEquals(
                "transform(ARRAY[unbound_long, unbound_long2], x -> 1 + 1)",
                "transform(ARRAY[unbound_long, unbound_long2], x -> 2)");
        assertOptimizedEquals(
                "transform(ARRAY[unbound_long, unbound_long2], x -> cast('123' AS integer))",
                "transform(ARRAY[unbound_long, unbound_long2], x -> 123)");
        assertOptimizedEquals(
                "transform(ARRAY[unbound_long, unbound_long2], x -> cast(json_parse('[1, 2]') AS ARRAY<INTEGER>)[1] + 1)",
                "transform(ARRAY[unbound_long, unbound_long2], x -> 2)");

        // Nested lambda constant folding.
        assertOptimizedEquals(
                "transform(ARRAY[unbound_long, unbound_long2], x -> transform(ARRAY[1, 2], y -> 1 + 1))",
                "transform(ARRAY[unbound_long, unbound_long2], x -> transform(ARRAY[1, 2], y -> 2))");
        // Multiple lambda occurrences constant folding.
        assertOptimizedEquals(
                "filter(transform(ARRAY[unbound_long, unbound_long2], x -> 1 + 1), x -> true and false)",
                "filter(transform(ARRAY[unbound_long, unbound_long2], x -> 2), x -> false)");
    }

    private void assertOptimizedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        RowExpression optimizedActual = optimize(actual, ExpressionOptimizer.Level.OPTIMIZED);
        RowExpression optimizedExpected = optimize(expected, ExpressionOptimizer.Level.OPTIMIZED);
        assertRowExpressionEvaluationEquals(optimizedActual, optimizedExpected);
    }

    private RowExpression optimize(@Language("SQL") String expression, ExpressionOptimizer.Level level)
    {
        RowExpression parsedExpression = sqlToRowExpression(expression);
        Function<com.facebook.presto.spi.relation.VariableReferenceExpression, Object> variableResolver = variable -> null;
        return expressionOptimizer.optimize(parsedExpression, level, TEST_SESSION.toConnectorSession(), variableResolver);
    }

    private RowExpression sqlToRowExpression(String expression)
    {
        Expression parsedExpression = FunctionAssertions.createExpression(expression, metadata, SYMBOL_TYPES);
        return translator.translate(parsedExpression, SYMBOL_TYPES);
    }

    private NativeExpressionOptimizer getNativeExpressionOptimizer(FunctionAndTypeManager functionAndTypeManager, NodeManager nodeManager)
    {
        Module base = createSidecarTestModule(functionAndTypeManager, nodeManager);
        Module extras = binder -> {
            binder.bind(com.facebook.presto.spi.function.FunctionMetadataManager.class).toInstance(functionAndTypeManager);
            binder.bind(com.facebook.presto.spi.function.StandardFunctionResolution.class).toInstance(
                    new com.facebook.presto.sql.relational.FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver()));
            binder.bind(com.facebook.presto.sidecar.expressions.NativeSidecarExpressionInterpreter.class).in(Scopes.SINGLETON);
        };

        Bootstrap app = new Bootstrap(ImmutableList.of(base, extras));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();

        return injector.getInstance(NativeExpressionOptimizer.class);
    }
}
