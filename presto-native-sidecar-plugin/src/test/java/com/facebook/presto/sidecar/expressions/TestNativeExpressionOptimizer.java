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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.sidecar.NativeSidecarPluginQueryRunner;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.function.Function;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.FIELD_NAMES_IN_JSON_CAST_ENABLED;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sidecar.expressions.NativeExpressionOptimizerFactory.NAME;
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
        this.metadata = createTestMetadataManager(functionAndTypeManager);
        this.translator = new TestingRowExpressionTranslator(metadata);
        this.expressionOptimizer = (NativeExpressionOptimizer) queryRunner.getCoordinator()
                .getExpressionManager()
                .getExpressionOptimizer(NAME);
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

    @Test
    public void testSessionPropertiesPropagatingThroughOptimizer()
    {
        assertOptimizedEquals(
                "JSON_FORMAT(CAST(ROW(1 + 2, CONCAT('a', 'b')) AS JSON))",
                "'[3,\"ab\"]'",
                Session.builder(queryRunner.getDefaultSession())
                        .setSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, "false")
                        .build());
        assertOptimizedEquals(
                "JSON_FORMAT(CAST(ROW(1 + 2, CONCAT('a', 'b')) AS JSON))",
                "'{\"\":3,\"\":\"ab\"}'",
                Session.builder(queryRunner.getDefaultSession())
                        .setSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, "true")
                        .build());

        assertOptimizedEquals(
                "JSON_FORMAT(CAST(CAST(ROW(1 + 2, CONCAT('a', 'b')) AS ROW(id BIGINT, name VARCHAR)) AS JSON))",
                "'[3,\"ab\"]'",
                Session.builder(queryRunner.getDefaultSession())
                        .setSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, "false")
                        .build());
        assertOptimizedEquals(
                "JSON_FORMAT(CAST(CAST(ROW(1 + 2, CONCAT('a', 'b')) AS ROW(id BIGINT, name VARCHAR)) AS JSON))",
                "'{\"id\":3,\"name\":\"ab\"}'",
                Session.builder(queryRunner.getDefaultSession())
                        .setSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, "true")
                        .build());
    }

    private void assertOptimizedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertOptimizedEquals(actual, expected, TEST_SESSION);
    }

    private void assertOptimizedEquals(@Language("SQL") String actual, @Language("SQL") String expected, Session session)
    {
        RowExpression optimizedActual = optimize(actual, ExpressionOptimizer.Level.OPTIMIZED, session);
        RowExpression optimizedExpected = optimize(expected, ExpressionOptimizer.Level.OPTIMIZED, session);
        assertRowExpressionEvaluationEquals(optimizedActual, optimizedExpected);
    }

    private RowExpression optimize(@Language("SQL") String expression, ExpressionOptimizer.Level level, Session session)
    {
        RowExpression parsedExpression = sqlToRowExpression(expression);
        Function<com.facebook.presto.spi.relation.VariableReferenceExpression, Object> variableResolver = variable -> null;
        return expressionOptimizer.optimize(parsedExpression, level, session.toConnectorSession(), variableResolver);
    }

    private RowExpression sqlToRowExpression(String expression)
    {
        Expression parsedExpression = FunctionAssertions.createExpression(expression, metadata, SYMBOL_TYPES);
        return translator.translate(parsedExpression, SYMBOL_TYPES);
    }
}
