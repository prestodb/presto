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
import com.facebook.airlift.json.JsonModule;
import com.facebook.drift.codec.guice.ThriftCodecModule;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.sidecar.ForSidecarInfo;
import com.facebook.presto.sidecar.NativeSidecarPluginQueryRunner;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.function.Function;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.expressions.AbstractTestExpressionInterpreter.SYMBOL_TYPES;
import static com.facebook.presto.sql.expressions.AbstractTestExpressionInterpreter.assertRowExpressionEvaluationEquals;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

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
        assertOptimizedEquals("transform(ARRAY[unbound_long, unbound_long2], x -> 1 + 1)",
                "transform(ARRAY[unbound_long, unbound_long2], x -> 2)");
        assertOptimizedEquals("transform(ARRAY[unbound_long, unbound_long2], x -> cast('123' AS integer))", "transform(ARRAY[unbound_long, unbound_long2], x -> 123)");
        assertOptimizedEquals("transform(ARRAY[unbound_long, unbound_long2], x -> cast(json_parse('[1, 2]') AS ARRAY<INTEGER>)[1] + 1)",
                "transform(ARRAY[unbound_long, unbound_long2], x -> 2)");
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
        // The test module wires required classes; reuse same bindings as other tests
        Module testModule = binder -> {
            binder.bind(NodeManager.class).toInstance(nodeManager);
            binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule(functionAndTypeManager.getHandleResolver()));
            binder.bind(ConnectorManager.class).toProvider(() -> null).in(Scopes.SINGLETON);
            binder.install(new ThriftCodecModule());
            configBinder(binder).bindConfig(FeaturesConfig.class);

            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);
            binder.bind(com.facebook.presto.spi.function.FunctionMetadataManager.class).toInstance(functionAndTypeManager);
            binder.bind(com.facebook.presto.spi.function.StandardFunctionResolution.class).toInstance(
                    new com.facebook.presto.sql.relational.FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver()));
            binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
            newSetBinder(binder, BlockEncoding.class);
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
            jsonCodecBinder(binder).bindListJsonCodec(RowExpression.class);
            jsonCodecBinder(binder).bindListJsonCodec(RowExpressionOptimizationResult.class);

            httpClientBinder(binder).bindHttpClient("sidecar", ForSidecarInfo.class);

            binder.bind(NativeSidecarExpressionInterpreter.class).in(Scopes.SINGLETON);
            binder.bind(NativeExpressionOptimizer.class).in(Scopes.SINGLETON);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(testModule));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();

        return injector.getInstance(NativeExpressionOptimizer.class);
    }
}
