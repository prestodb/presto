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
package com.facebook.presto.sidecar;

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
import com.facebook.presto.scalar.sql.NativeSqlInvokedFunctionsPlugin;
import com.facebook.presto.sidecar.expressions.NativeExpressionOptimizerFactory;
import com.facebook.presto.sidecar.functionNamespace.NativeFunctionNamespaceManagerFactory;
import com.facebook.presto.sidecar.sessionpropertyproviders.NativeSystemSessionPropertyProviderFactory;
import com.facebook.presto.sidecar.typemanager.NativeTypeManagerFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class NativeSidecarPluginQueryRunnerUtils
{
    private NativeSidecarPluginQueryRunnerUtils() {}

    public static void setupNativeSidecarPlugin(QueryRunner queryRunner)
    {
        queryRunner.installCoordinatorPlugin(new NativeSidecarPlugin());
        queryRunner.loadSessionPropertyProvider(
                NativeSystemSessionPropertyProviderFactory.NAME,
                ImmutableMap.of());

        // Register native catalog for built-in functions
        queryRunner.loadFunctionNamespaceManager(
                NativeFunctionNamespaceManagerFactory.NAME,
                "native",
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP"));

        // Register hive catalog for hive-specific functions.
        // Note: The C++ PrestoServer registers hive functions only when a hive connector is present.
        // Since tests always setup the hive connector, hive functions will be available.
        queryRunner.loadFunctionNamespaceManager(
                NativeFunctionNamespaceManagerFactory.NAME,
                "hive",
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP"));

        queryRunner.loadTypeManager(NativeTypeManagerFactory.NAME);
        queryRunner.loadPlanCheckerProviderManager("native", ImmutableMap.of());
        queryRunner.getExpressionManager().loadExpressionOptimizerFactory(NativeExpressionOptimizerFactory.NAME, "native", ImmutableMap.of());
        queryRunner.installPlugin(new NativeSqlInvokedFunctionsPlugin());
    }

    /**
     * Return a Guice Module wired up with components needed for sidecar expression tests.
     * Tests should use this module as a base and add any extra, test-specific bindings.
     */
    public static Module createSidecarTestModule(FunctionAndTypeManager functionAndTypeManager, NodeManager nodeManager)
    {
        return binder -> {
            binder.bind(NodeManager.class).toInstance(nodeManager);
            binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule(functionAndTypeManager.getHandleResolver()));
            binder.bind(ConnectorManager.class).toProvider(() -> null).in(Scopes.SINGLETON);
            binder.install(new ThriftCodecModule());
            configBinder(binder).bindConfig(com.facebook.presto.sql.analyzer.FeaturesConfig.class);

            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
            newSetBinder(binder, BlockEncoding.class);
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
            jsonCodecBinder(binder).bindListJsonCodec(com.facebook.presto.spi.relation.RowExpression.class);
            jsonCodecBinder(binder).bindListJsonCodec(com.facebook.presto.sidecar.expressions.RowExpressionOptimizationResult.class);

            httpClientBinder(binder).bindHttpClient("sidecar", com.facebook.presto.sidecar.ForSidecarInfo.class);
        };
    }
}
