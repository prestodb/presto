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
import com.facebook.airlift.jaxrs.JsonMapper;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.sidecar.NativeSidecarCommunicationModule;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.expressions.JsonCodecRowExpressionSerde;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;

import java.net.URI;

import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class NativeExpressionUtils
{
    private NativeExpressionUtils() {}

    public static ExpressionOptimizer getExpressionOptimizer(Metadata metadata, HandleResolver handleResolver, URI sidecarURI)
    {
        // Set up dependencies in main for this module
        NodeManager nodeManager = getNodeManagerWithSidecar(sidecarURI);
        Injector prestoMainInjector = getPrestoMainInjector(metadata, handleResolver, nodeManager);
        RowExpressionSerde rowExpressionSerde = prestoMainInjector.getInstance(RowExpressionSerde.class);
        FunctionAndTypeManager functionMetadataManager = prestoMainInjector.getInstance(FunctionAndTypeManager.class);

        // Create the native row expression interpreter service
        return createExpressionOptimizer(nodeManager, rowExpressionSerde, functionMetadataManager);
    }

    private static NodeManager getNodeManagerWithSidecar(URI sidecarUri)
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(new ConnectorId("test"), new InternalNode("test", sidecarUri, NodeVersion.UNKNOWN, false, false, false, true));
        return new PluginNodeManager(nodeManager);
    }

    private static ExpressionOptimizer createExpressionOptimizer(NodeManager nodeManager, RowExpressionSerde rowExpressionSerde, FunctionAndTypeManager functionMetadataManager)
    {
        Bootstrap app = new Bootstrap(
                // Specially use a testing HTTP client instead of a real one
                new NativeSidecarCommunicationModule(),
                // Otherwise use the exact same module as the native row expression interpreter service
                new NativeExpressionsModule(nodeManager, rowExpressionSerde, functionMetadataManager, new FunctionResolution(functionMetadataManager.getFunctionAndTypeResolver())));

        Injector injector = app
                .noStrictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(ImmutableMap.of())
                .quiet()
                .initialize();
        return injector.getInstance(NativeExpressionOptimizer.class);
    }

    private static Injector getPrestoMainInjector(Metadata metadata, HandleResolver handleResolver, NodeManager nodeManager)
    {
        Module module = binder -> {
            // Installs the JSON codec
            binder.install(new JsonModule());
            // Required to deserialize function handles
            binder.install(new HandleJsonModule(handleResolver));
            // Required for this test in the JaxrsTestingHttpProcessor because the underlying object mapper
            // must be the same as all other object mappers
            binder.bind(JsonMapper.class);

            // These dependencies are needed to serialize and deserialize types (found in expressions)
            FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
            binder.bind(FunctionAndTypeManager.class).toInstance(functionAndTypeManager);
            binder.bind(NodeManager.class).toInstance(nodeManager);
            binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            // These dependencies are needed to serialize and deserialize blocks (found in constant values of expressions)
            binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
            newSetBinder(binder, BlockEncoding.class);
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);

            // Create the serde which is used by the plugin to serialize and deserialize expressions
            jsonCodecBinder(binder).bindJsonCodec(RowExpression.class);
            binder.bind(RowExpressionSerde.class).to(JsonCodecRowExpressionSerde.class).in(Scopes.SINGLETON);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        return injector;
    }
}
