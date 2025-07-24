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
package com.facebook.presto.server;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.event.client.EventClient;
import com.facebook.airlift.event.client.NullEventClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.TableFunctionRegistry;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.transaction.NoOpTransactionManager;
import com.facebook.presto.transaction.TransactionManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;

public class FunctionServerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        jaxrsBinder(binder).bind(FunctionResource.class);
        binder.bind(FunctionAndTypeManager.class).in(Scopes.SINGLETON);
        binder.bind(TableFunctionRegistry.class).in(Scopes.SINGLETON);
        binder.bind(TransactionManager.class).to(NoOpTransactionManager.class).in(Scopes.SINGLETON);
        binder.bind(HandleResolver.class).in(Scopes.SINGLETON);
        install(new InternalCommunicationModule());
        binder.bind(EventClient.class).to(NullEventClient.class);
        binder.bind(ObjectMapper.class).toProvider(JsonObjectMapperProvider.class);
        binder.bind(new TypeLiteral<JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>>>() {})
                .toInstance(new JsonCodecFactory().mapJsonCodec(String.class, listJsonCodec(JsonBasedUdfFunctionMetadata.class)));
        binder.bind(FunctionPluginManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(PluginManagerConfig.class);
        configBinder(binder).bindConfig(FunctionsConfig.class);
        configBinder(binder).bindConfig(FeaturesConfig.class);
    }

    @Provides
    public BlockEncodingSerde provideBlockEncodingSerde()
    {
        return new BlockEncodingManager();
    }

    @Provides
    public Set<Type> provideTypes()
    {
        return ImmutableSet.of();
    }

    @Provides
    public NodeInfo provideNodeInfo()
    {
        return new NodeInfo("function_server");
    }
}
