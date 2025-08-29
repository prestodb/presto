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
package com.facebook.presto.sidecar.functionNamespace;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.presto.functionNamespace.JsonBasedUdfFunctionMetadata;
import com.facebook.presto.functionNamespace.ServingCatalog;
import com.facebook.presto.functionNamespace.SqlInvokedFunctionNamespaceManagerConfig;
import com.facebook.presto.functionNamespace.execution.SqlFunctionLanguageConfig;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;

import java.util.List;
import java.util.Map;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.google.inject.Scopes.SINGLETON;
import static java.util.Objects.requireNonNull;

public class NativeFunctionNamespaceManagerModule
        implements Module
{
    private final String catalogName;
    private final NodeManager nodeManager;
    private final FunctionMetadataManager functionMetadataManager;

    public NativeFunctionNamespaceManagerModule(String catalogName, NodeManager nodeManager, FunctionMetadataManager functionMetadataManager)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(new TypeLiteral<String>() {}).annotatedWith(ServingCatalog.class).toInstance(catalogName);

        configBinder(binder).bindConfig(SqlInvokedFunctionNamespaceManagerConfig.class);
        configBinder(binder).bindConfig(SqlFunctionLanguageConfig.class);
        configBinder(binder).bindConfig(NativeFunctionNamespaceManagerConfig.class);
        binder.bind(FunctionDefinitionProvider.class).to(NativeFunctionDefinitionProvider.class).in(SINGLETON);
        binder.bind(new TypeLiteral<JsonCodec<Map<String, List<JsonBasedUdfFunctionMetadata>>>>() {})
                .toInstance(new JsonCodecFactory().mapJsonCodec(String.class, listJsonCodec(JsonBasedUdfFunctionMetadata.class)));
        binder.bind(NativeFunctionNamespaceManager.class).in(SINGLETON);
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(FunctionMetadataManager.class).toInstance(functionMetadataManager);
    }
}
