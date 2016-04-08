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
package com.facebook.presto.atop;

import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.node.NodeConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class AtopModule
        implements Module
{
    private final Class<? extends AtopFactory> atopFactoryClass;
    private final TypeManager typeManager;
    private final NodeManager nodeManager;
    private final NodeConfig nodeConfig;
    private final String connectorId;

    public AtopModule(Class<? extends AtopFactory> atopFactoryClass, TypeManager typeManager, NodeManager nodeManager, NodeConfig nodeConfig, String connectorId)
    {
        this.atopFactoryClass = requireNonNull(atopFactoryClass, "atopFactoryClass is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeConfig = requireNonNull(nodeConfig, "nodeConfig is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(NodeConfig.class).toInstance(nodeConfig);
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(AtopConnectorId.class).toInstance(new AtopConnectorId(connectorId));
        binder.bind(AtopConnector.class).in(Scopes.SINGLETON);
        binder.bind(AtopMetadata.class).in(Scopes.SINGLETON);
        binder.bind(AtopSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(AtopFactory.class).to(atopFactoryClass).in(Scopes.SINGLETON);
        binder.bind(AtopPageSourceProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(AtopConnectorConfig.class);
    }
}
