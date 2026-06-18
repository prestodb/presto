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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.NodeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class MemoryModule
        implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;
    private final NodeManager nodeManager;

    public MemoryModule(String connectorId, TypeManager typeManager, NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(NodeManager.class).toInstance(nodeManager);

        binder.bind(MemoryConnector.class).in(Scopes.SINGLETON);
        binder.bind(MemoryConnectorId.class).toInstance(new MemoryConnectorId(connectorId));
        binder.bind(MemoryMetadata.class).in(Scopes.SINGLETON);
        binder.bind(MemorySplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MemoryPagesStore.class).in(Scopes.SINGLETON);
        binder.bind(MemoryPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(MemoryPageSinkProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(MemoryConfig.class);
    }
}
