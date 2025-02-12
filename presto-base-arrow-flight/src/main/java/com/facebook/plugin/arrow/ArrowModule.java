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
package com.facebook.plugin.arrow;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class ArrowModule
        implements Module
{
    protected final String connectorId;

    public ArrowModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
    }

    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(ArrowFlightConfig.class);
        binder.bind(BufferAllocator.class).to(RootAllocator.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(ArrowSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(ArrowMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ArrowConnector.class).in(Scopes.SINGLETON);
        binder.bind(ArrowConnectorId.class).toInstance(new ArrowConnectorId(connectorId));
        binder.bind(ConnectorHandleResolver.class).to(ArrowHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(ArrowPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(ArrowPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(Connector.class).to(ArrowConnector.class).in(Scopes.SINGLETON);
        binder.bind(ArrowBlockBuilder.class).in(Scopes.SINGLETON);
    }
}
