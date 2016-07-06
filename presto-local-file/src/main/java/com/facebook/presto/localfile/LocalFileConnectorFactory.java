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
package com.facebook.presto.localfile;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LocalFileConnectorFactory
        implements ConnectorFactory
{
    private final NodeManager nodeManager;

    public LocalFileConnectorFactory(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public String getName()
    {
        return "localfile";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new LocalFileHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    binder -> binder.bind(NodeManager.class).toInstance(nodeManager),
                    new LocalFileModule(connectorId));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(LocalFileConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
