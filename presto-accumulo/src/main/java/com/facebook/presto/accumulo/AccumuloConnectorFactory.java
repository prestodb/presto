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
package com.facebook.presto.accumulo;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class AccumuloConnectorFactory
        implements ConnectorFactory
{
    public static final String CONNECTOR_NAME = "accumulo";

    @Override
    public String getName()
    {
        return CONNECTOR_NAME;
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(config, "requiredConfig is null");
        requireNonNull(context, "context is null");

        try {
            // A plugin is not required to use Guice; it is just very convenient
            // Unless you don't really know how to Guice, then it is less convenient
            Bootstrap app = new Bootstrap(new JsonModule(), new AccumuloModule(connectorId, context.getTypeManager()));
            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();
            return injector.getInstance(AccumuloConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new AccumuloHandleResolver();
    }
}
