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
package com.facebook.presto.connector.jmx;

import com.facebook.presto.connector.jmx.util.RebindSafeMBeanServer;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import io.airlift.bootstrap.Bootstrap;

import javax.management.MBeanServer;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class JmxConnectorFactory
        implements ConnectorFactory
{
    private final MBeanServer mbeanServer;

    public JmxConnectorFactory(MBeanServer mbeanServer)
    {
        this.mbeanServer = requireNonNull(mbeanServer, "mbeanServer is null");
    }

    @Override
    public String getName()
    {
        return "jmx";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new JmxHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        try {
            Bootstrap app = new Bootstrap(
                    binder -> {
                        configBinder(binder).bindConfig(JmxConnectorConfig.class);
                        binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(mbeanServer));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(String.class).annotatedWith(Names.named(JmxConnector.CONNECTOR_ID_PARAMETER)).toInstance(connectorId);
                        binder.bind(JmxConnector.class).in(Scopes.SINGLETON);
                        binder.bind(JmxHistoricalData.class).in(Scopes.SINGLETON);
                        binder.bind(JmxMetadata.class).in(Scopes.SINGLETON);
                        binder.bind(JmxSplitManager.class).in(Scopes.SINGLETON);
                        binder.bind(JmxPeriodicSampler.class).in(Scopes.SINGLETON);
                        binder.bind(JmxRecordSetProvider.class).in(Scopes.SINGLETON);
                    }
            );

            Injector injector = app.strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(JmxConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
