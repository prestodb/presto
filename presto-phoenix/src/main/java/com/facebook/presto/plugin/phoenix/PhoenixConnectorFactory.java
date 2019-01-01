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
package com.facebook.presto.plugin.phoenix;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.json.JsonModule;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PhoenixConnectorFactory
        implements ConnectorFactory
{
    private final ClassLoader classLoader;

    public PhoenixConnectorFactory(ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return "phoenix";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new PhoenixHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(new JsonModule(), new PhoenixClientModule(connectorId, context.getTypeManager()));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            PhoenixMetadataFactory metadataFactory = injector.getInstance(PhoenixMetadataFactory.class);
            ConnectorSplitManager splitManager = injector.getInstance(ConnectorSplitManager.class);
            ConnectorPageSourceProvider pageSourceProvider = injector.getInstance(ConnectorPageSourceProvider.class);
            ConnectorPageSinkProvider pageSinkProvider = injector.getInstance(ConnectorPageSinkProvider.class);
            PhoenixSessionProperties sessionProperties = injector.getInstance(PhoenixSessionProperties.class);
            PhoenixTableProperties tableProperties = injector.getInstance(PhoenixTableProperties.class);

            return new PhoenixConnector(
                lifeCycleManager,
                metadataFactory,
                new ClassLoaderSafeConnectorSplitManager(splitManager, classLoader),
                new ClassLoaderSafeConnectorPageSourceProvider(pageSourceProvider, classLoader),
                new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader),
                sessionProperties,
                tableProperties);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
