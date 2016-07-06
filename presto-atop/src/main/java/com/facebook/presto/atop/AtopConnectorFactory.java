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

import com.facebook.presto.plugin.base.security.AllowAllAccessControlModule;
import com.facebook.presto.plugin.base.security.FileBasedAccessControlModule;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.node.NodeConfig;

import java.util.Map;

import static com.facebook.presto.atop.AtopConnectorConfig.SECURITY_FILE;
import static com.facebook.presto.atop.AtopConnectorConfig.SECURITY_NONE;
import static com.facebook.presto.atop.ConditionalModule.installModuleIf;
import static java.util.Objects.requireNonNull;

public class AtopConnectorFactory
        implements ConnectorFactory
{
    private final Class<? extends AtopFactory> atopFactoryClass;
    private final Map<String, String> optionalConfig;
    private final TypeManager typeManager;
    private final NodeManager nodeManager;
    private final ClassLoader classLoader;
    private final NodeConfig nodeConfig;

    public AtopConnectorFactory(Class<? extends AtopFactory> atopFactoryClass, Map<String, String> optionalConfig, ClassLoader classLoader, TypeManager typeManager, NodeManager nodeManager, NodeConfig nodeConfig)
    {
        this.atopFactoryClass = requireNonNull(atopFactoryClass, "atopFactoryClass is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.nodeConfig = nodeConfig;
    }

    @Override
    public String getName()
    {
        return "atop";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new AtopHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new AtopModule(atopFactoryClass, typeManager, nodeManager, nodeConfig, connectorId),
                    installModuleIf(
                        AtopConnectorConfig.class,
                            config -> config.getSecurity().equalsIgnoreCase(SECURITY_NONE),
                            new AllowAllAccessControlModule()
                    ),
                    installModuleIf(
                            AtopConnectorConfig.class,
                            config -> config.getSecurity().equalsIgnoreCase(SECURITY_FILE),
                            binder -> {
                                binder.install(new FileBasedAccessControlModule());
                                binder.install(new JsonModule());
                            })
                    );

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(AtopConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
