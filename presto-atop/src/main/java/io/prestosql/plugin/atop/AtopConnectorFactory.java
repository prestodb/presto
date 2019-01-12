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
package io.prestosql.plugin.atop;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.plugin.base.security.AllowAllAccessControlModule;
import io.prestosql.plugin.base.security.FileBasedAccessControlModule;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.prestosql.plugin.atop.AtopConnectorConfig.SECURITY_FILE;
import static io.prestosql.plugin.atop.AtopConnectorConfig.SECURITY_NONE;
import static java.util.Objects.requireNonNull;

public class AtopConnectorFactory
        implements ConnectorFactory
{
    private final Class<? extends AtopFactory> atopFactoryClass;
    private final ClassLoader classLoader;

    public AtopConnectorFactory(Class<? extends AtopFactory> atopFactoryClass, ClassLoader classLoader)
    {
        this.atopFactoryClass = requireNonNull(atopFactoryClass, "atopFactoryClass is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
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
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new AtopModule(
                            atopFactoryClass,
                            context.getTypeManager(),
                            context.getNodeManager(),
                            context.getNodeManager().getEnvironment(),
                            catalogName),
                    installModuleIf(
                            AtopConnectorConfig.class,
                            config -> config.getSecurity().equalsIgnoreCase(SECURITY_NONE),
                            new AllowAllAccessControlModule()),
                    installModuleIf(
                            AtopConnectorConfig.class,
                            config -> config.getSecurity().equalsIgnoreCase(SECURITY_FILE),
                            binder -> {
                                binder.install(new FileBasedAccessControlModule());
                                binder.install(new JsonModule());
                            }));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(AtopConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
