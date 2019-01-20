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
package io.prestosql.plugin.thrift;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.drift.transport.netty.client.DriftNettyClientModule;
import io.prestosql.plugin.thrift.util.RebindSafeMBeanServer;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.type.TypeManager;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Objects.requireNonNull;

public class ThriftConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Module locationModule;

    public ThriftConnectorFactory(String name, Module locationModule)
    {
        this.name = requireNonNull(name, "name is null");
        this.locationModule = requireNonNull(locationModule, "locationModule is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new ThriftHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        try {
            Bootstrap app = new Bootstrap(
                    new MBeanModule(),
                    new ConnectorObjectNameGeneratorModule(catalogName),
                    new DriftNettyClientModule(),
                    binder -> {
                        binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(getPlatformMBeanServer()));
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    },
                    locationModule,
                    new ThriftModule());

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(ThriftConnector.class);
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while creating connector", ie);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
