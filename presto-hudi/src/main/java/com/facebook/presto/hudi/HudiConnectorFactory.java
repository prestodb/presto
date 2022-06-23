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

package com.facebook.presto.hudi;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.event.client.EventModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.cache.CachingModule;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.authentication.HiveAuthenticationModule;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HiveMetastoreModule;
import com.facebook.presto.hive.s3.HiveS3Module;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class HudiConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final ClassLoader classLoader;
    private final Optional<ExtendedHiveMetastore> metastore;

    public HudiConnectorFactory(
            String name,
            ClassLoader classLoader,
            Optional<ExtendedHiveMetastore> metastore)
    {
        this.name = requireNonNull(name, "name is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new HudiHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        try (ThreadContextClassLoader ignore = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new EventModule(),
                    new MBeanModule(),
                    new JsonModule(),
                    new HudiModule(classLoader, catalogName),
                    new HiveS3Module(catalogName),
                    new HiveAuthenticationModule(),
                    new HiveMetastoreModule(catalogName, metastore),
                    new CachingModule(),
                    binder -> {
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(PageIndexerFactory.class).toInstance(context.getPageIndexerFactory());
                    });

            return app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize()
                    .getInstance(Connector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException("Could not create hudi connector for catalog " + catalogName, e);
        }
    }
}
