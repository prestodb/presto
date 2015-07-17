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
package com.facebook.presto.redis;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates Redis Connectors based off connectorId and specific configuration.
 */
public class RedisConnectorFactory
        implements ConnectorFactory
{
    private final TypeManager typeManager;
    private final NodeManager nodeManager;
    private final Optional<Supplier<Map<SchemaTableName, RedisTableDescription>>> tableDescriptionSupplier;
    private final Map<String, String> optionalConfig;

    RedisConnectorFactory(TypeManager typeManager,
                          NodeManager nodeManager,
                          Optional<Supplier<Map<SchemaTableName, RedisTableDescription>>> tableDescriptionSupplier,
                          Map<String, String> optionalConfig)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.optionalConfig = checkNotNull(optionalConfig, "optionalConfig is null");
        this.tableDescriptionSupplier = checkNotNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
    }

    @Override
    public String getName()
    {
        return "redis";
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> config)
    {
        checkNotNull(connectorId, "connectorId is null");
        checkNotNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new RedisConnectorModule(),
                    new Module()
                    {
                        @Override
                        public void configure(Binder binder)
                        {
                            binder.bindConstant().annotatedWith(Names.named("connectorId")).to(connectorId);
                            binder.bind(TypeManager.class).toInstance(typeManager);
                            binder.bind(NodeManager.class).toInstance(nodeManager);

                            if (tableDescriptionSupplier.isPresent()) {
                                binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, RedisTableDescription>>>() {}).toInstance(tableDescriptionSupplier.get());
                            }
                            else {
                                binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, RedisTableDescription>>>() {}).to(RedisTableDescriptionSupplier.class).in(Scopes.SINGLETON);
                            }
                        }
                    }
            );

            Injector injector = app.strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(RedisConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
