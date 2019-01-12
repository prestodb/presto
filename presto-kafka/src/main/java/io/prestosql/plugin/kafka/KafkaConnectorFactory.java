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
package io.prestosql.plugin.kafka;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

/**
 * Creates Kafka Connectors based off connectorId and specific configuration.
 */
public class KafkaConnectorFactory
        implements ConnectorFactory
{
    private final Optional<Supplier<Map<SchemaTableName, KafkaTopicDescription>>> tableDescriptionSupplier;

    KafkaConnectorFactory(Optional<Supplier<Map<SchemaTableName, KafkaTopicDescription>>> tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
    }

    @Override
    public String getName()
    {
        return "kafka";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new KafkaHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new KafkaConnectorModule(),
                    binder -> {
                        binder.bind(KafkaConnectorId.class).toInstance(new KafkaConnectorId(catalogName));
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());

                        if (tableDescriptionSupplier.isPresent()) {
                            binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, KafkaTopicDescription>>>() {}).toInstance(tableDescriptionSupplier.get());
                        }
                        else {
                            binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, KafkaTopicDescription>>>() {}).to(KafkaTableDescriptionSupplier.class).in(Scopes.SINGLETON);
                        }
                    });

            Injector injector = app.strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(KafkaConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
