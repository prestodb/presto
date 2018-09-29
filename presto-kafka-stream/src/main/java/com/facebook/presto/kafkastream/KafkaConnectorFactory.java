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
package com.facebook.presto.kafkastream;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class KafkaConnectorFactory
        implements ConnectorFactory
{
    private static final Logger log = Logger.get(KafkaConnectorFactory.class);
    private static final String ADAPTER_NAME = "kafkastream";

    @Override
    public String getName()
    {
        return ADAPTER_NAME;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new KafkaHandleResolver();
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> requiredConfig, ConnectorContext context)
    {
        log.info("Creating kafka Connector");
        requireNonNull(requiredConfig, "requiredConfig is null");
        try {
            // A plugin is not required to use Guice; it is just very convenient
            Bootstrap app = new Bootstrap(new JsonModule(), new KafkaModule(connectorId, context.getTypeManager()));
            Injector injector = null;
            injector = app.strictConfig().doNotInitializeLogging().setRequiredConfigurationProperties(requiredConfig)
                    .initialize();
            return injector.getInstance(KafkaConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
