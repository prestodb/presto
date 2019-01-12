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
package io.prestosql.plugin.kudu;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class KuduConnectorFactory
        implements ConnectorFactory
{
    public KuduConnectorFactory()
    {
    }

    @Override
    public String getName()
    {
        return "kudu";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new KuduHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(new JsonModule(),
                    new KuduModule(catalogName, context.getTypeManager()));

            Injector injector =
                    app.strictConfig().doNotInitializeLogging().setRequiredConfigurationProperties(config)
                            .initialize();

            return injector.getInstance(KuduConnector.class);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
