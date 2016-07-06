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
package com.facebook.presto.transaction;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LegacyTransactionConnectorFactory
        implements ConnectorFactory
{
    private final com.facebook.presto.spi.ConnectorFactory connectorFactory;

    public LegacyTransactionConnectorFactory(com.facebook.presto.spi.ConnectorFactory connectorFactory)
    {
        this.connectorFactory = requireNonNull(connectorFactory, "connectorFactory is null");
    }

    @Override
    public String getName()
    {
        return connectorFactory.getName();
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new LegacyTransactionHandleResolver(connectorFactory.getHandleResolver());
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        return new LegacyTransactionConnector(connectorId, connectorFactory.create(connectorId, config, context));
    }

    public com.facebook.presto.spi.ConnectorFactory getConnectorFactory()
    {
        return connectorFactory;
    }
}
