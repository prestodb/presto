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

import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.transaction.TransactionalConnector;
import com.facebook.presto.spi.transaction.TransactionalConnectorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LegacyTransactionConnectorFactory
        implements TransactionalConnectorFactory
{
    private final ConnectorFactory connectorFactory;

    public LegacyTransactionConnectorFactory(ConnectorFactory connectorFactory)
    {
        this.connectorFactory = requireNonNull(connectorFactory, "connectorFactory is null");
    }

    @Override
    public String getName()
    {
        return connectorFactory.getName();
    }

    @Override
    public TransactionalConnector create(String connectorId, Map<String, String> config)
    {
        return new LegacyTransactionConnector(connectorId, connectorFactory.create(connectorId, config));
    }
}
