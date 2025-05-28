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
package com.facebook.presto.connector;

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSpecificCodec;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorSpecificCodecProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class ConnectorSpecificCodecManager
{
    private final Map<ConnectorId, ConnectorSpecificCodecProvider> connectorSpecificCodecProviders = new ConcurrentHashMap<>();

    @Inject
    public ConnectorSpecificCodecManager() {}

    public void addConnectorSpecificCodecProvider(ConnectorId connectorId, ConnectorSpecificCodecProvider connectorSpecificCodecProvider)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(connectorSpecificCodecProvider, "connectorSpecificCodecProvider is null");
        connectorSpecificCodecProviders.put(connectorId, connectorSpecificCodecProvider);
    }

    public ConnectorSpecificCodec<ConnectorSplit> getConnectorSplitCodec(ConnectorId connectorId)
    {
        requireNonNull(connectorId, "connectorId is null");
        return Optional.ofNullable(connectorSpecificCodecProviders.get(connectorId)).map(ConnectorSpecificCodecProvider::getConnectorSplitCodec)
                .orElseThrow(() -> new IllegalArgumentException("Can not find split codec for connector: " + connectorId.getCatalogName()));
    }

    public ConnectorSpecificCodec<ConnectorTransactionHandle> getConnectorTransactionHandleCodec(ConnectorId connectorId)
    {
        requireNonNull(connectorId, "connectorId is null");
        return Optional.ofNullable(connectorSpecificCodecProviders.get(connectorId)).map(ConnectorSpecificCodecProvider::getConnectorTransactionHandleCodec)
                .orElseThrow(() -> new IllegalArgumentException("Can not find transactionHandle codec for connector: " + connectorId.getCatalogName()));
    }

    public boolean isConnectorSpecificCodecAvailable(ConnectorId connectorId)
    {
        requireNonNull(connectorId, "connectorId is null");
        return connectorSpecificCodecProviders.containsKey(connectorId);
    }
}
