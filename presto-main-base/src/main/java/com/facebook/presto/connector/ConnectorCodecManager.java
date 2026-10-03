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

import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMergeTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorCodecProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.thrift.RemoteCodecProvider;
import com.google.inject.Provider;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.operator.ExchangeOperator.REMOTE_CONNECTOR_ID;
import static java.util.Objects.requireNonNull;

public class ConnectorCodecManager
{
    private final Map<String, ConnectorCodecProvider> connectorCodecProviders = new ConcurrentHashMap<>();

    @Inject
    public ConnectorCodecManager(Provider<ThriftCodecManager> thriftCodecManagerProvider)
    {
        requireNonNull(thriftCodecManagerProvider, "thriftCodecManager is null");

        connectorCodecProviders.put(REMOTE_CONNECTOR_ID.toString(), new RemoteCodecProvider(thriftCodecManagerProvider));
    }

    public void addConnectorCodecProvider(String connectorName, ConnectorCodecProvider connectorCodecProvider)
    {
        requireNonNull(connectorName, "connectorName is null");
        requireNonNull(connectorCodecProvider, "connectorThriftCodecProvider is null");
        // Catalogs sharing a connector register equivalent providers; the first wins.
        connectorCodecProviders.putIfAbsent(connectorName, connectorCodecProvider);
    }

    public Optional<ConnectorCodec<ConnectorSplit>> getConnectorSplitCodec(String connectorName)
    {
        requireNonNull(connectorName, "connectorName is null");
        return Optional.ofNullable(connectorCodecProviders.get(connectorName)).flatMap(ConnectorCodecProvider::getConnectorSplitCodec);
    }

    public Optional<ConnectorCodec<ConnectorTransactionHandle>> getTransactionHandleCodec(String connectorName)
    {
        requireNonNull(connectorName, "connectorName is null");
        return Optional.ofNullable(connectorCodecProviders.get(connectorName)).flatMap(ConnectorCodecProvider::getConnectorTransactionHandleCodec);
    }

    public Optional<ConnectorCodec<ConnectorOutputTableHandle>> getOutputTableHandleCodec(String connectorName)
    {
        requireNonNull(connectorName, "connectorName is null");
        return Optional.ofNullable(connectorCodecProviders.get(connectorName)).flatMap(ConnectorCodecProvider::getConnectorOutputTableHandleCodec);
    }

    public Optional<ConnectorCodec<ConnectorInsertTableHandle>> getInsertTableHandleCodec(String connectorName)
    {
        requireNonNull(connectorName, "connectorName is null");
        return Optional.ofNullable(connectorCodecProviders.get(connectorName)).flatMap(ConnectorCodecProvider::getConnectorInsertTableHandleCodec);
    }

    public Optional<ConnectorCodec<ConnectorDeleteTableHandle>> getDeleteTableHandleCodec(String connectorName)
    {
        requireNonNull(connectorName, "connectorName is null");
        return Optional.ofNullable(connectorCodecProviders.get(connectorName)).flatMap(ConnectorCodecProvider::getConnectorDeleteTableHandleCodec);
    }

    public Optional<ConnectorCodec<ConnectorMergeTableHandle>> getMergeTableHandleCodec(String connectorName)
    {
        requireNonNull(connectorName, "connectorName is null");
        return Optional.ofNullable(connectorCodecProviders.get(connectorName)).flatMap(ConnectorCodecProvider::getConnectorMergeTableHandleCodec);
    }

    public Optional<ConnectorCodec<ConnectorTableLayoutHandle>> getTableLayoutHandleCodec(String connectorName)
    {
        requireNonNull(connectorName, "connectorName is null");
        return Optional.ofNullable(connectorCodecProviders.get(connectorName)).flatMap(ConnectorCodecProvider::getConnectorTableLayoutHandleCodec);
    }

    public Optional<ConnectorCodec<ConnectorTableHandle>> getTableHandleCodec(String connectorName)
    {
        requireNonNull(connectorName, "connectorName is null");
        return Optional.ofNullable(connectorCodecProviders.get(connectorName)).flatMap(ConnectorCodecProvider::getConnectorTableHandleCodec);
    }

    public Optional<ConnectorCodecProvider> getConnectorCodecProvider(String connectorName)
    {
        requireNonNull(connectorName, "connectorName is null");
        return Optional.ofNullable(connectorCodecProviders.get(connectorName));
    }
}
