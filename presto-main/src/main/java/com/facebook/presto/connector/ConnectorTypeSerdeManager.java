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
import com.facebook.presto.spi.ConnectorMetadataUpdateHandle;
import com.facebook.presto.spi.ConnectorTypeSerde;
import com.facebook.presto.spi.connector.ConnectorTypeSerdeProvider;
import com.google.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ConnectorTypeSerdeManager
{
    private final Map<ConnectorId, ConnectorTypeSerdeProvider> connectorTypeSerdeProviderMap = new ConcurrentHashMap<>();

    @Inject
    public ConnectorTypeSerdeManager() {}

    public void addConnectorTypeSerdeProvider(ConnectorId connectorId, ConnectorTypeSerdeProvider connectorTypeSerdeProvider)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(connectorTypeSerdeProvider, "connectorTypeSerdeProvider is null");
        checkArgument(
                connectorTypeSerdeProviderMap.putIfAbsent(connectorId, connectorTypeSerdeProvider) == null,
                "ConnectorMetadataUpdateHandleSerdeProvider for connector '%s' is already registered", connectorId);
    }

    public void removeConnectorTypeSerdeProvider(ConnectorId connectorId)
    {
        requireNonNull(connectorId, "connectorId is null");
        connectorTypeSerdeProviderMap.remove(connectorId);
    }

    public Optional<ConnectorTypeSerde<ConnectorMetadataUpdateHandle>> getMetadataUpdateHandleSerde(ConnectorId connectorId)
    {
        requireNonNull(connectorId, "connectorId is null");
        return Optional.ofNullable(connectorTypeSerdeProviderMap.get(connectorId)).map(ConnectorTypeSerdeProvider::getConnectorMetadataUpdateHandleSerde);
    }
}
