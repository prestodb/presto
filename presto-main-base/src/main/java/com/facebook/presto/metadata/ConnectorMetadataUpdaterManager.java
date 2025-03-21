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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.connector.ConnectorMetadataUpdater;
import com.facebook.presto.spi.connector.ConnectorMetadataUpdaterProvider;
import com.google.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ConnectorMetadataUpdaterManager
{
    private final Map<ConnectorId, ConnectorMetadataUpdaterProvider> metadataUpdaterProviderMap = new ConcurrentHashMap<>();

    @Inject
    public ConnectorMetadataUpdaterManager() {}

    public void addMetadataUpdaterProvider(ConnectorId connectorId, ConnectorMetadataUpdaterProvider metadataUpdaterProvider)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(metadataUpdaterProvider, "metadataUpdaterProvider is null");
        checkArgument(metadataUpdaterProviderMap.putIfAbsent(connectorId, metadataUpdaterProvider) == null,
                "ConnectorMetadataUpdaterProvider for connector '%s' is already registered", connectorId);
    }

    public void removeMetadataUpdaterProvider(ConnectorId connectorId)
    {
        requireNonNull(connectorId, "connectorId is null");
        metadataUpdaterProviderMap.remove(connectorId);
    }

    public Optional<ConnectorMetadataUpdater> getMetadataUpdater(ConnectorId connectorId)
    {
        requireNonNull(connectorId, "connectorId is null");
        return Optional.ofNullable(metadataUpdaterProviderMap.get(connectorId)).map(ConnectorMetadataUpdaterProvider::getMetadataUpdater);
    }
}
