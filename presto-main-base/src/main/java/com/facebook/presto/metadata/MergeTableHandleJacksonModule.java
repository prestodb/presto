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

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorMergeTableHandle;
import com.facebook.presto.spi.connector.ConnectorCodecProvider;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import jakarta.inject.Provider;

import javax.inject.Inject;

import java.util.Optional;
import java.util.function.Function;

public class MergeTableHandleJacksonModule
        extends AbstractTypedJacksonModule<ConnectorMergeTableHandle>
{
    @Inject
    public MergeTableHandleJacksonModule(
            HandleResolver handleResolver,
            Provider<ConnectorManager> connectorManagerProvider,
            FeaturesConfig featuresConfig)
    {
        super(ConnectorMergeTableHandle.class,
                handleResolver::getId,
                handleResolver::getMergeTableHandleClass,
                featuresConfig.isUseConnectorProvidedSerializationCodecs(),
                connectorId -> connectorManagerProvider.get()
                        .getConnectorCodecProvider(connectorId)
                        .flatMap(ConnectorCodecProvider::getConnectorMergeTableHandleCodec));
    }

    public MergeTableHandleJacksonModule(
            HandleResolver handleResolver,
            FeaturesConfig featuresConfig,
            Function<ConnectorId, Optional<ConnectorCodec<ConnectorMergeTableHandle>>> codecExtractor)
    {
        super(ConnectorMergeTableHandle.class,
                handleResolver::getId,
                handleResolver::getMergeTableHandleClass,
                featuresConfig.isUseConnectorProvidedSerializationCodecs(),
                codecExtractor);
    }
}
