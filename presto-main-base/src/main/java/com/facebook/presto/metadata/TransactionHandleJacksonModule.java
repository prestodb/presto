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
import com.facebook.presto.spi.connector.ConnectorCodecProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.Optional;
import java.util.function.Function;

public class TransactionHandleJacksonModule
        extends AbstractTypedJacksonModule<ConnectorTransactionHandle>
{
    @Inject
    public TransactionHandleJacksonModule(
            HandleResolver handleResolver,
            Provider<ConnectorManager> connectorManagerProvider,
            FeaturesConfig featuresConfig)
    {
        super(ConnectorTransactionHandle.class,
                handleResolver::getId,
                handleResolver::getTransactionHandleClass,
                featuresConfig.isUseConnectorProvidedSerializationCodecs(),
                connectorId -> connectorManagerProvider.get()
                        .getConnectorCodecProvider(connectorId)
                        .flatMap(ConnectorCodecProvider::getConnectorTransactionHandleCodec));
    }

    public TransactionHandleJacksonModule(
            HandleResolver handleResolver,
            FeaturesConfig featuresConfig,
            Function<ConnectorId, Optional<ConnectorCodec<ConnectorTransactionHandle>>> codecExtractor)
    {
        super(ConnectorTransactionHandle.class,
                handleResolver::getId,
                handleResolver::getTransactionHandleClass,
                featuresConfig.isUseConnectorProvidedSerializationCodecs(),
                codecExtractor);
    }
}
