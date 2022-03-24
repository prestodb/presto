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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.connector.presto.PinotScatterGatherQueryClient;
import org.apache.pinot.connector.presto.grpc.PinotStreamingQueryClient;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.pinot.common.utils.grpc.GrpcQueryClient.Config.CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE;
import static org.apache.pinot.common.utils.grpc.GrpcQueryClient.Config.CONFIG_USE_PLAIN_TEXT;

public class PinotPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final String connectorId;
    private final PinotConfig pinotConfig;
    private final PinotScatterGatherQueryClient pinotQueryClient;
    private final PinotStreamingQueryClient pinotStreamingQueryClient;
    private final PinotClusterInfoFetcher clusterInfoFetcher;
    private final ObjectMapper objectMapper;

    @Inject
    public PinotPageSourceProvider(
            ConnectorId connectorId,
            PinotConfig pinotConfig,
            PinotClusterInfoFetcher clusterInfoFetcher,
            ObjectMapper objectMapper)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pinotConfig = requireNonNull(pinotConfig, "pinotConfig is null");
        this.pinotQueryClient = new PinotScatterGatherQueryClient(new PinotScatterGatherQueryClient.Config(
                pinotConfig.getIdleTimeout().toMillis(),
                pinotConfig.getThreadPoolSize(),
                pinotConfig.getMinConnectionsPerServer(),
                pinotConfig.getMaxBacklogPerServer(),
                pinotConfig.getMaxConnectionsPerServer()));
        this.pinotStreamingQueryClient = new PinotStreamingQueryClient(extractGrpcQueryClientConfig(pinotConfig));
        this.clusterInfoFetcher = requireNonNull(clusterInfoFetcher, "cluster info fetcher is null");
        this.objectMapper = requireNonNull(objectMapper, "object mapper is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle tableLayoutHandle,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        requireNonNull(split, "split is null");

        PinotSplit pinotSplit = (PinotSplit) split;
        checkArgument(pinotSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        List<PinotColumnHandle> handles = new ArrayList<>();
        for (ColumnHandle handle : columns) {
            handles.add((PinotColumnHandle) handle);
        }

        switch (pinotSplit.getSplitType()) {
            case SEGMENT:
                if (pinotConfig.isUseStreamingForSegmentQueries() && pinotSplit.getGrpcPort().orElse(-1) > 0) {
                    return new PinotSegmentStreamingPageSource(
                        session,
                        pinotConfig,
                        pinotStreamingQueryClient,
                        pinotSplit,
                        handles);
                }
                return new PinotSegmentPageSource(
                    session,
                    pinotConfig,
                    pinotQueryClient,
                    pinotSplit,
                    handles);
            case BROKER:
                switch (pinotSplit.getBrokerPinotQuery().get().getFormat()) {
                    case SQL:
                        return new PinotBrokerPageSourceSql(
                            pinotConfig,
                            session,
                            pinotSplit.getBrokerPinotQuery().get(),
                            handles,
                            pinotSplit.getExpectedColumnHandles(),
                            clusterInfoFetcher,
                            objectMapper);
                    case PQL:
                        return new PinotBrokerPageSourcePql(
                            pinotConfig,
                            session,
                            pinotSplit.getBrokerPinotQuery().get(),
                            handles,
                            pinotSplit.getExpectedColumnHandles(),
                            clusterInfoFetcher,
                            objectMapper);
                }
            default:
                throw new UnsupportedOperationException("Unknown Pinot split type: " + pinotSplit.getSplitType());
        }
    }

    @VisibleForTesting
    static GrpcQueryClient.Config extractGrpcQueryClientConfig(PinotConfig config)
    {
        Map<String, Object> target = new HashMap<>();
        target.put(CONFIG_USE_PLAIN_TEXT, !config.isUseSecureConnection());
        target.put(CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE, config.getStreamingServerGrpcMaxInboundMessageBytes());
        if (config.isUseSecureConnection()) {
            setOrRemoveProperty(target, "tls.keystore.path", config.getGrpcTlsKeyStorePath());
            setOrRemoveProperty(target, "tls.keystore.password", config.getGrpcTlsKeyStorePassword());
            setOrRemoveProperty(target, "tls.keystore.type", config.getGrpcTlsKeyStoreType());
            setOrRemoveProperty(target, "tls.truststore.path", config.getGrpcTlsTrustStorePath());
            setOrRemoveProperty(target, "tls.truststore.password", config.getGrpcTlsTrustStorePassword());
            setOrRemoveProperty(target, "tls.truststore.type", config.getGrpcTlsTrustStoreType());
        }
        return new GrpcQueryClient.Config(target);
    }
    // The method is created because Pinot Config does not like null as value, if value is null, we should
    // remove the key instead.
    private static void setOrRemoveProperty(Map<String, Object> prop, String key, Object value)
    {
        if (value == null) {
            prop.remove(key);
        }
        else {
            prop.put(key, value);
        }
    }
}
