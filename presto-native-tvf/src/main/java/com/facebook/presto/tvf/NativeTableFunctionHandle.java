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
package com.facebook.presto.tvf;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.index.IndexHandleJacksonModule;
import com.facebook.presto.metadata.ColumnHandleJacksonModule;
import com.facebook.presto.metadata.DeleteTableHandleJacksonModule;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.FunctionHandleJacksonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InsertTableHandleJacksonModule;
import com.facebook.presto.metadata.OutputTableHandleJacksonModule;
import com.facebook.presto.metadata.PartitioningHandleJacksonModule;
import com.facebook.presto.metadata.SplitJacksonModule;
import com.facebook.presto.metadata.TableFunctionJacksonHandleModule;
import com.facebook.presto.metadata.TableHandleJacksonModule;
import com.facebook.presto.metadata.TableLayoutHandleJacksonModule;
import com.facebook.presto.metadata.TransactionHandleJacksonModule;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.TableFunctionHandleResolver;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.presto.tvf.NativeTVFProvider.getWorkerLocation;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.util.Objects.requireNonNull;

public class NativeTableFunctionHandle
        implements ConnectorTableFunctionHandle
{
    private static final String TVF_SPLITS_ENDPOINT = "/v1/tvf/splits";

    private final QualifiedObjectName functionName;
    private final String serializedTableFunctionHandle;

    @JsonCreator
    public NativeTableFunctionHandle(
            @JsonProperty("serializedTableFunctionHandle") String serializedTableFunctionHandle,
            @JsonProperty("functionName") QualifiedObjectName functionName)
    {
        this.serializedTableFunctionHandle = requireNonNull(serializedTableFunctionHandle, "serializedTableFunctionHandle is null");
        this.functionName = requireNonNull(functionName, "functionName is null");
    }

    @JsonProperty
    public String getSerializedTableFunctionHandle()
    {
        return serializedTableFunctionHandle;
    }

    @JsonProperty("functionName")
    public QualifiedObjectName getFunctionName()
    {
        return functionName;
    }

    public static class Resolver
            implements TableFunctionHandleResolver
    {
        @Override
        public Set<Class<? extends ConnectorTableFunctionHandle>> getTableFunctionHandleClasses()
        {
            return ImmutableSet.of(NativeTableFunctionHandle.class);
        }
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, NodeManager nodeManager, Object functionAndTypeManager)
    {
        if (functionAndTypeManager instanceof FunctionAndTypeManager) {
            ObjectMapper objectMapper = new ObjectMapper();
            HandleResolver handleResolver = ((FunctionAndTypeManager) functionAndTypeManager).getHandleResolver();

            FeaturesConfig featuresConfig = new FeaturesConfig();
            featuresConfig.setUseConnectorProvidedSerializationCodecs(true);

            objectMapper.registerModule(new TableHandleJacksonModule(handleResolver, featuresConfig, connectorId -> Optional.empty()));
            objectMapper.registerModule(new TableLayoutHandleJacksonModule(handleResolver, featuresConfig, connectorId -> Optional.empty()));
            objectMapper.registerModule(new ColumnHandleJacksonModule(handleResolver, featuresConfig, connectorId -> Optional.empty()));
            objectMapper.registerModule(new SplitJacksonModule(handleResolver, featuresConfig, connectorId -> Optional.empty()));
            objectMapper.registerModule(new OutputTableHandleJacksonModule(handleResolver, featuresConfig, connectorId -> Optional.empty()));
            objectMapper.registerModule(new InsertTableHandleJacksonModule(handleResolver, featuresConfig, connectorId -> Optional.empty()));
            objectMapper.registerModule(new DeleteTableHandleJacksonModule(handleResolver, featuresConfig, connectorId -> Optional.empty()));
            objectMapper.registerModule(new IndexHandleJacksonModule(handleResolver, featuresConfig, connectorId -> Optional.empty()));
            objectMapper.registerModule(new TransactionHandleJacksonModule(handleResolver, featuresConfig, connectorId -> Optional.empty()));
            objectMapper.registerModule(new PartitioningHandleJacksonModule(handleResolver, featuresConfig, connectorId -> Optional.empty()));
            objectMapper.registerModule(new FunctionHandleJacksonModule(handleResolver));
            objectMapper.registerModule(new TableFunctionJacksonHandleModule(handleResolver, featuresConfig, connectorId -> Optional.empty()));
            JsonCodecFactory jsonCodecFactory = new JsonCodecFactory(() -> objectMapper);
            JsonCodec<ConnectorTableFunctionHandle> nativeTableFunctionHandleCodec = jsonCodecFactory.jsonCodec(ConnectorTableFunctionHandle.class);

            return new FixedSplitSource(
                    HttpClientHolder.getHttpClient().execute(
                            preparePost()
                                    .setUri(getWorkerLocation(nodeManager, TVF_SPLITS_ENDPOINT))
                                    .setBodyGenerator(jsonBodyGenerator(nativeTableFunctionHandleCodec, this))
                                    .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                                    .setHeader(ACCEPT, JSON_UTF_8.toString())
                                    .build(),
                            createJsonResponseHandler(JsonCodec.listJsonCodec(NativeTableFunctionSplit.class))));
        }

        throw new UnsupportedOperationException();
    }
}
