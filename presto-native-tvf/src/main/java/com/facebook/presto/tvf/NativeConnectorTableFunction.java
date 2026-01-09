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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.block.BlockJsonSerde.Serializer;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.table.AbstractConnectorTableFunction;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ArgumentSpecification;
import com.facebook.presto.spi.function.table.ReturnTypeSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.facebook.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.presto.spi.StandardErrorCode.TABLE_FUNCTION_ANALYSIS_FAILED;
import static com.facebook.presto.tvf.NativeTVFProvider.getWorkerLocation;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.util.Objects.requireNonNull;

public class NativeConnectorTableFunction
        extends AbstractConnectorTableFunction
{
    private final HttpClient httpClient;
    private final NodeManager nodeManager;
    private final TypeManager typeManager;
    private static final String TVF_ANALYZE_ENDPOINT = "/v1/tvf/analyze";
    private static final JsonCodec<ConnectorTableMetadata> connectorTableMetadataJsonCodec;
    private static final JsonCodec<NativeTableFunctionAnalysis> tableFunctionAnalysisJsonCodec =
            JsonCodec.jsonCodec(NativeTableFunctionAnalysis.class);
    private final QualifiedObjectName functionName;

    static {
        JsonObjectMapperProvider provider = new JsonObjectMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(
                Block.class, new Serializer(new BlockEncodingManager())));

        ObjectMapper mapper = provider.get();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider);
        connectorTableMetadataJsonCodec = codecFactory.jsonCodec(ConnectorTableMetadata.class);
    }

    public NativeConnectorTableFunction(
            @ForWorkerInfo HttpClient httpClient,
            NodeManager nodeManager,
            TypeManager typeManager,
            QualifiedObjectName functionName,
            List<ArgumentSpecification> arguments,
            ReturnTypeSpecification returnTypeSpecification)
    {
        super("builtin", functionName.getObjectName(), arguments, returnTypeSpecification);
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionName = requireNonNull(functionName, "functionName is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
    {
        try {
            return httpClient.execute(
                    getWorkerRequest(arguments),
                    createJsonResponseHandler(tableFunctionAnalysisJsonCodec)).toTableFunctionAnalysis(typeManager);
        }
        catch (Exception e) {
            throw new PrestoException(TABLE_FUNCTION_ANALYSIS_FAILED, "Failed to analyze function.", e);
        }
    }

    private Request getWorkerRequest(Map<String, Argument> arguments)
    {
        return preparePost()
                .setUri(getWorkerLocation(nodeManager, TVF_ANALYZE_ENDPOINT))
                .setBodyGenerator(
                        jsonBodyGenerator(connectorTableMetadataJsonCodec, new ConnectorTableMetadata(functionName, arguments)))
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(ACCEPT, JSON_UTF_8.toString())
                .build();
    }
}
