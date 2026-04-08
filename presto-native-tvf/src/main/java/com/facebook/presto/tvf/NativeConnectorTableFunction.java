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
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.presto.common.block.BlockSerdeUtil.writeBlock;
import static com.facebook.presto.spi.StandardErrorCode.TABLE_FUNCTION_ANALYSIS_FAILED;
import static com.facebook.presto.tvf.NativeTVFProvider.extractReasonFromVeloxError;
import static com.facebook.presto.tvf.NativeTVFProvider.getWorkerLocation;
import static com.fasterxml.jackson.core.Base64Variants.MIME_NO_LINEFEEDS;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
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
                Block.class, new BlockSerializer(new BlockEncodingManager())));
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
        return httpClient.execute(
                getWorkerRequest(arguments),
                new AnalyzeResponseHandler(tableFunctionAnalysisJsonCodec, typeManager));
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

    private static class BlockSerializer
            extends JsonSerializer<Block>
    {
        private final BlockEncodingSerde blockEncodingSerde;

        public BlockSerializer(BlockEncodingSerde blockEncodingSerde)
        {
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public void serialize(Block block, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            //  Encoding name is length prefixed as are other block data encodings
            SliceOutput output = new DynamicSliceOutput(toIntExact(block.getSizeInBytes() + block.getEncodingName().length() + (2 * Integer.BYTES)));
            writeBlock(blockEncodingSerde, output, block);
            Slice slice = output.slice();
            jsonGenerator.writeBinary(MIME_NO_LINEFEEDS, slice.byteArray(), slice.byteArrayOffset(), slice.length());
        }
    }

    private static class AnalyzeResponseHandler
            implements ResponseHandler<TableFunctionAnalysis, RuntimeException>
    {
        private final JsonCodec<NativeTableFunctionAnalysis> codec;
        private final TypeManager typeManager;

        public AnalyzeResponseHandler(JsonCodec<NativeTableFunctionAnalysis> codec, TypeManager typeManager)
        {
            this.codec = requireNonNull(codec, "codec is null");
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        public TableFunctionAnalysis handleException(Request request, Exception exception)
        {
            throw new PrestoException(TABLE_FUNCTION_ANALYSIS_FAILED, "Failed to analyze function: " + exception.getMessage(), exception);
        }

        @Override
        public TableFunctionAnalysis handle(Request request, Response response)
        {
            try {
                String body = CharStreams.toString(new InputStreamReader(response.getInputStream(), UTF_8));

                if (response.getStatusCode() != 200) {
                    // Extract just the "Reason:" line from Velox exception message
                    String errorMessage = extractReasonFromVeloxError(body);
                    throw new PrestoException(TABLE_FUNCTION_ANALYSIS_FAILED, errorMessage);
                }

                return codec.fromJson(body).toTableFunctionAnalysis(typeManager);
            }
            catch (PrestoException e) {
                throw e;
            }
            catch (IOException e) {
                throw new PrestoException(TABLE_FUNCTION_ANALYSIS_FAILED, "Failed to read response: " + e.getMessage(), e);
            }
            catch (Exception e) {
                throw new PrestoException(TABLE_FUNCTION_ANALYSIS_FAILED, "Failed to parse response: " + e.getMessage(), e);
            }
        }
    }
}
