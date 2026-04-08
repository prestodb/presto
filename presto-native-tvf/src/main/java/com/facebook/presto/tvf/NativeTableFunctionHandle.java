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

import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.TableFunctionHandleResolver;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Set;

import static com.facebook.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.tvf.HttpClientHolder.getHttpClient;
import static com.facebook.presto.tvf.NativeTVFProvider.extractReasonFromVeloxError;
import static com.facebook.presto.tvf.NativeTVFProvider.getWorkerLocation;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.nio.charset.StandardCharsets.UTF_8;
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

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, NodeManager nodeManager)
    {
        return new FixedSplitSource(
                getHttpClient().execute(
                        prepareSplitsPostRequest(nodeManager, this),
                        new SplitResponseHandler()));
    }

    private static Request prepareSplitsPostRequest(NodeManager nodeManager, NativeTableFunctionHandle nativeTableFunctionHandle)
    {
        return preparePost()
                .setUri(getWorkerLocation(nodeManager, TVF_SPLITS_ENDPOINT))
                .setBodyGenerator(jsonBodyGenerator(
                        JsonCodec.jsonCodec(ManualNativeTableFunctionHandleJsonHandler.class),
                        new ManualNativeTableFunctionHandleJsonHandler(
                                // The handle resolver for TVF providers is always the factory name suffixed by a colon.
                                NativeTVFProviderFactory.NAME + ":" + NativeTableFunctionHandle.class.getName(),
                                nativeTableFunctionHandle.serializedTableFunctionHandle,
                                nativeTableFunctionHandle.functionName)))
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(ACCEPT, JSON_UTF_8.toString())
                .build();
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

    private static class SplitResponseHandler
            implements ResponseHandler<List<NativeTableFunctionSplit>, RuntimeException>
    {
        private final JsonCodec<List<NativeTableFunctionSplit>> codec = listJsonCodec(NativeTableFunctionSplit.class);

        @Override
        public List<NativeTableFunctionSplit> handleException(Request request, Exception exception)
        {
            throw new PrestoException(INVALID_ARGUMENTS, "Failed to get splits: " + exception.getMessage(), exception);
        }

        @Override
        public List<NativeTableFunctionSplit> handle(Request request, Response response)
        {
            try {
                String body = CharStreams.toString(new InputStreamReader(response.getInputStream(), UTF_8));

                if (response.getStatusCode() != 200) {
                    // Extract just the "Reason:" line from Velox exception message
                    String errorMessage = extractReasonFromVeloxError(body);
                    throw new PrestoException(INVALID_ARGUMENTS, errorMessage);
                }
                return codec.fromJson(body);
            }
            catch (PrestoException e) {
                throw e;
            }
            catch (IOException e) {
                throw new PrestoException(INVALID_ARGUMENTS, "Failed to read response: " + e.getMessage(), e);
            }
            catch (Exception e) {
                throw new PrestoException(INVALID_ARGUMENTS, "Failed to parse response: " + e.getMessage(), e);
            }
        }
    }
}
