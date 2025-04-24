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
package com.facebook.presto.functionNamespace.rest;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.functionNamespace.ForRestServer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.RemoteScalarFunctionImplementation;
import com.facebook.presto.spi.function.RestFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionExecutor;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.SliceInput;
import jakarta.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.airlift.concurrent.MoreFutures.failedFuture;
import static com.facebook.airlift.concurrent.MoreFutures.toCompletableFuture;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.presto.functionNamespace.rest.RestErrorCode.REST_SERVER_BAD_RESPONSE;
import static com.facebook.presto.functionNamespace.rest.RestErrorCode.REST_SERVER_CONNECT_ERROR;
import static com.facebook.presto.functionNamespace.rest.RestErrorCode.REST_SERVER_ERROR;
import static com.facebook.presto.functionNamespace.rest.RestErrorCode.REST_SERVER_IO_ERROR;
import static com.facebook.presto.functionNamespace.rest.RestErrorCode.REST_SERVER_NOT_FOUND;
import static com.facebook.presto.functionNamespace.rest.RestErrorCode.REST_SERVER_TIMEOUT;
import static com.facebook.presto.spi.function.FunctionImplementationType.REST;
import static com.facebook.presto.spi.page.PagesSerdeUtil.readSerializedPage;
import static com.facebook.presto.spi.page.PagesSerdeUtil.writeSerializedPage;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_SERVER_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class RestSqlFunctionExecutor
        implements SqlFunctionExecutor
{
    private BlockEncodingSerde blockEncodingSerde;
    private static PagesSerde pageSerde;
    private HttpClient httpClient;
    private final RestBasedFunctionNamespaceManagerConfig restBasedFunctionNamespaceManagerConfig;
    public static final String PRESTO_PAGES = "application/X-presto-pages";

    @Inject
    public RestSqlFunctionExecutor(RestBasedFunctionNamespaceManagerConfig restBasedFunctionNamespaceManagerConfig, @ForRestServer HttpClient httpClient)
    {
        this.restBasedFunctionNamespaceManagerConfig = requireNonNull(restBasedFunctionNamespaceManagerConfig, "restBasedFunctionNamespaceManagerConfig is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    public FunctionImplementationType getImplementationType()
    {
        return REST;
    }

    @Override
    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        checkState(this.blockEncodingSerde == null, "blockEncodingSerde already set");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.pageSerde = new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Override
    public CompletableFuture<SqlFunctionResult> executeFunction(
            String source,
            RemoteScalarFunctionImplementation functionImplementation,
            Page input,
            List<Integer> channels,
            List<Type> argumentTypes,
            Type returnType)
    {
        SqlFunctionHandle functionHandle = functionImplementation.getFunctionHandle();
        checkArgument(functionHandle instanceof RestFunctionHandle, "Expected RestFunctionHandle but got %s", functionHandle.getClass().getName());
        RestFunctionHandle restFunctionHandle = (RestFunctionHandle) functionHandle;
        SqlFunctionId functionId = functionHandle.getFunctionId();
        String functionVersion = functionHandle.getVersion();
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput((int) input.getRetainedSizeInBytes());
        writeSerializedPage(sliceOutput, pageSerde.serialize(input));
        try {
            Request request = preparePost()
                    .setUri(getExecutionEndpoint(restFunctionHandle, functionId, functionVersion))
                    .setBodyGenerator(createStaticBodyGenerator(sliceOutput.slice().byteArray()))
                    .setHeader(CONTENT_TYPE, PRESTO_PAGES)
                    .setHeader(ACCEPT, PRESTO_PAGES)
                    .build();
            HttpClient.HttpResponseFuture<SqlFunctionResult> future = httpClient.executeAsync(request, new SqlFunctionResultResponseHandler());
            Futures.addCallback(future, new SqlResultFutureCallback(), directExecutor());
            return toCompletableFuture(future);
        }
        catch (Exception e) {
            return failedFuture(e);
        }
    }

    private URI getExecutionEndpoint(RestFunctionHandle restFunctionHandle, SqlFunctionId functionId, String functionVersion)
    {
        String encodedFunctionId;
        encodedFunctionId = URLEncoder.encode(functionId.toJsonString(), UTF_8);

        // Use execution endpoint from handle if present, otherwise use default
        URI baseUri = restFunctionHandle.getExecutionEndpoint()
                .orElse(URI.create(restBasedFunctionNamespaceManagerConfig.getRestUrl()));
        String path = format("/v1/functions/%s/%s/%s/%s",
                functionId.getFunctionName().getSchemaName(),
                functionId.getFunctionName().getObjectName(),
                encodedFunctionId,
                functionVersion);

        return URI.create(String.format("%s%s", baseUri, path));
    }

    public static class SqlFunctionResultResponseHandler
            implements ResponseHandler<SqlFunctionResult, RuntimeException>
    {
        @Override
        public SqlFunctionResult handleException(Request request, Exception exception)
        {
            if (exception instanceof java.net.SocketTimeoutException) {
                throw new PrestoException(REST_SERVER_TIMEOUT, "Request to REST server timed out. Request: " + request, exception);
            }
            else if (exception instanceof java.net.ConnectException) {
                throw new PrestoException(REST_SERVER_CONNECT_ERROR, "Failed to connect to REST server. Request: " + request, exception);
            }
            else {
                throw new PrestoException(REST_SERVER_ERROR, "Unexpected error during REST call. Request: " + request + ", Exception: " + exception.getMessage(), exception);
            }
        }

        @Override
        public SqlFunctionResult handle(Request request, Response response)
        {
            if (response.getStatusCode() == HTTP_NOT_FOUND) {
                throw new PrestoException(REST_SERVER_NOT_FOUND, "Resource not found on REST server. Request: " + request);
            }
            else if (response.getStatusCode() == HTTP_SERVER_ERROR) {
                throw new PrestoException(REST_SERVER_ERROR, "Internal server error on REST server. Request: " + request);
            }
            else if (response.getStatusCode() != HTTP_OK) {
                throw new PrestoException(REST_SERVER_BAD_RESPONSE, "Unexpected response code: " + response.getStatusCode() + ". Request: " + request);
            }

            try {
                SliceInput input = new InputStreamSliceInput(response.getInputStream());
                SerializedPage serializedPage = readSerializedPage(input);
                Page page = pageSerde.deserialize(serializedPage);
                checkArgument(page.getChannelCount() == 1, "Expected only one channel in the function output");
                SqlFunctionResult output = new SqlFunctionResult(page.getBlock(0), 1);
                return output;
            }
            catch (IOException e) {
                throw new PrestoException(REST_SERVER_IO_ERROR, "Error deserializing REST server response: " + e.getMessage(), e);
            }
        }
    }

    public static class SqlResultFutureCallback
            implements FutureCallback<SqlFunctionResult>
    {
        @Override
        public void onSuccess(SqlFunctionResult result)
        {
            result.getResult();
        }

        @Override
        public void onFailure(Throwable t)
        {
            if (t instanceof PrestoException) {
                throw (PrestoException) t;
            }
            else if (t instanceof java.net.SocketTimeoutException) {
                throw new PrestoException(REST_SERVER_TIMEOUT, "REST server execution timed out. Error: " + t.getMessage(), t);
            }
            else {
                throw new PrestoException(REST_SERVER_ERROR, "Unknown error during REST execution. Error: " + t.getMessage(), t);
            }
        }
    }
}
