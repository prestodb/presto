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
package com.facebook.presto.functionNamespace.execution.rest;

import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RestURIManager;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.RemoteScalarFunctionImplementation;
import com.facebook.presto.spi.function.SqlFunctionExecutor;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.function.FunctionImplementationType.REST;
import static com.facebook.presto.spi.page.PagesSerdeUtil.readSerializedPage;
import static com.facebook.presto.spi.page.PagesSerdeUtil.writeSerializedPage;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class RestSqlFunctionExecutor
        implements SqlFunctionExecutor
{
    private BlockEncodingSerde blockEncodingSerde;
    private PagesSerde pageSerde;
    private OkHttpClient okHttpClient;
    private NodeManager nodeManager;
    private final RestURIManager restURIManager;

    @Inject
    public RestSqlFunctionExecutor(NodeManager nodeManager, RestURIManager restURIManager)
    {
        this.nodeManager = nodeManager;
        this.restURIManager = restURIManager;
    }

    @PostConstruct
    public void init()
    {
        this.okHttpClient = new OkHttpClient();
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
        requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.blockEncodingSerde = blockEncodingSerde;
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
        SqlFunctionId functionId = functionHandle.getFunctionId();
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput((int) input.getRetainedSizeInBytes());
        writeSerializedPage(sliceOutput, pageSerde.serialize(input));
        RequestBody body = RequestBody.create(MediaType.parse("text/plain; charset=utf-8"), sliceOutput.slice().byteArray());
        Request request = new Request.Builder()
                .url(getNativeWorkerUri(nodeManager, functionId, returnType).toString())
                .post(body)
                .build();
        CallbackFuture future = new CallbackFuture();
        okHttpClient.newCall(request).enqueue(future);
        return future;
    }

    private URI getNativeWorkerUri(NodeManager nodeManager, SqlFunctionId functionId, Type returnType)
    {
        List<String> functionArgumentTypes = functionId.getArgumentTypes().stream().map(TypeSignature::toString).collect(toImmutableList());
        if (restURIManager.getRestUriManager() == null) {
            throw new PrestoException(NOT_FOUND, "failed to find native node !");
        }
        HttpUriBuilder uri = uriBuilderFrom(URI.create("http://" + restURIManager.getRestUriManager()))
                .appendPath("/v1/function/" + functionId.getFunctionName().getObjectName())
                .addParameter("returnType", returnType.toString())
                .addParameter("numArgs", "" + functionArgumentTypes.size());
        for (int i = 1; i <= functionArgumentTypes.size(); i++) {
            uri.addParameter("argType" + i, functionArgumentTypes.get(i - 1));
        }
        return uri.build();
    }

    private class CallbackFuture
            extends CompletableFuture<SqlFunctionResult>
            implements Callback
    {
        @Override
        public void onFailure(Call call, IOException e)
        {
            super.completeExceptionally(e);
        }
        @Override
        public void onResponse(Call call, Response response)
        {
            try {
                if (response.code() != 200) {
                    super.completeExceptionally(new IllegalStateException("Failed to get response for rest function call. Response code: " + response.code()));
                }
                Slice slice = Slices.wrappedBuffer(response.body().bytes());
                SerializedPage serializedPage = readSerializedPage(new BasicSliceInput(slice));
                SqlFunctionResult output = new SqlFunctionResult(pageSerde.deserialize(serializedPage).getBlock(0), response.receivedResponseAtMillis());
                super.complete(output);
            }
            catch (IOException | NullPointerException e) {
                super.completeExceptionally(new IllegalStateException("Failed to get response for rest function call, " + e.getMessage()));
            }
        }
    }
}
