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
package com.facebook.presto.grpc;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.grpc.udf.GrpcFunctionHandle;
import com.facebook.presto.grpc.udf.GrpcSerializedPage;
import com.facebook.presto.grpc.udf.GrpcUdfInvokeGrpc;
import com.facebook.presto.grpc.udf.GrpcUdfPage;
import com.facebook.presto.grpc.udf.GrpcUdfPageFormat;
import com.facebook.presto.grpc.udf.GrpcUdfRequest;
import com.facebook.presto.grpc.udf.GrpcUdfResult;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.RemoteScalarFunctionImplementation;
import com.facebook.presto.spi.function.RoutineCharacteristics.Language;
import com.facebook.presto.spi.function.SqlFunctionExecutor;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.google.inject.Inject;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.facebook.airlift.concurrent.MoreFutures.toCompletableFuture;
import static com.facebook.presto.common.Page.wrapBlocksWithoutCopy;
import static com.facebook.presto.grpc.api.udf.GrpcUtils.toGrpcSerializedPage;
import static com.facebook.presto.grpc.api.udf.GrpcUtils.toGrpcUdfPage;
import static com.facebook.presto.grpc.api.udf.GrpcUtils.toPrestoPage;
import static com.facebook.presto.spi.function.FunctionImplementationType.GRPC;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class GrpcSqlFunctionExecutor
        implements SqlFunctionExecutor
{
    private static final int DEFAULT_RETRY_ATTEMPTS = 3;
    private final Map<Language, GrpcSqlFunctionExecutionConfig> grpcUdfConfigs;
    private final Map<Language, GrpcUdfInvokeGrpc.GrpcUdfInvokeFutureStub> futureStubs = new HashMap<>();

    private BlockEncodingSerde blockEncodingSerde;

    @Inject
    public GrpcSqlFunctionExecutor(Map<Language, GrpcSqlFunctionExecutionConfig> grpcUdfConfigs)
    {
        this.grpcUdfConfigs = requireNonNull(grpcUdfConfigs, "grpcUdfConfigs is null");
        grpcUdfConfigs.entrySet().forEach(entry -> {
            String grpcAddress = entry.getValue().getGrpcAddress();
            ManagedChannel channel = ManagedChannelBuilder.forTarget(grpcAddress).build();
            futureStubs.put(entry.getKey(), GrpcUdfInvokeGrpc.newFutureStub(channel));
        });
    }

    @Override
    public FunctionImplementationType getImplementationType()
    {
        return GRPC;
    }

    @Override
    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        checkState(this.blockEncodingSerde == null, "blockEncodingSerde already set");
        checkArgument(blockEncodingSerde != null, "blockEncodingSerde is null");
        this.blockEncodingSerde = blockEncodingSerde;
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
        GrpcUdfPage grpcUdfPage = buildGrpcUdfPage(input, channels, grpcUdfConfigs.get(functionImplementation.getLanguage()).getGrpcUdfPageFormat());
        SqlFunctionHandle functionHandle = functionImplementation.getFunctionHandle();
        SqlFunctionId functionId = functionHandle.getFunctionId();

        GrpcFunctionHandle grpcFunctionHandle = GrpcFunctionHandle.newBuilder()
                .setFunctionName(functionId.getFunctionName().toString())
                .addAllArgumentTypes(functionId.getArgumentTypes().stream()
                        .map(TypeSignature::toString)
                        .collect(toImmutableList()))
                .setReturnType(returnType.toString())
                .setVersion(functionHandle.getVersion())
                .build();

        GrpcUdfRequest grpcUdfRequest = GrpcUdfRequest.newBuilder()
                .setSource(source)
                .setGrpcFunctionHandle(grpcFunctionHandle)
                .setInputs(grpcUdfPage)
                .build();
        return invokeUdfWithRetry(futureStubs.get(functionImplementation.getLanguage()), grpcUdfRequest).thenApply(grpcResult -> toSqlFunctionResult(grpcResult));
    }

    private CompletableFuture<GrpcUdfResult> invokeUdf(GrpcUdfInvokeGrpc.GrpcUdfInvokeFutureStub futureStub, GrpcUdfRequest grpcUdfRequest)
    {
        try {
            return toCompletableFuture(futureStub.invokeUdf(grpcUdfRequest));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<GrpcUdfResult> invokeUdfWithRetry(GrpcUdfInvokeGrpc.GrpcUdfInvokeFutureStub futureStub, GrpcUdfRequest grpcUdfRequest)
    {
        CompletableFuture<GrpcUdfResult> resultFuture = invokeUdf(futureStub, grpcUdfRequest);
        for (int i = 0; i < DEFAULT_RETRY_ATTEMPTS; i++) {
            resultFuture = resultFuture.thenApply(CompletableFuture::completedFuture)
                    .exceptionally(t -> {
                        return invokeUdf(futureStub, grpcUdfRequest);
                    })
                    .thenCompose(Function.identity());
        }
        return resultFuture;
    }

    private GrpcUdfPage buildGrpcUdfPage(Page input, List<Integer> channels, GrpcUdfPageFormat grpcUdfPageFormat)
    {
        Block[] blocks = new Block[channels.size()];
        for (int i = 0; i < channels.size(); i++) {
            blocks[i] = input.getBlock(channels.get(i));
        }

        switch (grpcUdfPageFormat) {
            case Presto:
                checkState(blockEncodingSerde != null, "blockEncodingSerde not set");
                Page inputPage = wrapBlocksWithoutCopy(input.getPositionCount(), blocks);
                GrpcSerializedPage grpcSerializedPage = toGrpcSerializedPage(blockEncodingSerde, inputPage);
                return toGrpcUdfPage(grpcUdfPageFormat, grpcSerializedPage);
            default:
                throw new IllegalArgumentException(format("Unknown page format: %s", grpcUdfPageFormat));
        }
    }

    private SqlFunctionResult toSqlFunctionResult(GrpcUdfResult grpcUdfResult)
    {
        checkState(blockEncodingSerde != null, "blockEncodingSerde not set");
        GrpcUdfPage grpcUdfPage = grpcUdfResult.getResult();

        switch (grpcUdfPage.getGrpcUdfPageFormat()) {
            case Presto:
                Page resultPage = toPrestoPage(blockEncodingSerde, grpcUdfPage.getGrpcSerializedPage());
                return new SqlFunctionResult(resultPage.getBlock(0), grpcUdfResult.getUdfStats().getTotalCpuTimeMs());
            default:
                throw new IllegalArgumentException(format("Unknown page format: %s", grpcUdfPage.getGrpcUdfPageFormat()));
        }
    }
}
