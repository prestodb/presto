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
package com.facebook.presto.functionNamespace.execution.thrift;

import com.facebook.drift.TException;
import com.facebook.drift.client.DriftClient;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.RoutineCharacteristics.Language;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.ThriftScalarFunctionImplementation;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.thrift.api.datatypes.PrestoThriftBlock;
import com.facebook.presto.thrift.api.udf.PrestoThriftPage;
import com.facebook.presto.thrift.api.udf.ThriftFunctionHandle;
import com.facebook.presto.thrift.api.udf.ThriftUdfPage;
import com.facebook.presto.thrift.api.udf.ThriftUdfPageFormat;
import com.facebook.presto.thrift.api.udf.ThriftUdfRequest;
import com.facebook.presto.thrift.api.udf.ThriftUdfResult;
import com.facebook.presto.thrift.api.udf.ThriftUdfService;
import com.facebook.presto.thrift.api.udf.ThriftUdfServiceException;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.common.Page.wrapBlocksWithoutCopy;
import static com.facebook.presto.functionNamespace.execution.ExceptionUtils.toPrestoException;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.thrift.api.udf.ThriftUdfPage.prestoPage;
import static com.facebook.presto.thrift.api.udf.ThriftUdfPage.thriftPage;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ThriftSqlFunctionExecutor
{
    private final DriftClient<ThriftUdfService> thriftUdfClient;
    private final Map<Language, ThriftSqlFunctionExecutionConfig> executionConfigs;
    private BlockEncodingSerde blockEncodingSerde;

    @Inject
    public ThriftSqlFunctionExecutor(DriftClient<ThriftUdfService> thriftUdfClient, Map<Language, ThriftSqlFunctionExecutionConfig> executionConfigs)
    {
        this.thriftUdfClient = requireNonNull(thriftUdfClient, "thriftUdfClient is null");
        this.executionConfigs = requireNonNull(executionConfigs, "executionConfigs is null");
    }

    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        checkState(this.blockEncodingSerde == null, "blockEncodingSerde already set");
        checkArgument(blockEncodingSerde != null, "blockEncodingSerde is null");
        this.blockEncodingSerde = blockEncodingSerde;
    }

    public CompletableFuture<Block> executeFunction(
            String source,
            ThriftScalarFunctionImplementation functionImplementation,
            Page input,
            List<Integer> channels,
            List<Type> argumentTypes,
            Type returnType)
    {
        ThriftUdfPage page = buildThriftPage(functionImplementation, input, channels, argumentTypes);
        SqlFunctionHandle functionHandle = functionImplementation.getFunctionHandle();
        SqlFunctionId functionId = functionHandle.getFunctionId();
        ThriftFunctionHandle thriftFunctionHandle = new ThriftFunctionHandle(
                functionId.getFunctionName().toString(),
                functionId.getArgumentTypes().stream()
                        .map(TypeSignature::toString)
                        .collect(toImmutableList()),
                returnType.toString(),
                functionHandle.getVersion());
        ThriftUdfService thriftUdfService = thriftUdfClient.get(Optional.of(functionImplementation.getLanguage().getLanguage()));
        return invokeUdfAndHandleException(thriftUdfService, new ThriftUdfRequest(source, thriftFunctionHandle, page))
                .thenApply(result -> getResultBlock(result, returnType));
    }

    public static CompletableFuture<ThriftUdfResult> invokeUdfAndHandleException(
            ThriftUdfService thriftUdfService,
            ThriftUdfRequest request)
    {
        ListenableFuture<ThriftUdfResult> resultFuture;
        try {
            resultFuture = thriftUdfService.invokeUdf(request);
        }
        catch (ThriftUdfServiceException | TException e) {
            // Those exceptions are declared in ThriftUdfService.invokedUdf but
            // we don't expect them to be thrown here. Instead, the exceptions
            // are thrown when the resultFuture is consumed.
            throw new RuntimeException(e);
        }

        // Create a CompletableFuture which sources from resultFuture, while
        // transforming the exception to a PrestoException if case of failure.
        CompletableFuture<ThriftUdfResult> future = new CompletableFuture<>();
        future.exceptionally(throwable -> {
            if (throwable instanceof CancellationException) {
                resultFuture.cancel(true);
            }
            return null;
        });

        FutureCallback<ThriftUdfResult> callback = new FutureCallback<ThriftUdfResult>()
        {
            @Override
            public void onSuccess(ThriftUdfResult result)
            {
                future.complete(result);
            }

            @Override
            public void onFailure(Throwable t)
            {
                PrestoException prestoException = t instanceof ThriftUdfServiceException ?
                        toPrestoException((ThriftUdfServiceException) t) :
                        new PrestoException(GENERIC_INTERNAL_ERROR, t);
                future.completeExceptionally(prestoException);
            }
        };
        addCallback(resultFuture, callback, directExecutor());
        return future;
    }

    private ThriftUdfPage buildThriftPage(ThriftScalarFunctionImplementation functionImplementation, Page input, List<Integer> channels, List<Type> argumentTypes)
    {
        ThriftUdfPageFormat pageFormat = executionConfigs.get(functionImplementation.getLanguage()).getThriftPageFormat();
        Block[] blocks = new Block[channels.size()];

        for (int i = 0; i < channels.size(); i++) {
            blocks[i] = input.getBlock(channels.get(i));
        }

        switch (pageFormat) {
            case PRESTO_THRIFT:
                ImmutableList.Builder<PrestoThriftBlock> thriftBlocks = ImmutableList.builder();
                for (int i = 0; i < blocks.length; i++) {
                    thriftBlocks.add(PrestoThriftBlock.fromBlock(blocks[i], argumentTypes.get(i)));
                }
                return thriftPage(new PrestoThriftPage(thriftBlocks.build(), input.getPositionCount()));
            case PRESTO_SERIALIZED:
                checkState(blockEncodingSerde != null, "blockEncodingSerde not set");
                PagesSerde pagesSerde = new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), Optional.empty());
                return prestoPage(pagesSerde.serialize(wrapBlocksWithoutCopy(input.getPositionCount(), blocks)));
            default:
                throw new IllegalArgumentException(format("Unknown page format: %s", pageFormat));
        }
    }

    private Block getResultBlock(ThriftUdfResult result, Type returnType)
    {
        ThriftUdfPage page = result.getResult();
        switch (page.getPageFormat()) {
            case PRESTO_THRIFT:
                return getOnlyElement(page.getThriftPage().getThriftBlocks()).toBlock(returnType);
            case PRESTO_SERIALIZED:
                checkState(blockEncodingSerde != null, "blockEncodingSerde not set");
                PagesSerde pagesSerde = new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), Optional.empty());
                return pagesSerde.deserialize(page.getPrestoPage().toSerializedPage()).getBlock(0);
            default:
                throw new IllegalArgumentException(format("Unknown page format: %s", page.getPageFormat()));
        }
    }
}
