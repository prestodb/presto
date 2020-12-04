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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.ThriftScalarFunctionImplementation;
import com.facebook.presto.thrift.api.datatypes.PrestoThriftBlock;
import com.facebook.presto.thrift.api.udf.ThriftFunctionHandle;
import com.facebook.presto.thrift.api.udf.ThriftUdfService;
import com.facebook.presto.thrift.api.udf.ThriftUdfServiceException;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.airlift.concurrent.MoreFutures.toCompletableFuture;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.thrift.api.udf.ThriftUdfPage.thriftPage;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class ThriftSqlFunctionExecutor
{
    private final DriftClient<ThriftUdfService> thriftUdfClient;

    @Inject
    public ThriftSqlFunctionExecutor(DriftClient<ThriftUdfService> thriftUdfClient)
    {
        this.thriftUdfClient = requireNonNull(thriftUdfClient, "thriftUdfClient is null");
    }

    public CompletableFuture<Block> executeFunction(ThriftScalarFunctionImplementation functionImplementation, Page input, List<Integer> channels, List<Type> argumentTypes, Type returnType)
    {
        ImmutableList.Builder<PrestoThriftBlock> blocks = ImmutableList.builder();
        for (int i = 0; i < channels.size(); i++) {
            Block block = input.getBlock(channels.get(i));
            blocks.add(PrestoThriftBlock.fromBlock(block, argumentTypes.get(i)));
        }
        SqlFunctionHandle functionHandle = functionImplementation.getFunctionHandle();
        SqlFunctionId functionId = functionHandle.getFunctionId();
        try {
            return toCompletableFuture(thriftUdfClient.get(Optional.of(functionImplementation.getLanguage().getLanguage())).invokeUdf(
                    new ThriftFunctionHandle(
                            functionId.getFunctionName().toString(),
                            functionId.getArgumentTypes().stream()
                                    .map(TypeSignature::toString)
                                    .collect(toImmutableList()),
                            returnType.toString(),
                            functionHandle.getVersion()),
                    thriftPage(blocks.build())))
                    .thenApply(result -> getOnlyElement(result.getResult().getThriftBlocks()).toBlock(returnType));
        }
        catch (ThriftUdfServiceException | TException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
