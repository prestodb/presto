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
package com.facebook.presto.udf.thrift;

import com.facebook.presto.thrift.api.udf.ThriftFunctionHandle;
import com.facebook.presto.thrift.api.udf.ThriftUdfPage;
import com.facebook.presto.thrift.api.udf.ThriftUdfResult;
import com.facebook.presto.thrift.api.udf.ThriftUdfService;
import com.facebook.presto.thrift.api.udf.ThriftUdfStats;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import static io.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.presto.thrift.api.udf.ThriftUdfPageFormat.PRESTO_THRIFT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class EchoFirstInputThriftUdfService
        implements ThriftUdfService
{
    private final ListeningExecutorService executor = listeningDecorator(
            newFixedThreadPool(Runtime.getRuntime().availableProcessors(), threadsNamed("udf-thrift-%s")));

    @Override
    public ListenableFuture<ThriftUdfResult> invokeUdf(
            ThriftFunctionHandle functionHandle,
            ThriftUdfPage inputs)
    {
        checkArgument(inputs.getPageFormat().equals(PRESTO_THRIFT));
        return executor.submit(() -> new ThriftUdfResult(
                new ThriftUdfPage(PRESTO_THRIFT, ImmutableList.of(inputs.getThriftBlocks().get(0))),
                new ThriftUdfStats(0)));
    }
}
