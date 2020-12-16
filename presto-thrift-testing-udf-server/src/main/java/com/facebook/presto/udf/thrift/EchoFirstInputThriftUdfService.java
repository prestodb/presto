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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.thrift.api.udf.PrestoThriftPage;
import com.facebook.presto.thrift.api.udf.ThriftFunctionHandle;
import com.facebook.presto.thrift.api.udf.ThriftUdfPage;
import com.facebook.presto.thrift.api.udf.ThriftUdfResult;
import com.facebook.presto.thrift.api.udf.ThriftUdfService;
import com.facebook.presto.thrift.api.udf.ThriftUdfStats;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.thrift.api.udf.ThriftUdfPage.prestoPage;
import static com.facebook.presto.thrift.api.udf.ThriftUdfPage.thriftPage;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class EchoFirstInputThriftUdfService
        implements ThriftUdfService
{
    private final BlockEncodingSerde blockEncodingSerde;

    @Inject
    public EchoFirstInputThriftUdfService(BlockEncodingSerde blockEncodingSerde)
    {
        this.blockEncodingSerde = blockEncodingSerde;
    }

    @Override
    public ListenableFuture<ThriftUdfResult> invokeUdf(
            ThriftFunctionHandle functionHandle,
            ThriftUdfPage inputs)
    {
        ThriftUdfPage result;
        switch (inputs.getPageFormat()) {
            case PRESTO_THRIFT:
                PrestoThriftPage thriftPage = inputs.getThriftPage();
                if (thriftPage.getThriftBlocks().isEmpty()) {
                    throw new UnsupportedOperationException("No input to echo");
                }
                result = thriftPage(new PrestoThriftPage(ImmutableList.of(thriftPage.getThriftBlocks().get(0)), inputs.getThriftPage().getPositionCount()));
                break;
            case PRESTO_SERIALIZED:
                PagesSerde pagesSerde = new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), Optional.empty());
                Page page = pagesSerde.deserialize(inputs.getPrestoPage().toSerializedPage());
                result = prestoPage(pagesSerde.serialize(new Page(page.getBlock(0))));
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return immediateFuture(new ThriftUdfResult(
                result,
                new ThriftUdfStats(0)));
    }
}
