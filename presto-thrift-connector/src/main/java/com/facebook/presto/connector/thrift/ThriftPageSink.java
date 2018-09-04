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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.connector.thrift.api.PrestoThriftBlock;
import com.facebook.presto.connector.thrift.api.PrestoThriftPage;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.client.DriftClient;
import io.airlift.drift.client.address.AddressSelector;
import io.airlift.drift.transport.client.Address;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

public class ThriftPageSink
        implements ConnectorPageSink
{
    private final PrestoThriftService client;
    private final ThriftInsertTableHandle insertTableHandle;
    private ListenableFuture<Void> future;
    private UUID insertId = UUID.randomUUID();

    public ThriftPageSink(DriftClient<PrestoThriftService> client, AddressSelector<Address> addressSelector, Map<String, String> thriftHeader,
            ThriftInsertTableHandle insertTableHandle)
    {
        this.insertTableHandle = requireNonNull(insertTableHandle, "insertTableHandle is null");
        if (this.insertTableHandle.getBucketProperty().isPresent()) {
            /*
                When a table is bucketed by some set of columns, we want to ensure that all data from a Presto worker is sent
                to only one connector worker.
                Otherwise, bucketed data will be separated which defeats the purpose of bucketing.
                Use the addressSelector to choose only one address to maintain a "sticky" connection with.
             */
            Address selectedAddress = addressSelector.selectAddress(Optional.empty()).get();
            this.client = client.get(Optional.of(selectedAddress.getHostAndPort().toString()), thriftHeader);
        }
        else {
            this.client = client.get(thriftHeader);
        }
        future = null;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        /*
            Currently, bucketed data is not sorted back into buckets in appendPage like it does so in HivePageSink. Instead, we give all the data,
            no matter how many buckets this Presto worker has, to the connector worker, and we expect that it separates out the data if it needs to.

            To sort the data on the Presto side, consider using the bucketProperty to recreate the ThriftBucketFunction here. Look at the HivePageSink implementation for more details.
            Note that the ThriftBucketFunction was used in ThriftNodePartitioningManager to partition the data into buckets to give to Presto workers.
         */
        List<Type> columnTypes = insertTableHandle.getColumnTypes();
        List<PrestoThriftBlock> prestoThriftBlocks = IntStream.range(0, page.getChannelCount())
                .mapToObj(i -> PrestoThriftBlock.fromBlock(page.getBlock(i), columnTypes.get(i)))
                .collect(Collectors.toList());
        PrestoThriftPage prestoThriftPage = new PrestoThriftPage(prestoThriftBlocks, page.getPositionCount());
        future = client.addRows(
                new PrestoThriftSchemaTableName(
                        insertTableHandle.getSchemaName(),
                        insertTableHandle.getTableName()),
                prestoThriftPage,
                insertId.toString());
        // We wait on this call to ensure that all appendPage calls occur before the finish method is called.
        getFutureValue(future);
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // Signal the end of sending data to be inserted so that the system can store the data.
        ListenableFuture<Void> finished = client.finishAddRows(insertId.toString());
        getFutureValue(finished);
        return CompletableFuture.completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        ListenableFuture<Void> finished = client.abortAddRows(insertId.toString());
        getFutureValue(finished);
    }
}
