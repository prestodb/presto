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
import com.facebook.presto.connector.thrift.api.PrestoThriftId;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableToken;
import com.facebook.presto.connector.thrift.api.PrestoThriftPage;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.client.DriftClient;
import io.airlift.drift.client.address.AddressSelector;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ThriftPageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(ThriftPageSink.class);

    // This is the class that the worker will be used to actually place the data somewhere.
    private final PrestoThriftService client;
    private final ThriftInsertTableHandle insertTableHandle;
    private final ThriftBucketFunction bucketFunction;
    private ListenableFuture<PrestoThriftNullableToken> future;
    private PrestoThriftId nextToken;

    public <T> ThriftPageSink(DriftClient<PrestoThriftService> client, AddressSelector<?> addressSelector, ThriftInsertTableHandle insertTableHandle, Optional<ThriftBucketProperty> bucketProperty)
    {
        this.client = client.get(Optional.of(addressSelector.selectAddress(Optional.empty()).get().toString()));
        // this.client = client.get();
        this.insertTableHandle = insertTableHandle;
        if (bucketProperty.isPresent()) {
            bucketFunction = new ThriftBucketFunction(bucketProperty.get().getBucketCount());
        }
        else {
            bucketFunction = null;
        }
        future = null;
        nextToken = null;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        // TODO: sort the data back into the buckets that they were in when presto sent the data to this worker. Check out the Hive PageSink operations for inspiration on how to do this.
        // This is the append page, where I will start the process of inserting the data.
        // If I were supporting transactions / concurrent inserts, this section would be
        // more of a precommit. However, a simple implementation will just save the data
        // where it needs to already.
        log.info("appendPage called!");
        log.info("The thrift object id is %s", this.toString());
        log.info("client is %s", client.toString());
        List<PrestoThriftBlock> prestoThriftBlocks = new ArrayList<>();
        List<Type> columnTypes = insertTableHandle.getColumnTypes();
        for (int i = 0; i < page.getChannelCount(); i++) {
            Block block = page.getBlock(i);
            prestoThriftBlocks.add(PrestoThriftBlock.fromBlock(block, columnTypes.get(i)));
        }

        PrestoThriftPage prestoThriftPage = new PrestoThriftPage(prestoThriftBlocks, page.getPositionCount());

//        while (future == null || nextToken != null) {
        try {
            future = client.addRows(
                new PrestoThriftSchemaTableName(
                        insertTableHandle.getSchemaName(),
                        insertTableHandle.getTableName()),
                prestoThriftPage,
                new PrestoThriftNullableToken(nextToken));
            nextToken = future.get().getToken();
        }
        // TODO: implement transaction-like process for inserts so that if the execution gets interrupted, a rollback occurs.
        catch (ExecutionException | InterruptedException e) {
//            break;
        }
//        }
        log.info("finished appendPage!");
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // This method is the commit part of the process of inserting the data.
        // However, since I'm not worrying about transactions / concurrent inserts,
        // I don't need to do anything here.
        return CompletableFuture.completedFuture(new ArrayList<>());
    }

    @Override
    public void abort()
    {
        // This method is called when the transaction must abort. Since I'm not worrying
        // about concurrent inserts, I don't have to worry about aborting.
    }
}
