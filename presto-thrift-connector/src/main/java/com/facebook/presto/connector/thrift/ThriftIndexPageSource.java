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

import com.facebook.presto.connector.thrift.api.PrestoThriftId;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableToken;
import com.facebook.presto.connector.thrift.api.PrestoThriftPageResult;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplit;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftTupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.client.DriftClient;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.connector.thrift.api.PrestoThriftPageResult.fromRecordSet;
import static com.facebook.presto.connector.thrift.util.ThriftExceptions.catchingThriftException;
import static com.facebook.presto.connector.thrift.util.TupleDomainConversion.tupleDomainToThriftTupleDomain;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class ThriftIndexPageSource
        implements ConnectorPageSource
{
    private static final int MAX_SPLIT_COUNT = 10_000_000;

    private final DriftClient<PrestoThriftService> client;
    private final Map<String, String> thriftHeaders;
    private final PrestoThriftSchemaTableName schemaTableName;
    private final List<String> lookupColumnNames;
    private final List<String> outputColumnNames;
    private final List<Type> outputColumnTypes;
    private final PrestoThriftTupleDomain outputConstraint;
    private final PrestoThriftPageResult keys;
    private final long maxBytesPerResponse;
    private final int lookupRequestsConcurrency;

    private final AtomicLong readTimeNanos = new AtomicLong(0);
    private long completedBytes;

    private CompletableFuture<?> statusFuture;
    private ListenableFuture<PrestoThriftSplitBatch> splitFuture;
    private ListenableFuture<PrestoThriftPageResult> dataSignalFuture;

    private final List<PrestoThriftSplit> splits = new ArrayList<>();
    private final Queue<ListenableFuture<PrestoThriftPageResult>> dataRequests = new LinkedList<>();
    private final Map<ListenableFuture<PrestoThriftPageResult>, RunningSplitContext> contexts;
    private final ThriftConnectorStats stats;

    private int splitIndex;
    private boolean haveSplits;
    private boolean finished;

    public ThriftIndexPageSource(
            DriftClient<PrestoThriftService> client,
            Map<String, String> thriftHeaders,
            ThriftConnectorStats stats,
            ThriftIndexHandle indexHandle,
            List<ColumnHandle> lookupColumns,
            List<ColumnHandle> outputColumns,
            RecordSet keys,
            long maxBytesPerResponse,
            int lookupRequestsConcurrency)
    {
        this.client = requireNonNull(client, "client is null");
        this.thriftHeaders = requireNonNull(thriftHeaders, "thriftHeaders is null");
        this.stats = requireNonNull(stats, "stats is null");

        requireNonNull(indexHandle, "indexHandle is null");
        this.schemaTableName = new PrestoThriftSchemaTableName(indexHandle.getSchemaTableName());
        this.outputConstraint = tupleDomainToThriftTupleDomain(indexHandle.getTupleDomain());

        requireNonNull(lookupColumns, "lookupColumns is null");
        this.lookupColumnNames = lookupColumns.stream()
                .map(ThriftColumnHandle.class::cast)
                .map(ThriftColumnHandle::getColumnName)
                .collect(toImmutableList());

        requireNonNull(outputColumns, "outputColumns is null");
        ImmutableList.Builder<String> outputColumnNames = new ImmutableList.Builder<>();
        ImmutableList.Builder<Type> outputColumnTypes = new ImmutableList.Builder<>();
        for (ColumnHandle columnHandle : outputColumns) {
            ThriftColumnHandle thriftColumnHandle = (ThriftColumnHandle) columnHandle;
            outputColumnNames.add(thriftColumnHandle.getColumnName());
            outputColumnTypes.add(thriftColumnHandle.getColumnType());
        }
        this.outputColumnNames = outputColumnNames.build();
        this.outputColumnTypes = outputColumnTypes.build();

        this.keys = fromRecordSet(requireNonNull(keys, "keys is null"));

        checkArgument(maxBytesPerResponse > 0, "maxBytesPerResponse is zero or negative");
        this.maxBytesPerResponse = maxBytesPerResponse;
        checkArgument(lookupRequestsConcurrency >= 1, "lookupRequestsConcurrency is less than one");
        this.lookupRequestsConcurrency = lookupRequestsConcurrency;

        this.contexts = new HashMap<>(lookupRequestsConcurrency);
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos.get();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return statusFuture == null ? NOT_BLOCKED : statusFuture;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }
        if (!loadAllSplits()) {
            return null;
        }

        // check if any data requests were started
        if (dataSignalFuture == null) {
            // no data requests were started, start a number of initial requests
            checkState(contexts.isEmpty() && dataRequests.isEmpty(), "some splits are already started");
            if (splits.isEmpty()) {
                // all done: no splits
                finished = true;
                return null;
            }
            for (int i = 0; i < min(lookupRequestsConcurrency, splits.size()); i++) {
                startDataFetchForNextSplit();
            }
            updateSignalAndStatusFutures();
        }
        // check if any data request is finished
        if (!dataSignalFuture.isDone()) {
            // not finished yet
            return null;
        }

        // at least one of data requests completed
        ListenableFuture<PrestoThriftPageResult> resultFuture = getAndRemoveNextCompletedRequest();
        RunningSplitContext resultContext = contexts.remove(resultFuture);
        checkState(resultContext != null, "no associated context for the request");
        PrestoThriftPageResult pageResult = getFutureValue(resultFuture);
        Page page = pageResult.toPage(outputColumnTypes);
        if (page != null) {
            long pageSize = page.getSizeInBytes();
            completedBytes += pageSize;
            stats.addIndexPageSize(pageSize);
        }
        else {
            stats.addIndexPageSize(0);
        }
        if (pageResult.getNextToken() != null) {
            // can get more data
            sendDataRequest(resultContext, pageResult.getNextToken());
            updateSignalAndStatusFutures();
            return page;
        }

        // are there more splits available
        if (splitIndex < splits.size()) {
            // can send data request for a new split
            startDataFetchForNextSplit();
            updateSignalAndStatusFutures();
        }
        else if (!dataRequests.isEmpty()) {
            // no more new splits, but some requests are still in progress, wait for them
            updateSignalAndStatusFutures();
        }
        else {
            // all done: no more new splits, no requests in progress
            dataSignalFuture = null;
            statusFuture = null;
            finished = true;
        }
        return page;
    }

    private boolean loadAllSplits()
    {
        if (haveSplits) {
            return true;
        }
        // check if request for splits was sent
        if (splitFuture == null) {
            // didn't start fetching splits, send the first request now
            splitFuture = sendSplitRequest(null);
            statusFuture = toCompletableFuture(nonCancellationPropagating(splitFuture));
        }
        if (!splitFuture.isDone()) {
            // split request is in progress
            return false;
        }
        // split request is ready
        PrestoThriftSplitBatch batch = getFutureValue(splitFuture);
        splits.addAll(batch.getSplits());
        // check if it's possible to request more splits
        if (batch.getNextToken() != null) {
            // can get more splits, send request
            splitFuture = sendSplitRequest(batch.getNextToken());
            statusFuture = toCompletableFuture(nonCancellationPropagating(splitFuture));
            return false;
        }
        else {
            // no more splits
            splitFuture = null;
            statusFuture = null;
            haveSplits = true;
            return true;
        }
    }

    private void updateSignalAndStatusFutures()
    {
        dataSignalFuture = whenAnyComplete(dataRequests);
        statusFuture = toCompletableFuture(nonCancellationPropagating(dataSignalFuture));
    }

    private void startDataFetchForNextSplit()
    {
        PrestoThriftSplit split = splits.get(splitIndex);
        splitIndex++;
        RunningSplitContext context = new RunningSplitContext(openClient(split), split);
        sendDataRequest(context, null);
    }

    private ListenableFuture<PrestoThriftSplitBatch> sendSplitRequest(@Nullable PrestoThriftId nextToken)
    {
        long start = System.nanoTime();
        ListenableFuture<PrestoThriftSplitBatch> future = client.get(thriftHeaders).getIndexSplits(
                schemaTableName,
                lookupColumnNames,
                outputColumnNames,
                keys,
                outputConstraint,
                MAX_SPLIT_COUNT,
                new PrestoThriftNullableToken(nextToken));
        future = catchingThriftException(future);
        future.addListener(() -> readTimeNanos.addAndGet(System.nanoTime() - start), directExecutor());
        return future;
    }

    private void sendDataRequest(RunningSplitContext context, @Nullable PrestoThriftId nextToken)
    {
        long start = System.nanoTime();
        ListenableFuture<PrestoThriftPageResult> future = context.getClient().getRows(
                context.getSplit().getSplitId(),
                outputColumnNames,
                maxBytesPerResponse,
                new PrestoThriftNullableToken(nextToken));
        future = catchingThriftException(future);
        future.addListener(() -> readTimeNanos.addAndGet(System.nanoTime() - start), directExecutor());
        dataRequests.add(future);
        contexts.put(future, context);
    }

    private PrestoThriftService openClient(PrestoThriftSplit split)
    {
        if (split.getHosts().isEmpty()) {
            return client.get(thriftHeaders);
        }
        String hosts = split.getHosts().stream()
                .map(host -> host.toHostAddress().toString())
                .collect(joining(","));
        return client.get(Optional.of(hosts), thriftHeaders);
    }

    @Override
    public void close()
    {
        // cancel futures if available
        cancelQuietly(splitFuture);
        dataRequests.forEach(ThriftIndexPageSource::cancelQuietly);
    }

    private ListenableFuture<PrestoThriftPageResult> getAndRemoveNextCompletedRequest()
    {
        Iterator<ListenableFuture<PrestoThriftPageResult>> iterator = dataRequests.iterator();
        while (iterator.hasNext()) {
            ListenableFuture<PrestoThriftPageResult> future = iterator.next();
            if (future.isDone()) {
                iterator.remove();
                return future;
            }
        }
        throw new IllegalStateException("No completed splits in the queue");
    }

    private static void cancelQuietly(Future<?> future)
    {
        if (future != null) {
            future.cancel(true);
        }
    }

    private static final class RunningSplitContext
    {
        private final PrestoThriftService client;
        private final PrestoThriftSplit split;

        public RunningSplitContext(PrestoThriftService client, PrestoThriftSplit split)
        {
            this.client = client;
            this.split = split;
        }

        public PrestoThriftService getClient()
        {
            return client;
        }

        public PrestoThriftSplit getSplit()
        {
            return split;
        }
    }
}
