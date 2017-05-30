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
package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.raptorx.storage.PageBuffer;
import com.facebook.presto.raptorx.storage.PageStore;
import com.facebook.presto.raptorx.storage.StorageManager;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.facebook.presto.raptorx.RaptorNodePartitioningProvider.getBucketFunction;
import static com.facebook.presto.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.Integer.toUnsignedLong;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RaptorPageSink
        implements ConnectorPageSink
{
    private static final JsonCodec<ChunkInfo> CHUNK_INFO_CODEC = jsonCodec(ChunkInfo.class);

    private final StorageManager storageManager;
    private final PageSorter pageSorter;
    private final long transactionId;
    private final long tableId;
    private final List<Long> columnIds;
    private final List<Type> columnTypes;
    private final List<Integer> sortFields;
    private final List<SortOrder> sortOrders;
    private final int bucketCount;
    private final int[] bucketFields;
    private final CompressionType compressionType;
    private final long maxBufferBytes;
    private final OptionalInt temporalColumnIndex;
    private final Optional<Type> temporalColumnType;

    private final PageWriter pageWriter;

    public RaptorPageSink(
            StorageManager storageManager,
            PageSorter pageSorter,
            long transactionId,
            long tableId,
            List<Long> columnIds,
            List<Type> columnTypes,
            List<Long> sortColumnIds,
            int bucketCount,
            List<Long> bucketColumnIds,
            Optional<RaptorColumnHandle> temporalColumnHandle,
            CompressionType compressionType,
            DataSize maxBufferSize)
    {
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.transactionId = transactionId;
        this.tableId = tableId;
        this.columnIds = ImmutableList.copyOf(requireNonNull(columnIds, "columnIds is null"));
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.compressionType = requireNonNull(compressionType, "compressionType is null");
        this.maxBufferBytes = requireNonNull(maxBufferSize, "maxBufferSize is null").toBytes();

        this.sortFields = sortColumnIds.stream().map(columnIds::indexOf).collect(toImmutableList());
        this.sortOrders = nCopies(sortFields.size(), ASC_NULLS_LAST);

        checkArgument(bucketCount > 0, "bucketCount must be greater than zero");
        this.bucketCount = bucketCount;
        this.bucketFields = bucketColumnIds.stream().mapToInt(columnIds::indexOf).toArray();

        if (temporalColumnHandle.isPresent() && columnIds.contains(temporalColumnHandle.get().getColumnId())) {
            temporalColumnIndex = OptionalInt.of(columnIds.indexOf(temporalColumnHandle.get().getColumnId()));
            temporalColumnType = Optional.of(columnTypes.get(temporalColumnIndex.getAsInt()));
            checkArgument(temporalColumnType.get().equals(DATE) || temporalColumnType.get().equals(TIMESTAMP),
                    "temporalColumnType can only be DATE or TIMESTAMP");
        }
        else {
            temporalColumnIndex = OptionalInt.empty();
            temporalColumnType = Optional.empty();
        }

        pageWriter = new PageWriter();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }

        pageWriter.appendPage(page);
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return toCompletableFuture(transform(allAsList(commitBuffers()), RaptorPageSink::flatten));
    }

    private List<ListenableFuture<List<Slice>>> commitBuffers()
    {
        return pageWriter.getPageBuffers().stream()
                .map(PageBuffer::commit)
                .map(future -> transform(future, RaptorPageSink::serializeChunks, directExecutor()))
                .collect(toImmutableList());
    }

    private static List<Slice> serializeChunks(List<ChunkInfo> chunks)
    {
        return chunks.stream()
                .map(CHUNK_INFO_CODEC::toJsonBytes)
                .map(Slices::wrappedBuffer)
                .collect(toImmutableList());
    }

    private static <T> List<T> flatten(List<List<T>> list)
    {
        return list.stream().flatMap(List::stream).collect(toImmutableList());
    }

    @Override
    public void abort()
    {
        pageWriter.rollback();
    }

    private PageBuffer createPageBuffer(int bucketNumber)
    {
        return new PageBuffer(
                maxBufferBytes,
                storageManager.createStoragePageSink(
                        transactionId,
                        tableId,
                        bucketNumber,
                        columnIds,
                        columnTypes,
                        compressionType),
                columnTypes,
                sortFields,
                sortOrders,
                pageSorter);
    }

    private class PageWriter
    {
        private final BucketFunction bucketFunction;
        private final Optional<TemporalFunction> temporalFunction;
        private final Long2ObjectMap<PageStore> pageStores = new Long2ObjectOpenHashMap<>();

        public PageWriter()
        {
            checkArgument(temporalColumnIndex.isPresent() == temporalColumnType.isPresent(),
                    "temporalColumnIndex and temporalColumnType must be both present or absent");

            List<Type> bucketTypes = Arrays.stream(bucketFields)
                    .mapToObj(columnTypes::get)
                    .collect(toImmutableList());

            this.bucketFunction = getBucketFunction(bucketTypes, bucketCount);
            this.temporalFunction = temporalColumnType.map(TemporalFunction::create);
        }

        public void appendPage(Page page)
        {
            Block temporalBlock = null;
            if (temporalColumnIndex.isPresent()) {
                temporalBlock = page.getBlock(temporalColumnIndex.getAsInt());
                validateTemporalBlock(temporalBlock);
            }

            Page bucketArgs = getBucketArgsPage(page);
            validateBucketingBlock(bucketArgs);

            for (int position = 0; position < page.getPositionCount(); position++) {
                int bucket = bucketFunction.getBucket(bucketArgs, position);
                int day = temporalFunction.isPresent() ? temporalFunction.get().getDay(temporalBlock, position) : 0;

                long partition = (((long) bucket) << 32) | toUnsignedLong(day);
                PageStore store = pageStores.get(partition);
                if (store == null) {
                    PageBuffer buffer = createPageBuffer(bucket);
                    store = new PageStore(buffer, columnTypes);
                    pageStores.put(partition, store);
                }

                store.appendPosition(page, position);
            }

            flushIfNecessary();
        }

        private Page getBucketArgsPage(Page page)
        {
            Block[] blocks = new Block[bucketFields.length];
            for (int i = 0; i < bucketFields.length; i++) {
                blocks[i] = page.getBlock(bucketFields[i]);
            }
            return new Page(page.getPositionCount(), blocks);
        }

        public List<PageBuffer> getPageBuffers()
        {
            ImmutableList.Builder<PageBuffer> list = ImmutableList.builder();
            for (PageStore store : pageStores.values()) {
                store.flushToPageBuffer();
                store.getPageBuffer().flush();
                list.add(store.getPageBuffer());
            }
            return list.build();
        }

        public void rollback()
        {
            RuntimeException error = new RuntimeException("Exception during rollback");
            for (PageStore store : pageStores.values()) {
                try {
                    store.getPageBuffer().rollback();
                }
                catch (Throwable t) {
                    error.addSuppressed(t);
                }
            }
            if (error.getSuppressed().length > 0) {
                throw error;
            }
        }

        private void flushIfNecessary()
        {
            long totalBytes = 0;
            long maxBytes = 0;
            PageBuffer maxBuffer = null;

            for (PageStore store : pageStores.values()) {
                long bytes = store.getUsedMemoryBytes();
                totalBytes += bytes;

                if ((maxBuffer == null) || (bytes > maxBytes)) {
                    maxBuffer = store.getPageBuffer();
                    maxBytes = bytes;
                }
            }

            if ((totalBytes > maxBufferBytes) && (maxBuffer != null)) {
                maxBuffer.flush();
            }
        }
    }

    private static void validateTemporalBlock(Block block)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                throw new PrestoException(CONSTRAINT_VIOLATION, "Temporal column cannot be null");
            }
        }
    }

    private static void validateBucketingBlock(Page page)
    {
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (block.isNull(position)) {
                    throw new PrestoException(CONSTRAINT_VIOLATION, "Bucket column cannot be null");
                }
            }
        }
    }

    private interface TemporalFunction
    {
        int getDay(Block temporalBlock, int position);

        static TemporalFunction create(Type temporalColumnType)
        {
            if (temporalColumnType.equals(DATE)) {
                return (temporalBlock, position) -> toIntExact(DATE.getLong(temporalBlock, position));
            }

            if (temporalColumnType.equals(TIMESTAMP)) {
                return (temporalBlock, position) -> toIntExact(MILLISECONDS.toDays(temporalBlock.getLong(position, 0)));
            }

            throw new PrestoException(RAPTOR_INTERNAL_ERROR, "Wrong type for temporal column: " + temporalColumnType);
        }
    }
}
