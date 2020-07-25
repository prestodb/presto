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
package com.facebook.presto.raptor;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.organization.TemporalFunction;
import com.facebook.presto.raptor.util.PageBuffer;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
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

import static com.facebook.airlift.concurrent.MoreFutures.allAsList;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_TOO_MANY_FILES_CREATED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RaptorPageSink
        implements ConnectorPageSink
{
    private static final JsonCodec<ShardInfo> SHARD_INFO_CODEC = jsonCodec(ShardInfo.class);

    private final long transactionId;
    private final StorageManager storageManager;
    private final PageSorter pageSorter;
    private final List<Long> columnIds;
    private final List<Type> columnTypes;
    private final List<Integer> sortFields;
    private final List<SortOrder> sortOrders;
    private final OptionalInt bucketCount;
    private final int[] bucketFields;
    private final long maxBufferBytes;
    private final OptionalInt temporalColumnIndex;
    private final Optional<Type> temporalColumnType;
    private final TemporalFunction temporalFunction;
    private final int maxAllowedFilesPerWriter;
    private final HdfsContext context;

    private final PageWriter pageWriter;

    public RaptorPageSink(
            HdfsContext context,
            PageSorter pageSorter,
            StorageManager storageManager,
            TemporalFunction temporalFunction,
            long transactionId,
            List<Long> columnIds,
            List<Type> columnTypes,
            List<Long> sortColumnIds,
            List<SortOrder> sortOrders,
            OptionalInt bucketCount,
            List<Long> bucketColumnIds,
            Optional<RaptorColumnHandle> temporalColumnHandle,
            DataSize maxBufferSize,
            int maxAllowedFilesPerWriter)
    {
        this.transactionId = transactionId;
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.temporalFunction = requireNonNull(temporalFunction, "temporalFunction is null");
        this.columnIds = ImmutableList.copyOf(requireNonNull(columnIds, "columnIds is null"));
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.maxBufferBytes = requireNonNull(maxBufferSize, "maxBufferSize is null").toBytes();
        this.maxAllowedFilesPerWriter = maxAllowedFilesPerWriter;

        this.sortFields = ImmutableList.copyOf(sortColumnIds.stream().map(columnIds::indexOf).collect(toList()));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));

        this.bucketCount = bucketCount;
        this.bucketFields = bucketColumnIds.stream().mapToInt(columnIds::indexOf).toArray();
        this.context = requireNonNull(context, "context is null");

        if (temporalColumnHandle.isPresent() && columnIds.contains(temporalColumnHandle.get().getColumnId())) {
            temporalColumnIndex = OptionalInt.of(columnIds.indexOf(temporalColumnHandle.get().getColumnId()));
            temporalColumnType = Optional.of(columnTypes.get(temporalColumnIndex.getAsInt()));
            checkArgument(temporalColumnType.get() == DATE || temporalColumnType.get() == TIMESTAMP,
                    "temporalColumnType can only be DATE or TIMESTAMP");
        }
        else {
            temporalColumnIndex = OptionalInt.empty();
            temporalColumnType = Optional.empty();
        }

        this.pageWriter = (bucketCount.isPresent() || temporalColumnIndex.isPresent()) ? new PartitionedPageWriter() : new SimplePageWriter();
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
        List<CompletableFuture<? extends List<Slice>>> futureSlices = pageWriter.getPageBuffers().stream().map(pageBuffer -> {
            pageBuffer.flush();
            CompletableFuture<List<ShardInfo>> futureShards = pageBuffer.getStoragePageSink().commit();
            return futureShards.thenApply(shards -> shards.stream()
                    .map(shard -> Slices.wrappedBuffer(SHARD_INFO_CODEC.toJsonBytes(shard)))
                    .collect(toList()));
        }).collect(toList());

        return allAsList(futureSlices).thenApply(lists -> lists.stream()
                .flatMap(Collection::stream)
                .collect(toList()));
    }

    @Override
    public void abort()
    {
        RuntimeException error = new RuntimeException("Exception during rollback");
        for (PageBuffer pageBuffer : pageWriter.getPageBuffers()) {
            try {
                pageBuffer.getStoragePageSink().rollback();
            }
            catch (Throwable t) {
                // Self-suppression not permitted
                if (error != t) {
                    error.addSuppressed(t);
                }
            }
        }
        if (error.getSuppressed().length > 0) {
            throw error;
        }
    }

    private PageBuffer createPageBuffer(OptionalInt bucketNumber)
    {
        return new PageBuffer(
                maxBufferBytes,
                storageManager.createStoragePageSink(context, transactionId, bucketNumber, columnIds, columnTypes, true),
                columnTypes,
                sortFields,
                sortOrders,
                pageSorter);
    }

    private interface PageWriter
    {
        void appendPage(Page page);

        List<PageBuffer> getPageBuffers();
    }

    private class SimplePageWriter
            implements PageWriter
    {
        private final PageBuffer pageBuffer = createPageBuffer(OptionalInt.empty());

        @Override
        public void appendPage(Page page)
        {
            pageBuffer.add(page);
        }

        @Override
        public List<PageBuffer> getPageBuffers()
        {
            return ImmutableList.of(pageBuffer);
        }
    }

    private class PartitionedPageWriter
            implements PageWriter
    {
        private final Optional<BucketFunction> bucketFunction;
        private final Long2ObjectMap<PageStore> pageStores = new Long2ObjectOpenHashMap<>();

        public PartitionedPageWriter()
        {
            checkArgument(temporalColumnIndex.isPresent() == temporalColumnType.isPresent(),
                    "temporalColumnIndex and temporalColumnType must be both present or absent");

            List<Type> bucketTypes = Arrays.stream(bucketFields)
                    .mapToObj(columnTypes::get)
                    .collect(toList());

            this.bucketFunction = bucketCount.isPresent() ? Optional.of(new RaptorBucketFunction(bucketCount.getAsInt(), bucketTypes)) : Optional.empty();
        }

        @Override
        public void appendPage(Page page)
        {
            Block temporalBlock = temporalColumnIndex.isPresent() ? page.getBlock(temporalColumnIndex.getAsInt()) : null;

            Page bucketArgs = bucketFunction.isPresent() ? getBucketArgsPage(page) : null;

            for (int position = 0; position < page.getPositionCount(); position++) {
                int bucket = bucketFunction.isPresent() ? bucketFunction.get().getBucket(bucketArgs, position) : 0;
                int day = temporalColumnType.isPresent() ? temporalFunction.getDay(temporalColumnType.get(), temporalBlock, position) : 0;

                long partition = (((long) bucket) << 32) | (day & 0xFFFF_FFFFL);
                PageStore store = pageStores.get(partition);
                if (store == null) {
                    OptionalInt bucketNumber = bucketFunction.isPresent() ? OptionalInt.of(bucket) : OptionalInt.empty();
                    PageBuffer buffer = createPageBuffer(bucketNumber);
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

        @Override
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

        private void flushIfNecessary()
        {
            long totalBytes = 0;
            long maxBytes = 0;
            PageBuffer maxBuffer = null;

            if (pageStores.size() > maxAllowedFilesPerWriter) {
                throw new PrestoException(RAPTOR_TOO_MANY_FILES_CREATED, format("Number of files created: %s , has exceeded the limit of %s files created per worker per query", pageStores.size(), maxAllowedFilesPerWriter));
            }

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

    private static class PageStore
    {
        private final PageBuffer pageBuffer;
        private final PageBuilder pageBuilder;

        public PageStore(PageBuffer pageBuffer, List<Type> columnTypes)
        {
            this.pageBuffer = requireNonNull(pageBuffer, "pageBuffer is null");
            this.pageBuilder = new PageBuilder(columnTypes);
        }

        public long getUsedMemoryBytes()
        {
            return pageBuilder.getSizeInBytes() + pageBuffer.getUsedMemoryBytes();
        }

        public PageBuffer getPageBuffer()
        {
            return pageBuffer;
        }

        public void appendPosition(Page page, int position)
        {
            pageBuilder.declarePosition();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
                pageBuilder.getType(channel).appendTo(block, position, blockBuilder);
            }

            if (pageBuilder.isFull()) {
                flushToPageBuffer();
            }
        }

        public void flushToPageBuffer()
        {
            if (!pageBuilder.isEmpty()) {
                pageBuffer.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
    }
}
