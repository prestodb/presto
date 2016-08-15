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

import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.util.PageBuffer;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class RaptorPageSink
        implements ConnectorPageSink
{
    private final long transactionId;
    private final StorageManager storageManager;
    private final JsonCodec<ShardInfo> shardInfoCodec;
    private final int sampleWeightField;
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

    private final PageWriter pageWriter;

    public RaptorPageSink(
            PageSorter pageSorter,
            StorageManager storageManager,
            JsonCodec<ShardInfo> shardInfoCodec,
            long transactionId,
            List<Long> columnIds,
            List<Type> columnTypes,
            Optional<Long> sampleWeightColumnId,
            List<Long> sortColumnIds,
            List<SortOrder> sortOrders,
            OptionalInt bucketCount,
            List<Long> bucketColumnIds,
            Optional<RaptorColumnHandle> temporalColumnHandle,
            DataSize maxBufferSize)
    {
        this.transactionId = transactionId;
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.columnIds = ImmutableList.copyOf(requireNonNull(columnIds, "columnIds is null"));
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.shardInfoCodec = requireNonNull(shardInfoCodec, "shardInfoCodec is null");
        this.maxBufferBytes = requireNonNull(maxBufferSize, "maxBufferSize is null").toBytes();

        requireNonNull(sampleWeightColumnId, "sampleWeightColumnId is null");
        this.sampleWeightField = columnIds.indexOf(sampleWeightColumnId.orElse(-1L));

        this.sortFields = ImmutableList.copyOf(sortColumnIds.stream().map(columnIds::indexOf).collect(toList()));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));

        this.bucketCount = bucketCount;
        this.bucketFields = bucketColumnIds.stream().mapToInt(columnIds::indexOf).toArray();

        for (int field : bucketFields) {
            if (!columnTypes.get(field).equals(BIGINT)) {
                throw new PrestoException(NOT_SUPPORTED, "Bucketing is only supported for BIGINT columns");
            }
        }

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
    public CompletableFuture<?> appendPage(Page page, Block sampleWeightBlock)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }

        if (sampleWeightField >= 0) {
            page = createPageWithSampleWeightBlock(page, sampleWeightBlock);
        }

        pageWriter.appendPage(page);
        return NOT_BLOCKED;
    }

    @Override
    public Collection<Slice> finish()
    {
        List<ShardInfo> shards = new ArrayList<>();
        for (PageBuffer pageBuffer : pageWriter.getPageBuffers()) {
            pageBuffer.flush();
            shards.addAll(pageBuffer.getStoragePageSink().commit());
        }

        ImmutableList.Builder<Slice> fragments = ImmutableList.builder();
        for (ShardInfo shard : shards) {
            fragments.add(Slices.wrappedBuffer(shardInfoCodec.toJsonBytes(shard)));
        }
        return fragments.build();
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
                storageManager.createStoragePageSink(transactionId, bucketNumber, columnIds, columnTypes, true),
                columnTypes,
                sortFields,
                sortOrders,
                pageSorter);
    }

    /**
     * @return page with the sampleWeightBlock at the sampleWeightField index
     */
    private Page createPageWithSampleWeightBlock(Page page, Block sampleWeightBlock)
    {
        checkArgument(page.getPositionCount() == sampleWeightBlock.getPositionCount(), "position count of page and sampleWeightBlock must match");
        int outputChannelCount = page.getChannelCount() + 1;
        Block[] blocks = new Block[outputChannelCount];
        blocks[sampleWeightField] = sampleWeightBlock;

        int pageChannel = 0;
        for (int channel = 0; channel < outputChannelCount; channel++) {
            if (channel == sampleWeightField) {
                continue;
            }
            blocks[channel] = page.getBlock(pageChannel);
            pageChannel++;
        }
        return new Page(blocks);
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
        private final Optional<TemporalFunction> temporalFunction;
        private final Long2ObjectMap<PageStore> pageStores = new Long2ObjectOpenHashMap<>();

        public PartitionedPageWriter()
        {
            checkArgument(temporalColumnIndex.isPresent() == temporalColumnType.isPresent(),
                    "temporalColumnIndex and temporalColumnType must be both present or absent");

            this.bucketFunction = bucketCount.isPresent() ? Optional.of(new RaptorBucketFunction(bucketCount.getAsInt())) : Optional.empty();
            this.temporalFunction = temporalColumnType.map(type -> TemporalFunction.create(temporalColumnType.get()));
        }

        @Override
        public void appendPage(Page page)
        {
            Block temporalBlock = temporalColumnIndex.isPresent() ? page.getBlock(temporalColumnIndex.getAsInt()) : null;

            Page bucketArgs = bucketFunction.isPresent() ? getBucketArgsPage(page) : null;

            for (int position = 0; position < page.getPositionCount(); position++) {
                int bucket = bucketFunction.isPresent() ? bucketFunction.get().getBucket(bucketArgs, position) : 0;
                int day = temporalFunction.isPresent() ? temporalFunction.get().getDay(temporalBlock, position) : 0;

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

    private interface TemporalFunction
    {
        int getDay(Block temporalBlock, int position);

        static TemporalFunction create(Type temporalColumnType)
        {
            if (temporalColumnType.equals(DATE)) {
                return (temporalBlock, position) -> Ints.checkedCast(DATE.getLong(temporalBlock, position));
            }

            if (temporalColumnType.equals(TIMESTAMP)) {
                return (temporalBlock, position) -> Ints.checkedCast(MILLISECONDS.toDays(temporalBlock.getLong(position, 0)));
            }

            throw new IllegalArgumentException("Wrong type for temporal column: " + temporalColumnType);
        }
    }
}
