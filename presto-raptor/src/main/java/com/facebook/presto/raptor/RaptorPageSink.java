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
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
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
    private final long maxBufferBytes;

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

        this.pageWriter = new SimplePageWriter();
    }

    @Override
    public void appendPage(Page page, Block sampleWeightBlock)
    {
        if (page.getPositionCount() == 0) {
            return;
        }

        if (sampleWeightField >= 0) {
            page = createPageWithSampleWeightBlock(page, sampleWeightBlock);
        }

        pageWriter.appendPage(page);
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
                error.addSuppressed(t);
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
                storageManager.createStoragePageSink(transactionId, bucketNumber, columnIds, columnTypes),
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
}
