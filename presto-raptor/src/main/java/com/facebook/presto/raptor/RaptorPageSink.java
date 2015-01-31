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
import com.facebook.presto.raptor.storage.StoragePageSink;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

public class RaptorPageSink
        implements ConnectorPageSink
{
    private final StoragePageSink storagePageSink;
    private final JsonCodec<ShardInfo> shardInfoCodec;
    private final int sampleWeightField;

    private final PageSorter pageSorter;
    private final List<Type> columnTypes;
    private final List<Type> sortTypes;
    private final List<Integer> sortFields;
    private final List<SortOrder> sortOrders;

    private final long maxRowCount;
    private final PageBuffer pageBuffer;

    private long rowCount;

    public RaptorPageSink(
            PageSorter pageSorter,
            StorageManager storageManager,
            JsonCodec<ShardInfo> shardInfoCodec,
            List<Long> columnIds,
            List<Type> columnTypes,
            Optional<Long> sampleWeightColumnId,
            List<Long> sortColumnIds,
            List<SortOrder> sortOrders)
    {
        this.pageSorter = checkNotNull(pageSorter, "pageSorter is null");
        this.columnTypes = ImmutableList.copyOf(checkNotNull(columnTypes, "columnTypes is null"));

        checkNotNull(storageManager, "storageManager is null");
        this.storagePageSink = storageManager.createStoragePageSink(columnIds, columnTypes);
        this.shardInfoCodec = checkNotNull(shardInfoCodec, "shardInfoCodec is null");

        checkNotNull(sampleWeightColumnId, "sampleWeightColumnId is null");
        this.sampleWeightField = columnIds.indexOf(sampleWeightColumnId.orElse(-1L));

        this.sortFields = ImmutableList.copyOf(sortColumnIds.stream().map(columnIds::indexOf).collect(toList()));
        this.sortTypes = ImmutableList.copyOf(sortFields.stream().map(columnTypes::get).collect(toList()));
        this.sortOrders = ImmutableList.copyOf(checkNotNull(sortOrders, "sortOrders is null"));

        this.maxRowCount = storageManager.getMaxRowCount();
        this.pageBuffer = new PageBuffer(storageManager.getMaxBufferSize().toBytes());

        this.rowCount = 0;
    }

    @Override
    public void appendPage(Page page, Block sampleWeightBlock)
    {
        flushPageBufferIfNecessary(page.getPositionCount());

        if (sampleWeightField >= 0) {
            page = createPageWithSampleWeightBlock(page, sampleWeightBlock);
        }

        pageBuffer.add(page);
        rowCount += page.getPositionCount();
    }

    @Override
    public Collection<Slice> commit()
    {
        flushPages(pageBuffer.getPages());
        List<ShardInfo> shards = storagePageSink.commit();

        ImmutableList.Builder<Slice> fragments = ImmutableList.builder();
        for (ShardInfo shard : shards) {
            fragments.add(Slices.wrappedBuffer(shardInfoCodec.toJsonBytes(shard)));
        }
        return fragments.build();
    }

    @Override
    public void rollback()
    {
        // TODO: clean up open resources
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

    /**
     * Flushes pages in the PageBuffer to StoragePageSink if ANY of the following is true:
     * <ul>
     * <li>rows written to the StoragePageSink >= maxRowsCount</li>
     * <li>pageBuffer has maximum allowable bytes</li>
     * <li>pageBuffer has more than Integer.MAX_VALUE rows (PagesSorter.sort can sort Integer.MAX_VALUE rows at a time)</li>
     * </ul>
     */
    private void flushPageBufferIfNecessary(int rowsToAdd)
    {
        if (rowCount >= maxRowCount) {
            // This StoragePageSink is full, flush it for the next batch of pages
            flushPages(pageBuffer.getPages());
            pageBuffer.reset();
            rowCount = 0;
            storagePageSink.flush();
            return;
        }

        int maxRemainingRows = Integer.MAX_VALUE - Ints.checkedCast(pageBuffer.getRowCount());
        if (pageBuffer.isFull() || (!sortFields.isEmpty() && (rowsToAdd > maxRemainingRows))) {
            flushPages(pageBuffer.getPages());
            pageBuffer.reset();
        }
    }

    private void flushPages(List<Page> pages)
    {
        if (pages.isEmpty()) {
            return;
        }

        if (sortFields.isEmpty()) {
            storagePageSink.appendPages(pages);
        }
        else {
            checkState(pageBuffer.getRowCount() <= Integer.MAX_VALUE);

            long[] orderedAddresses = pageSorter.sort(columnTypes, pages, sortTypes, sortFields, sortOrders, Ints.checkedCast(pageBuffer.getRowCount()));
            int[] orderedPageIndex = new int[orderedAddresses.length];
            int[] orderedPositionIndex = new int[orderedAddresses.length];
            for (int i = 0; i < orderedAddresses.length; i++) {
                orderedPageIndex[i] = pageSorter.decodePageIndex(orderedAddresses[i]);
                orderedPositionIndex[i] = pageSorter.decodePositionIndex(orderedAddresses[i]);
            }

            storagePageSink.appendPages(pages, orderedPageIndex, orderedPositionIndex);
        }
    }
}
