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

import com.facebook.presto.raptor.storage.StorageManager;
import com.facebook.presto.raptor.storage.StorageOutputHandle;
import com.facebook.presto.raptor.storage.StoragePageSink;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.stream.Collectors.toList;

public class RaptorPageSink
        implements ConnectorPageSink
{
    private final String nodeId;
    private final StorageManager storageManager;
    private final StorageOutputHandle storageOutputHandle;
    private final int sampleWeightField;

    private final PageSorter pageSorter;
    private final List<Type> columnTypes;
    private final List<Type> sortTypes;
    private final List<Integer> sortFields;
    private final List<SortOrder> sortOrders;

    private final long maxRowCount;
    private final PageBuffer pageBuffer;

    private StoragePageSink storagePageSink;
    private long rowCount;

    public RaptorPageSink(
            String nodeId,
            PageSorter pageSorter,
            StorageManager storageManager,
            List<Long> columnIds,
            List<Type> columnTypes,
            Optional<Long> sampleWeightColumnId,
            List<Long> sortColumnIds,
            List<SortOrder> sortOrders)
    {
        this.nodeId = checkNotNull(nodeId, "nodeId is null");
        this.pageSorter = checkNotNull(pageSorter, "pageSorter is null");
        this.columnTypes = ImmutableList.copyOf(checkNotNull(columnTypes, "columnTypes is null"));

        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.storageOutputHandle = storageManager.createStorageOutputHandle(columnIds, columnTypes);

        checkNotNull(sampleWeightColumnId, "sampleWeightColumnId is null");
        this.sampleWeightField = columnIds.indexOf(sampleWeightColumnId.orElse(-1L));

        this.sortFields = ImmutableList.copyOf(sortColumnIds.stream().map(columnIds::indexOf).collect(toList()));
        this.sortTypes = ImmutableList.copyOf(sortFields.stream().map(columnTypes::get).collect(toList()));
        this.sortOrders = ImmutableList.copyOf(checkNotNull(sortOrders, "sortOrders is null"));

        this.maxRowCount = storageManager.getMaxRowCount();
        this.pageBuffer = new PageBuffer(storageManager.getMaxBufferSize().toBytes());

        this.storagePageSink = createStoragePageSink(storageManager, storageOutputHandle);
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
        storagePageSink.close();
        List<UUID> shardUuids = storageManager.commit(storageOutputHandle);

        // Format of each fragment: nodeId:shardUuid
        return shardUuids.stream()
                .map(shardUuid -> utf8Slice(nodeId + ":" + shardUuid))
                .collect(toList());
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
            // This StoragePageSink is full, create a new one for the next batch of pages
            flushPages(pageBuffer.getPages());
            pageBuffer.reset();
            rowCount = 0;
            storagePageSink.close();
            storagePageSink = createStoragePageSink(storageManager, storageOutputHandle);
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

    private static StoragePageSink createStoragePageSink(StorageManager storageManager, StorageOutputHandle storageOutputHandle)
    {
        return storageManager.createStoragePageSink(storageOutputHandle);
    }

    // This code is duplicated from SyntheticAddress
    public static int decodeSliceIndex(long sliceAddress)
    {
        return ((int) (sliceAddress >> 32));
    }

    public static int decodePosition(long sliceAddress)
    {
        // low order bits contain the raw offset, so a simple cast here will suffice
        return (int) sliceAddress;
    }
}
