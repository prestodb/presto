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
package io.prestosql.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.addExact;
import static java.util.Objects.requireNonNull;

public class SortBuffer
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SortBuffer.class).instanceSize();

    private final long maxMemoryBytes;
    private final List<Type> types;
    private final List<Integer> sortFields;
    private final List<SortOrder> sortOrders;
    private final PageSorter pageSorter;
    private final List<Page> pages = new ArrayList<>();
    private final PageBuilder pageBuilder;

    private long usedMemoryBytes;
    private int rowCount;

    public SortBuffer(
            DataSize maxMemory,
            List<Type> types,
            List<Integer> sortFields,
            List<SortOrder> sortOrders,
            PageSorter pageSorter)
    {
        checkArgument(maxMemory.toBytes() > 0, "maxMemory is zero");
        this.maxMemoryBytes = maxMemory.toBytes();
        this.types = requireNonNull(types, "types is null");
        this.sortFields = ImmutableList.copyOf(requireNonNull(sortFields, "sortFields is null"));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.pageBuilder = new PageBuilder(types);
    }

    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + usedMemoryBytes;
    }

    public boolean isEmpty()
    {
        return pages.isEmpty();
    }

    public boolean canAdd(Page page)
    {
        return (usedMemoryBytes < maxMemoryBytes) &&
                ((((long) rowCount) + page.getPositionCount()) <= Integer.MAX_VALUE);
    }

    public void add(Page page)
    {
        checkState(canAdd(page), "page buffer is full");
        pages.add(page);
        usedMemoryBytes += page.getRetainedSizeInBytes();
        rowCount = addExact(rowCount, page.getPositionCount());
    }

    public static void appendPositionTo(Page page, int position, PageBuilder pageBuilder)
    {
        pageBuilder.declarePosition();
        for (int i = 0; i < page.getChannelCount(); i++) {
            Type type = pageBuilder.getType(i);
            Block block = page.getBlock(i);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
            type.appendTo(block, position, blockBuilder);
        }
    }

    public void flushTo(Consumer<Page> consumer)
    {
        checkState(!pages.isEmpty(), "page buffer is empty");

        long[] addresses = pageSorter.sort(types, pages, sortFields, sortOrders, rowCount);

        int[] pageIndex = new int[addresses.length];
        int[] positionIndex = new int[addresses.length];
        for (int i = 0; i < addresses.length; i++) {
            pageIndex[i] = pageSorter.decodePageIndex(addresses[i]);
            positionIndex[i] = pageSorter.decodePositionIndex(addresses[i]);
        }

        verify(pageBuilder.isEmpty());

        for (int i = 0; i < pageIndex.length; i++) {
            Page page = pages.get(pageIndex[i]);
            int position = positionIndex[i];
            appendPositionTo(page, position, pageBuilder);

            if (pageBuilder.isFull()) {
                consumer.accept(pageBuilder.build());
                pageBuilder.reset();
            }
        }

        if (!pageBuilder.isEmpty()) {
            consumer.accept(pageBuilder.build());
            pageBuilder.reset();
        }

        pages.clear();
        rowCount = 0;
        usedMemoryBytes = 0;
    }
}
