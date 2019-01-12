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
package com.facebook.presto.operator;

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.openjdk.jol.info.ClassLayout;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * This class finds the top N rows defined by {@param comparator} for each group specified by {@param groupByHash}.
 */
public class GroupedTopNBuilder
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(GroupedTopNBuilder.class).instanceSize();
    // compact a page when 50% of its positions are unreferenced
    private static final int COMPACT_THRESHOLD = 2;

    private final List<Type> sourceTypes;
    private final int topN;
    private final boolean produceRowNumber;
    private final GroupByHash groupByHash;

    // a map of heaps, each of which records the top N rows
    private final ObjectBigArray<RowHeap> groupedRows = new ObjectBigArray<>();
    // a list of input pages, each of which has information of which row in which heap references which position
    private final ObjectBigArray<PageReference> pageReferences = new ObjectBigArray<>();
    // for heap element comparison
    private final Comparator<Row> comparator;
    // when there is no row referenced in a page, it will be removed instead of compacted; use a list to record those empty slots to reuse them
    private final IntFIFOQueue emptyPageReferenceSlots;

    // keeps track sizes of input pages and heaps
    private long memorySizeInBytes;
    private int currentPageCount;

    public GroupedTopNBuilder(
            List<Type> sourceTypes,
            PageWithPositionComparator comparator,
            int topN,
            boolean produceRowNumber,
            GroupByHash groupByHash)
    {
        this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
        checkArgument(topN > 0, "topN must be > 0");
        this.topN = topN;
        this.produceRowNumber = produceRowNumber;
        this.groupByHash = requireNonNull(groupByHash, "groupByHash is not null");

        requireNonNull(comparator, "comparator is null");
        this.comparator = (left, right) -> comparator.compareTo(
                pageReferences.get(left.getPageId()).getPage(),
                left.getPosition(),
                pageReferences.get(right.getPageId()).getPage(),
                right.getPosition());
        this.emptyPageReferenceSlots = new IntFIFOQueue();
    }

    public Work<?> processPage(Page page)
    {
        return new TransformWork<>(
                groupByHash.getGroupIds(page),
                groupIds -> {
                    processPage(page, groupIds);
                    return null;
                });
    }

    public Iterator<Page> buildResult()
    {
        return new ResultIterator();
    }

    public long getEstimatedSizeInBytes()
    {
        return INSTANCE_SIZE +
                memorySizeInBytes +
                groupByHash.getEstimatedSize() +
                groupedRows.sizeOf() +
                pageReferences.sizeOf() +
                emptyPageReferenceSlots.getEstimatedSizeInBytes();
    }

    @VisibleForTesting
    List<Page> getBufferedPages()
    {
        return IntStream.range(0, currentPageCount)
                .filter(i -> pageReferences.get(i) != null)
                .mapToObj(i -> pageReferences.get(i).getPage())
                .collect(toImmutableList());
    }

    private void processPage(Page newPage, GroupByIdBlock groupIds)
    {
        checkArgument(newPage != null);
        checkArgument(groupIds != null);

        // save the new page
        PageReference newPageReference = new PageReference(newPage);
        memorySizeInBytes += newPageReference.getEstimatedSizeInBytes();
        int newPageId;
        if (emptyPageReferenceSlots.isEmpty()) {
            // all the previous slots are full; create a new one
            pageReferences.ensureCapacity(currentPageCount + 1);
            newPageId = currentPageCount;
            currentPageCount++;
        }
        else {
            // reuse a previously removed page's slot
            newPageId = emptyPageReferenceSlots.dequeueInt();
        }
        verify(pageReferences.get(newPageId) == null, "should not overwrite a non-empty slot");
        pageReferences.set(newPageId, newPageReference);

        // update the affected heaps and record candidate pages that need compaction
        IntSet pagesToCompact = new IntOpenHashSet();
        for (int position = 0; position < newPage.getPositionCount(); position++) {
            long groupId = groupIds.getGroupId(position);
            groupedRows.ensureCapacity(groupId + 1);

            RowHeap rows = groupedRows.get(groupId);
            if (rows == null) {
                // a new group
                rows = new RowHeap(Ordering.from(comparator).reversed());
                groupedRows.set(groupId, rows);
            }
            else {
                // update an existing group;
                // remove the memory usage for this group for now; add it back after update
                memorySizeInBytes -= rows.getEstimatedSizeInBytes();
            }

            if (rows.size() < topN) {
                // still have space for the current group
                Row row = new Row(newPageId, position);
                rows.enqueue(row);
                newPageReference.reference(row);
            }
            else {
                // may compare with the topN-th element with in the heap to decide if update is necessary
                Row previousRow = rows.first();
                Row newRow = new Row(newPageId, position);
                if (comparator.compare(newRow, previousRow) < 0) {
                    // update reference and the heap
                    rows.dequeue();
                    PageReference previousPageReference = pageReferences.get(previousRow.getPageId());
                    previousPageReference.dereference(previousRow.getPosition());
                    newPageReference.reference(newRow);
                    rows.enqueue(newRow);

                    // compact a page if it is not the current input page and the reference count is below the threshold
                    if (previousPageReference.getPage() != newPage &&
                            previousPageReference.getUsedPositionCount() * COMPACT_THRESHOLD < previousPageReference.getPage().getPositionCount()) {
                        pagesToCompact.add(previousRow.getPageId());
                    }
                }
            }

            memorySizeInBytes += rows.getEstimatedSizeInBytes();
        }

        // may compact the new page as well
        if (newPageReference.getUsedPositionCount() * COMPACT_THRESHOLD < newPage.getPositionCount()) {
            verify(!pagesToCompact.contains(newPageId));
            pagesToCompact.add(newPageId);
        }

        // compact pages
        IntIterator iterator = pagesToCompact.iterator();
        while (iterator.hasNext()) {
            int pageId = iterator.nextInt();
            PageReference pageReference = pageReferences.get(pageId);
            if (pageReference.getUsedPositionCount() == 0) {
                pageReferences.set(pageId, null);
                emptyPageReferenceSlots.enqueue(pageId);
                memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
            }
            else {
                memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
                pageReference.compact();
                memorySizeInBytes += pageReference.getEstimatedSizeInBytes();
            }
        }
    }

    /**
     * The class is a pointer to a row in a page.
     * The actual position in the page is mutable because as pages are compacted, the position will change.
     */
    private class Row
    {
        private final int pageId;
        private int position;

        private Row(int pageId, int position)
        {
            this.pageId = pageId;
            reset(position);
        }

        public void reset(int position)
        {
            this.position = position;
        }

        public int getPageId()
        {
            return pageId;
        }

        public int getPosition()
        {
            return position;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("pageId", pageId)
                    .add("position", position)
                    .toString();
        }
    }

    private static class PageReference
    {
        private static final long INSTANCE_SIZE = ClassLayout.parseClass(PageReference.class).instanceSize();

        private Page page;
        private Row[] reference;

        private int usedPositionCount;

        public PageReference(Page page)
        {
            this.page = requireNonNull(page, "page is null");
            this.reference = new Row[page.getPositionCount()];
        }

        public void reference(Row row)
        {
            int position = row.getPosition();
            reference[position] = row;
            usedPositionCount++;
        }

        public void dereference(int position)
        {
            checkArgument(reference[position] != null && usedPositionCount > 0);
            reference[position] = null;
            usedPositionCount--;
        }

        public int getUsedPositionCount()
        {
            return usedPositionCount;
        }

        public void compact()
        {
            checkState(usedPositionCount > 0);

            if (usedPositionCount == page.getPositionCount()) {
                return;
            }

            // re-assign reference
            Row[] newReference = new Row[usedPositionCount];
            int[] positions = new int[usedPositionCount];
            int index = 0;
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (reference[i] != null) {
                    newReference[index] = reference[i];
                    positions[index] = i;
                    index++;
                }
            }
            verify(index == usedPositionCount);

            // compact page
            Block[] blocks = new Block[page.getChannelCount()];
            for (int i = 0; i < page.getChannelCount(); i++) {
                Block block = page.getBlock(i);
                blocks[i] = block.copyPositions(positions, 0, usedPositionCount);
            }

            // update all the elements in the heaps that reference the current page
            for (int i = 0; i < usedPositionCount; i++) {
                // this does not change the elements in the heap;
                // it only updates the value of the elements; while keeping the same order
                newReference[i].reset(i);
            }
            page = new Page(usedPositionCount, blocks);
            reference = newReference;
        }

        public Page getPage()
        {
            return page;
        }

        public long getEstimatedSizeInBytes()
        {
            return page.getRetainedSizeInBytes() + sizeOf(reference) + INSTANCE_SIZE;
        }
    }

    // this class is for precise memory tracking
    private static class IntFIFOQueue
            extends IntArrayFIFOQueue
    {
        private static final long INSTANCE_SIZE = ClassLayout.parseClass(IntFIFOQueue.class).instanceSize();

        private long getEstimatedSizeInBytes()
        {
            return INSTANCE_SIZE + sizeOf(array);
        }
    }

    // this class is for precise memory tracking
    private static class RowHeap
            extends ObjectHeapPriorityQueue<Row>
    {
        private static final long INSTANCE_SIZE = ClassLayout.parseClass(RowHeap.class).instanceSize();
        private static final long ROW_ENTRY_SIZE = ClassLayout.parseClass(Row.class).instanceSize();

        private RowHeap(Comparator<Row> comparator)
        {
            super(1, comparator);
        }

        private long getEstimatedSizeInBytes()
        {
            return INSTANCE_SIZE + sizeOf(heap) + size() * ROW_ENTRY_SIZE;
        }
    }

    private class ResultIterator
            extends AbstractIterator<Page>
    {
        private final PageBuilder pageBuilder;
        // we may have 0 groups if there is no input page processed
        private final int groupCount = groupByHash.getGroupCount();

        private int currentGroupNumber;
        private long currentGroupSizeInBytes;

        // the row number of the current position in the group
        private int currentGroupPosition;
        // number of rows in the group
        private int currentGroupSize;

        private ObjectBigArray<Row> currentRows = nextGroupedRows();

        ResultIterator()
        {
            if (produceRowNumber) {
                pageBuilder = new PageBuilder(new ImmutableList.Builder<Type>().addAll(sourceTypes).add(BIGINT).build());
            }
            else {
                pageBuilder = new PageBuilder(sourceTypes);
            }
        }

        @Override
        protected Page computeNext()
        {
            pageBuilder.reset();
            while (!pageBuilder.isFull()) {
                if (currentRows == null) {
                    // no more groups
                    break;
                }
                if (currentGroupPosition == currentGroupSize) {
                    // the current group has produced all its rows
                    memorySizeInBytes -= currentGroupSizeInBytes;
                    currentGroupPosition = 0;
                    currentRows = nextGroupedRows();
                    continue;
                }

                Row row = currentRows.get(currentGroupPosition);
                for (int i = 0; i < sourceTypes.size(); i++) {
                    sourceTypes.get(i).appendTo(pageReferences.get(row.getPageId()).getPage().getBlock(i), row.getPosition(), pageBuilder.getBlockBuilder(i));
                }

                if (produceRowNumber) {
                    BIGINT.writeLong(pageBuilder.getBlockBuilder(sourceTypes.size()), currentGroupPosition + 1);
                }
                pageBuilder.declarePosition();
                currentGroupPosition++;

                // deference the row; no need to compact the pages but remove them if completely unused
                PageReference pageReference = pageReferences.get(row.getPageId());
                pageReference.dereference(row.getPosition());
                if (pageReference.getUsedPositionCount() == 0) {
                    pageReferences.set(row.getPageId(), null);
                    memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
                }
            }

            if (pageBuilder.isEmpty()) {
                return endOfData();
            }
            return pageBuilder.build();
        }

        private ObjectBigArray<Row> nextGroupedRows()
        {
            if (currentGroupNumber < groupCount) {
                RowHeap rows = groupedRows.get(currentGroupNumber);
                verify(rows != null && !rows.isEmpty(), "impossible to have inserted a group without a witness row");
                groupedRows.set(currentGroupNumber, null);
                currentGroupSizeInBytes = rows.getEstimatedSizeInBytes();
                currentGroupNumber++;
                currentGroupSize = rows.size();

                // sort output rows in a big array in case there are too many rows
                ObjectBigArray<Row> sortedRows = new ObjectBigArray<>();
                sortedRows.ensureCapacity(currentGroupSize);
                int index = currentGroupSize - 1;
                while (!rows.isEmpty()) {
                    sortedRows.set(index, rows.dequeue());
                    index--;
                }

                return sortedRows;
            }
            return null;
        }
    }
}
