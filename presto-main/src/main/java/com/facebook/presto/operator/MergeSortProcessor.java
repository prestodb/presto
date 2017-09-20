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

import com.facebook.presto.memory.AggregatedMemoryContext;
import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class MergeSortProcessor
        implements Closeable
{
    private static final ListenableFuture<?> NOT_BLOCKED = immediateFuture(null);

    private final AggregatedMemoryContext memoryContext;
    private final List<MergeSource> mergeSources;
    private final PriorityQueue<PageWithPosition> mergeQueue;

    private int lastPooledIndex = -1;

    private ListenableFuture<?> blocked;

    public MergeSortProcessor(PageComparator comparator, List<? extends PageSupplier> pageSuppliers, AggregatedMemoryContext memoryContext)
    {
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        requireNonNull(pageSuppliers, "pageSuppliers is null");
        ImmutableList.Builder<MergeSource> mergeSources = ImmutableList.builder();
        for (int i = 0; i < pageSuppliers.size(); i++) {
            mergeSources.add(new MergeSource(i, pageSuppliers.get(i), memoryContext.newLocalMemoryContext()));
        }
        this.mergeSources = mergeSources.build();
        this.mergeQueue = new PriorityQueue<>(
                pageSuppliers.size(),
                (left, right) -> comparator.compareTo(left.getPage(), left.getPosition(), right.getPage(), right.getPosition()));
    }

    @Nullable
    public PageWithPosition poll()
    {
        if (!isQueueInitialized()) {
            List<PageWithPosition> initialRows = new ArrayList<>();
            for (MergeSource mergeSource : mergeSources) {
                PageWithPosition page = mergeSource.getPage();
                if (page == null) {
                    if (mergeSource.isFinished()) {
                        continue;
                    }
                    else {
                        return null;
                    }
                }
                initialRows.add(page);
            }
            mergeQueue.addAll(initialRows);
        }
        else {
            MergeSource mergeSource = mergeSources.get(lastPooledIndex);
            PageWithPosition page = mergeSource.getPage();
            if (page == null && !mergeSource.isFinished()) {
                return null;
            }
            if (page != null) {
                mergeQueue.add(page);
            }
        }

        PageWithPosition result = mergeQueue.poll();
        if (result == null) {
            return null;
        }
        lastPooledIndex = result.getStreamIndex();
        return result;
    }

    private boolean isQueueInitialized()
    {
        return lastPooledIndex >= 0;
    }

    public ListenableFuture<?> isBlocked()
    {
        if (blocked != null && !blocked.isDone()) {
            return blocked;
        }

        if (mergeSources.stream().allMatch(source -> source.isBlocked().isDone())) {
            return NOT_BLOCKED;
        }

        blocked = Futures.allAsList(mergeSources.stream().map(MergeSource::isBlocked).collect(Collectors.toList()));
        return blocked;
    }

    public boolean isFinished()
    {
        return mergeSources.stream().allMatch(MergeSource::isFinished);
    }

    @Override
    public void close()
    {
        memoryContext.close();
    }

    private static class MergeSource
    {
        private final int streamIndex;
        private final PageSupplier pageSupplier;
        private final LocalMemoryContext localMemoryContext;

        @Nullable
        private PageWithPosition currentPage;
        @Nullable
        private ListenableFuture<?> blocked;

        public MergeSource(int streamIndex, PageSupplier pageSupplier, LocalMemoryContext localMemoryContext)
        {
            this.streamIndex = streamIndex;
            this.pageSupplier = requireNonNull(pageSupplier, "pageSupplier is null");
            this.localMemoryContext = requireNonNull(localMemoryContext, "localMemoryContext is null");
        }

        public ListenableFuture<?> isBlocked()
        {
            if (blocked != null && !blocked.isDone()) {
                return blocked;
            }

            if (currentPage != null && !currentPage.isFinished()) {
                return NOT_BLOCKED;
            }

            blocked = pageSupplier.isBlocked();
            return blocked;
        }

        public boolean isFinished()
        {
            return pageSupplier.isFinished() && (currentPage == null || currentPage.isFinished());
        }

        @Nullable
        public PageWithPosition getPage()
        {
            if (currentPage != null && !currentPage.isFinished()) {
                return currentPage;
            }

            if (blocked == null) {
                return null;
            }

            if (!isBlocked().isDone()) {
                return null;
            }

            if (pageSupplier.isFinished()) {
                localMemoryContext.setBytes(0);
                return null;
            }

            Page page = requireNonNull(pageSupplier.pollPage(), "page is null");

            currentPage = new PageWithPosition(streamIndex, page);
            localMemoryContext.setBytes(currentPage.getPage().getRetainedSizeInBytes());
            return currentPage;
        }
    }

    public static class PageWithPosition
    {
        private final int streamIndex;
        private final Page page;
        private int position = 0;

        public PageWithPosition(int streamIndex, Page page)
        {
            this.streamIndex = streamIndex;
            this.page = requireNonNull(page, "page is null");
        }

        private int getStreamIndex()
        {
            return streamIndex;
        }

        public Page getPage()
        {
            return page;
        }

        public int getPosition()
        {
            return position;
        }

        public void incrementPosition()
        {
            checkPosition();
            position++;
        }

        public boolean isFinished()
        {
            return position == page.getPositionCount();
        }

        public void appendTo(PageBuilder pageBuilder, List<Integer> outputChannels, List<Type> outputTypes)
        {
            pageBuilder.declarePosition();
            for (int i = 0; i < outputChannels.size(); i++) {
                Type type = outputTypes.get(i);
                Block block = page.getBlock(outputChannels.get(i));
                type.appendTo(block, position, pageBuilder.getBlockBuilder(i));
            }
            // TODO add memory accounting either here or in MergeOperator
            position++;
        }

        private void checkPosition()
        {
            int positionCount = page.getPositionCount();
            checkState(position < positionCount, "Invalid position: %d of %d", position, positionCount);
        }
    }
}
