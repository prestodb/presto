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
package com.facebook.presto.hive.util;

import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.hive.util.SortBuffer.appendPositionTo;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.mergeSorted;
import static com.google.common.collect.Iterators.transform;
import static java.util.Comparator.naturalOrder;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class MergingPageIterator
        extends AbstractIterator<Page>
{
    private final List<Type> types;
    private final List<Integer> sortFields;
    private final List<SortOrder> sortOrders;
    private final PageBuilder pageBuilder;
    private final Iterator<PagePosition> pagePositions;

    public MergingPageIterator(
            Collection<Iterator<Page>> iterators,
            List<Type> types,
            List<Integer> sortFields,
            List<SortOrder> sortOrders)
    {
        requireNonNull(sortFields, "sortFields is null");
        requireNonNull(sortOrders, "sortOrders is null");
        checkArgument(sortFields.size() == sortOrders.size(), "sortFields and sortOrders size must match");

        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.sortFields = ImmutableList.copyOf(sortFields);
        this.sortOrders = ImmutableList.copyOf(sortOrders);
        this.pageBuilder = new PageBuilder(types);
        this.pagePositions = mergeSorted(
                iterators.stream()
                        .map(pages -> concat(transform(pages, PagePositionIterator::new)))
                        .collect(toList()),
                naturalOrder());
    }

    @Override
    protected Page computeNext()
    {
        while (!pageBuilder.isFull() && pagePositions.hasNext()) {
            pagePositions.next().appendTo(pageBuilder);
        }

        if (pageBuilder.isEmpty()) {
            return endOfData();
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private class PagePositionIterator
            extends AbstractIterator<PagePosition>
    {
        private final Page page;
        private int position = -1;

        private PagePositionIterator(Page page)
        {
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        protected PagePosition computeNext()
        {
            position++;
            if (position == page.getPositionCount()) {
                return endOfData();
            }
            return new PagePosition(page, position);
        }
    }

    @SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
    private class PagePosition
            implements Comparable<PagePosition>
    {
        private final Page page;
        private final int position;

        public PagePosition(Page page, int position)
        {
            this.page = requireNonNull(page, "page is null");
            this.position = position;
        }

        public void appendTo(PageBuilder pageBuilder)
        {
            appendPositionTo(page, position, pageBuilder);
        }

        @Override
        public int compareTo(PagePosition other)
        {
            for (int i = 0; i < sortFields.size(); i++) {
                int channel = sortFields.get(i);
                SortOrder order = sortOrders.get(i);
                Type type = types.get(channel);

                Block block = page.getBlock(channel);
                Block otherBlock = other.page.getBlock(channel);

                int result;
                try {
                    result = order.compareBlockValue(type, block, position, otherBlock, other.position);
                }
                catch (NotSupportedException e) {
                    throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
                }

                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }
    }
}
