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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * This class performs merge of previously hash sorted pages streams.
 *
 * Positions are compared using their hash value. It is possible
 * that two distinct values to have same hash value, thus returned
 * stream of Pages can have interleaved positions with same hash value.
 */
public class MergeHashSort
{
    private MergeHashSort()
    {
    }

    public static Iterator<Page> merge(List<Type> keyTypes, List<Type> allTypes, List<Iterator<Page>> channels)
    {
        List<Iterator<PagePosition>> channelIterators = channels.stream().map(SingleChannelPagePositions::new).collect(toList());

        int[] hashChannels = new int[keyTypes.size()];
        for (int i = 0; i < keyTypes.size(); i++) {
            hashChannels[i] = i;
        }
        HashGenerator hashGenerator = new InterpretedHashGenerator(keyTypes, hashChannels);

        return new PageRewriteIterator(
                hashGenerator,
                allTypes,
                Iterators.mergeSorted(
                        channelIterators,
                        (PagePosition left, PagePosition right) -> comparePages(hashGenerator, left, right)));
    }

    private static int comparePages(HashGenerator hashGenerator, PagePosition left, PagePosition right)
    {
        if (left.isPositionOutOfPage() && right.isPositionOutOfPage()) {
            return 0;
        }
        if (left.isPositionOutOfPage()) {
            return -1;
        }
        if (right.isPositionOutOfPage()) {
            return 1;
        }

        long leftHash = hashGenerator.hashPosition(left.getPosition(), left.getPage());
        long rightHash = hashGenerator.hashPosition(right.getPosition(), right.getPage());

        return Long.compare(leftHash, rightHash);
    }

    public static class PagePosition
    {
        private final Page page;
        private final int position;

        public PagePosition(Page page, int position)
        {
            this.page = requireNonNull(page, "page is null");
            this.position = requireNonNull(position, "position is null");
        }

        public Page getPage()
        {
            return page;
        }

        public int getPosition()
        {
            return position;
        }

        public boolean isPositionOutOfPage()
        {
            return position >= page.getPositionCount();
        }
    }

    public static interface PagePositions extends Iterator<PagePosition>
    {
    }

    public static class SingleChannelPagePositions
            implements PagePositions
    {
        private final Iterator<Page> channel;
        private PagePosition current;

        public SingleChannelPagePositions(Iterator<Page> channel)
        {
            this.channel = requireNonNull(channel, "channel is null");
        }

        @Override
        public boolean hasNext()
        {
            return channel.hasNext() || (current != null && current.getPosition() + 1 < current.getPage().getPositionCount());
        }

        @Override
        public PagePosition next()
        {
            if (current == null || current.getPosition() + 1 >= current.getPage().getPositionCount()) {
                current = new PagePosition(channel.next(), 0);
            }
            else {
                current = new PagePosition(current.getPage(), current.getPosition() + 1);
            }
            return current;
        }
    }

    /**
     * This class rewrites iterator over PagePosition to iterator over Pages.
     * Positions with same hash value are guaranteed to be in the same result page.
     */
    public static class PageRewriteIterator
            implements Iterator<Page>
    {
        private final List<Type> allTypes;
        private final Iterator<PagePosition> pagePositions;
        private final HashGenerator hashGenerator;
        private final PageBuilder builder;
        private PagePosition currentPage = null;

        public PageRewriteIterator(HashGenerator hashGenerator, List<Type> allTypes, Iterator<PagePosition> pagePositions)
        {
            this.hashGenerator = hashGenerator;
            this.allTypes = allTypes;
            this.pagePositions = pagePositions;
            this.builder = new PageBuilder(allTypes);
        }

        @Override
        public boolean hasNext()
        {
            return currentPage != null || pagePositions.hasNext();
        }

        @Override
        public Page next()
        {
            builder.reset();

            if (currentPage == null) {
                currentPage = pagePositions.next();
            }

            PagePosition previousPage = currentPage;

            while (comparePages(hashGenerator, currentPage, previousPage) == 0 || !builder.isFull()) {
                if (!currentPage.isPositionOutOfPage()) {
                    builder.declarePosition();
                    for (int column = 0; column < allTypes.size(); column++) {
                        Type type = allTypes.get(column);
                        type.appendTo(currentPage.getPage().getBlock(column), currentPage.getPosition(), builder.getBlockBuilder(column));
                    }
                    previousPage = currentPage;
                }

                if (pagePositions.hasNext()) {
                    currentPage = pagePositions.next();
                }
                else {
                    currentPage = null;
                    break;
                }
            }
            return builder.build();
        }
    }
}
