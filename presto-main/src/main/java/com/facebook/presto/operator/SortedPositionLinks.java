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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.array.AdaptiveLongBigArray;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import org.openjdk.jol.info.ClassLayout;

import java.util.Iterator;
import java.util.List;
import java.util.function.IntFunction;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * Maintains position links in sorted order by build side expression.
 * Then iteration over position links uses set of @{code searchFunctions} which needs to be compatible
 * with expression used for sorting.
 * The binary search is used to quickly skip positions which would not match filter function from join condition.
 */
public final class SortedPositionLinks
        implements PositionLinks
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SortedPositionLinks.class).instanceSize();

    public static class FactoryBuilder
            implements PositionLinks.FactoryBuilder
    {
        // Cache lambda instance for use with createIfAbsent that ensures the method resolves to computeIfAbsent(int, IntFunction<T>)
        // instead of computeIfAbsent(int, Int2ObjectFunction<T>) which does (slightly) more work internally
        private static final IntFunction<IntArrayList> NEW_INT_LIST = (ignored) -> new IntArrayList();

        private final Int2ObjectOpenHashMap<IntArrayList> positionLinks;
        private final int size;
        private final PositionComparator comparator;
        private final PagesHashStrategy pagesHashStrategy;
        private final AdaptiveLongBigArray addresses;

        public FactoryBuilder(int size, PagesHashStrategy pagesHashStrategy, AdaptiveLongBigArray addresses)
        {
            this.size = size;
            this.comparator = new PositionComparator(pagesHashStrategy, addresses);
            this.pagesHashStrategy = pagesHashStrategy;
            this.addresses = addresses;
            positionLinks = new Int2ObjectOpenHashMap<>();
        }

        @Override
        public int link(int from, int to)
        {
            // don't add _from_ row to chain if its sort channel value is null
            if (isNull(from)) {
                // _to_ row sort channel value might be null. However, in such
                // case it will be the only element in the chain, so sorted position
                // links enumeration will produce correct results.
                return to;
            }

            // don't add _to_ row to chain if its sort channel value is null
            if (isNull(to)) {
                return from;
            }

            // make sure that from value is the smaller one
            if (comparator.compare(from, to) > 0) {
                // _from_ is larger so, just add to current chain _to_
                positionLinks.computeIfAbsent(to, NEW_INT_LIST).add(from);
                return to;
            }
            else {
                // _to_ is larger so, move the chain to _from_
                IntArrayList links = positionLinks.remove(to);
                if (links == null) {
                    links = new IntArrayList();
                }
                links.add(to);
                checkState(positionLinks.put(from, links) == null, "sorted links is corrupted");
                return from;
            }
        }

        private boolean isNull(int position)
        {
            long pageAddress = addresses.get(position);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);
            return pagesHashStrategy.isSortChannelPositionNull(blockIndex, blockPosition);
        }

        @Override
        public Factory build()
        {
            ArrayPositionLinks.FactoryBuilder arrayPositionLinksFactoryBuilder = ArrayPositionLinks.builder(size);
            int[][] sortedPositionLinks = new int[size][];

            Iterator<Int2ObjectMap.Entry<IntArrayList>> iterator = positionLinks.int2ObjectEntrySet().fastIterator();
            while (iterator.hasNext()) {
                Int2ObjectOpenHashMap.Entry<IntArrayList> entry = iterator.next();
                int key = entry.getIntKey();
                IntArrayList positionsList = entry.getValue();

                int[] positions = positionsList.toIntArray();
                sortedPositionLinks[key] = positions;

                if (positions.length > 0) {
                    // Use the positionsList array for the merge sort temporary work buffer to avoid an extra redundant
                    // copy. This works because we know that initially it has the same values as the array being sorted
                    IntArrays.mergeSort(positions, 0, positions.length, comparator, positionsList.elements());
                    // add link from starting position to position links chain
                    arrayPositionLinksFactoryBuilder.link(key, positions[0]);
                    // add links for the sorted internal elements
                    for (int i = 0; i < positions.length - 1; i++) {
                        arrayPositionLinksFactoryBuilder.link(positions[i], positions[i + 1]);
                    }
                }
            }

            return createFactory(sortedPositionLinks, arrayPositionLinksFactoryBuilder.build());
        }

        @Override
        public boolean isEmpty()
        {
            return positionLinks.isEmpty();
        }

        // Separate static method to avoid embedding references to "this"
        private static Factory createFactory(int[][] sortedPositionLinks, Factory arrayPositionLinksFactory)
        {
            requireNonNull(sortedPositionLinks, "sortedPositionLinks is null");
            requireNonNull(arrayPositionLinksFactory, "arrayPositionLinksFactory is null");
            return new Factory()
            {
                @Override
                public PositionLinks create(List<JoinFilterFunction> searchFunctions)
                {
                    return new SortedPositionLinks(
                            arrayPositionLinksFactory.create(ImmutableList.of()),
                            sortedPositionLinks,
                            searchFunctions);
                }

                @Override
                public long checksum()
                {
                    // For spill/unspill state restoration, sorted position links do not matter
                    return arrayPositionLinksFactory.checksum();
                }
            };
        }
    }

    private final PositionLinks positionLinks;
    private final int[][] sortedPositionLinks;
    private final long sizeInBytes;
    private final JoinFilterFunction[] searchFunctions;

    private SortedPositionLinks(PositionLinks positionLinks, int[][] sortedPositionLinks, List<JoinFilterFunction> searchFunctions)
    {
        this.positionLinks = requireNonNull(positionLinks, "positionLinks is null");
        this.sortedPositionLinks = requireNonNull(sortedPositionLinks, "sortedPositionLinks is null");
        this.sizeInBytes = INSTANCE_SIZE + positionLinks.getSizeInBytes() + sizeOfPositionLinks(sortedPositionLinks);
        requireNonNull(searchFunctions, "searchFunctions is null");
        checkState(!searchFunctions.isEmpty(), "Using sortedPositionLinks with no search functions");
        this.searchFunctions = searchFunctions.toArray(new JoinFilterFunction[0]);
    }

    private static long sizeOfPositionLinks(int[][] sortedPositionLinks)
    {
        long retainedSize = sizeOf(sortedPositionLinks);
        for (int[] element : sortedPositionLinks) {
            retainedSize += sizeOf(element);
        }
        return retainedSize;
    }

    public static FactoryBuilder builder(int size, PagesHashStrategy pagesHashStrategy, AdaptiveLongBigArray addresses)
    {
        return new FactoryBuilder(size, pagesHashStrategy, addresses);
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public int next(int position, int probePosition, Page allProbeChannelsPage)
    {
        int nextPosition = positionLinks.next(position, probePosition, allProbeChannelsPage);
        if (nextPosition < 0) {
            return -1;
        }
        if (!applyAllSearchFunctions(nextPosition, probePosition, allProbeChannelsPage)) {
            // break a position links chain if next position should be filtered out
            return -1;
        }
        return nextPosition;
    }

    @Override
    public int start(int startingPosition, int probePosition, Page allProbeChannelsPage)
    {
        if (applyAllSearchFunctions(startingPosition, probePosition, allProbeChannelsPage)) {
            return startingPosition;
        }
        int[] links = sortedPositionLinks[startingPosition];
        if (links == null) {
            return -1;
        }
        int currentStartOffset = 0;
        for (JoinFilterFunction searchFunction : searchFunctions) {
            currentStartOffset = findStartPositionForFunction(searchFunction, links, currentStartOffset, probePosition, allProbeChannelsPage);
            // return as soon as a mismatch is found, since we are handling only AND predicates (conjuncts)
            if (currentStartOffset == -1) {
                return -1;
            }
        }
        return links[currentStartOffset];
    }

    private boolean applyAllSearchFunctions(int buildPosition, int probePosition, Page allProbeChannelsPage)
    {
        for (JoinFilterFunction searchFunction : searchFunctions) {
            if (!applySearchFunction(searchFunction, buildPosition, probePosition, allProbeChannelsPage)) {
                return false;
            }
        }
        return true;
    }

    private static int findStartPositionForFunction(JoinFilterFunction searchFunction, int[] links, int startOffset, int probePosition, Page allProbeChannelsPage)
    {
        if (applySearchFunction(searchFunction, links, startOffset, probePosition, allProbeChannelsPage)) {
            // MAJOR HACK: if searchFunction is of shape `f(probe) > build_symbol` it is not fit for binary search below,
            // but it does not imply extra constraints on start position; so we just ignore it.
            // It does not break logic for `f(probe) < build_symbol` as the binary search below would return same value.

            // todo: Explicitly handle less-than and greater-than functions separately.
            return startOffset;
        }

        // do a binary search for the first position for which filter function applies
        int offset = lowerBound(searchFunction, links, startOffset, links.length - 1, probePosition, allProbeChannelsPage);
        if (!applySearchFunction(searchFunction, links, offset, probePosition, allProbeChannelsPage)) {
            return -1;
        }
        return offset;
    }

    /**
     * Find the first element in position links that is NOT smaller than probePosition
     */
    private static int lowerBound(JoinFilterFunction searchFunction, int[] links, int first, int last, int probePosition, Page allProbeChannelsPage)
    {
        int middle;
        int step;
        int count = last - first;
        while (count > 0) {
            step = count / 2;
            middle = first + step;
            if (!applySearchFunction(searchFunction, links, middle, probePosition, allProbeChannelsPage)) {
                first = ++middle;
                count -= step + 1;
            }
            else {
                count = step;
            }
        }
        return first;
    }

    private static boolean applySearchFunction(JoinFilterFunction searchFunction, int[] links, int linkOffset, int probePosition, Page allProbeChannelsPage)
    {
        return searchFunction.filter(links[linkOffset], probePosition, allProbeChannelsPage);
    }

    private static boolean applySearchFunction(JoinFilterFunction searchFunction, int buildPosition, int probePosition, Page allProbeChannelsPage)
    {
        return searchFunction.filter(buildPosition, probePosition, allProbeChannelsPage);
    }

    private static final class PositionComparator
            implements IntComparator
    {
        private final PagesHashStrategy pagesHashStrategy;
        private final AdaptiveLongBigArray addresses;

        PositionComparator(PagesHashStrategy pagesHashStrategy, AdaptiveLongBigArray addresses)
        {
            this.pagesHashStrategy = pagesHashStrategy;
            this.addresses = addresses;
        }

        @Override
        public int compare(int leftPosition, int rightPosition)
        {
            long leftPageAddress = addresses.get(leftPosition);
            int leftBlockIndex = decodeSliceIndex(leftPageAddress);
            int leftBlockPosition = decodePosition(leftPageAddress);

            long rightPageAddress = addresses.get(rightPosition);
            int rightBlockIndex = decodeSliceIndex(rightPageAddress);
            int rightBlockPosition = decodePosition(rightPageAddress);

            return pagesHashStrategy.compareSortChannelPositions(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
        }

        @Override
        public int compare(Integer leftPosition, Integer rightPosition)
        {
            return compare(leftPosition.intValue(), rightPosition.intValue());
        }
    }
}
