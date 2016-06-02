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
import com.facebook.presto.spi.block.Block;
import com.google.common.primitives.Ints;
import io.airlift.slice.XxHash64;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Arrays;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfBooleanArray;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public final class InMemoryJoinHash
        implements LookupSource
{
    private final LongArrayList addresses;
    private final PagesHashStrategy pagesHashStrategy;

    private final int channelCount;
    private final int mask;
    private final int[] key;
    private final int[] positionLinks;
    private final long size;
    private final boolean filterFunctionPresent;

    public InMemoryJoinHash(LongArrayList addresses, PagesHashStrategy pagesHashStrategy)
    {
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        this.channelCount = pagesHashStrategy.getChannelCount();
        this.filterFunctionPresent = pagesHashStrategy.getFilterFunction().isPresent();

        // reserve memory for the arrays
        int hashSize = HashCommon.arraySize(addresses.size(), 0.75f);
        size = sizeOfIntArray(hashSize) + sizeOfBooleanArray(hashSize) + sizeOfIntArray(addresses.size())
                + sizeOf(addresses.elements()) + pagesHashStrategy.getSizeInBytes();

        mask = hashSize - 1;
        key = new int[hashSize];
        Arrays.fill(key, -1);

        this.positionLinks = new int[addresses.size()];
        Arrays.fill(positionLinks, -1);

        // index pages
        for (int position = 0; position < addresses.size(); position++) {
            if (isPositionNull(position)) {
                continue;
            }

            int pos = (int) getHashPosition(hashPosition(position), mask);

            // look for an empty slot or a slot containing this key
            while (key[pos] != -1) {
                int currentKey = key[pos];
                if (positionEqualsPositionIgnoreNulls(currentKey, position)) {
                    // found a slot for this key
                    // link the new key position to the current key position
                    positionLinks[position] = currentKey;

                    // key[pos] updated outside of this loop
                    break;
                }
                // increment position and mask to handler wrap around
                pos = (pos + 1) & mask;
            }

            key[pos] = position;
        }
    }

    @Override
    public final int getChannelCount()
    {
        return channelCount;
    }

    @Override
    public int getJoinPositionCount()
    {
        return positionLinks.length;
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return size;
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
    {
        return getJoinPosition(position, hashChannelsPage, allChannelsPage, pagesHashStrategy.hashRow(position, hashChannelsPage));
    }

    @Override
    public long getJoinPosition(int rightPosition, Page hashChannelsPage, Page allChannelsPage, long rawHash)
    {
        int pos = (int) getHashPosition(rawHash, mask);

        while (key[pos] != -1) {
            if (positionEqualsCurrentRowIgnoreNulls(key[pos], rightPosition, hashChannelsPage)) {
                return getNextJoinPositionFrom(key[pos], rightPosition, allChannelsPage);
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    @Override
    public final long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        return getNextJoinPositionFrom(positionLinks[Ints.checkedCast(currentJoinPosition)], probePosition, allProbeChannelsPage);
    }

    private long getNextJoinPositionFrom(int startJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        long currentJoinPosition = startJoinPosition;
        while (filterFunctionPresent && currentJoinPosition != -1 && !applyFilterFilterFunction(Ints.checkedCast(currentJoinPosition), probePosition, allProbeChannelsPage.getBlocks())) {
            currentJoinPosition = positionLinks[Ints.checkedCast(currentJoinPosition)];
        }
        return currentJoinPosition;
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long pageAddress = addresses.getLong(Ints.checkedCast(position));
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        pagesHashStrategy.appendTo(blockIndex, blockPosition, pageBuilder, outputChannelOffset);
    }

    @Override
    public void close()
    {
    }

    private boolean isPositionNull(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.isPositionNull(blockIndex, blockPosition);
    }

    private long hashPosition(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.hashPosition(blockIndex, blockPosition);
    }

    private boolean positionEqualsCurrentRowIgnoreNulls(int leftPosition, int rightPosition, Page rightPage)
    {
        long pageAddress = addresses.getLong(leftPosition);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionEqualsRowIgnoreNulls(blockIndex, blockPosition, rightPosition, rightPage);
    }

    private boolean applyFilterFilterFunction(int leftPosition, int rightPosition, Block[] rightBlocks)
    {
        if (!filterFunctionPresent) {
            return true;
        }

        long pageAddress = addresses.getLong(leftPosition);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.applyFilterFunction(blockIndex, blockPosition, rightPosition, rightBlocks);
    }

    private boolean positionEqualsPositionIgnoreNulls(int leftPosition, int rightPosition)
    {
        long leftPageAddress = addresses.getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = addresses.getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        return pagesHashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
    }

    private static long getHashPosition(long rawHash, long mask)
    {
        return (XxHash64.hash(rawHash)) & mask;
    }
}
