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
import com.facebook.presto.spi.block.array.IntArray;
import com.facebook.presto.spi.block.array.LongArrayList;
import com.facebook.presto.spi.block.resource.BlockResourceContext;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.HashCommon;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public final class InMemoryJoinHash
        implements LookupSource
{
    private static final DataSize CACHE_SIZE = new DataSize(128, KILOBYTE);
    private final LongArrayList addresses;
    private final PagesHashStrategy pagesHashStrategy;
    private final int channelCount;
    private final int mask;
    private final IntArray key;
    private final IntArray positionLinks;
    private final long size;
    private final boolean filterFunctionPresent;
    // Native array of hashes for faster collisions resolution compared
    // to accessing values in blocks. We use bytes to reduce memory foot print
    // and there is no performance gain from storing full hashes
    private final Slice positionToHashes;

    public InMemoryJoinHash(LongArrayList addresses, PagesHashStrategy pagesHashStrategy)
    {
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        this.channelCount = pagesHashStrategy.getChannelCount();
        this.filterFunctionPresent = pagesHashStrategy.getFilterFunction().isPresent();
        BlockResourceContext blockResourceContext = addresses.getBlockResourceContext();
        // reserve memory for the arrays
        int hashSize = HashCommon.arraySize(addresses.size(), 0.75f);
        mask = hashSize - 1;
        key = blockResourceContext.newIntArray(hashSize);
        key.fill(-1);
        this.positionLinks = blockResourceContext.newIntArray(addresses.size());
        positionLinks.fill(-1);
        positionToHashes = blockResourceContext.newSlice(addresses.size());
        // We will process addresses in batches, to save memory on array of hashes.
        int positionsInStep = Math.min(addresses.size() + 1, (int) CACHE_SIZE.toBytes() / Integer.SIZE);
        long[] positionToFullHashes = new long[positionsInStep];
        for (int step = 0; step * positionsInStep <= addresses.size(); step++) {
            int stepBeginPosition = step * positionsInStep;
            int stepEndPosition = Math.min((step + 1) * positionsInStep, addresses.size());
            int stepSize = stepEndPosition - stepBeginPosition;
            // First extract all hashes from blocks to native array.
            // Somehow having this as a separate loop is much faster compared
            // to extracting hashes on the fly in the loop below.
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                long hash = readHashPosition(realPosition);
                positionToFullHashes[position] = hash;
                positionToHashes.setByte(realPosition, (byte) hash);
            }
            // index pages
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                if (isPositionNull(realPosition)) {
                    continue;
                }
                long hash = positionToFullHashes[position];
                int pos = getHashPosition(hash, mask);
                // look for an empty slot or a slot containing this key
                while (key.get(pos) != -1) {
                    int currentKey = key.get(pos);
                    if (((byte) hash) == positionToHashes.getByte(currentKey) && positionEqualsPositionIgnoreNulls(currentKey, realPosition)) {
                        // found a slot for this key
                        // link the new key position to the current key position
                        positionLinks.set(realPosition, currentKey);
                        // key[pos] updated outside of this loop
                        break;
                    }
                    // increment position and mask to handler wrap around
                    pos = (pos + 1) & mask;
                }
                key.set(pos, realPosition);
            }
        }
        size = addresses.sizeOf() + pagesHashStrategy.getSizeInBytes() + key.getRetainedSize() + positionLinks.getRetainedSize() + positionToHashes.getRetainedSize();
    }

    @Override
    public final int getChannelCount()
    {
        return channelCount;
    }

    @Override
    public int getJoinPositionCount()
    {
        return positionLinks.length();
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
        int pos = getHashPosition(rawHash, mask);
        while (key.get(pos) != -1) {
            if (positionEqualsCurrentRowIgnoreNulls(key.get(pos), (byte) rawHash, rightPosition, hashChannelsPage)) {
                return getNextJoinPositionFrom(key.get(pos), rightPosition, allChannelsPage);
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    @Override
    public final long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        return getNextJoinPositionFrom(positionLinks.get(Ints.checkedCast(currentJoinPosition)), probePosition, allProbeChannelsPage);
    }

    private long getNextJoinPositionFrom(int startJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        long currentJoinPosition = startJoinPosition;
        while (filterFunctionPresent && currentJoinPosition != -1 && !applyFilterFilterFunction(Ints.checkedCast(currentJoinPosition), probePosition, allProbeChannelsPage.getBlocks())) {
            currentJoinPosition = positionLinks.get(Ints.checkedCast(currentJoinPosition));
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
    {}

    private boolean isPositionNull(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);
        return pagesHashStrategy.isPositionNull(blockIndex, blockPosition);
    }

    private long readHashPosition(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);
        return pagesHashStrategy.hashPosition(blockIndex, blockPosition);
    }

    private boolean positionEqualsCurrentRowIgnoreNulls(int leftPosition, byte rawHash, int rightPosition, Page rightPage)
    {
        if (positionToHashes.getByte(leftPosition) != rawHash) {
            return false;
        }
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

    private static int getHashPosition(long rawHash, long mask)
    {
        // Avalanches the bits of a long integer by applying the finalisation step of MurmurHash3.
        //
        // This function implements the finalisation step of Austin Appleby's <a href="http://sites.google.com/site/murmurhash/">MurmurHash3</a>.
        // Its purpose is to avalanche the bits of the argument to within 0.25% bias. It is used, among other things, to scramble quickly (but deeply) the hash
        // values returned by {@link Object#hashCode()}.
        //
        rawHash ^= rawHash >>> 33;
        rawHash *= 0xff51afd7ed558ccdL;
        rawHash ^= rawHash >>> 33;
        rawHash *= 0xc4ceb9fe1a85ec53L;
        rawHash ^= rawHash >>> 33;
        return (int) (rawHash & mask);
    }
}
