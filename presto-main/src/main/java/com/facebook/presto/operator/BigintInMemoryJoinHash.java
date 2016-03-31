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
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.util.MathUtils.estimateNumberOfHashCollisions;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public final class BigintInMemoryJoinHash
        implements LookupSource
{
    private final LongArrayList addresses;
    private final PagesHashStrategy pagesHashStrategy;

    private final int channelCount;
    private final int mask;
    private final int[] key;
    private Optional<int[]> positionLinks = Optional.empty(); // lazy initialized
    private final long totalMemoryUsage;
    private final long[] values;
    private Optional<BitSet> nullsMask = Optional.empty(); // lazy initialized

    private long hashCollisions;
    private double expectedHashCollisions;

    public BigintInMemoryJoinHash(LongArrayList addresses, PagesHashStrategy pagesHashStrategy, int hashBuildConcurrency, List<List<Block>> channels, List<Integer> joinChannels)
    {
        checkState(requireNonNull(joinChannels, "joinChannels is null").size() == 1, "Only single channel joins are supported");
        int joinChannel = joinChannels.get(0);
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        this.channelCount = pagesHashStrategy.getChannelCount();

        // reserve memory for the arrays
        int hashSize = HashCommon.arraySize(addresses.size(), 0.75f);

        // This implementation assumes arrays used in the hash are always a power of 2
        mask = hashSize - 1;
        key = new int[hashSize];
        Arrays.fill(key, -1);

        values = new long[addresses.size()];

        // First extract all values from blocks to native array.
        // Somehow having this as a separate loop is much faster compared
        // to extracting hashes on the fly in the loop below..
        extractValues(addresses, channels.get(joinChannel));

        for (int position = 0; position < addresses.size(); position++) {
            long value = values[position];
            int hashPos = getHashPosition(value, mask);

            // look for an empty slot or a slot containing this key
            int currentKey = key[hashPos];
            while (currentKey != -1) {
                long currentValue = values[currentKey];
                if (currentValue == value && bothPositionsNullOrNonNull(currentKey, position)) {
                    // found a slot for this key
                    // link the new key position to the current key position
                    setPositionLinks(position, currentKey);

                    // key[pos] updated outside of this loop
                    break;
                }
                hashPos = (hashPos + 1) & mask;
                currentKey = key[hashPos];
                hashCollisions++;
            }

            key[hashPos] = position;
        }

        totalMemoryUsage = sizeOf(key) + sizeOf(values)
                + sizeOf(addresses.elements()) + pagesHashStrategy.getSizeInBytes() / hashBuildConcurrency
                + (positionLinks.isPresent() ? sizeOf(positionLinks.get()) : 0)
                + (nullsMask.isPresent() ? nullsMask.get().size() / 8 : 0);
        expectedHashCollisions = estimateNumberOfHashCollisions(addresses.size(), hashSize);
    }

    private void setPositionLinks(int position, int currentKey)
    {
        if (!positionLinks.isPresent()) {
            positionLinks = Optional.of(new int[addresses.size()]);
            Arrays.fill(positionLinks.get(), -1);
        }

        positionLinks.get()[position] = currentKey;
    }

    private void extractValues(LongArrayList addresses, List<Block> joinBlocks)
    {
        for (int position = 0; position < addresses.size(); position++) {
            long pageAddress = addresses.getLong(Ints.checkedCast(position));
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            long value;
            if (joinBlocks.get(blockIndex).isNull(blockPosition)) {
                if (!nullsMask.isPresent()) {
                    nullsMask = Optional.of(new BitSet(addresses.size()));
                }
                nullsMask.get().set(position, true);
                value = Long.MIN_VALUE;
            }
            else {
                value = BIGINT.getLong(joinBlocks.get(blockIndex), blockPosition);
            }
            values[position] = value;
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
        return addresses.size();
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return totalMemoryUsage;
    }

    @Override
    public long getHashCollisions()
    {
        return hashCollisions;
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions;
    }

    @Override
    public long getJoinPosition(int position, Page page, int rawHash)
    {
        return getJoinPosition(position, page);
    }

    @Override
    public long getJoinPosition(int position, Page page)
    {
        long probeValue;
        boolean isNull;
        if (page.getBlock(0).isNull(position)) {
            probeValue = Long.MIN_VALUE;
            isNull = true;
        }
        else {
            probeValue = BIGINT.getLong(page.getBlock(0), position);
            isNull = false;
        }
        int hashPosition = getHashPosition(probeValue, mask);
        int buildPosition = key[hashPosition];
        while (buildPosition != -1) {
            long buildValue = values[buildPosition];

            if (buildValue == probeValue && bothNullOrNonNull(buildPosition, isNull)) {
                return buildPosition;
            }
            // increment position and mask to handler wrap around
            hashPosition = (hashPosition + 1) & mask;
            buildPosition = key[hashPosition];
        }
        return -1;
    }

    @Override
    public final long getNextJoinPosition(long currentPosition)
    {
        if (!positionLinks.isPresent()) {
            return -1;
        }
        return positionLinks.get()[Ints.checkedCast(currentPosition)];
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

    private boolean bothNullOrNonNull(int position, boolean otherIsNull)
    {
        if (!nullsMask.isPresent()) {
            return !otherIsNull;
        }
        return !(nullsMask.get().get(position) ^ otherIsNull);
    }

    private boolean bothPositionsNullOrNonNull(int position, int otherPosition)
    {
        if (!nullsMask.isPresent()) {
            return true;
        }
        return !(nullsMask.get().get(position) ^ nullsMask.get().get(otherPosition));
    }

    private static int getHashPosition(long rawHash, int mask)
    {
        return ((int) XxHash64.hash(rawHash)) & mask;
    }
}
