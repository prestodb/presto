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

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.Murmur3;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.sizeOfIntArray;

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
    private final List<Type> hashTypes;

    public InMemoryJoinHash(LongArrayList addresses, List<Type> hashTypes, PagesHashStrategy pagesHashStrategy, OperatorContext operatorContext)
    {
        this.addresses = checkNotNull(addresses, "addresses is null");
        this.hashTypes = ImmutableList.copyOf(checkNotNull(hashTypes, "hashTypes is null"));
        this.pagesHashStrategy = checkNotNull(pagesHashStrategy, "pagesHashStrategy is null");
        this.channelCount = pagesHashStrategy.getChannelCount();

        checkNotNull(operatorContext, "operatorContext is null");

        // reserve memory for the arrays
        int hashSize = HashCommon.arraySize(addresses.size(), 0.75f);
        operatorContext.reserveMemory(sizeOfIntArray(hashSize) + sizeOfIntArray(addresses.size()));

        mask = hashSize - 1;
        key = new int[hashSize];
        Arrays.fill(key, -1);

        this.positionLinks = new int[addresses.size()];
        Arrays.fill(positionLinks, -1);

        // index pages
        for (int position = 0; position < addresses.size(); position++) {
            int pos = ((int) Murmur3.hash64(hashPosition(position))) & mask;

            // look for an empty slot or a slot containing this key
            while (key[pos] != -1) {
                int currentKey = key[pos];
                if (positionEqualsPosition(currentKey, position)) {
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
    public long getJoinPosition(int position, Block... blocks)
    {
        int pos = ((int) Murmur3.hash64(pagesHashStrategy.hashRow(position, blocks))) & mask;

        while (key[pos] != -1) {
            if (positionEqualsCurrentRow(key[pos], position, blocks)) {
                return key[pos];
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    @Override
    public final long getNextJoinPosition(long currentPosition)
    {
        return positionLinks[Ints.checkedCast(currentPosition)];
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

    private int hashPosition(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.hashPosition(blockIndex, blockPosition);
    }

    private boolean positionEqualsCurrentRow(int leftPosition, int rightPosition, Block... rightBlocks)
    {
        long pageAddress = addresses.getLong(leftPosition);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionEqualsRow(blockIndex, blockPosition, rightPosition, rightBlocks);
    }

    private boolean positionEqualsPosition(int leftPosition, int rightPosition)
    {
        long leftPageAddress = addresses.getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = addresses.getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        return pagesHashStrategy.positionEqualsPosition(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
    }
}
