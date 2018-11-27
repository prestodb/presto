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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Verify;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.type.TypeUtils.hashPosition;
import static com.facebook.presto.type.TypeUtils.positionEqualsPosition;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class HashTable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TypedSet.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;
    private static final int EMPTY_SLOT = -1;

    private final Type type;
    private final Block block;

    private int[] positionByHash;
    private int hashCapacity;
    private int maxFill;
    private int hashMask;
    private int size;

    public HashTable(Type type, Block block, int expectedSize)
    {
        this.type = requireNonNull(type, "type is null");
        this.block = requireNonNull(block, "block is null");
        checkArgument(expectedSize >= 0, "expectedSize is negative");

        hashCapacity = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        hashMask = hashCapacity - 1;
        positionByHash = new int[hashCapacity];
        Arrays.fill(positionByHash, EMPTY_SLOT);
    }

    public boolean contains(Block block, int position)
    {
        checkArgument(!isNull(block), "block is null");
        checkArgument(position >= 0, "position is negative");
        return positionByHash[getHashPosition(block, position)] != EMPTY_SLOT;
    }

    public boolean addIfAbsent(Block block, int position)
    {
        checkArgument(!isNull(block), "block is null");
        checkArgument(position >= 0, "position is negative");
        int hashPosition = getHashPosition(block, position);
        if (positionByHash[hashPosition] == EMPTY_SLOT) {
            if (this.block instanceof BlockBuilder) {
                BlockBuilder blockBuilder = (BlockBuilder) this.block;
                type.appendTo(block, position, blockBuilder);
                int positionInBuilder = blockBuilder.getPositionCount() - 1;
                positionByHash[hashPosition] = positionInBuilder;
            }
            else {
                Verify.verify(block == this.block);
                positionByHash[hashPosition] = position;
            }

            size++;
            if (size >= maxFill) {
                rehash();
            }

            return true;
        }
        else {
            return false;
        }
    }

    public Block getBlock()
    {
        if (block instanceof BlockBuilder) {
            return ((BlockBuilder) block).build();
        }

        int[] positions = new int[size];
        int j = 0;
        for (int i = 0; i < positionByHash.length; i++) {
            int blockPosition = positionByHash[i];
            if (blockPosition != EMPTY_SLOT) {
                positions[j++] = blockPosition;
            }
        }

        return block.getPositions(positions, 0, size);
    }

    public int size()
    {
        return size;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(positionByHash) + block.getRetainedSizeInBytes();
    }

    private int getHashPosition(Block block, int position)
    {
        int hashPosition = getMaskedHash(hashPosition(type, block, position));
        while (true) {
            if (positionByHash[hashPosition] == EMPTY_SLOT) {
                return hashPosition;
            }
            // TODO: replace positionEqualsPosition to isDistinctFrom operator
            else if (positionEqualsPosition(type, this.block, positionByHash[hashPosition], block, position)) {
                return hashPosition;
            }
            hashPosition = getMaskedHash(hashPosition + 1);
        }
    }

    private void rehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = (int) newCapacityLong;
        hashCapacity = newCapacity;
        hashMask = newCapacity - 1;
        maxFill = calculateMaxFill(newCapacity);
        int[] oldPositionByHash = positionByHash;
        positionByHash = new int[newCapacity];
        Arrays.fill(positionByHash, EMPTY_SLOT);
        for (int position : oldPositionByHash) {
            if (position != EMPTY_SLOT) {
                positionByHash[getHashPosition(this.block, position)] = position;
            }
        }
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    private int getMaskedHash(long rawHash)
    {
        return (int) (rawHash & hashMask);
    }
}
