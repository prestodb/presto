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

package com.facebook.presto.common.block;

import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.common.block.BlockUtil.appendNullToIsNullArray;
import static com.facebook.presto.common.block.BlockUtil.appendNullToOffsetsArray;
import static com.facebook.presto.common.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.common.block.BlockUtil.checkValidPositions;
import static com.facebook.presto.common.block.BlockUtil.checkValidRegion;
import static com.facebook.presto.common.block.BlockUtil.compactArray;
import static com.facebook.presto.common.block.BlockUtil.compactOffsets;
import static com.facebook.presto.common.block.BlockUtil.internalPositionInRange;
import static com.facebook.presto.common.block.MapBlock.createMapBlockInternal;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static java.util.Objects.requireNonNull;

public abstract class AbstractMapBlock
        implements Block
{
    // inverse of hash fill ratio, must be integer
    static final int HASH_MULTIPLIER = 2;

    protected volatile long logicalSizeInBytes;

    protected abstract Block getRawKeyBlock();

    protected abstract Block getRawValueBlock();

    protected abstract HashTables getHashTables();

    /**
     * offset is entry-based, not position-based. In other words,
     * if offset[1] is 6, it means the first map has 6 key-value pairs,
     * not 6 key/values (which would be 3 pairs).
     */
    protected abstract int[] getOffsets();

    /**
     * offset is entry-based, not position-based. (see getOffsets)
     */
    public abstract int getOffsetBase();

    @Nullable
    protected abstract boolean[] getMapIsNull();

    protected abstract void ensureHashTableLoaded(MethodHandle keyBlockHashCode);

    int getOffset(int position)
    {
        return getOffsets()[position + getOffsetBase()];
    }

    @Override
    public String getEncodingName()
    {
        return MapBlockEncoding.NAME;
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int[] newOffsets = new int[length + 1];
        boolean[] newMapIsNull = new boolean[length];

        IntArrayList entriesPositions = new IntArrayList();
        int newPosition = 0;
        for (int i = offset; i < offset + length; ++i) {
            int position = positions[i];
            if (isNull(position)) {
                newMapIsNull[newPosition] = true;
                newOffsets[newPosition + 1] = newOffsets[newPosition];
            }
            else {
                int entriesStartOffset = getOffset(position);
                int entriesEndOffset = getOffset(position + 1);
                int entryCount = entriesEndOffset - entriesStartOffset;

                newOffsets[newPosition + 1] = newOffsets[newPosition] + entryCount;

                for (int elementIndex = entriesStartOffset; elementIndex < entriesEndOffset; elementIndex++) {
                    entriesPositions.add(elementIndex);
                }
            }
            newPosition++;
        }

        int[] rawHashTables = getHashTables().get();
        int[] newRawHashTables = null;
        int newHashTableEntries = newOffsets[newOffsets.length - 1] * HASH_MULTIPLIER;
        if (rawHashTables != null) {
            newRawHashTables = new int[newHashTableEntries];
            int newHashIndex = 0;
            for (int i = offset; i < offset + length; ++i) {
                int position = positions[i];
                int entriesStartOffset = getOffset(position);
                int entriesEndOffset = getOffset(position + 1);
                for (int hashIndex = entriesStartOffset * HASH_MULTIPLIER; hashIndex < entriesEndOffset * HASH_MULTIPLIER; hashIndex++) {
                    newRawHashTables[newHashIndex] = rawHashTables[hashIndex];
                    newHashIndex++;
                }
            }
        }

        Block newKeys = getRawKeyBlock().copyPositions(entriesPositions.elements(), 0, entriesPositions.size());
        Block newValues = getRawValueBlock().copyPositions(entriesPositions.elements(), 0, entriesPositions.size());
        return createMapBlockInternal(
                0,
                length,
                Optional.of(newMapIsNull),
                newOffsets,
                newKeys,
                newValues,
                new HashTables(Optional.ofNullable(newRawHashTables), length, newHashTableEntries));
    }

    @Override
    public Block getRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        return createMapBlockInternal(
                position + getOffsetBase(),
                length,
                Optional.ofNullable(getMapIsNull()),
                getOffsets(),
                getRawKeyBlock(),
                getRawValueBlock(),
                getHashTables());
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int entriesStart = getOffsets()[getOffsetBase() + position];
        int entriesEnd = getOffsets()[getOffsetBase() + position + length];
        int entryCount = entriesEnd - entriesStart;

        return getRawKeyBlock().getRegionSizeInBytes(entriesStart, entryCount) +
                getRawValueBlock().getRegionSizeInBytes(entriesStart, entryCount) +
                (Integer.BYTES + Byte.BYTES) * (long) length +
                Integer.BYTES * HASH_MULTIPLIER * (long) entryCount +
                getHashTables().getInstanceSizeInBytes();
    }

    @Override
    public long getLogicalSizeInBytes()
    {
        if (logicalSizeInBytes < 0) {
            calculateLogicalSize();
        }
        return logicalSizeInBytes;
    }

    @Override
    public long getRegionLogicalSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int entriesStart = getOffsets()[getOffsetBase() + position];
        int entriesEnd = getOffsets()[getOffsetBase() + position + length];
        int entryCount = entriesEnd - entriesStart;

        return getRawKeyBlock().getRegionLogicalSizeInBytes(entriesStart, entryCount) +
                getRawValueBlock().getRegionLogicalSizeInBytes(entriesStart, entryCount) +
                (Integer.BYTES + Byte.BYTES) * length +
                Integer.BYTES * HASH_MULTIPLIER * entryCount;
    }

    @Override
    public long getApproximateRegionLogicalSizeInBytes(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int entriesStart = getOffset(position);
        int entriesEnd = getOffset(position + length);
        int entryCount = entriesEnd - entriesStart;

        return getRawKeyBlock().getApproximateRegionLogicalSizeInBytes(entriesStart, entryCount) +
                getRawValueBlock().getApproximateRegionLogicalSizeInBytes(entriesStart, entryCount) +
                (Integer.BYTES + Byte.BYTES) * length +         // offsets and mapIsNull
                Integer.BYTES * HASH_MULTIPLIER * entryCount;   // hashtables
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        // We can use either the getRegionSizeInBytes or getPositionsSizeInBytes
        // from the underlying raw blocks to implement this function. We chose
        // getPositionsSizeInBytes with the assumption that constructing a
        // positions array is cheaper than calling getRegionSizeInBytes for each
        // used position.
        int positionCount = getPositionCount();
        checkValidPositions(positions, positionCount);
        boolean[] entryPositions = new boolean[getRawKeyBlock().getPositionCount()];
        int usedEntryCount = 0;
        int usedPositionCount = 0;
        for (int i = 0; i < positions.length; ++i) {
            if (positions[i]) {
                usedPositionCount++;
                int entriesStart = getOffsets()[getOffsetBase() + i];
                int entriesEnd = getOffsets()[getOffsetBase() + i + 1];
                for (int j = entriesStart; j < entriesEnd; j++) {
                    entryPositions[j] = true;
                }
                usedEntryCount += (entriesEnd - entriesStart);
            }
        }
        return getRawKeyBlock().getPositionsSizeInBytes(entryPositions) +
                getRawValueBlock().getPositionsSizeInBytes(entryPositions) +
                (Integer.BYTES + Byte.BYTES) * (long) usedPositionCount +
                Integer.BYTES * HASH_MULTIPLIER * (long) usedEntryCount +
                getHashTables().getInstanceSizeInBytes();
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, position, length);

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + length);
        Block newKeys = getRawKeyBlock().copyRegion(startValueOffset, endValueOffset - startValueOffset);
        Block newValues = getRawValueBlock().copyRegion(startValueOffset, endValueOffset - startValueOffset);

        int[] newOffsets = compactOffsets(getOffsets(), position + getOffsetBase(), length);
        boolean[] mapIsNull = getMapIsNull();
        boolean[] newMapIsNull = mapIsNull == null ? null : compactArray(mapIsNull, position + getOffsetBase(), length);

        int[] rawHashTables = getHashTables().get();
        int[] newRawHashTables = null;
        int expectedNewHashTableEntries = (endValueOffset - startValueOffset) * HASH_MULTIPLIER;
        if (rawHashTables != null) {
            newRawHashTables = compactArray(rawHashTables, startValueOffset * HASH_MULTIPLIER, expectedNewHashTableEntries);
        }

        if (newKeys == getRawKeyBlock() && newValues == getRawValueBlock() && newOffsets == getOffsets() && newMapIsNull == mapIsNull && newRawHashTables == rawHashTables) {
            return this;
        }
        return createMapBlockInternal(
                0,
                length,
                Optional.ofNullable(newMapIsNull),
                newOffsets,
                newKeys,
                newValues,
                new HashTables(Optional.ofNullable(newRawHashTables), length, expectedNewHashTableEntries));
    }

    @Override
    public Block getBlock(int position)
    {
        checkReadablePosition(position);

        int startEntryOffset = getOffset(position);
        int endEntryOffset = getOffset(position + 1);
        return new SingleMapBlock(
                position,
                startEntryOffset * 2,
                (endEntryOffset - startEntryOffset) * 2,
                this);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.appendStructureInternal(this, position);
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        if (isNull(position)) {
            output.writeByte(0);
        }
        else {
            int startValueOffset = getOffset(position);
            int endValueOffset = getOffset(position + 1);
            int numberOfElements = endValueOffset - startValueOffset;

            output.writeByte(1);
            output.writeInt(numberOfElements);
            Block rawKeyBlock = getRawKeyBlock();
            Block rawValueBlock = getRawValueBlock();
            for (int i = startValueOffset; i < endValueOffset; i++) {
                rawKeyBlock.writePositionTo(i, output);
                rawValueBlock.writePositionTo(i, output);
            }
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + 1);
        int valueLength = endValueOffset - startValueOffset;
        Block newKeys = getRawKeyBlock().copyRegion(startValueOffset, valueLength);
        Block newValues = getRawValueBlock().copyRegion(startValueOffset, valueLength);

        int[] rawHashTables = getHashTables().get();
        int[] newRawHashTables = null;
        int expectedNewHashTableEntries = (endValueOffset - startValueOffset) * HASH_MULTIPLIER;
        if (rawHashTables != null) {
            newRawHashTables = Arrays.copyOfRange(rawHashTables, startValueOffset * HASH_MULTIPLIER, endValueOffset * HASH_MULTIPLIER);
        }

        return createMapBlockInternal(
                0,
                1,
                Optional.of(new boolean[] {isNull(position)}),
                new int[] {0, valueLength},
                newKeys,
                newValues,
                new HashTables(Optional.ofNullable(newRawHashTables), 1, expectedNewHashTableEntries));
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkReadablePosition(position);

        if (isNull(position)) {
            return 0;
        }

        int startValueOffset = getOffset(position);
        int endValueOffset = getOffset(position + 1);

        long size = 0;
        Block rawKeyBlock = getRawKeyBlock();
        Block rawValueBlock = getRawValueBlock();
        for (int i = startValueOffset; i < endValueOffset; i++) {
            size += rawKeyBlock.getEstimatedDataSizeForStats(i);
            size += rawValueBlock.getEstimatedDataSizeForStats(i);
        }
        return size;
    }

    @Override
    public boolean mayHaveNull()
    {
        return getMapIsNull() != null;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        boolean[] mapIsNull = getMapIsNull();
        return mapIsNull != null && mapIsNull[position + getOffsetBase()];
    }

    public boolean isHashTablesPresent()
    {
        return getHashTables().get() != null;
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    public static class HashTables
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(HashTables.class).instanceSize();

        // Hash to location in map. Writes to the field by MapBlock is protected by "HashTables" monitor in MapBlock.
        // MapBlockBuilder instances have their dedicated hashTables instances, so the write accesses to the hashTables
        // fields do not need to be synchronized in that class.
        @Nullable
        private volatile int[] hashTables;

        // The number of hash tables. Each map row corresponds to one hash table if it's built.
        private int expectedHashTableCount;

        // The total number of entries of all hashTables as if they are always built. It's used to calculate the retained size.
        private int expectedEntryCount;

        HashTables(Optional<int[]> hashTables, int expectedHashTableCount, int expectedEntryCount)
        {
            if (hashTables.isPresent() && hashTables.get().length != expectedEntryCount) {
                throw new IllegalArgumentException("hashTables size does not match expectedEntryCount");
            }

            this.hashTables = hashTables.orElse(null);
            this.expectedEntryCount = expectedEntryCount;
            this.expectedHashTableCount = expectedHashTableCount;
        }

        @Nullable
        int[] get()
        {
            return hashTables;
        }

        void set(int[] hashTables)
        {
            requireNonNull(hashTables, "hashTables is null");
            this.hashTables = hashTables;

            // The passed in hashTables are always sized as if they are fully built.
            this.expectedEntryCount = hashTables.length;
        }

        int getExpectedHashTableCount()
        {
            return expectedHashTableCount;
        }

        public long getInstanceSizeInBytes()
        {
            return INSTANCE_SIZE;
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + sizeOfIntArray(expectedEntryCount);
        }
    }

    @Override
    public Block getBlockUnchecked(int internalPosition)
    {
        assert internalPositionInRange(internalPosition, this.getOffsetBase(), getPositionCount());

        int startEntryOffset = getOffsets()[internalPosition];
        int endEntryOffset = getOffsets()[internalPosition + 1];
        return new SingleMapBlock(internalPosition - getOffsetBase(), startEntryOffset * 2, (endEntryOffset - startEntryOffset) * 2, this);
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        assert mayHaveNull() : "no nulls present";
        assert internalPositionInRange(internalPosition, this.getOffsetBase(), getPositionCount());
        return getMapIsNull()[internalPosition];
    }

    @Override
    public Block appendNull()
    {
        boolean[] mapIsNull = appendNullToIsNullArray(getMapIsNull(), getOffsetBase(), getPositionCount());
        int[] offsets = appendNullToOffsetsArray(getOffsets(), getOffsetBase(), getPositionCount());

        return createMapBlockInternal(
                getOffsetBase(),
                getPositionCount() + 1,
                Optional.of(mapIsNull),
                offsets,
                getRawKeyBlock(),
                getRawValueBlock(),
                getHashTables());
    }

    private void calculateLogicalSize()
    {
        int entriesStart = getOffset(0);
        int entriesEnd = getOffset(getPositionCount());
        int entryCount = entriesEnd - entriesStart;
        logicalSizeInBytes = getRawKeyBlock().getRegionLogicalSizeInBytes(entriesStart, entryCount) +
                getRawValueBlock().getRegionLogicalSizeInBytes(entriesStart, entryCount) +
                (Integer.BYTES + Byte.BYTES) * (long) this.getPositionCount() +
                Integer.BYTES * HASH_MULTIPLIER * (long) entryCount;
    }
}
