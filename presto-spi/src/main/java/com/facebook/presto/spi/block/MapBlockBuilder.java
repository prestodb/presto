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

package com.facebook.presto.spi.block;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiConsumer;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.block.BlockUtil.calculateBlockResetSize;
import static com.facebook.presto.spi.block.MapBlock.createMapBlockInternal;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MapBlockBuilder
        extends AbstractMapBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapBlockBuilder.class).instanceSize();

    private final MethodHandle keyBlockEquals;

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;

    private int positionCount;
    private int[] offsets;
    private boolean[] mapIsNull;
    private final BlockBuilder keyBlockBuilder;
    private final BlockBuilder valueBlockBuilder;
    private int[] hashTables;

    private boolean currentEntryOpened;

    public MapBlockBuilder(
            Type keyType,
            Type valueType,
            MethodHandle keyBlockNativeEquals,
            MethodHandle keyBlockEquals,
            MethodHandle keyNativeHashCode,
            MethodHandle keyBlockHashCode,
            BlockBuilderStatus blockBuilderStatus,
            int expectedEntries)
    {
        this(
                keyType,
                keyBlockNativeEquals,
                keyBlockEquals,
                keyNativeHashCode,
                keyBlockHashCode,
                blockBuilderStatus,
                keyType.createBlockBuilder(blockBuilderStatus, expectedEntries),
                valueType.createBlockBuilder(blockBuilderStatus, expectedEntries),
                new int[expectedEntries + 1],
                new boolean[expectedEntries],
                newNegativeOneFilledArray(expectedEntries * HASH_MULTIPLIER));
    }

    private MapBlockBuilder(
            Type keyType,
            MethodHandle keyBlockNativeEquals,
            MethodHandle keyBlockEquals,
            MethodHandle keyNativeHashCode,
            MethodHandle keyBlockHashCode,
            @Nullable BlockBuilderStatus blockBuilderStatus,
            BlockBuilder keyBlockBuilder,
            BlockBuilder valueBlockBuilder,
            int[] offsets,
            boolean[] mapIsNull,
            int[] hashTables)
    {
        super(keyType, keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode);

        this.keyBlockEquals = requireNonNull(keyBlockEquals, "keyBlockEquals is null");
        this.blockBuilderStatus = blockBuilderStatus;

        this.positionCount = 0;
        this.offsets = requireNonNull(offsets, "offsets is null");
        this.mapIsNull = requireNonNull(mapIsNull, "mapIsNull is null");
        this.keyBlockBuilder = requireNonNull(keyBlockBuilder, "keyBlockBuilder is null");
        this.valueBlockBuilder = requireNonNull(valueBlockBuilder, "valueBlockBuilder is null");
        this.hashTables = requireNonNull(hashTables, "hashTables is null");
    }

    @Override
    protected Block getRawKeyBlock()
    {
        return keyBlockBuilder;
    }

    @Override
    protected Block getRawValueBlock()
    {
        return valueBlockBuilder;
    }

    @Override
    protected int[] getHashTables()
    {
        return hashTables;
    }

    @Override
    protected int[] getOffsets()
    {
        return offsets;
    }

    @Override
    protected int getOffsetBase()
    {
        return 0;
    }

    @Override
    protected boolean[] getMapIsNull()
    {
        return mapIsNull;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return keyBlockBuilder.getSizeInBytes() + valueBlockBuilder.getSizeInBytes() +
                (Integer.BYTES + Byte.BYTES) * (long) positionCount +
                Integer.BYTES * HASH_MULTIPLIER * (long) keyBlockBuilder.getPositionCount();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE
                + keyBlockBuilder.getRetainedSizeInBytes()
                + valueBlockBuilder.getRetainedSizeInBytes()
                + sizeOf(offsets)
                + sizeOf(mapIsNull)
                + sizeOf(hashTables);
        if (blockBuilderStatus != null) {
            size += BlockBuilderStatus.INSTANCE_SIZE;
        }
        return size;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(keyBlockBuilder, keyBlockBuilder.getRetainedSizeInBytes());
        consumer.accept(valueBlockBuilder, valueBlockBuilder.getRetainedSizeInBytes());
        consumer.accept(offsets, sizeOf(offsets));
        consumer.accept(mapIsNull, sizeOf(mapIsNull));
        consumer.accept(hashTables, sizeOf(hashTables));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public SingleMapBlockWriter beginBlockEntry()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }
        currentEntryOpened = true;
        return new SingleMapBlockWriter(keyBlockBuilder.getPositionCount() * 2, keyBlockBuilder, valueBlockBuilder);
    }

    @Override
    public BlockBuilder closeEntry()
    {
        if (!currentEntryOpened) {
            throw new IllegalStateException("Expected entry to be opened but was closed");
        }

        entryAdded(false);
        currentEntryOpened = false;

        ensureHashTableSize();
        int previousAggregatedEntryCount = offsets[positionCount - 1];
        int aggregatedEntryCount = offsets[positionCount];
        int entryCount = aggregatedEntryCount - previousAggregatedEntryCount;
        buildHashTable(
                keyBlockBuilder,
                previousAggregatedEntryCount,
                entryCount,
                keyBlockHashCode,
                hashTables,
                previousAggregatedEntryCount * HASH_MULTIPLIER,
                entryCount * HASH_MULTIPLIER);
        return this;
    }

    /**
     * This method will check duplicate keys and close entry.
     * <p>
     * When duplicate keys are discovered, the block is guaranteed to be in
     * a consistent state before {@link DuplicateMapKeyException} is thrown.
     * In other words, one can continue to use this BlockBuilder.
     */
    public BlockBuilder closeEntryStrict()
            throws DuplicateMapKeyException
    {
        if (!currentEntryOpened) {
            throw new IllegalStateException("Expected entry to be opened but was closed");
        }

        entryAdded(false);
        currentEntryOpened = false;

        ensureHashTableSize();
        int previousAggregatedEntryCount = offsets[positionCount - 1];
        int aggregatedEntryCount = offsets[positionCount];
        int entryCount = aggregatedEntryCount - previousAggregatedEntryCount;
        buildHashTableStrict(
                keyBlockBuilder,
                previousAggregatedEntryCount,
                entryCount,
                keyBlockEquals,
                keyBlockHashCode,
                hashTables,
                previousAggregatedEntryCount * HASH_MULTIPLIER,
                entryCount * HASH_MULTIPLIER);
        return this;
    }

    private BlockBuilder closeEntry(@Nullable int[] providedHashTable, int providedHashTableOffset)
    {
        if (!currentEntryOpened) {
            throw new IllegalStateException("Expected entry to be opened but was closed");
        }

        entryAdded(false);
        currentEntryOpened = false;

        ensureHashTableSize();
        int previousAggregatedEntryCount = offsets[positionCount - 1];
        int aggregatedEntryCount = offsets[positionCount];

        if (providedHashTable != null) {
            // Directly copy instead of building hashtable if providedHashTable is not null
            int hashTableOffset = previousAggregatedEntryCount * HASH_MULTIPLIER;
            int hashTableSize = (aggregatedEntryCount - previousAggregatedEntryCount) * HASH_MULTIPLIER;
            for (int i = 0; i < hashTableSize; i++) {
                hashTables[hashTableOffset + i] = providedHashTable[providedHashTableOffset + i];
            }
        }
        else {
            // Build hash table for this map entry.
            int entryCount = aggregatedEntryCount - previousAggregatedEntryCount;
            buildHashTable(
                    keyBlockBuilder,
                    previousAggregatedEntryCount,
                    entryCount,
                    keyBlockHashCode,
                    hashTables,
                    previousAggregatedEntryCount * HASH_MULTIPLIER,
                    entryCount * HASH_MULTIPLIER);
        }
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        entryAdded(true);
        return this;
    }

    private void entryAdded(boolean isNull)
    {
        if (keyBlockBuilder.getPositionCount() != valueBlockBuilder.getPositionCount()) {
            throw new IllegalStateException(format("keyBlock and valueBlock has different size: %s %s", keyBlockBuilder.getPositionCount(), valueBlockBuilder.getPositionCount()));
        }
        if (mapIsNull.length <= positionCount) {
            int newSize = BlockUtil.calculateNewArraySize(mapIsNull.length);
            mapIsNull = Arrays.copyOf(mapIsNull, newSize);
            offsets = Arrays.copyOf(offsets, newSize + 1);
        }
        offsets[positionCount + 1] = keyBlockBuilder.getPositionCount();
        mapIsNull[positionCount] = isNull;
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
            blockBuilderStatus.addBytes((offsets[positionCount] - offsets[positionCount - 1]) * HASH_MULTIPLIER * Integer.BYTES);
        }
    }

    private void ensureHashTableSize()
    {
        if (hashTables.length < offsets[positionCount] * HASH_MULTIPLIER) {
            int newSize = BlockUtil.calculateNewArraySize(offsets[positionCount] * HASH_MULTIPLIER);
            int oldSize = hashTables.length;
            hashTables = Arrays.copyOf(hashTables, newSize);
            Arrays.fill(hashTables, oldSize, hashTables.length, -1);
        }
    }

    @Override
    public Block build()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        return createMapBlockInternal(
                0,
                positionCount,
                Optional.of(mapIsNull),
                offsets,
                keyBlockBuilder.build(),
                valueBlockBuilder.build(),
                Optional.of(Arrays.copyOf(hashTables, offsets[positionCount] * HASH_MULTIPLIER)),
                keyType,
                keyBlockNativeEquals,
                keyNativeHashCode,
                keyBlockHashCode);
    }

    @Override
    public String toString()
    {
        return "MapBlockBuilder{" +
                "positionCount=" + getPositionCount() +
                '}';
    }

    @Override
    public BlockBuilder appendStructure(Block block)
    {
        if (!(block instanceof SingleMapBlock)) {
            throw new IllegalArgumentException("Expected SingleMapBlock");
        }
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }
        currentEntryOpened = true;

        SingleMapBlock singleMapBlock = (SingleMapBlock) block;
        int blockPositionCount = singleMapBlock.getPositionCount();
        if (blockPositionCount % 2 != 0) {
            throw new IllegalArgumentException(format("block position count is not even: %s", blockPositionCount));
        }
        for (int i = 0; i < blockPositionCount; i += 2) {
            if (singleMapBlock.isNull(i)) {
                throw new IllegalArgumentException("Map keys must not be null");
            }
            else {
                singleMapBlock.writePositionTo(i, keyBlockBuilder);
            }
            if (singleMapBlock.isNull(i + 1)) {
                valueBlockBuilder.appendNull();
            }
            else {
                singleMapBlock.writePositionTo(i + 1, valueBlockBuilder);
            }
        }

        closeEntry(singleMapBlock.getHashTable(), singleMapBlock.getOffset() / 2 * HASH_MULTIPLIER);
        return this;
    }

    @Override
    public BlockBuilder appendStructureInternal(Block block, int position)
    {
        if (!(block instanceof AbstractMapBlock)) {
            throw new IllegalArgumentException("Expected AbstractMapBlock");
        }
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }
        currentEntryOpened = true;

        AbstractMapBlock mapBlock = (AbstractMapBlock) block;
        int startValueOffset = mapBlock.getOffset(position);
        int endValueOffset = mapBlock.getOffset(position + 1);
        for (int i = startValueOffset; i < endValueOffset; i++) {
            if (mapBlock.getRawKeyBlock().isNull(i)) {
                throw new IllegalArgumentException("Map keys must not be null");
            }
            else {
                mapBlock.getRawKeyBlock().writePositionTo(i, keyBlockBuilder);
            }
            if (mapBlock.getRawValueBlock().isNull(i)) {
                valueBlockBuilder.appendNull();
            }
            else {
                mapBlock.getRawValueBlock().writePositionTo(i, valueBlockBuilder);
            }
        }

        closeEntry(mapBlock.getHashTables(), startValueOffset * HASH_MULTIPLIER);
        return this;
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        int newSize = calculateBlockResetSize(getPositionCount());
        return new MapBlockBuilder(
                keyType,
                keyBlockNativeEquals,
                keyBlockEquals,
                keyNativeHashCode,
                keyBlockHashCode,
                blockBuilderStatus,
                keyBlockBuilder.newBlockBuilderLike(blockBuilderStatus),
                valueBlockBuilder.newBlockBuilderLike(blockBuilderStatus),
                new int[newSize + 1],
                new boolean[newSize],
                newNegativeOneFilledArray(newSize * HASH_MULTIPLIER));
    }

    @Override
    protected void ensureHashTableLoaded() {}

    private static int[] newNegativeOneFilledArray(int size)
    {
        int[] hashTable = new int[size];
        Arrays.fill(hashTable, -1);
        return hashTable;
    }

    /**
     * This method assumes that {@code keyBlock} has no duplicated entries (in the specified range)
     */
    static void buildHashTable(Block keyBlock, int keyOffset, int keyCount, MethodHandle keyBlockHashCode, int[] outputHashTable, int hashTableOffset, int hashTableSize)
    {
        for (int i = 0; i < keyCount; i++) {
            int hash = getHashPosition(keyBlock, keyOffset + i, keyBlockHashCode, hashTableSize);
            while (true) {
                if (outputHashTable[hashTableOffset + hash] == -1) {
                    outputHashTable[hashTableOffset + hash] = i;
                    break;
                }
                hash++;
                if (hash == hashTableSize) {
                    hash = 0;
                }
            }
        }
    }

    /**
     * This method checks whether {@code keyBlock} has duplicated entries (in the specified range)
     */
    private static void buildHashTableStrict(
            Block keyBlock,
            int keyOffset,
            int keyCount,
            MethodHandle keyBlockEquals,
            MethodHandle keyBlockHashCode,
            int[] outputHashTable,
            int hashTableOffset,
            int hashTableSize)
            throws DuplicateMapKeyException
    {
        for (int i = 0; i < keyCount; i++) {
            int hash = getHashPosition(keyBlock, keyOffset + i, keyBlockHashCode, hashTableSize);
            while (true) {
                if (outputHashTable[hashTableOffset + hash] == -1) {
                    outputHashTable[hashTableOffset + hash] = i;
                    break;
                }

                Boolean isDuplicateKey;
                try {
                    // assuming maps with indeterminate keys are not supported
                    isDuplicateKey = (Boolean) keyBlockEquals.invokeExact(keyBlock, keyOffset + i, keyBlock, keyOffset + outputHashTable[hashTableOffset + hash]);
                }
                catch (RuntimeException e) {
                    throw e;
                }
                catch (Throwable throwable) {
                    throw new RuntimeException(throwable);
                }

                if (isDuplicateKey == null) {
                    throw new PrestoException(NOT_SUPPORTED, "map key cannot be null or contain nulls");
                }

                if (isDuplicateKey) {
                    throw new DuplicateMapKeyException(keyBlock, keyOffset + i);
                }

                hash++;
                if (hash == hashTableSize) {
                    hash = 0;
                }
            }
        }
    }

    private static int getHashPosition(Block keyBlock, int position, MethodHandle keyBlockHashCode, int hashTableSize)
    {
        if (keyBlock.isNull(position)) {
            throw new IllegalArgumentException("map keys cannot be null");
        }

        long hashCode;
        try {
            hashCode = (long) keyBlockHashCode.invokeExact(keyBlock, position);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }

        return computePosition(hashCode, hashTableSize);
    }

    // This function reduces the 64 bit hashcode to [0, hashTableSize) uniformly. It first reduces the hashcode to 32 bit
    // integer x then normalize it to x / 2^32 * hashSize to reduce the range of x from [0, 2^32) to [0, hashTableSize)
    static int computePosition(long hashcode, int hashTableSize)
    {
        return (int) ((Integer.toUnsignedLong(Long.hashCode(hashcode)) * hashTableSize) >> 32);
    }
}
