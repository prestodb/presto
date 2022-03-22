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

import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.SliceInput;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiConsumer;

import static com.facebook.presto.common.block.BlockUtil.calculateBlockResetSize;
import static com.facebook.presto.common.block.MapBlock.createMapBlockInternal;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MapBlockBuilder
        extends AbstractMapBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapBlockBuilder.class).instanceSize();

    private final MethodHandle keyBlockEquals;
    private final MethodHandle keyBlockHashCode;

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;

    private int positionCount;
    private int[] offsets;
    private boolean[] mapIsNull;
    private final BlockBuilder keyBlockBuilder;
    private final BlockBuilder valueBlockBuilder;
    private final HashTables hashTables;

    private boolean currentEntryOpened;

    public MapBlockBuilder(
            Type keyType,
            Type valueType,
            MethodHandle keyBlockEquals,
            MethodHandle keyBlockHashCode,
            BlockBuilderStatus blockBuilderStatus,
            int expectedEntries)
    {
        this(
                keyBlockEquals,
                keyBlockHashCode,
                blockBuilderStatus,
                keyType.createBlockBuilder(blockBuilderStatus, expectedEntries),
                valueType.createBlockBuilder(blockBuilderStatus, expectedEntries),
                expectedEntries,
                null);
    }

    private MapBlockBuilder(
            MethodHandle keyBlockEquals,
            MethodHandle keyBlockHashCode,
            @Nullable BlockBuilderStatus blockBuilderStatus,
            BlockBuilder keyBlockBuilder,
            BlockBuilder valueBlockBuilder,
            int expectedEntries,
            @Nullable int[] rawHashTables)
    {
        this.keyBlockEquals = requireNonNull(keyBlockEquals, "keyBlockEquals is null");
        this.keyBlockHashCode = requireNonNull(keyBlockHashCode, "keyBlockHashCode is null");
        this.blockBuilderStatus = blockBuilderStatus;

        this.positionCount = 0;
        this.offsets = new int[expectedEntries + 1];
        this.mapIsNull = new boolean[expectedEntries];
        this.keyBlockBuilder = requireNonNull(keyBlockBuilder, "keyBlockBuilder is null");
        this.valueBlockBuilder = requireNonNull(valueBlockBuilder, "valueBlockBuilder is null");
        this.hashTables = new HashTables(Optional.ofNullable(rawHashTables), 0);
        this.logicalSizeInBytes = -1;
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
    protected HashTables getHashTables()
    {
        return hashTables;
    }

    @Override
    protected int[] getOffsets()
    {
        return offsets;
    }

    @Override
    public int getOffsetBase()
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
                + hashTables.getRetainedSizeInBytes();
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
        consumer.accept(hashTables, hashTables.getRetainedSizeInBytes());
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        return getSingleValueBlockInternal(position);
    }

    public BlockBuilder getKeyBlockBuilder()
    {
        return keyBlockBuilder;
    }

    public BlockBuilder getValueBlockBuilder()
    {
        return valueBlockBuilder;
    }

    /**
     * Recommended way to build a Map is to first call beginBlockEntry which returns a BlockBuilder
     * On this returned BlockBuilder, write key and value alternatively and call closeEntry.
     * This works well for Presto, but when using the writer externally outside of Presto as a library
     * the caller of the library, can't produce vectorized code due to mixing of key and value blocks.
     * This method beginDirectEntry along with exposing keyBlockBuilder and valueBlockBuilder addresses
     * this concern. BenchmarkMapBlockBuilder shows that both approaches are comparable.
     */
    public void beginDirectEntry()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Expected current entry to be closed but was opened");
        }
        currentEntryOpened = true;
    }

    @Override
    public SingleMapBlockWriter beginBlockEntry()
    {
        beginDirectEntry();
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

        if (isHashTablesPresent()) {
            int[] rawHashTables = ensureHashTableSize();
            int previousAggregatedEntryCount = offsets[positionCount - 1];
            int aggregatedEntryCount = offsets[positionCount];
            int entryCount = aggregatedEntryCount - previousAggregatedEntryCount;
            verify(rawHashTables != null, "rawHashTables is null");
            buildHashTable(
                    keyBlockBuilder,
                    previousAggregatedEntryCount,
                    entryCount,
                    keyBlockHashCode,
                    rawHashTables,
                    previousAggregatedEntryCount * HASH_MULTIPLIER,
                    entryCount * HASH_MULTIPLIER);
        }
        return this;
    }

    /**
     * This method will check duplicate keys and close entry.
     * <p>
     * When duplicate keys are discovered, the block is guaranteed to be in
     * a consistent state before {@link DuplicateMapKeyException} is thrown.
     * In other words, one can continue to use this BlockBuilder.
     */
    public BlockBuilder closeEntryStrict(MethodHandle keyBlockEquals, MethodHandle keyBlockHashCode)
            throws DuplicateMapKeyException, NotSupportedException
    {
        if (!currentEntryOpened) {
            throw new IllegalStateException("Expected entry to be opened but was closed");
        }

        ensureHashTableLoaded(keyBlockHashCode);
        entryAdded(false);
        currentEntryOpened = false;

        int[] rawHashTables = ensureHashTableSize();
        int previousAggregatedEntryCount = offsets[positionCount - 1];
        int aggregatedEntryCount = offsets[positionCount];
        int entryCount = aggregatedEntryCount - previousAggregatedEntryCount;
        verify(rawHashTables != null, "rawHashTables is null");
        buildHashTableStrict(
                keyBlockBuilder,
                previousAggregatedEntryCount,
                entryCount,
                keyBlockEquals,
                keyBlockHashCode,
                rawHashTables,
                previousAggregatedEntryCount * HASH_MULTIPLIER,
                entryCount * HASH_MULTIPLIER);
        return this;
    }

    private void closeEntry(@Nullable int[] providedHashTable, int providedHashTableOffset)
    {
        if (!currentEntryOpened) {
            throw new IllegalStateException("Expected entry to be opened but was closed");
        }

        if (providedHashTable != null) {
            ensureHashTableLoaded(keyBlockHashCode);
        }

        entryAdded(false);
        currentEntryOpened = false;

        if (isHashTablesPresent()) {
            int[] rawHashTables = ensureHashTableSize();
            int previousAggregatedEntryCount = offsets[positionCount - 1];
            int aggregatedEntryCount = offsets[positionCount];

            if (providedHashTable != null) {
                // Directly copy instead of building hashtable if providedHashTable is not null
                int hashTableOffset = previousAggregatedEntryCount * HASH_MULTIPLIER;
                int hashTableSize = (aggregatedEntryCount - previousAggregatedEntryCount) * HASH_MULTIPLIER;
                System.arraycopy(providedHashTable, providedHashTableOffset, rawHashTables, hashTableOffset, hashTableSize);
            }
            else {
                // Build hash table for this map entry.
                int entryCount = aggregatedEntryCount - previousAggregatedEntryCount;
                buildHashTable(
                        keyBlockBuilder,
                        previousAggregatedEntryCount,
                        entryCount,
                        keyBlockHashCode,
                        rawHashTables,
                        previousAggregatedEntryCount * HASH_MULTIPLIER,
                        entryCount * HASH_MULTIPLIER);
            }
        }
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
        hashTables.setExpectedHashTableCount(positionCount);

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
            blockBuilderStatus.addBytes((offsets[positionCount] - offsets[positionCount - 1]) * HASH_MULTIPLIER * Integer.BYTES);
        }
    }

    private int[] ensureHashTableSize()
    {
        int[] rawHashTables = hashTables.get();
        verify(rawHashTables != null, "rawHashTables is null");
        if (rawHashTables.length < offsets[positionCount] * HASH_MULTIPLIER) {
            int newSize = BlockUtil.calculateNewArraySize(offsets[positionCount] * HASH_MULTIPLIER);
            int[] newRawHashTables = Arrays.copyOf(rawHashTables, newSize);
            Arrays.fill(newRawHashTables, rawHashTables.length, newSize, -1);
            hashTables.set(newRawHashTables);
            return newRawHashTables;
        }
        return rawHashTables;
    }

    @Override
    public BlockBuilder readPositionFrom(SliceInput input)
    {
        boolean isNull = input.readByte() == 0;
        if (isNull) {
            appendNull();
        }
        else {
            int length = input.readInt();
            SingleMapBlockWriter singleMapBlockWriter = beginBlockEntry();
            for (int i = 0; i < length; i++) {
                singleMapBlockWriter.readPositionFrom(input);
                singleMapBlockWriter.readPositionFrom(input);
            }
            closeEntry();
        }
        return this;
    }

    @Override
    public Block build()
    {
        if (currentEntryOpened) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }

        int[] rawHashTables = hashTables.get();
        int hashTablesEntries = offsets[positionCount] * HASH_MULTIPLIER;
        int[] mapBlockHashTables = (rawHashTables == null) ? null : Arrays.copyOf(rawHashTables, hashTablesEntries);

        return createMapBlockInternal(
                0,
                positionCount,
                Optional.of(mapIsNull),
                offsets,
                keyBlockBuilder.build(),
                valueBlockBuilder.build(),
                new HashTables(Optional.ofNullable(mapBlockHashTables), positionCount));
    }

    @Override
    public String toString()
    {
        return format("MapBlockBuilder(%d){positionCount=%d}", hashCode(), getPositionCount());
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

        closeEntry(singleMapBlock.getHashTable(), singleMapBlock.getOffsetBase() / 2 * HASH_MULTIPLIER);
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

        closeEntry(mapBlock.getHashTables().get(), startValueOffset * HASH_MULTIPLIER);
        return this;
    }

    private int[] getNewHashTables(int newSize)
    {
        if (hashTables.get() != null) {
            return newNegativeOneFilledArray(newSize * HASH_MULTIPLIER);
        }
        return null;
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        int newSize = calculateBlockResetSize(getPositionCount());
        return new MapBlockBuilder(
                keyBlockEquals,
                keyBlockHashCode,
                blockBuilderStatus,
                keyBlockBuilder.newBlockBuilderLike(blockBuilderStatus),
                valueBlockBuilder.newBlockBuilderLike(blockBuilderStatus),
                newSize,
                getNewHashTables(newSize));
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        int newSize = max(calculateBlockResetSize(getPositionCount()), expectedEntries);
        int nestedExpectedEntries = BlockUtil.calculateNestedStructureResetSize(offsets[positionCount], positionCount, expectedEntries);
        return new MapBlockBuilder(
                keyBlockEquals,
                keyBlockHashCode,
                blockBuilderStatus,
                keyBlockBuilder.newBlockBuilderLike(blockBuilderStatus, nestedExpectedEntries),
                valueBlockBuilder.newBlockBuilderLike(blockBuilderStatus, nestedExpectedEntries),
                newSize,
                getNewHashTables(newSize));
    }

    public void loadHashTables()
    {
        ensureHashTableLoaded(keyBlockHashCode);
    }

    @Override
    protected void ensureHashTableLoaded(MethodHandle keyBlockHashCode)
    {
        // MapBlockBuilder does not support concurrent threads, so synchronization is not required.
        if (!isHashTablesPresent()) {
            hashTables.loadHashTables(positionCount, offsets, mapIsNull, keyBlockBuilder, keyBlockHashCode);
        }
    }

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
            throws DuplicateMapKeyException, NotSupportedException
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
                    throw new NotSupportedException("map key cannot be null or contain nulls");
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

    static void verify(boolean assertion, String message)
    {
        if (!assertion) {
            throw new AssertionError(message);
        }
    }
}
