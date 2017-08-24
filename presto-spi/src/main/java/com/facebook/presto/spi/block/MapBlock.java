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

import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.function.BiConsumer;

import static com.facebook.presto.spi.block.MapBlockBuilder.buildHashTable;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MapBlock
        extends AbstractMapBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapBlock.class).instanceSize();

    private final int startOffset;
    private final int positionCount;

    private final boolean[] mapIsNull;
    private final int[] offsets;
    private final Block keyBlock;
    private final Block valueBlock;
    private final int[] hashTables; // hash to location in map;

    private volatile long sizeInBytes;
    private final long retainedSizeInBytes;

    /**
     * @param keyBlockNativeEquals (T, Block, int)boolean
     * @param keyNativeHashCode (T)long
     */
    MapBlock(
            int startOffset,
            int positionCount,
            boolean[] mapIsNull,
            int[] offsets,
            Block keyBlock,
            Block valueBlock,
            int[] hashTables,
            Type keyType,
            MethodHandle keyBlockNativeEquals,
            MethodHandle keyNativeHashCode)
    {
        super(keyType, keyNativeHashCode, keyBlockNativeEquals);

        this.startOffset = startOffset;
        this.positionCount = positionCount;
        this.mapIsNull = mapIsNull;
        this.offsets = requireNonNull(offsets, "offsets is null");
        this.keyBlock = requireNonNull(keyBlock, "keyBlock is null");
        this.valueBlock = requireNonNull(valueBlock, "valueBlock is null");
        if (keyBlock.getPositionCount() != valueBlock.getPositionCount()) {
            throw new IllegalArgumentException(format("keyBlock and valueBlock has different size: %s %s", keyBlock.getPositionCount(), valueBlock.getPositionCount()));
        }
        if (hashTables.length < keyBlock.getPositionCount() * HASH_MULTIPLIER) {
            throw new IllegalArgumentException(format("keyBlock/valueBlock size does not match hash table size: %s %s", keyBlock.getPositionCount(), hashTables.length));
        }
        this.hashTables = hashTables;

        this.sizeInBytes = -1;
        this.retainedSizeInBytes = INSTANCE_SIZE + keyBlock.getRetainedSizeInBytes() + valueBlock.getRetainedSizeInBytes() + sizeOf(offsets) + sizeOf(mapIsNull) + sizeOf(hashTables);
    }

    @Override
    protected Block getKeys()
    {
        return keyBlock;
    }

    @Override
    protected Block getValues()
    {
        return valueBlock;
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
        return startOffset;
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
        if (sizeInBytes < 0) {
            calculateSize();
        }
        return sizeInBytes;
    }

    private void calculateSize()
    {
        int entriesStart = offsets[startOffset];
        int entriesEnd = offsets[startOffset + positionCount];
        int entryCount = entriesEnd - entriesStart;
        sizeInBytes = keyBlock.getRegionSizeInBytes(entriesStart, entryCount) +
                valueBlock.getRegionSizeInBytes(entriesStart, entryCount) +
                (Integer.BYTES + Byte.BYTES) * (long) this.positionCount +
                Integer.BYTES * HASH_MULTIPLIER * (long) entryCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(keyBlock, keyBlock.getRetainedSizeInBytes());
        consumer.accept(valueBlock, valueBlock.getRetainedSizeInBytes());
        consumer.accept(offsets, sizeOf(offsets));
        consumer.accept(mapIsNull, sizeOf(mapIsNull));
        consumer.accept(hashTables, sizeOf(hashTables));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("MapBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    public static MapBlock fromKeyValueBlock(
            boolean[] mapIsNull,
            int[] offsets,
            Block keyBlock,
            Block valueBlock,
            MapType mapType,
            MethodHandle keyBlockNativeEquals,
            MethodHandle keyNativeHashCode,
            MethodHandle keyBlockHashCode)
    {
        if (keyBlock.getPositionCount() != valueBlock.getPositionCount()) {
            throw new IllegalArgumentException(format("keyBlock position count does not match valueBlock position count. %s %s", keyBlock.getPositionCount(), valueBlock.getPositionCount()));
        }
        int elementCount = keyBlock.getPositionCount();
        if (mapIsNull.length != offsets.length - 1) {
            throw new IllegalArgumentException(format("mapIsNull.length-1 does not match offsets.length. %s %s", mapIsNull.length - 1, offsets.length));
        }
        int mapCount = mapIsNull.length;
        if (offsets[mapCount] != elementCount) {
            throw new IllegalArgumentException(format("Last element of offsets does not match keyBlock position count. %s %s", offsets[mapCount], keyBlock.getPositionCount()));
        }
        int[] hashTables = new int[elementCount * HASH_MULTIPLIER];
        Arrays.fill(hashTables, -1);
        for (int i = 0; i < mapCount; i++) {
            int keyOffset = offsets[i];
            int keyCount = offsets[i + 1] - keyOffset;
            if (keyCount < 0) {
                throw new IllegalArgumentException(format("Offset is not monotonically ascending. offsets[%s]=%s, offsets[%s]=%s", i, offsets[i], i + 1, offsets[i + 1]));
            }
            buildHashTable(keyBlock, keyOffset, keyCount, keyBlockHashCode, hashTables, keyOffset * HASH_MULTIPLIER, keyCount * HASH_MULTIPLIER);
        }

        return new MapBlock(
                0,
                mapCount,
                mapIsNull,
                offsets,
                keyBlock,
                valueBlock,
                hashTables,
                mapType.getKeyType(),
                keyBlockNativeEquals,
                keyNativeHashCode);
    }
}
