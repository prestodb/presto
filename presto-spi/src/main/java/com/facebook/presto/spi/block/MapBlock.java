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

import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.block.BlockUtil.intSaturatedCast;
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

    private int sizeInBytes;
    private final int retainedSizeInBytes;

    /**
     * @param keyBlockNativeEquals (T, Block, int)boolean
     * @param keyNativeHashCode (T)long
     */
    public MapBlock(
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
        this.retainedSizeInBytes = intSaturatedCast(
                INSTANCE_SIZE + keyBlock.getRetainedSizeInBytes() + valueBlock.getRetainedSizeInBytes() + sizeOf(offsets) + sizeOf(mapIsNull) + sizeOf(hashTables));
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
    public int getSizeInBytes()
    {
        // this is racy but is safe because sizeInBytes is an int and the calculation is stable
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
                (Integer.BYTES + Byte.BYTES) * this.positionCount +
                Integer.BYTES * HASH_MULTIPLIER * entryCount;
    }

    @Override
    public int getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("MapBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }
}
