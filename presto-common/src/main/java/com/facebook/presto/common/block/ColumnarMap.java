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

import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class ColumnarMap
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ColumnarMap.class).instanceSize();

    private final Block nullCheckBlock;
    private final int offsetsOffset;
    private final int[] offsets;
    private final Block keysBlock;
    private final Block valuesBlock;
    private final int[] hashTables;
    private final long retainedSizeInBytes;
    private final long estimatedSerializedSizeInBytes;

    public static ColumnarMap toColumnarMap(Block block)
    {
        requireNonNull(block, "block is null");

        if (block instanceof DictionaryBlock) {
            return toColumnarMap((DictionaryBlock) block);
        }
        if (block instanceof RunLengthEncodedBlock) {
            return toColumnarMap((RunLengthEncodedBlock) block);
        }

        if (!(block instanceof AbstractMapBlock)) {
            throw new IllegalArgumentException("Invalid map block: " + block.getClass().getName());
        }

        AbstractMapBlock mapBlock = (AbstractMapBlock) block;

        int offsetBase = mapBlock.getOffsetBase();
        int[] offsets = mapBlock.getOffsets();

        // get the keys and values for visible region
        int firstEntryPosition = mapBlock.getOffset(0);
        int totalEntryCount = mapBlock.getOffset(block.getPositionCount()) - firstEntryPosition;
        Block keysBlock = mapBlock.getRawKeyBlock().getRegion(firstEntryPosition, totalEntryCount);
        Block valuesBlock = mapBlock.getRawValueBlock().getRegion(firstEntryPosition, totalEntryCount);
        int[] hashTables = mapBlock.getHashTables().get();

        return new ColumnarMap(
                block,
                offsetBase,
                offsets,
                keysBlock,
                valuesBlock,
                hashTables,
                INSTANCE_SIZE + block.getRetainedSizeInBytes(),
                block.getSizeInBytes());
    }

    private static ColumnarMap toColumnarMap(DictionaryBlock dictionaryBlock)
    {
        ColumnarMap columnarMap = toColumnarMap(dictionaryBlock.getDictionary());
        int positionCount = dictionaryBlock.getPositionCount();

        // build new offsets
        int[] offsets = new int[positionCount + 1];
        for (int position = 0; position < positionCount; position++) {
            int dictionaryId = dictionaryBlock.getId(position);
            offsets[position + 1] = offsets[position] + columnarMap.getEntryCount(dictionaryId);
        }

        // reindex dictionary
        int[] dictionaryIds = new int[offsets[positionCount]];
        int nextDictionaryIndex = 0;
        for (int position = 0; position < positionCount; position++) {
            int dictionaryId = dictionaryBlock.getId(position);
            int entryCount = columnarMap.getEntryCount(dictionaryId);

            int startOffset = columnarMap.getOffset(dictionaryId);
            for (int entryIndex = 0; entryIndex < entryCount; entryIndex++) {
                dictionaryIds[nextDictionaryIndex] = startOffset + entryIndex;
                nextDictionaryIndex++;
            }
        }

        Block keysBlock = columnarMap.getKeysBlock();
        Block valuesBlock = columnarMap.getValuesBlock();
        return new ColumnarMap(
                dictionaryBlock,
                0,
                offsets,
                new DictionaryBlock(dictionaryIds.length, keysBlock, dictionaryIds),
                new DictionaryBlock(dictionaryIds.length, valuesBlock, dictionaryIds),
                columnarMap.getHashTables(),
                INSTANCE_SIZE + dictionaryBlock.getRetainedSizeInBytes() + sizeOf(offsets) + sizeOf(dictionaryIds),
                // The estimated serialized size is the sum of the following
                // 1) the offsets size: Integer.BYTES * positionCount
                // 2) nulls array size: Byte.BYTES * positionCount
                // 3) the estimated serialized size for the keysBlock and valuesBlock which were just constructed as new DictionaryBlocks:
                //     the average row size: keysBlock.getSizeInBytes() / (double) keysBlock.getPositionCount() + valuesBlock.getSizeInBytes() / (double) valuesBlock.getPositionCount() * the number of rows: offsets[positionCount]
                (Integer.BYTES + Byte.BYTES) * positionCount +
                        (long) ((keysBlock.getPositionCount() == 0 ? 0 : keysBlock.getSizeInBytes() / (double) keysBlock.getPositionCount() + valuesBlock.getPositionCount() == 0 ? 0 : valuesBlock.getSizeInBytes() / (double) valuesBlock.getPositionCount()) * offsets[positionCount]));
    }

    private static ColumnarMap toColumnarMap(RunLengthEncodedBlock rleBlock)
    {
        ColumnarMap columnarMap = toColumnarMap(rleBlock.getValue());
        int positionCount = rleBlock.getPositionCount();

        // build new offsets block
        int[] offsets = new int[positionCount + 1];
        int entryCount = columnarMap.getEntryCount(0);
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = i * entryCount;
        }

        // create indexes for a dictionary block of the elements
        int[] dictionaryIds = new int[positionCount * entryCount];
        int nextDictionaryIndex = 0;
        for (int position = 0; position < positionCount; position++) {
            for (int entryIndex = 0; entryIndex < entryCount; entryIndex++) {
                dictionaryIds[nextDictionaryIndex] = entryIndex;
                nextDictionaryIndex++;
            }
        }

        Block keysBlock = columnarMap.getKeysBlock();
        Block valuesBlock = columnarMap.getValuesBlock();
        return new ColumnarMap(
                rleBlock,
                0,
                offsets,
                new DictionaryBlock(dictionaryIds.length, keysBlock, dictionaryIds),
                new DictionaryBlock(dictionaryIds.length, valuesBlock, dictionaryIds),
                columnarMap.getHashTables(),
                INSTANCE_SIZE + rleBlock.getRetainedSizeInBytes() + sizeOf(offsets) + sizeOf(dictionaryIds),
                // The estimated serialized size is the sum of the following
                // 1) the offsets size: Integer.BYTES * positionCount
                // 2) nulls array size: Byte.BYTES * positionCount
                // 3) the estimated serialized size for the keysBlock and valuesBlock which were just constructed as new DictionaryBlocks:
                //     the average row size: keysBlock.getSizeInBytes() / (double) keysBlock.getPositionCount() + valuesBlock.getSizeInBytes() / (double) valuesBlock.getPositionCount() * the number of rows: offsets[positionCount]
                (Integer.BYTES + Byte.BYTES) * positionCount + (long) ((keysBlock.getSizeInBytes() / (double) keysBlock.getPositionCount() + valuesBlock.getSizeInBytes() / (double) valuesBlock.getPositionCount()) * offsets[positionCount]));
    }

    private ColumnarMap(Block nullCheckBlock, int offsetsOffset, int[] offsets, Block keysBlock, Block valuesBlock, @Nullable int[] hashTables, long retainedSizeInBytes, long estimatedSerializedSizeInBytes)
    {
        this.nullCheckBlock = nullCheckBlock;
        this.offsetsOffset = offsetsOffset;
        this.offsets = offsets;
        this.keysBlock = keysBlock;
        this.valuesBlock = valuesBlock;
        this.hashTables = hashTables;
        this.retainedSizeInBytes = retainedSizeInBytes;
        this.estimatedSerializedSizeInBytes = estimatedSerializedSizeInBytes;
    }

    public int getPositionCount()
    {
        return nullCheckBlock.getPositionCount();
    }

    public boolean isNull(int position)
    {
        return nullCheckBlock.isNull(position);
    }

    public int getEntryCount(int position)
    {
        return (offsets[position + 1 + offsetsOffset] - offsets[position + offsetsOffset]);
    }

    public int getOffset(int position)
    {
        return (offsets[position + offsetsOffset] - offsets[offsetsOffset]);
    }

    public int getAbsoluteOffset(int position)
    {
        return offsets[position + offsetsOffset];
    }

    public Block getKeysBlock()
    {
        return keysBlock;
    }

    public Block getValuesBlock()
    {
        return valuesBlock;
    }

    public Block getNullCheckBlock()
    {
        return nullCheckBlock;
    }

    @Nullable
    public int[] getHashTables()
    {
        return hashTables;
    }

    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    public long getEstimatedSerializedSizeInBytes()
    {
        return estimatedSerializedSizeInBytes;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append("{");
        sb.append("positionCount=").append(getPositionCount()).append(",");
        sb.append("offsetsOffset=").append(offsetsOffset).append(",");
        sb.append("nullCheckBlock=").append(nullCheckBlock.toString()).append(",");
        sb.append("keysBlock=").append(keysBlock.toString()).append(",");
        sb.append("valuesBlock=").append(valuesBlock.toString()).append(",");
        sb.append("hashTablesSize=").append(hashTables == null ? 0 : hashTables.length).append(",");
        sb.append('}');
        return sb.toString();
    }
}
