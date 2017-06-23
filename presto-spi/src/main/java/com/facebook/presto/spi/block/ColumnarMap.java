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

import static java.util.Objects.requireNonNull;

public class ColumnarMap
{
    private final Block nullCheckBlock;
    private final int offsetsOffset;
    private final int[] offsets;
    private final Block nullSuppressedKeysBlock;
    private final Block nullSuppressedValuesBlock;

    public static ColumnarMap toColumnarMap(Block block)
    {
        requireNonNull(block, "block is null");

        if (block instanceof DictionaryBlock) {
            return toColumnarMap((DictionaryBlock) block);
        }
        if (block instanceof RunLengthEncodedBlock) {
            return toColumnarMap((RunLengthEncodedBlock) block);
        }

        if (!(block instanceof AbstractArrayBlock)) {
            throw new IllegalArgumentException("Invalid map block");
        }

        AbstractArrayBlock arrayBlock = (AbstractArrayBlock) block;
        Block arrayBlockValues = arrayBlock.getValues();
        if (!(arrayBlockValues instanceof AbstractInterleavedBlock)) {
            throw new IllegalArgumentException("Invalid map block");
        }
        AbstractInterleavedBlock interleavedBlock = (AbstractInterleavedBlock) arrayBlockValues;

        // get the keys and values for visible region
        int interleavedBlockOffset = 0;
        int interleavedBlockLength = 0;
        if (arrayBlock.getPositionCount() > 0) {
            interleavedBlockOffset = arrayBlock.getOffset(0);
            interleavedBlockLength = arrayBlock.getOffset(arrayBlock.getPositionCount()) - interleavedBlockOffset;
        }
        Block[] keysAndValues = interleavedBlock.computeSerializableSubBlocks(interleavedBlockOffset, interleavedBlockLength);

        return new ColumnarMap(block, arrayBlock.getOffsetBase(), arrayBlock.getOffsets(), keysAndValues[0], keysAndValues[1]);
    }

    private static ColumnarMap toColumnarMap(DictionaryBlock dictionaryBlock)
    {
        ColumnarMap columnarMap = toColumnarMap(dictionaryBlock.getDictionary());

        // build new offsets
        int[] offsets = new int[dictionaryBlock.getPositionCount() + 1];
        for (int i = 1; i < offsets.length; i++) {
            int dictionaryId = dictionaryBlock.getId(i - 1);
            offsets[i] = offsets[i - 1] + (columnarMap.getLength(dictionaryId) * 2);
        }

        // reindex dictionary
        int[] dictionaryIds = new int[offsets[dictionaryBlock.getPositionCount()] / 2];
        int nextDictionaryIndex = 0;
        for (int i = 1; i < offsets.length; i++) {
            int dictionaryId = dictionaryBlock.getId(i - 1);
            int length = columnarMap.getLength(dictionaryId);

            int startOffset = columnarMap.getOffset(dictionaryId);
            for (int entryIndex = 0; entryIndex < length; entryIndex++) {
                dictionaryIds[nextDictionaryIndex] = startOffset + entryIndex;
                nextDictionaryIndex++;
            }
        }

        return new ColumnarMap(
                dictionaryBlock,
                0,
                offsets,
                new DictionaryBlock(dictionaryIds.length, columnarMap.getNullSuppressedKeysBlock(), dictionaryIds),
                new DictionaryBlock(dictionaryIds.length, columnarMap.getNullSuppressedValuesBlock(), dictionaryIds));
    }

    private static ColumnarMap toColumnarMap(RunLengthEncodedBlock rleBlock)
    {
        ColumnarMap columnarMap = toColumnarMap(rleBlock.getValue());

        // build new offsets block
        int[] offsets = new int[rleBlock.getPositionCount() + 1];
        int valueLength = columnarMap.getLength(0);
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = i * valueLength * 2;
        }

        // create indexes for a dictionary block of the elements
        int[] dictionaryIds = new int[rleBlock.getPositionCount() * valueLength];
        int nextDictionaryIndex = 0;
        for (int i = 1; i < offsets.length; i++) {
            for (int entryIndex = 0; entryIndex < valueLength; entryIndex++) {
                dictionaryIds[nextDictionaryIndex] = entryIndex;
                nextDictionaryIndex++;
            }
        }

        return new ColumnarMap(
                rleBlock,
                0,
                offsets,
                new DictionaryBlock(dictionaryIds.length, columnarMap.getNullSuppressedKeysBlock(), dictionaryIds),
                new DictionaryBlock(dictionaryIds.length, columnarMap.getNullSuppressedValuesBlock(), dictionaryIds));
    }

    private ColumnarMap(Block nullCheckBlock, int offsetsOffset, int[] offsets, Block nullSuppressedKeysBlock, Block nullSuppressedValuesBlock)
    {
        this.nullCheckBlock = nullCheckBlock;
        this.offsetsOffset = offsetsOffset;
        this.offsets = offsets;
        this.nullSuppressedKeysBlock = nullSuppressedKeysBlock;
        this.nullSuppressedValuesBlock = nullSuppressedValuesBlock;
    }

    public int getPositionCount()
    {
        return nullCheckBlock.getPositionCount();
    }

    public boolean isNull(int position)
    {
        return nullCheckBlock.isNull(position);
    }

    public int getLength(int position)
    {
        return (offsets[position + 1 + offsetsOffset] - offsets[position + offsetsOffset]) / 2;
    }

    private int getOffset(int position)
    {
        return offsets[position + offsetsOffset] / 2;
    }

    public Block getNullSuppressedKeysBlock()
    {
        return nullSuppressedKeysBlock;
    }

    public Block getNullSuppressedValuesBlock()
    {
        return nullSuppressedValuesBlock;
    }
}
