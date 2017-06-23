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

public class ColumnarArray
{
    private final Block nullCheckBlock;
    private final int offsetsOffset;
    private final int[] offsets;
    private final Block nullSuppressedElementsBlock;

    public static ColumnarArray toColumnarArray(Block block)
    {
        requireNonNull(block, "block is null");

        if (block instanceof DictionaryBlock) {
            return toColumnarArray((DictionaryBlock) block);
        }
        if (block instanceof RunLengthEncodedBlock) {
            return toColumnarArray((RunLengthEncodedBlock) block);
        }

        if (!(block instanceof AbstractArrayBlock)) {
            throw new IllegalArgumentException("Invalid row block");
        }

        AbstractArrayBlock arrayBlock = (AbstractArrayBlock) block;
        Block elementsBlock = arrayBlock.getValues();

        // trim elements to just visible region
        int elementsOffset = 0;
        int elementsLength = 0;
        if (arrayBlock.getPositionCount() > 0) {
            elementsOffset = arrayBlock.getOffset(0);
            elementsLength = arrayBlock.getOffset(arrayBlock.getPositionCount()) - elementsOffset;
        }
        elementsBlock = elementsBlock.getRegion(elementsOffset, elementsLength);

        return new ColumnarArray(block, arrayBlock.getOffsetBase(), arrayBlock.getOffsets(), elementsBlock);
    }

    private static ColumnarArray toColumnarArray(DictionaryBlock dictionaryBlock)
    {
        ColumnarArray columnarMap = toColumnarArray(dictionaryBlock.getDictionary());

        // build new offsets
        int[] offsets = new int[dictionaryBlock.getPositionCount() + 1];
        for (int i = 1; i < offsets.length; i++) {
            int dictionaryId = dictionaryBlock.getId(i - 1);
            offsets[i] = offsets[i - 1] + columnarMap.getLength(dictionaryId);
        }

        // reindex dictionary
        int[] dictionaryIds = new int[offsets[dictionaryBlock.getPositionCount()]];
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

        return new ColumnarArray(
                dictionaryBlock,
                0,
                offsets,
                new DictionaryBlock(dictionaryIds.length, columnarMap.getNullSuppressedElementsBlock(), dictionaryIds));
    }

    private static ColumnarArray toColumnarArray(RunLengthEncodedBlock rleBlock)
    {
        Block rleValue = rleBlock.getValue();

        if (rleValue.isNull(0) || rleBlock.getPositionCount() == 0) {
            return new ColumnarArray(
                    rleBlock,
                    0,
                    new int[rleBlock.getPositionCount() + 1],
                    new RunLengthEncodedBlock(rleValue, 0));
        }

        ColumnarArray columnarMap = toColumnarArray(rleValue);

        // build new offsets block
        int[] offsets = new int[rleBlock.getPositionCount() + 1];
        int valueLength = columnarMap.getLength(0);
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = i * valueLength;
        }

        // create indexes for a dictionary block of the elements
        int[] elementDictionaryIds = new int[rleBlock.getPositionCount() * valueLength];
        int elementDictionaryIndex = 0;
        for (int i = 1; i < offsets.length; i++) {
            for (int entryIndex = 0; entryIndex < valueLength; entryIndex++) {
                elementDictionaryIds[elementDictionaryIndex] = entryIndex;
                elementDictionaryIndex++;
            }
        }

        // if row is null then produce no rows since we are suppressing null rows
        return new ColumnarArray(
                rleBlock,
                0,
                offsets,
                new DictionaryBlock(elementDictionaryIds.length, columnarMap.getNullSuppressedElementsBlock(), elementDictionaryIds));
    }

    private ColumnarArray(Block nullCheckBlock, int offsetsOffset, int[] offsets, Block nullSuppressedElementsBlock)
    {
        this.nullCheckBlock = nullCheckBlock;
        this.offsetsOffset = offsetsOffset;
        this.offsets = offsets;
        this.nullSuppressedElementsBlock = nullSuppressedElementsBlock;
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
        return getOffset(position + 1) - getOffset(position);
    }

    private int getOffset(int position)
    {
        return offsets[position + offsetsOffset];
    }

    public Block getNullSuppressedElementsBlock()
    {
        return nullSuppressedElementsBlock;
    }
}
