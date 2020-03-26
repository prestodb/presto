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

import org.openjdk.jol.info.ClassLayout;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class ColumnarArray
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ColumnarArray.class).instanceSize();

    private final Block nullCheckBlock;
    private final int offsetsOffset;
    private final int[] offsets;
    private final Block elementsBlock;
    private final long retainedSizeInBytes;

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
            throw new IllegalArgumentException("Invalid array block: " + block.getClass().getName());
        }

        AbstractArrayBlock arrayBlock = (AbstractArrayBlock) block;
        Block elementsBlock = arrayBlock.getRawElementBlock();

        // trim elements to just visible region
        int elementsOffset = 0;
        int elementsLength = 0;
        if (arrayBlock.getPositionCount() > 0) {
            elementsOffset = arrayBlock.getOffset(0);
            elementsLength = arrayBlock.getOffset(arrayBlock.getPositionCount()) - elementsOffset;
        }
        elementsBlock = elementsBlock.getRegion(elementsOffset, elementsLength);

        return new ColumnarArray(block, arrayBlock.getOffsetBase(), arrayBlock.getOffsets(), elementsBlock, INSTANCE_SIZE + block.getRetainedSizeInBytes());
    }

    private static ColumnarArray toColumnarArray(DictionaryBlock dictionaryBlock)
    {
        ColumnarArray columnarArray = toColumnarArray(dictionaryBlock.getDictionary());

        // build new offsets
        int[] offsets = new int[dictionaryBlock.getPositionCount() + 1];
        for (int position = 0; position < dictionaryBlock.getPositionCount(); position++) {
            int dictionaryId = dictionaryBlock.getId(position);
            offsets[position + 1] = offsets[position] + columnarArray.getLength(dictionaryId);
        }

        // reindex dictionary
        int[] dictionaryIds = new int[offsets[dictionaryBlock.getPositionCount()]];
        int nextDictionaryIndex = 0;
        for (int position = 0; position < dictionaryBlock.getPositionCount(); position++) {
            int dictionaryId = dictionaryBlock.getId(position);
            int length = columnarArray.getLength(dictionaryId);

            int startOffset = columnarArray.getOffset(dictionaryId);
            for (int entryIndex = 0; entryIndex < length; entryIndex++) {
                dictionaryIds[nextDictionaryIndex] = startOffset + entryIndex;
                nextDictionaryIndex++;
            }
        }

        return new ColumnarArray(
                dictionaryBlock,
                0,
                offsets,
                new DictionaryBlock(dictionaryIds.length, columnarArray.getElementsBlock(), dictionaryIds),
                INSTANCE_SIZE + dictionaryBlock.getRetainedSizeInBytes() + sizeOf(offsets) + sizeOf(dictionaryIds));
    }

    private static ColumnarArray toColumnarArray(RunLengthEncodedBlock rleBlock)
    {
        ColumnarArray columnarArray = toColumnarArray(rleBlock.getValue());

        // build new offsets block
        int[] offsets = new int[rleBlock.getPositionCount() + 1];
        int valueLength = columnarArray.getLength(0);
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = i * valueLength;
        }

        // create indexes for a dictionary block of the elements
        int[] dictionaryIds = new int[rleBlock.getPositionCount() * valueLength];
        int nextDictionaryIndex = 0;
        for (int position = 0; position < rleBlock.getPositionCount(); position++) {
            for (int entryIndex = 0; entryIndex < valueLength; entryIndex++) {
                dictionaryIds[nextDictionaryIndex] = entryIndex;
                nextDictionaryIndex++;
            }
        }

        return new ColumnarArray(
                rleBlock,
                0,
                offsets,
                new DictionaryBlock(dictionaryIds.length, columnarArray.getElementsBlock(), dictionaryIds),
                INSTANCE_SIZE + rleBlock.getRetainedSizeInBytes() + sizeOf(offsets) + sizeOf(dictionaryIds));
    }

    private ColumnarArray(Block nullCheckBlock, int offsetsOffset, int[] offsets, Block elementsBlock, long retainedSizeInBytes)
    {
        this.nullCheckBlock = nullCheckBlock;
        this.offsetsOffset = offsetsOffset;
        this.offsets = offsets;
        this.elementsBlock = elementsBlock;
        this.retainedSizeInBytes = retainedSizeInBytes;
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
        return (offsets[position + 1 + offsetsOffset] - offsets[position + offsetsOffset]);
    }

    public int getOffset(int position)
    {
        return (offsets[position + offsetsOffset] - offsets[offsetsOffset]);
    }

    public Block getElementsBlock()
    {
        return elementsBlock;
    }

    public Block getNullCheckBlock()
    {
        return nullCheckBlock;
    }

    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append("{");
        sb.append("positionCount=").append(getPositionCount()).append(",");
        sb.append("offsetsOffset=").append(offsetsOffset).append(",");
        sb.append("nullCheckBlock=").append(nullCheckBlock.toString()).append(",");
        sb.append("elementsBlock=").append(elementsBlock.toString()).append(",");
        sb.append('}');
        return sb.toString();
    }
}
