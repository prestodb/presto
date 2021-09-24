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

import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public final class ColumnarRow
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ColumnarRow.class).instanceSize();

    private final Block nullCheckBlock;
    private final Block[] fields;
    private final long retainedSizeInBytes;
    private final long estimatedSerializedSizeInBytes;

    public static ColumnarRow toColumnarRow(Block block)
    {
        requireNonNull(block, "block is null");

        if (block instanceof DictionaryBlock) {
            return toColumnarRow((DictionaryBlock) block);
        }
        if (block instanceof RunLengthEncodedBlock) {
            return toColumnarRow((RunLengthEncodedBlock) block);
        }

        if (!(block instanceof AbstractRowBlock)) {
            throw new IllegalArgumentException("Invalid row block: " + block.getClass().getName());
        }

        AbstractRowBlock rowBlock = (AbstractRowBlock) block;

        // get fields for visible region
        int firstRowPosition = rowBlock.getFieldBlockOffset(0);
        int totalRowCount = rowBlock.getFieldBlockOffset(block.getPositionCount()) - firstRowPosition;
        Block[] fieldBlocks = new Block[rowBlock.numFields];
        for (int i = 0; i < fieldBlocks.length; i++) {
            fieldBlocks[i] = rowBlock.getRawFieldBlocks()[i].getRegion(firstRowPosition, totalRowCount);
        }

        return new ColumnarRow(block, fieldBlocks, block.getRetainedSizeInBytes(), block.getSizeInBytes());
    }

    private static ColumnarRow toColumnarRow(DictionaryBlock dictionaryBlock)
    {
        // build a mapping from the old dictionary to a new dictionary with nulls removed
        Block dictionary = dictionaryBlock.getDictionary();
        int[] newDictionaryIndex = new int[dictionary.getPositionCount()];
        int nextNewDictionaryIndex = 0;
        for (int position = 0; position < dictionary.getPositionCount(); position++) {
            if (!dictionary.isNull(position)) {
                newDictionaryIndex[position] = nextNewDictionaryIndex;
                nextNewDictionaryIndex++;
            }
        }

        // reindex the dictionary
        int positionCount = dictionaryBlock.getPositionCount();
        int[] dictionaryIds = new int[positionCount];
        int nonNullPositionCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (!dictionaryBlock.isNull(position)) {
                int oldDictionaryId = dictionaryBlock.getId(position);
                dictionaryIds[nonNullPositionCount] = newDictionaryIndex[oldDictionaryId];
                nonNullPositionCount++;
            }
        }

        ColumnarRow columnarRow = toColumnarRow(dictionary);
        Block[] fields = new Block[columnarRow.getFieldCount()];
        long averageRowSize = 0;
        for (int i = 0; i < columnarRow.getFieldCount(); i++) {
            Block field = columnarRow.getField(i);
            fields[i] = new DictionaryBlock(nonNullPositionCount, field, dictionaryIds);
            averageRowSize += field.getPositionCount() == 0 ? 0 : field.getSizeInBytes() / field.getPositionCount();
        }
        return new ColumnarRow(
                dictionaryBlock,
                fields,
                INSTANCE_SIZE + dictionaryBlock.getRetainedSizeInBytes() + sizeOf(dictionaryIds),
                // The estimated serialized size is the sum of the following:
                // 1) the offsets size: Integer.BYTES * positionCount. Note that even though ColumnarRow doesn't have the offsets array, the serialized RowBlock still has it. Please see RowBlockEncodingBuffer.
                // 2) nulls array size: Byte.BYTES * positionCount
                // 3) the estimated serialized size for the fields Blocks which were just constructed as new DictionaryBlocks:
                //     the average row size: averageRowSize * the number of rows: nonNullPositionCount
                (Integer.BYTES + Byte.BYTES) * positionCount + averageRowSize * nonNullPositionCount);
    }

    private static ColumnarRow toColumnarRow(RunLengthEncodedBlock rleBlock)
    {
        Block rleValue = rleBlock.getValue();
        int positionCount = rleBlock.getPositionCount();
        ColumnarRow columnarRow = toColumnarRow(rleValue);

        Block[] fields = new Block[columnarRow.getFieldCount()];
        long averageRowSize = 0;
        for (int i = 0; i < columnarRow.getFieldCount(); i++) {
            Block nullSuppressedField = columnarRow.getField(i);
            if (rleValue.isNull(0)) {
                // the rle value is a null row so, all null-suppressed fields should empty
                if (nullSuppressedField.getPositionCount() != 0) {
                    throw new IllegalArgumentException("Invalid row block");
                }
                fields[i] = nullSuppressedField;
            }
            else {
                fields[i] = new RunLengthEncodedBlock(nullSuppressedField, positionCount);
                averageRowSize += nullSuppressedField.getSizeInBytes() / nullSuppressedField.getPositionCount();
            }
        }
        return new ColumnarRow(
                rleBlock,
                fields,
                INSTANCE_SIZE + rleBlock.getRetainedSizeInBytes(),
                // The estimated serialized size is the sum of the following:
                // 1) the offsets size: Integer.BYTES * positionCount. Note that even though ColumnarRow doesn't have the offsets array, the serialized RowBlock still has it. Please see RowBlockEncodingBuffer.
                // 2) nulls array size: Byte.BYTES * positionCount
                // 3) the estimated serialized size for the fields Blocks which were just constructed as new RunLengthEncodedBlocks:
                //     the average row size: averageRowSize * the number of rows: positionCount
                (Integer.BYTES + Byte.BYTES) * positionCount + averageRowSize * positionCount);
    }

    private ColumnarRow(Block nullCheckBlock, Block[] fields, long retainedSizeInBytes, long estimatedSerializedSizeInBytes)
    {
        this.nullCheckBlock = nullCheckBlock;
        this.fields = fields.clone();
        this.retainedSizeInBytes = retainedSizeInBytes;
        this.estimatedSerializedSizeInBytes = estimatedSerializedSizeInBytes;
    }

    public int getPositionCount()
    {
        return nullCheckBlock.getPositionCount();
    }

    public int getNonNullPositionCount()
    {
        if (!nullCheckBlock.mayHaveNull()) {
            return getPositionCount();
        }

        int count = 0;
        for (int i = 0; i < getPositionCount(); i++) {
            if (!isNull(i)) {
                count++;
            }
        }
        return count;
    }

    public boolean isNull(int position)
    {
        return nullCheckBlock.isNull(position);
    }

    public int getFieldCount()
    {
        return fields.length;
    }

    /**
     * Gets the specified field for all rows as a column.
     * <p>
     * Note: A null row will not have an entry in the block, so the block
     * will be the size of the non-null rows.  This block may still contain
     * null values when the row is non-null but the field value is null.
     */
    public Block getField(int index)
    {
        return fields[index];
    }

    public int getOffset(int position)
    {
        return ((AbstractRowBlock) nullCheckBlock).getFieldBlockOffset(position);
    }

    public Block getNullCheckBlock()
    {
        return nullCheckBlock;
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
        sb.append("fieldsCount=").append(fields.length).append(",");
        for (int i = 0; i < fields.length; i++) {
            sb.append("field_").append(i).append("=").append(fields[i].toString()).append(",");
        }
        sb.append('}');
        return sb.toString();
    }
}
