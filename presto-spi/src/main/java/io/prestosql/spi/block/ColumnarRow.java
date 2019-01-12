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
package io.prestosql.spi.block;

import static java.util.Objects.requireNonNull;

public final class ColumnarRow
{
    private final Block nullCheckBlock;
    private final Block[] fields;

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

        return new ColumnarRow(block, fieldBlocks);
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
        int[] dictionaryIds = new int[dictionaryBlock.getPositionCount()];
        int nonNullPositionCount = 0;
        for (int position = 0; position < dictionaryBlock.getPositionCount(); position++) {
            if (!dictionaryBlock.isNull(position)) {
                int oldDictionaryId = dictionaryBlock.getId(position);
                dictionaryIds[nonNullPositionCount] = newDictionaryIndex[oldDictionaryId];
                nonNullPositionCount++;
            }
        }

        ColumnarRow columnarRow = toColumnarRow(dictionaryBlock.getDictionary());
        Block[] fields = new Block[columnarRow.getFieldCount()];
        for (int i = 0; i < columnarRow.getFieldCount(); i++) {
            fields[i] = new DictionaryBlock(nonNullPositionCount, columnarRow.getField(i), dictionaryIds);
        }
        return new ColumnarRow(dictionaryBlock, fields);
    }

    private static ColumnarRow toColumnarRow(RunLengthEncodedBlock rleBlock)
    {
        Block rleValue = rleBlock.getValue();
        ColumnarRow columnarRow = toColumnarRow(rleValue);

        Block[] fields = new Block[columnarRow.getFieldCount()];
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
                fields[i] = new RunLengthEncodedBlock(nullSuppressedField, rleBlock.getPositionCount());
            }
        }
        return new ColumnarRow(rleBlock, fields);
    }

    private ColumnarRow(Block nullCheckBlock, Block[] fields)
    {
        this.nullCheckBlock = nullCheckBlock;
        this.fields = fields.clone();
    }

    public int getPositionCount()
    {
        return nullCheckBlock.getPositionCount();
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
}
