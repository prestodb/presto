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

public class BlockBuilderUtils
{
    private BlockBuilderUtils()
    {
        // utility class
    }

    public static void writePositionToBlockBuilder(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block instanceof DictionaryBlock) {
            position = ((DictionaryBlock) block).getId(position);
            block = ((DictionaryBlock) block).getDictionary();
        }

        if (blockBuilder instanceof MapBlockBuilder) {
            writePositionToMapBuilder(block, position, (MapBlockBuilder) blockBuilder);
        }
        else if (blockBuilder instanceof ArrayBlockBuilder) {
            writePositionToArrayBuilder(block, position, (ArrayBlockBuilder) blockBuilder);
        }
        else if (blockBuilder instanceof RowBlockBuilder) {
            writePositionToRowBuilder(block, position, (RowBlockBuilder) blockBuilder);
        }
        else {
            block.writePositionTo(position, blockBuilder);
        }
    }

    public static void writePositionToMapBuilder(Block block, int position, MapBlockBuilder mapBlockBuilder)
    {
        if (!(block instanceof AbstractMapBlock)) {
            throw new IllegalArgumentException("Expected AbstractMapBlock");
        }

        mapBlockBuilder.beginBlockEntry();
        BlockBuilder keyBlockBuilder = mapBlockBuilder.getKeyBlockBuilder();
        BlockBuilder valueBlockBuilder = mapBlockBuilder.getValueBlockBuilder();

        AbstractMapBlock mapBlock = (AbstractMapBlock) block;
        int startOffset = mapBlock.getOffset(position);
        int endOffset = mapBlock.getOffset(position + 1);

        Block keyBlock = mapBlock.getRawKeyBlock();
        Block valueBlock = mapBlock.getRawValueBlock();

        for (int i = startOffset; i < endOffset; i++) {
            if (keyBlock.isNull(i)) {
                keyBlockBuilder.appendNull();
            }
            else {
                writePositionToBlockBuilder(keyBlock, i, keyBlockBuilder);
            }
        }

        for (int i = startOffset; i < endOffset; i++) {
            if (valueBlock.isNull(i)) {
                valueBlockBuilder.appendNull();
            }
            else {
                writePositionToBlockBuilder(valueBlock, i, valueBlockBuilder);
            }
        }
        mapBlockBuilder.closeEntry();
    }

    private static void writePositionToArrayBuilder(Block block, int position, ArrayBlockBuilder arrayBlockBuilder)
    {
        if (!(block instanceof AbstractArrayBlock)) {
            throw new IllegalArgumentException("Expected AbstractArrayBlock");
        }

        arrayBlockBuilder.beginBlockEntry();
        BlockBuilder elementBlockBuilder = arrayBlockBuilder.getElementBlockBuilder();

        AbstractArrayBlock arrayBlock = (AbstractArrayBlock) block;
        int startOffset = arrayBlock.getOffset(position);
        int endOffset = arrayBlock.getOffset(position + 1);

        Block elementBlock = arrayBlock.getRawElementBlock();

        for (int i = startOffset; i < endOffset; i++) {
            if (elementBlock.isNull(i)) {
                elementBlockBuilder.appendNull();
            }
            else {
                writePositionToBlockBuilder(elementBlock, i, elementBlockBuilder);
            }
        }
        arrayBlockBuilder.closeEntry();
    }

    private static void writePositionToRowBuilder(Block block, int position, RowBlockBuilder rowBlockBuilder)
    {
        if (!(block instanceof AbstractRowBlock)) {
            throw new IllegalArgumentException("Expected AbstractRowBlock");
        }

        rowBlockBuilder.beginBlockEntry();
        AbstractRowBlock rowBlock = (AbstractRowBlock) block;

        int offset = rowBlock.getFieldBlockOffset(position);
        for (int fieldIndex = 0; fieldIndex < rowBlock.numFields; fieldIndex++) {
            BlockBuilder fieldBlockBuilder = rowBlockBuilder.getBlockBuilder(fieldIndex);
            Block fieldBlock = rowBlock.getRawFieldBlocks()[fieldIndex];
            if (fieldBlock.isNull(offset)) {
                fieldBlockBuilder.appendNull();
            }
            else {
                writePositionToBlockBuilder(fieldBlock, offset, fieldBlockBuilder);
            }
        }
        rowBlockBuilder.closeEntry();
    }
}
