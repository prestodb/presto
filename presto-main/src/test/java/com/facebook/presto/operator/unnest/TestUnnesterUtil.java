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
package com.facebook.presto.operator.unnest;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.base.Verify.verify;
import static java.lang.Integer.max;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestUnnesterUtil
{
    private TestUnnesterUtil() {}

    static int[] calculateMaxCardinalities(Page page, List<Type> replicatedTypes, List<Type> unnestTypes)
    {
        int positionCount = page.getPositionCount();
        int[] maxCardinalities = new int[positionCount];

        int replicatedChannelCount = replicatedTypes.size();
        int unnestChannelCount = unnestTypes.size();
        for (int i = 0; i < unnestChannelCount; i++) {
            Type type = unnestTypes.get(i);
            Block block = page.getBlock(replicatedChannelCount + i);
            assertTrue(type instanceof ArrayType || type instanceof MapType);

            if (type instanceof ArrayType) {
                ColumnarArray columnarArray = ColumnarArray.toColumnarArray(block);
                for (int j = 0; j < positionCount; j++) {
                    maxCardinalities[j] = max(maxCardinalities[j], columnarArray.getLength(j));
                }
            }
            else if (type instanceof MapType) {
                ColumnarMap columnarMap = ColumnarMap.toColumnarMap(block);
                for (int j = 0; j < positionCount; j++) {
                    maxCardinalities[j] = max(maxCardinalities[j], columnarMap.getEntryCount(j));
                }
            }
            else {
                fail("expected an ArrayType or MapType, but found " + type);
            }
        }

        return maxCardinalities;
    }

    static Page buildExpectedPage(
            Page page,
            List<Type> replicatedTypes,
            List<Type> unnestTypes,
            List<Type> outputTypes,
            int[] maxCardinalities,
            boolean withOrdinality,
            boolean legacyUnnest)
    {
        int totalEntries = IntStream.of(maxCardinalities).sum();

        int channelCount = page.getChannelCount();
        assertTrue(channelCount > 1);

        Block[] outputBlocks = new Block[outputTypes.size()];

        int outputChannel = 0;

        for (int i = 0; i < replicatedTypes.size(); i++) {
            outputBlocks[outputChannel++] = buildExpectedReplicatedBlock(page.getBlock(i), replicatedTypes.get(i), maxCardinalities, totalEntries);
        }

        for (int i = 0; i < unnestTypes.size(); i++) {
            Type type = unnestTypes.get(i);
            Block inputBlock = page.getBlock(replicatedTypes.size() + i);

            if (type instanceof ArrayType) {
                Type elementType = ((ArrayType) type).getElementType();
                if (elementType instanceof RowType && !legacyUnnest) {
                    List<Type> rowTypes = ((RowType) elementType).getTypeParameters();
                    Block[] blocks = buildExpectedUnnestedArrayOfRowBlock(inputBlock, rowTypes, maxCardinalities, totalEntries);
                    for (Block block : blocks) {
                        outputBlocks[outputChannel++] = block;
                    }
                }
                else {
                    outputBlocks[outputChannel++] = buildExpectedUnnestedArrayBlock(inputBlock, ((ArrayType) unnestTypes.get(i)).getElementType(), maxCardinalities, totalEntries);
                }
            }
            else if (type instanceof MapType) {
                MapType mapType = (MapType) unnestTypes.get(i);
                Block[] blocks = buildExpectedUnnestedMapBlocks(inputBlock, mapType.getKeyType(), mapType.getValueType(), maxCardinalities, totalEntries);
                for (Block block : blocks) {
                    outputBlocks[outputChannel++] = block;
                }
            }
            else {
                fail("expected an ArrayType or MapType, but found " + type);
            }
        }

        if (withOrdinality) {
            outputBlocks[outputChannel++] = buildExpectedOrdinalityBlock(maxCardinalities, totalEntries);
        }

        return new Page(outputBlocks);
    }

    static List<Type> buildOutputTypes(List<Type> replicatedTypes, List<Type> unnestTypes, boolean withOrdinality, boolean legacyUnnest)
    {
        List<Type> outputTypes = new ArrayList<>();

        for (Type replicatedType : replicatedTypes) {
            outputTypes.add(replicatedType);
        }

        for (Type unnestType : unnestTypes) {
            if (unnestType instanceof ArrayType) {
                Type elementType = ((ArrayType) unnestType).getElementType();
                if (elementType instanceof RowType && !legacyUnnest) {
                    List<Type> rowTypes = ((RowType) elementType).getTypeParameters();
                    for (Type rowType : rowTypes) {
                        outputTypes.add(rowType);
                    }
                }
                else {
                    outputTypes.add(elementType);
                }
            }
            else if (unnestType instanceof MapType) {
                outputTypes.add(((MapType) unnestType).getKeyType());
                outputTypes.add(((MapType) unnestType).getValueType());
            }
        }

        if (withOrdinality) {
            outputTypes.add(BIGINT);
        }

        return outputTypes;
    }

    static Page mergePages(List<Type> types, List<Page> pages)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        int totalPositionCount = 0;
        for (Page page : pages) {
            verify(page.getChannelCount() == types.size(), format("Number of channels in page %d is not equal to number of types %d", page.getChannelCount(), types.size()));

            for (int i = 0; i < types.size(); i++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                Block block = page.getBlock(i);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        block.writePositionTo(position, blockBuilder);
                    }
                }
            }
            totalPositionCount += page.getPositionCount();
        }
        pageBuilder.declarePositions(totalPositionCount);
        return pageBuilder.build();
    }

    private static Block buildExpectedReplicatedBlock(Block block, Type type, int[] maxCardinalities, int totalEntries)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, totalEntries);
        int positionCount = block.getPositionCount();
        for (int i = 0; i < positionCount; i++) {
            int cardinality = maxCardinalities[i];
            for (int j = 0; j < cardinality; j++) {
                type.appendTo(block, i, blockBuilder);
            }
        }
        return blockBuilder.build();
    }

    private static Block buildExpectedUnnestedArrayBlock(Block block, Type type, int[] maxCardinalities, int totalEntries)
    {
        ColumnarArray columnarArray = ColumnarArray.toColumnarArray(block);
        Block elementBlock = columnarArray.getElementsBlock();

        BlockBuilder blockBuilder = type.createBlockBuilder(null, totalEntries);
        int positionCount = block.getPositionCount();
        int elementBlockPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int cardinality = columnarArray.getLength(i);
            for (int j = 0; j < cardinality; j++) {
                type.appendTo(elementBlock, elementBlockPosition++, blockBuilder);
            }

            int maxCardinality = maxCardinalities[i];
            for (int j = cardinality; j < maxCardinality; j++) {
                blockBuilder.appendNull();
            }
        }
        return blockBuilder.build();
    }

    private static Block[] buildExpectedUnnestedMapBlocks(Block block, Type keyType, Type valueType, int[] maxCardinalities, int totalEntries)
    {
        ColumnarMap columnarMap = ColumnarMap.toColumnarMap(block);
        Block keyBlock = columnarMap.getKeysBlock();
        Block valuesBlock = columnarMap.getValuesBlock();

        BlockBuilder keyBlockBuilder = keyType.createBlockBuilder(null, totalEntries);
        BlockBuilder valueBlockBuilder = valueType.createBlockBuilder(null, totalEntries);

        int positionCount = block.getPositionCount();
        int blockPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int cardinality = columnarMap.getEntryCount(i);
            for (int j = 0; j < cardinality; j++) {
                keyType.appendTo(keyBlock, blockPosition, keyBlockBuilder);
                valueType.appendTo(valuesBlock, blockPosition, valueBlockBuilder);
                blockPosition++;
            }

            int maxCardinality = maxCardinalities[i];
            for (int j = cardinality; j < maxCardinality; j++) {
                keyBlockBuilder.appendNull();
                valueBlockBuilder.appendNull();
            }
        }

        Block[] blocks = new Block[2];
        blocks[0] = keyBlockBuilder.build();
        blocks[1] = valueBlockBuilder.build();
        return blocks;
    }

    private static Block[] buildExpectedUnnestedArrayOfRowBlock(Block block, List<Type> rowTypes, int[] maxCardinalities, int totalEntries)
    {
        ColumnarArray columnarArray = ColumnarArray.toColumnarArray(block);
        Block elementBlock = columnarArray.getElementsBlock();
        ColumnarRow columnarRow = ColumnarRow.toColumnarRow(elementBlock);

        int fieldCount = columnarRow.getFieldCount();
        Block[] blocks = new Block[fieldCount];

        int positionCount = block.getPositionCount();
        for (int i = 0; i < fieldCount; i++) {
            BlockBuilder blockBuilder = rowTypes.get(i).createBlockBuilder(null, totalEntries);

            int nullRowsEncountered = 0;
            for (int j = 0; j < positionCount; j++) {
                int rowBlockIndex = columnarArray.getOffset(j);
                int cardinality = columnarArray.getLength(j);
                for (int k = 0; k < cardinality; k++) {
                    if (columnarRow.isNull(rowBlockIndex + k)) {
                        blockBuilder.appendNull();
                        nullRowsEncountered++;
                    }
                    else {
                        rowTypes.get(i).appendTo(columnarRow.getField(i), rowBlockIndex + k - nullRowsEncountered, blockBuilder);
                    }
                }

                int maxCardinality = maxCardinalities[j];
                for (int k = cardinality; k < maxCardinality; k++) {
                    blockBuilder.appendNull();
                }
            }

            blocks[i] = blockBuilder.build();
        }

        return blocks;
    }

    private static Block buildExpectedOrdinalityBlock(int[] maxCardinalities, int totalEntries)
    {
        BlockBuilder ordinalityBlockBuilder = BIGINT.createBlockBuilder(null, totalEntries);
        for (int i = 0; i < maxCardinalities.length; i++) {
            int maxCardinality = maxCardinalities[i];
            for (int ordinalityCount = 1; ordinalityCount <= maxCardinality; ordinalityCount++) {
                BIGINT.writeLong(ordinalityBlockBuilder, ordinalityCount);
            }
        }
        return ordinalityBlockBuilder.build();
    }
}
