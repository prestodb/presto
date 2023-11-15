package com.facebook.presto.common.block;

import com.facebook.presto.common.type.ArrayType;
import com.google.common.base.Preconditions;

import java.util.Random;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;

public class FlatMapBlockMerger
{
    private final int maxBatchSize = 64;
    ArrayType arrayType = new ArrayType(INTEGER);

    final static int TOTAL_ROWS = 10;
    private Block[] inMapBlocks;
    private Block[] valueBlocks;

    public void setup()
    {
        Random rnd = new Random(1);

        int BLOCKS_CNT = 10;

        BlockBuilder[] inMapBlockBuilders = new BlockBuilder[BLOCKS_CNT];
        BlockBuilder[] valueBlockBuilders = new BlockBuilder[BLOCKS_CNT];

        for (int i = 0; i < BLOCKS_CNT; i++) {
            inMapBlockBuilders[i] = BOOLEAN.createBlockBuilder(null, 1);
            valueBlockBuilders[i] = INTEGER.createBlockBuilder(null, 1);
        }

        int value = 0;
        for (int i = 0; i < TOTAL_ROWS; i++) {
            for (int j = 0; j < BLOCKS_CNT; j++) {
                boolean isInMap = rnd.nextBoolean();
                BOOLEAN.writeBoolean(inMapBlockBuilders[j], isInMap);
                if (isInMap) {
                    INTEGER.writeLong(valueBlockBuilders[j], value++);
                }
            }
        }

        this.inMapBlocks = new Block[BLOCKS_CNT];
        this.valueBlocks = new Block[BLOCKS_CNT];
        for (int i = 0; i < BLOCKS_CNT; i++) {
            inMapBlocks[i] = inMapBlockBuilders[i].build();
            valueBlocks[i] = valueBlockBuilders[i].build();
        }
    }

    public static void main(String[] args)
    {
        FlatMapBlockMerger m = new FlatMapBlockMerger();
        m.setup();
        m.merge(TOTAL_ROWS, m.inMapBlocks, m.valueBlocks);
    }

    public void merge(int rowCount, Block[] inMaps, Block[] valueBlocks)
    {
        Preconditions.checkArgument(inMaps.length > 0);
        Preconditions.checkArgument(inMaps.length == valueBlocks.length);
        int keyCount = inMaps.length;

        int[] valueBlockPositions = new int[keyCount];

        int position = 0;
        IntArrayBatchPositions batchPositions = new IntArrayBatchPositions();

        int destPosition = 0;
        while (rowCount > 0) {
            int batchSize = Math.min(rowCount, maxBatchSize);

            for (int row = 0; row < batchSize; row++) {
                for (int keyIndex = 0; keyIndex < keyCount; keyIndex++) {
                    if (BOOLEAN.getBoolean(inMaps[keyIndex], row)) {
                        int valueBlockPosition = valueBlockPositions[keyIndex];
                        batchPositions.capture(valueBlocks[keyIndex], valueBlockPosition, destPosition);
                        valueBlockPositions[keyIndex]++;
                        destPosition++;
                    }
                }
            }
            batchPositions.sink();
            rowCount -= batchSize;
            position += batchSize;
        }

        Block block = batchPositions.buildBlock();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                System.out.println("NULL");
            }
            else {
                System.out.println(block.getInt(i));
            }
        }
        System.out.println();
    }
}
