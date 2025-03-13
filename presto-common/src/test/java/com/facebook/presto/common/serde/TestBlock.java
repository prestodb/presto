package com.facebook.presto.common.serde;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.VariableWidthBlock;
import com.facebook.presto.common.experimental.BlockAdapter;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

public class TestBlock
{
    @Test
    void testVariableWidthBlock()
    {
        Slice compactSlice = Slices.copyOf(createExpectedValue(16));
        Slice incompactSlice = Slices.copyOf(createExpectedValue(20)).slice(0, 16);
        int[] offsets = {0, 1, 1, 2, 4, 8, 16};
        boolean[] valueIsNull = {false, true, false, false, false, false};

        Block block = new VariableWidthBlock(valueIsNull.length, compactSlice, offsets, Optional.of(valueIsNull));
        byte[] bytes = BlockAdapter.serialize(block);
        VariableWidthBlock deserializedBlock = (VariableWidthBlock) BlockAdapter.deserialize(bytes);

        assertEquals(deserializedBlock, block);
    }

    protected static Slice createExpectedValue(int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(16);
        for (int index = 0; index < length; index++) {
            dynamicSliceOutput.writeByte(length * (index + 1));
        }
        return dynamicSliceOutput.slice();
    }

    protected static void testIncompactBlock(Block block)
    {
        assertNotCompact(block);
        testCopyRegionCompactness(block);
    }

    protected static void assertNotCompact(Block block)
    {
        assertNotSame(block.copyRegion(0, block.getPositionCount()), block);
    }

    protected static void testCompactBlock(Block block)
    {
        assertCompact(block);
        testCopyRegionCompactness(block);
    }

    protected static void assertCompact(Block block)
    {
        assertSame(block.copyRegion(0, block.getPositionCount()), block);
    }

    protected static void testCopyRegionCompactness(Block block)
    {
        assertCompact(block.copyRegion(0, block.getPositionCount()));
        if (block.getPositionCount() > 0) {
            assertCompact(block.copyRegion(0, block.getPositionCount() - 1));
            assertCompact(block.copyRegion(1, block.getPositionCount() - 1));
        }
    }
}
