package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.block.rle.RunLengthEncodedBlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.Tuple;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.operator.aggregation.AggregationFunctions.combinerAggregation;
import static com.facebook.presto.operator.aggregation.AggregationFunctions.finalAggregation;
import static com.facebook.presto.operator.aggregation.AggregationFunctions.partialAggregation;
import static com.facebook.presto.operator.aggregation.AggregationFunctions.singleNodeAggregation;
import static com.facebook.presto.tuple.Tuples.nullTuple;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestAggregationFunction
{
    public abstract Block getSequenceBlock(int start, int length);

    public abstract AggregationFunction getFunction();

    public abstract Object getExpectedValue(int start, int length);

    public abstract Object getActualValue(AggregationFunctionStep function);

    @Test
    public void testNoPositions()
            throws Exception
    {
        testMultiplePositions(getSequenceBlock(0, 10).cursor(), getExpectedValue(0, 0), 0);
    }

    @Test
    public void testSinglePosition()
            throws Exception
    {
        testMultiplePositions(getSequenceBlock(0, 10).cursor(), getExpectedValue(0, 1), 1);
    }

    @Test
    public void testMultiplePositions()
    {
        testMultiplePositions(getSequenceBlock(0, 10).cursor(), getExpectedValue(0, 5), 5);
    }
    @Test
    public void testAllPositionsNull()
            throws Exception
    {
        BlockCursor nullsCursor = new RunLengthEncodedBlockCursor(nullTuple(getSequenceBlock(0, 10).getTupleInfo()), 11);
        testMultiplePositions(nullsCursor, getExpectedValue(0, 0), 10);
    }

    @Test
    public void testMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(0, 10));
        testMultiplePositions(alternatingNullsBlock.cursor(), getExpectedValue(0, 5), 10);
    }

    protected void testMultiplePositions(BlockCursor cursor, Object expectedValue, int positions)
    {
        AggregationFunctionStep function = singleNodeAggregation(getFunction());

        for (int i = 0; i < positions; i++) {
            assertTrue(cursor.advanceNextPosition());
            function.add(cursor);
        }

        assertEquals(getActualValue(function), expectedValue);
        if (positions > 0) {
            assertEquals(cursor.getPosition(), positions - 1);
        }
    }

    // todo enable when empty blocks are supported
    @Test(enabled = false)
    public void testVectorNoPositions()
            throws Exception
    {
        testVectorMultiplePositions(getSequenceBlock(0, 0), getExpectedValue(0, 0));
    }

    @Test
    public void testVectorSinglePosition()
            throws Exception
    {
        testVectorMultiplePositions(getSequenceBlock(0, 1), getExpectedValue(0, 1));
    }

    @Test
    public void testVectorMultiplePositions()
    {
        testVectorMultiplePositions(getSequenceBlock(0, 5), getExpectedValue(0, 5));
    }

    @Test
    public void testVectorAllPositionsNull()
            throws Exception
    {
        Block nullsBlock = new RunLengthEncodedBlock(nullTuple(getSequenceBlock(0, 1).getTupleInfo()), 11);
        testVectorMultiplePositions(nullsBlock, getExpectedValue(0, 0));
    }

    @Test
    public void testVectorMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(0, 5));
        testVectorMultiplePositions(alternatingNullsBlock, getExpectedValue(0, 5));
    }

    protected void testVectorMultiplePositions(Block block, Object expectedValue)
    {
        AggregationFunctionStep function = singleNodeAggregation(getFunction());
        function.add(new Page(block));
        assertEquals(getActualValue(function), expectedValue);
    }

    @Test
    public void testPartialWithMultiplePositions()
    {
        testPartialWithMultiplePositions(getSequenceBlock(0, 10), getExpectedValue(0, 10));
    }

    @Test
    public void testPartialWithMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(0, 10));
        testPartialWithMultiplePositions(alternatingNullsBlock, getExpectedValue(0, 10));
    }

    protected void testPartialWithMultiplePositions(Block block, Object expectedValue)
    {
        UncompressedBlock partialsBlock = performPartialAggregation(block);
        AggregationFunctionStep function = finalAggregation(getFunction());
        BlockCursor partialsCursor = partialsBlock.cursor();
        while (partialsCursor.advanceNextPosition()) {
            function.add(partialsCursor);
        }

        assertEquals(getActualValue(function), expectedValue);
    }

    @Test
    public void testVectorPartialWithMultiplePositions()
    {
        testVectorPartialWithMultiplePositions(getSequenceBlock(0, 10), getExpectedValue(0, 10));
    }

    @Test
    public void testVectorPartialWithMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(0, 10));
        testVectorPartialWithMultiplePositions(alternatingNullsBlock, getExpectedValue(0, 10));
    }

    public Block createAlternatingNullsBlock(Block sequenceBlock)
    {
        BlockBuilder blockBuilder = new BlockBuilder(sequenceBlock.getTupleInfo());
        BlockCursor cursor = sequenceBlock.cursor();
        while (cursor.advanceNextPosition()) {
            blockBuilder.appendNull().append(cursor.getTuple());
        }
        return blockBuilder.build();
    }

    protected void testVectorPartialWithMultiplePositions(Block block, Object expectedValue)
    {
        UncompressedBlock partialsBlock = performPartialAggregation(block);
        AggregationFunctionStep function = finalAggregation(getFunction());
        function.add(new Page(partialsBlock));
        assertEquals(getActualValue(function), expectedValue);
    }

    private UncompressedBlock performPartialAggregation(Block block)
    {
        BlockBuilder blockBuilder = new BlockBuilder(getFunction().getIntermediateTupleInfo());
        BlockCursor cursor = block.cursor();
        while (cursor.advanceNextPosition()) {
            AggregationFunctionStep function = partialAggregation(getFunction());
            function.add(cursor);
            Tuple tuple = function.evaluate();
            blockBuilder.append(tuple);
        }
        return blockBuilder.build();
    }

    @Test
    public void testCombinerWithMultiplePositions()
    {
        testCombinerWithMultiplePositions(getSequenceBlock(0, 10).cursor(), getExpectedValue(0, 5), 5);
    }

    @Test
    public void testCombinerWithMixedNullAndNonNullPositions()
    {
        AlternatingNullsBlockCursor cursor = new AlternatingNullsBlockCursor(getSequenceBlock(0, 10).cursor());
        testCombinerWithMultiplePositions(cursor, getExpectedValue(0, 5), 10);
    }

    protected void testCombinerWithMultiplePositions(BlockCursor cursor, Object expectedValue, int positions)
    {
        // "aggregate" each input value into a partial result
        List<Block> blocks = new ArrayList<>();
        for (int i = 0; i < positions; i++) {
            AggregationFunctionStep function = partialAggregation(getFunction());
            assertTrue(cursor.advanceNextPosition());
            function.add(cursor);

            Tuple tuple = function.evaluate();
            blocks.add(new BlockBuilder(getFunction().getIntermediateTupleInfo())
                    .append(tuple)
                    .build());
        }
        if (positions > 0) {
            assertEquals(cursor.getPosition(), positions - 1);
        }
        assertCombineFinalAggregation(blocks, expectedValue);
    }

    private void assertCombineFinalAggregation(List<Block> blocks, Object expectedValue)
    {
        // combine partial results together row at a time
        Block combinedBlock = null;
        for (Block block : blocks) {
            AggregationFunctionStep function = combinerAggregation(getFunction());
            if (combinedBlock != null) {
                BlockCursor intermediateCursor = combinedBlock.cursor();
                assertTrue(intermediateCursor.advanceNextPosition());
                function.add(intermediateCursor);
            }

            BlockCursor intermediateCursor = block.cursor();
            assertTrue(intermediateCursor.advanceNextPosition());
            function.add(intermediateCursor);

            Tuple tuple = function.evaluate();
            combinedBlock = new BlockBuilder(getFunction().getIntermediateTupleInfo())
                    .append(tuple)
                    .build();
        }

        // produce final result using combine block
        assertFinalAggregation(combinedBlock, expectedValue);
    }

    private void assertFinalAggregation(Block partialsBlock, Object expectedValue)
    {
        AggregationFunctionStep function = finalAggregation(getFunction());
        BlockCursor partialsCursor = partialsBlock.cursor();
        while (partialsCursor.advanceNextPosition()) {
            function.add(partialsCursor);
        }

        assertEquals(getActualValue(function), expectedValue);
    }

    @Test
    public void testNegativeOnlyValues()
            throws Exception
    {
        testVectorMultiplePositions(getSequenceBlock(-10, 5), getExpectedValue(-10, 5));
    }

    @Test
    public void testPositiveOnlyValues()
            throws Exception
    {
        testVectorMultiplePositions(getSequenceBlock(2, 4), getExpectedValue(2, 4));
    }
}
