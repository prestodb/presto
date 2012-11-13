package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.rle.RunLengthEncodedBlockCursor;
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
    public abstract BlockCursor getSequenceCursor(int max);
    public abstract FullAggregationFunction getFullFunction();
    public abstract Number getExpectedValue(int positions);
    public abstract Number getActualValue(AggregationFunction function);

    @Test
    public void testNoPositions()
            throws Exception
    {
        testMultiplePositions(getSequenceCursor(10), getExpectedValue(0), 0);
    }

    @Test
    public void testAllPositionsNull()
            throws Exception
    {
        BlockCursor nullsCursor = new RunLengthEncodedBlockCursor(nullTuple(getSequenceCursor(0).getTupleInfo()), 11);
        testMultiplePositions(nullsCursor, getExpectedValue(0), 10);
    }

    @Test
    public void testSinglePosition()
            throws Exception
    {
        testMultiplePositions(getSequenceCursor(10), getExpectedValue(1), 1);
    }

    @Test
    public void testMultiplePositions()
    {
        testMultiplePositions(getSequenceCursor(10), getExpectedValue(5), 5);
    }

    @Test
    public void testMixedNullAndNonNullPositions()
    {
        AlternatingNullsBlockCursor cursor = new AlternatingNullsBlockCursor(getSequenceCursor(10));
        testMultiplePositions(cursor, getExpectedValue(5), 10);
    }

    protected void testMultiplePositions(BlockCursor cursor, Number expectedValue, int positions)
    {
        AggregationFunction function = singleNodeAggregation(getFullFunction());

        for (int i = 0; i < positions; i++) {
            assertTrue(cursor.advanceNextPosition());
            function.add(cursor);
        }

        assertEquals(getActualValue(function), expectedValue);
        if (positions > 0) {
            assertEquals(cursor.getPosition(), positions - 1);
        }
    }

    @Test
    public void testPartialWithMultiplePositions()
    {
        testPartialWithMultiplePositions(getSequenceCursor(10), getExpectedValue(5), 5);
    }

    @Test
    public void testPartialWithMixedNullAndNonNullPositions()
    {
        AlternatingNullsBlockCursor cursor = new AlternatingNullsBlockCursor(getSequenceCursor(10));
        testPartialWithMultiplePositions(cursor, getExpectedValue(5), 10);
    }

    protected void testPartialWithMultiplePositions(BlockCursor cursor, Number expectedValue, int positions)
    {
        BlockBuilder blockBuilder = new BlockBuilder(getFullFunction().getIntermediateTupleInfo());
        for (int i = 0; i < positions; i++) {
            AggregationFunction function = partialAggregation(getFullFunction());
            assertTrue(cursor.advanceNextPosition());
            function.add(cursor);
            Tuple tuple = function.evaluate();
            blockBuilder.append(tuple);
        }

        AggregationFunction function = finalAggregation(getFullFunction());
        BlockCursor partialsCursor = blockBuilder.build().cursor();
        while (partialsCursor.advanceNextPosition()) {
            function.add(partialsCursor);
        }

        assertEquals(getActualValue(function), expectedValue);
        if (positions > 0) {
            assertEquals(cursor.getPosition(), positions - 1);
        }
    }

    @Test
    public void testCombinerWithMultiplePositions()
    {
        testCombinerWithMultiplePositions(getSequenceCursor(10), getExpectedValue(5), 5);
    }

    @Test
    public void testCombinerWithMixedNullAndNonNullPositions()
    {
        AlternatingNullsBlockCursor cursor = new AlternatingNullsBlockCursor(getSequenceCursor(10));
        testCombinerWithMultiplePositions(cursor, getExpectedValue(5), 10);
    }

    protected void testCombinerWithMultiplePositions(BlockCursor cursor, Number expectedValue, int positions)
    {
        // "aggregate" each input value into a partial result
        List<Block> blocks = new ArrayList<>();
        for (int i = 0; i < positions; i++) {
            AggregationFunction function = partialAggregation(getFullFunction());
            assertTrue(cursor.advanceNextPosition());
            function.add(cursor);

            Tuple tuple = function.evaluate();
            blocks.add(new BlockBuilder(getFullFunction().getIntermediateTupleInfo())
                    .append(tuple)
                    .build());
        }

        // combine partial results together row at a time
        Block combinedBlock = null;
        for (Block block : blocks) {
            AggregationFunction function = combinerAggregation(getFullFunction());
            if (combinedBlock != null) {
                BlockCursor intermediateCursor = combinedBlock.cursor();
                assertTrue(intermediateCursor.advanceNextPosition());
                function.add(intermediateCursor);
            }

            BlockCursor intermediateCursor = block.cursor();
            assertTrue(intermediateCursor.advanceNextPosition());
            function.add(intermediateCursor);

            Tuple tuple = function.evaluate();
            combinedBlock = new BlockBuilder(getFullFunction().getIntermediateTupleInfo())
                    .append(tuple)
                    .build();
        }

        // produce final result using combine block
        AggregationFunction function = finalAggregation(getFullFunction());
        BlockCursor combinedCursor = combinedBlock.cursor();
        assertTrue(combinedCursor.advanceNextPosition());
        function.add(combinedCursor);

        assertEquals(getActualValue(function), expectedValue);
        if (positions > 0) {
            assertEquals(cursor.getPosition(), positions - 1);
        }
    }
}
