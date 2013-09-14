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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.AggregationOperator.Aggregator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.AggregationOperator.createAggregator;
import static com.facebook.presto.tuple.Tuples.nullTuple;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestAggregationFunction
{
    public abstract Block getSequenceBlock(int start, int length);

    public abstract AggregationFunction getFunction();

    public abstract Object getExpectedValue(int start, int length);

    @Test
    public void testNoPositions()
            throws Exception
    {
        testMultiplePositions(getSequenceBlock(0, 10), getExpectedValue(0, 0), 0);
    }

    @Test
    public void testSinglePosition()
            throws Exception
    {
        testMultiplePositions(getSequenceBlock(0, 10), getExpectedValue(0, 1), 1);
    }

    @Test
    public void testMultiplePositions()
    {
        testMultiplePositions(getSequenceBlock(0, 10), getExpectedValue(0, 5), 5);
    }

    @Test
    public void testAllPositionsNull()
            throws Exception
    {
        Block block = new RunLengthEncodedBlock(nullTuple(getSequenceBlock(0, 10).getTupleInfo()), 11);
        testMultiplePositions(block, getExpectedValue(0, 0), 10);
    }

    @Test
    public void testMixedNullAndNonNullPositions()
    {
        Block alternatingNullsBlock = createAlternatingNullsBlock(getSequenceBlock(0, 10));
        testMultiplePositions(alternatingNullsBlock, getExpectedValue(0, 5), 10);
    }

    protected void testMultiplePositions(Block block, Object expectedValue, int positions)
    {
        // test with input at field 0
        testMultiplePositions(block, expectedValue, positions, 0);

        // test with input and field != 0
        testMultiplePositions(block, expectedValue, positions, 1);
    }

    private void testMultiplePositions(Block block, Object expectedValue, int positions, int field)
    {
        BlockCursor cursor = createCompositeTupleBlock(block, field).cursor();
        Aggregator function = createAggregator(aggregation(getFunction(), new Input(0, field)), Step.SINGLE);

        for (int i = 0; i < positions; i++) {
            assertTrue(cursor.advanceNextPosition());
            function.addValue(cursor);
        }

        Object actualValue = getActualValue(function);
        assertEquals(actualValue, expectedValue);
        if (positions > 0) {
            assertEquals(cursor.getPosition(), positions - 1);
        }
    }

    private Object getActualValue(Aggregator function)
    {
        return BlockAssertions.toValues(function.getResult()).get(0).get(0);
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
        // test with input at field 0
        testVectorMultiplePositions(block, expectedValue, 0);

        // test with input and field != 0
        testVectorMultiplePositions(block, expectedValue, 1);
    }

    private void testVectorMultiplePositions(Block block, Object expectedValue, int field)
    {
        Aggregator function = createAggregator(aggregation(getFunction(), new Input(0, field)), Step.SINGLE);

        function.addValue(new Page(createCompositeTupleBlock(block, field)));
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
        Aggregator function = createAggregator(aggregation(getFunction(), new Input(0, 0)), Step.FINAL);
        BlockCursor partialsCursor = partialsBlock.cursor();
        while (partialsCursor.advanceNextPosition()) {
            function.addValue(partialsCursor);
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

    /**
     * Produce a block with "field" number of columns and place column 0 from the
     * provided block into the last column of the resulting block
     * <p/>
     * Fields 0 to "field - 1" are set to null
     */
    private Block createCompositeTupleBlock(Block sequenceBlock, int field)
    {
        TupleInfo.Type[] types = new TupleInfo.Type[field + 1];
        Arrays.fill(types, TupleInfo.Type.VARIABLE_BINARY);

        types[field] = Iterables.getOnlyElement(sequenceBlock.getTupleInfo().getTypes());

        BlockBuilder blockBuilder = new BlockBuilder(new TupleInfo(types));
        BlockCursor cursor = sequenceBlock.cursor();
        while (cursor.advanceNextPosition()) {
            for (int i = 0; i < field; i++) {
                blockBuilder.appendNull();
            }
            blockBuilder.append(cursor.getTuple());
        }

        return blockBuilder.build();
    }

    protected void testVectorPartialWithMultiplePositions(Block block, Object expectedValue)
    {
        UncompressedBlock partialsBlock = performPartialAggregation(block);
        Aggregator function = createAggregator(aggregation(getFunction(), new Input(0, 0)), Step.FINAL);

        BlockCursor blockCursor = partialsBlock.cursor();
        while (blockCursor.advanceNextPosition()) {
            function.addValue(blockCursor);
        }
        assertEquals(getActualValue(function), expectedValue);
    }

    private UncompressedBlock performPartialAggregation(Block block)
    {
        BlockBuilder blockBuilder = new BlockBuilder(getFunction().getIntermediateTupleInfo());
        BlockCursor cursor = block.cursor();
        while (cursor.advanceNextPosition()) {
            Aggregator function = createAggregator(aggregation(getFunction(), new Input(0, 0)), Step.PARTIAL);
            function.addValue(cursor);
            BlockCursor result = function.getResult().cursor();
            assertTrue(result.advanceNextPosition());
            Tuple tuple = result.getTuple();
            blockBuilder.append(tuple);
        }
        return blockBuilder.build();
    }

//    @Test
//    public void testCombinerWithMultiplePositions()
//    {
//        testCombinerWithMultiplePositions(getSequenceBlock(0, 10).cursor(), getExpectedValue(0, 5), 5);
//    }
//
//    @Test
//    public void testCombinerWithMixedNullAndNonNullPositions()
//    {
//        AlternatingNullsBlockCursor cursor = new AlternatingNullsBlockCursor(getSequenceBlock(0, 10).cursor());
//        testCombinerWithMultiplePositions(cursor, getExpectedValue(0, 5), 10);
//    }
//
//    protected void testCombinerWithMultiplePositions(BlockCursor cursor, Object expectedValue, int positions)
//    {
//        // "aggregate" each input value into a partial result
//        List<Block> blocks = new ArrayList<>();
//        for (int i = 0; i < positions; i++) {
//            Aggregator function = createAggregator(aggregation(getFunction(), 0), Step.FINAL);
//            assertTrue(cursor.advanceNextPosition());
//            function.addValue(new BlockCursor[]{cursor});
//            BlockCursor result = function.getResult().cursor();
//            assertTrue(result.advanceNextPosition());
//            Tuple tuple = result.getTuple();
//
//            blocks.add(new BlockBuilder(getFunction().getIntermediateTupleInfo())
//                    .append(tuple)
//                    .build());
//        }
//        if (positions > 0) {
//            assertEquals(cursor.getPosition(), positions - 1);
//        }
//        assertCombineFinalAggregation(blocks, expectedValue);
//    }
//
//    private void assertCombineFinalAggregation(List<Block> blocks, Object expectedValue)
//    {
//        // combine partial results together row at a time
//        Block combinedBlock = null;
//        for (Block block : blocks) {
//            AggregationFunctionStep function = combinerAggregation(getFunction());
//            if (combinedBlock != null) {
//                BlockCursor intermediateCursor = combinedBlock.cursor();
//                assertTrue(intermediateCursor.advanceNextPosition());
//                function.add(intermediateCursor);
//            }
//
//            BlockCursor intermediateCursor = block.cursor();
//            assertTrue(intermediateCursor.advanceNextPosition());
//            function.add(intermediateCursor);
//
//            Tuple tuple = function.evaluate();
//            combinedBlock = new BlockBuilder(getFunction().getIntermediateTupleInfo())
//                    .append(tuple)
//                    .build();
//        }
//
//        // produce final result using combine block
//        assertFinalAggregation(combinedBlock, expectedValue);
//    }
//
//    private void assertFinalAggregation(Block partialsBlock, Object expectedValue)
//    {
//        AggregationFunctionStep function = finalAggregation(getFunction());
//        BlockCursor partialsCursor = partialsBlock.cursor();
//        while (partialsCursor.advanceNextPosition()) {
//            function.add(partialsCursor);
//        }
//
//        assertEquals(getActualValue(function), expectedValue);
//    }

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
