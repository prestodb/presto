package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.AggregationFunctionDefinition;
import com.facebook.presto.operator.AggregationOperator.Aggregator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.MaterializedResult;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.AggregationOperator.createAggregator;
import static com.facebook.presto.tuple.Tuples.createTuple;
import static org.testng.Assert.assertEquals;

public class TestApproximatePercentileAggregation
{
    @Test
    public void testLongSingleStep()
            throws Exception
    {
        assertSingleStep(LongApproximatePercentileAggregation.INSTANCE, createPage(new Long[] {}, 0.5), null);
        assertSingleStep(LongApproximatePercentileAggregation.INSTANCE, createPage(new Long[] { 1L }, 0.5), 1L);
        assertSingleStep(LongApproximatePercentileAggregation.INSTANCE, createPage(new Long[] { 1L, 2L, 2L, 2L, 2L, 2L, 2L, 3L, 3L, 3L, 3L, 4L, 5L, 6L, 7L }, 0.5), 3L);
        assertSingleStep(LongApproximatePercentileAggregation.INSTANCE, createPage(new Long[] { null, null, null }, 0.5), null);
        assertSingleStep(LongApproximatePercentileAggregation.INSTANCE, createPage(new Long[] { 1L, null, 2L, 2L, null, 2L, 2L, null, 2L, 2L, 3L, 3L, 3L, 3L, 4L, 5L, 6L, 7L }, 0.5), 3L);
        assertSingleStep(LongApproximatePercentileAggregation.INSTANCE, createPage(new Long[] { -7L, -6L, -5L, -4L, -3L, -2L, -1L, 0L, 1L, 2L, 3L }, 0.5), -2L);
    }

    @Test
    public void testDoubleSingleStep()
            throws Exception
    {
        assertSingleStep(DoubleApproximatePercentileAggregation.INSTANCE, createPage(new Double[] {}, 0.5), null);
        assertSingleStep(DoubleApproximatePercentileAggregation.INSTANCE, createPage(new Double[] { 1.0 }, 0.5), 1.0);
        assertSingleStep(DoubleApproximatePercentileAggregation.INSTANCE, createPage(new Double[] { 1.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0, 3.0, 4.0, 5.0, 6.0, 7.0 }, 0.5), 3.0);
        assertSingleStep(DoubleApproximatePercentileAggregation.INSTANCE, createPage(new Double[] { null, null, null }, 0.5), null);
        assertSingleStep(DoubleApproximatePercentileAggregation.INSTANCE, createPage(new Double[] { 1.0, null, 2.0, 2.0, null, 2.0, 2.0, null, 2.0, 2.0, 3.0, 3.0, 3.0, 3.0, 4.0, 5.0, 6.0, 7.0 }, 0.5), 3.0);
        assertSingleStep(DoubleApproximatePercentileAggregation.INSTANCE, createPage(new Double[] { -7.0, -6.0, -5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0 }, 0.5), -2.0);
    }

    @Test
    public void testLongPartialStep()
            throws Exception
    {
        assertFinal(LongApproximatePercentileAggregation.INSTANCE, new Page[] {
                createPage(new Long[] { null }, 0.5),
                createPage(new Long[] { null }, 0.5) },
                null);

        assertFinal(LongApproximatePercentileAggregation.INSTANCE, new Page[] {
                createPage(new Long[] { null }, 0.5),
                createPage(new Long[] { 1L }, 0.5) },
                1L);

        assertFinal(LongApproximatePercentileAggregation.INSTANCE, new Page[] {
                createPage(new Long[] { null }, 0.5),
                createPage(new Long[] { 1L, 2L, 3L }, 0.5) },
                2L);

        assertFinal(LongApproximatePercentileAggregation.INSTANCE, new Page[] {
                createPage(new Long[] { 1L }, 0.5),
                createPage(new Long[] { 2L, 3L }, 0.5) },
                2L);

        assertFinal(LongApproximatePercentileAggregation.INSTANCE, new Page[] {
                createPage(new Long[] { 1L, null, 2L, 2L, null, 2L, 2L, null }, 0.5),
                createPage(new Long[] { 2L, 2L, null, 3L, 3L, null, 3L, null, 3L, 4L, 5L, 6L, 7L }, 0.5) },
                3L);
    }

    @Test
    public void testDoublePartialStep()
            throws Exception
    {
        assertFinal(DoubleApproximatePercentileAggregation.INSTANCE, new Page[] {
                createPage(new Double[] { null }, 0.5),
                createPage(new Double[] { null }, 0.5) },
                null);

        assertFinal(DoubleApproximatePercentileAggregation.INSTANCE, new Page[] {
                createPage(new Double[] { null }, 0.5),
                createPage(new Double[] { 1.0 }, 0.5) },
                1.0);

        assertFinal(DoubleApproximatePercentileAggregation.INSTANCE, new Page[] {
                createPage(new Double[] { null }, 0.5),
                createPage(new Double[] { 1.0, 2.0, 3.0 }, 0.5) },
                2.0);

        assertFinal(DoubleApproximatePercentileAggregation.INSTANCE, new Page[] {
                createPage(new Double[] { 1.0 }, 0.5),
                createPage(new Double[] { 2.0, 3.0 }, 0.5) },
                2.0);

        assertFinal(DoubleApproximatePercentileAggregation.INSTANCE, new Page[] {
                createPage(new Double[] { 1.0, null, 2.0, 2.0, null, 2.0, 2.0, null }, 0.5),
                createPage(new Double[] { 2.0, 2.0, null, 3.0, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0 }, 0.5) },
                3.0);
    }


    private static void assertFinal(AggregationFunction function, Page[] inputs, Object expectedValue)
    {
        AggregationFunctionDefinition definition = aggregation(function, new Input(0, 0));

        MaterializedResult expected = MaterializedResult.resultBuilder(function.getFinalTupleInfo())
                .row(expectedValue)
                .build();

        // verify addValue(Page)
        Aggregator aggregator1 = createAggregator(definition, Step.FINAL);
        for (Page input : inputs) {
            aggregator1.addValue(computePartial(function, input));
        }

        assertEquals(getResult(function.getFinalTupleInfo(), aggregator1), expected);

        // verify addValue(BlockCursor...)
        Aggregator aggregator2 = createAggregator(definition, Step.FINAL);
        for (Page input : inputs) {
            Page partial = computePartial(function, input);

            BlockCursor[] cursors = new BlockCursor[partial.getBlocks().length];
            for (int i = 0; i < cursors.length; i++) {
                cursors[i] = partial.getBlock(i).cursor();
            }

            while (BlockAssertions.advanceAllCursorsToNextPosition(cursors)) {
                aggregator2.addValue(cursors);
            }
        }

        assertEquals(getResult(function.getFinalTupleInfo(), aggregator2), expected);

    }

    private static void assertSingleStep(AggregationFunction function, Page input, Object expectedValue)
    {
        AggregationFunctionDefinition definition = aggregation(function, new Input(0, 0), new Input(1, 0));

        MaterializedResult expected = MaterializedResult.resultBuilder(function.getFinalTupleInfo())
                .row(new Object[] { expectedValue })
                .build();

        // verify addInput(Page)
        Aggregator aggregator1 = createAggregator(definition, Step.SINGLE);
        aggregator1.addValue(input);
        assertEquals(getResult(function.getFinalTupleInfo(), aggregator1), expected);

        // verify addInput(BlockCursors...)
        Aggregator aggregator2 = createAggregator(definition, Step.SINGLE);

        BlockCursor[] cursors = new BlockCursor[input.getBlocks().length];
        for (int i = 0; i < cursors.length; i++) {
            cursors[i] = input.getBlock(i).cursor();
        }

        while (BlockAssertions.advanceAllCursorsToNextPosition(cursors)) {
            aggregator2.addValue(cursors);
        }

        assertEquals(getResult(function.getFinalTupleInfo(), aggregator2), expected);
    }

    private static Page createPage(Double[] values, double percentile)
    {
        Block valuesBlock;
        Block percentilesBlock;

        if (values.length == 0) {
            valuesBlock = new UncompressedBlock(0, TupleInfo.SINGLE_DOUBLE, Slices.EMPTY_SLICE);
            percentilesBlock = new UncompressedBlock(0, TupleInfo.SINGLE_DOUBLE, Slices.EMPTY_SLICE);
        }
        else {
            valuesBlock = createDoublesBlock(values);
            percentilesBlock = new RunLengthEncodedBlock(createTuple(percentile), values.length);
        }

        return new Page(valuesBlock, percentilesBlock);
    }

    private static Page createPage(Long[] values, double percentile)
    {
        Block valuesBlock;
        Block percentilesBlock;

        if (values.length == 0) {
            valuesBlock = new UncompressedBlock(0, TupleInfo.SINGLE_LONG, Slices.EMPTY_SLICE);
            percentilesBlock = new UncompressedBlock(0, TupleInfo.SINGLE_DOUBLE, Slices.EMPTY_SLICE);
        }
        else {
            valuesBlock = createLongsBlock(values);
            percentilesBlock = new RunLengthEncodedBlock(createTuple(percentile), values.length);
        }

        return new Page(valuesBlock, percentilesBlock);
    }

    private static Page computePartial(AggregationFunction function, Page input)
    {
        AggregationFunctionDefinition definition = aggregation(function, new Input(0, 0), new Input(1, 0));

        Aggregator aggregator = createAggregator(definition, Step.PARTIAL);
        aggregator.addValue(input);

        return new Page(aggregator.getResult());
    }

    private static MaterializedResult getResult(TupleInfo tupleInfo, Aggregator aggregator)
    {
        List<Tuple> tuples = new ArrayList<>();
        BlockCursor cursor = aggregator.getResult().cursor();
        while (cursor.advanceNextPosition()) {
            tuples.add(cursor.getTuple());
        }

        return new MaterializedResult(tuples, tupleInfo);
    }
}
