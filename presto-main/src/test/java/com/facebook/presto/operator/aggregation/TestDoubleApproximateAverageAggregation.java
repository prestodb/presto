package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;

import static com.facebook.presto.operator.aggregation.DoubleApproximateAverageAggregation.DOUBLE_APPROX_AVERAGE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;

public class TestDoubleApproximateAverageAggregation
    extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_DOUBLE);
        for (int i = start; i < start + length; i++) {
            blockBuilder.append((double) i);
        }
        return blockBuilder.build();
    }

    @Override
    public AggregationFunction getFunction()
    {
        return DOUBLE_APPROX_AVERAGE;
    }

    @Override
    public String getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        double sum = 0;
        for (int i = start; i < start + length; i++) {
            sum += i;
        }

        double mean = sum/length;
        double m2 = 0.0;
        for (int i = start; i < start + length; i++) {
            m2 += (i - mean) * (i - mean);
        }

        double variance = m2/length;

        StringBuilder sb = new StringBuilder();
        sb.append(mean);
        sb.append(" +/- ");
        sb.append((2.575 * Math.sqrt(variance / length)));

        return sb.toString();
    }

}
