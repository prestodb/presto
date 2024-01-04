package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createDoubleSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.google.common.base.Preconditions.checkArgument;

public class TestDoubleRegrCountAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        return new Block[] {createDoubleSequenceBlock(start, start + length), createDoubleSequenceBlock(start + 2, start + 2 + length)};
    }

    @Override
    protected String getFunctionName()
    {
        return "regr_count";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.DOUBLE, StandardTypes.DOUBLE);
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length <= 1) {
            return (double) length;
        }
        else {
            SimpleRegression regression = new SimpleRegression();
            for (int i = start; i < start + length; i++) {
                regression.addData(i + 2, i);
            }
            return (double) regression.getN();
        }
    }

    @Test
    public void testNonTrivialResult()
    {
        testNonTrivialAggregation(new Double[] {1.0, 2.0, 3.0, 4.0, 5.0}, new Double[] {1.0, 4.0, 9.0, 16.0, 25.0});
        testNonTrivialAggregation(new Double[] {1.0, 4.0, 9.0, 16.0, 25.0}, new Double[] {1.0, 2.0, 3.0, 4.0, 5.0});
    }

    private void testNonTrivialAggregation(Double[] y, Double[] x)
    {
        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < x.length; i++) {
            regression.addData(x[i], y[i]);
        }
        double expected = (double) regression.getN();
        checkArgument(Double.isFinite(expected) && expected != 0.0f, "Expected result is trivial");
        testAggregation(expected, createDoublesBlock(y), createDoublesBlock(x));
    }
}
