package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createBlockOfReals;
import static com.facebook.presto.block.BlockAssertions.createSequenceBlockOfReal;
import static com.google.common.base.Preconditions.checkArgument;

public class TestRealRegrSxxAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        return new Block[] {createSequenceBlockOfReal(start, start + length), createSequenceBlockOfReal(start + 2, start + 2 + length)};
    }

    @Override
    protected String getFunctionName()
    {
        return "regr_sxx";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.REAL, StandardTypes.REAL);
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length <= 1) {
            return (float) 0;
        }
        else {
            SimpleRegression regression = new SimpleRegression();
            for (int i = start; i < start + length; i++) {
                regression.addData(i + 2, i);
            }
            return (float) regression.getXSumSquares();
        }
    }

    @Test
    public void testNonTrivialResult()
    {
        testNonTrivialAggregation(new Float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f}, new Float[] {1.0f, 4.0f, 9.0f, 16.0f, 25.0f});
        testNonTrivialAggregation(new Float[] {1.0f, 4.0f, 9.0f, 16.0f, 25.0f}, new Float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f});
    }

    private void testNonTrivialAggregation(Float[] y, Float[] x)
    {
        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < x.length; i++) {
            regression.addData(x[i], y[i]);
        }
        float expected = (float) regression.getXSumSquares();
        checkArgument(Float.isFinite(expected) && expected != 0.0f, "Expected result is trivial");
        testAggregation(expected, createBlockOfReals(y), createBlockOfReals(x));
    }
}
