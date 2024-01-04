package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createBlockOfReals;
import static com.facebook.presto.block.BlockAssertions.createSequenceBlockOfReal;
import static com.google.common.base.Preconditions.checkArgument;

public class TestRealRegrAvgxAggregation
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
        return "regr_avgx";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.REAL, StandardTypes.REAL);
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return 0.0f;
        }

        float expected = 0.0f;
        for (int i = start; i < start + length; i++) {
            expected += (i + 2);
        }
        return expected / length;
    }

    @Test
    public void testNonTrivialResult()
    {
        testNonTrivialAggregation(new Float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f}, new Float[] {1.0f, 4.0f, 9.0f, 16.0f, 25.0f});
        testNonTrivialAggregation(new Float[] {1.0f, 4.0f, 9.0f, 16.0f, 25.0f}, new Float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f});
    }

    private void testNonTrivialAggregation(Float[] y, Float[] x)
    {
        float expected = 0.0f;
        for (int i = 0; i < x.length; i++) {
            expected += x[i];
        }
        expected = expected / x.length;
        checkArgument(Float.isFinite(expected) && expected != 0.0f, "Expected result is trivial");
        testAggregation(expected, createBlockOfReals(y), createBlockOfReals(x));
    }
}
