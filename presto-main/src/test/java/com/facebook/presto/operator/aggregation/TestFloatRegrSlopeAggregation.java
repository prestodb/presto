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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createFloatSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createFloatsBlock;
import static com.google.common.base.Preconditions.checkArgument;

public class TestFloatRegrSlopeAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        return new Block[] {createFloatSequenceBlock(start, start + length), createFloatSequenceBlock(start + 2, start + 2 + length)};
    }

    @Override
    protected String getFunctionName()
    {
        return "regr_slope";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.FLOAT, StandardTypes.FLOAT);
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length <= 1) {
            return null;
        }
        SimpleRegression regression = new SimpleRegression();
        for (int i = start; i < start + length; i++) {
            regression.addData(i + 2, i);
        }
        return (float) regression.getSlope();
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
        float expected = (float) regression.getSlope();
        checkArgument(Float.isFinite(expected) && expected != 0.0f, "Expected result is trivial");
        testAggregation(expected, createFloatsBlock(y), createFloatsBlock(x));
    }
}
