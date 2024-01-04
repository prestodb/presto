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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createBlockOfReals;
import static com.facebook.presto.block.BlockAssertions.createSequenceBlockOfReal;
import static com.google.common.base.Preconditions.checkArgument;

public class TestRealRegrAvgyAggregation
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
        return "regr_avgy";
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
            expected += i;
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
        for (int i = 0; i < y.length; i++) {
            expected += y[i];
        }
        expected = expected / y.length;
        checkArgument(Float.isFinite(expected) && expected != 0.0f, "Expected result is trivial");
        testAggregation(expected, createBlockOfReals(y), createBlockOfReals(x));
    }
}
