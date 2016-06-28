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
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createFloatSequenceBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.constructDoublePrimitiveArray;

public class TestFloatCorrelationAggregation
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
        return "corr";
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
        PearsonsCorrelation corr = new PearsonsCorrelation();
        return (float) corr.correlation(constructDoublePrimitiveArray(start + 2, length), constructDoublePrimitiveArray(start, length));
    }
}
