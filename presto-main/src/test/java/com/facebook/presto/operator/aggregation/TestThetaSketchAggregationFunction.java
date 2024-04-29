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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.operator.aggregation.sketch.theta.ThetaSketchAggregationFunction;
import com.google.common.collect.ImmutableList;
import org.apache.datasketches.theta.Union;

import java.util.List;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;

public class TestThetaSketchAggregationFunction
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            DOUBLE.writeDouble(blockBuilder, i);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected String getFunctionName()
    {
        return ThetaSketchAggregationFunction.NAME;
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of("double");
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        Block values = getSequenceBlocks(start, length)[0];
        Union union = Union.builder().buildUnion();
        for (int i = 0; i < length; i++) {
            union.update(DOUBLE.getDouble(values, i));
        }
        return new SqlVarbinary(union.getResult().toByteArray());
    }
}
