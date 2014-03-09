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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;

import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class TestCountAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus());
        for (int i = start; i < start + length; i++) {
            blockBuilder.append(i);
        }
        return blockBuilder.build();
    }

    @Override
    public AggregationFunction getFunction()
    {
        return COUNT;
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        return (long) length;
    }

    @Override
    public Object getExpectedValueIncludingNulls(int start, int length, int lengthIncludingNulls)
    {
        return (long) lengthIncludingNulls;
    }
}
