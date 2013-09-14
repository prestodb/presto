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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;

import static com.facebook.presto.operator.aggregation.BooleanMinAggregation.BOOLEAN_MIN;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_BOOLEAN;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

public class TestBooleanMinAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block getSequenceBlock(int start, int length)
    {
        BlockBuilder blockBuilder = new BlockBuilder(SINGLE_BOOLEAN);
        for (int i = start; i < start + length; i++) {
            // true, false, true, false...
            blockBuilder.append(i % 2 == 0);
        }
        return blockBuilder.build();
    }

    @Override
    public AggregationFunction getFunction()
    {
        return BOOLEAN_MIN;
    }

    @Override
    public Boolean getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        return length > 1 ? FALSE : TRUE;
    }
}
