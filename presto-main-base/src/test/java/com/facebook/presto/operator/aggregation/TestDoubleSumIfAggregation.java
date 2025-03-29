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
import com.facebook.presto.common.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;

public class TestDoubleSumIfAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder conditions = BOOLEAN.createBlockBuilder(null, length);
        BlockBuilder values = DOUBLE.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            BOOLEAN.writeBoolean(conditions, i % 2 == 0);
            DOUBLE.writeDouble(values, i);
        }
        return new Block[] {conditions.build(), values.build()};
    }

    @Override
    public Number getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        double sum = 0.0;
        boolean hasMatchingRows = false;

        for (int i = start; i < start + length; i++) {
            if (i % 2 == 0) {
                sum += i;
                hasMatchingRows = true;
            }
        }

        return hasMatchingRows ? sum : null;
    }

    @Override
    protected String getFunctionName()
    {
        return "sum_if";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.BOOLEAN, StandardTypes.DOUBLE);
    }

    @Test
    public void testAllValuesNullWithTrueCondition()
    {
        BlockBuilder conditions = BOOLEAN.createBlockBuilder(null, 5);
        BlockBuilder values = DOUBLE.createBlockBuilder(null, 5);

        for (int i = 0; i < 5; i++) {
            BOOLEAN.writeBoolean(conditions, true);
            values.appendNull();
        }

        Block[] blocks = new Block[] {conditions.build(), values.build()};
        testAggregation(null, blocks);
    }

    @Test
    public void testAllValuesNullWithMixedCondition()
    {
        BlockBuilder conditions = BOOLEAN.createBlockBuilder(null, 5);
        BlockBuilder values = DOUBLE.createBlockBuilder(null, 5);

        for (int i = 0; i < 5; i++) {
            BOOLEAN.writeBoolean(conditions, i % 2 == 0);
            values.appendNull();
        }

        Block[] blocks = new Block[] {conditions.build(), values.build()};
        testAggregation(null, blocks);
    }
}
