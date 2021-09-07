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
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;

public class TestLastFunctionIgnoreNulls
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, length);
        BlockBuilder booleanBuilder = BOOLEAN.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            if (i % 2 == 0) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, i);
            }
            BOOLEAN.writeBoolean(booleanBuilder, true);
        }
        return new Block[] {blockBuilder.build(), booleanBuilder.build()};
    }

    @Override
    protected String getFunctionName()
    {
        return "last";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.BIGINT, StandardTypes.BOOLEAN);
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        for (int i = start + length - 1; i >= start; i--) {
            if (i % 2 != 0) {
                return (long) i;
            }
        }
        return null;
    }

    @Override
    public Block[] createAlternatingNullsBlock(List<Type> types, Block... sequenceBlocks)
    {
        Block[] alternatingNullsBlocks = new Block[sequenceBlocks.length];
        for (int i = 0; i < sequenceBlocks.length; i++) {
            int positionCount = sequenceBlocks[i].getPositionCount();
            Type type = types.get(i);
            BlockBuilder blockBuilder = type.createBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                // append null
                if (i == sequenceBlocks.length - 1) {
                    type.appendTo(sequenceBlocks[i], position, blockBuilder);
                }
                else {
                    blockBuilder.appendNull();
                }
                // append value
                type.appendTo(sequenceBlocks[i], position, blockBuilder);
            }
            alternatingNullsBlocks[i] = blockBuilder.build();
        }
        return alternatingNullsBlocks;
    }
}
