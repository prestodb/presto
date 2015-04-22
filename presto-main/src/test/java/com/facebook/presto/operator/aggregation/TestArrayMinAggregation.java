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
import com.facebook.presto.spi.type.BigintType;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.type.ArrayType.toStackRepresentation;

public class TestArrayMinAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = VARBINARY.createBlockBuilder(new BlockBuilderStatus(), length);
        for (int i = start; i < start + length; i++) {
            VARBINARY.writeSlice(blockBuilder, toStackRepresentation(ImmutableList.of(i), BigintType.BIGINT));
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    public List<Long> getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        return ImmutableList.of((long) start);
    }

    @Override
    protected String getFunctionName()
    {
        return "min";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of("array<bigint>");
    }
}
