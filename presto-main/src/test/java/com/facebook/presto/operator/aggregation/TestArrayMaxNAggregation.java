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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.LongStream;

import static com.facebook.presto.block.BlockAssertions.createLongRepeatBlock;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestArrayMaxNAggregation
        extends AbstractTestAggregationFunction
{
    public static Block createLongArraysBlock(Long[] values)
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        BlockBuilder blockBuilder = arrayType.createBlockBuilder(new BlockBuilderStatus(), values.length);
        for (Long value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                BlockBuilder elementBlockBuilder = blockBuilder.beginBlockEntry();
                BIGINT.writeLong(elementBlockBuilder, value);
                blockBuilder.closeEntry();
            }
        }
        return blockBuilder.build();
    }

    public static Block createLongArraySequenceBlock(int start, int length)
    {
        return createLongArraysBlock(LongStream.range(start, length).boxed().toArray(Long[]::new));
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        return new Block[] {createLongArraySequenceBlock(start, start + length), createLongRepeatBlock(2, length)};
    }

    @Override
    protected String getFunctionName()
    {
        return "max";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of("array(bigint)", StandardTypes.BIGINT);
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        if (length == 1) {
            return ImmutableList.of(ImmutableList.of((long) start));
        }
        return ImmutableList.of(ImmutableList.of((long) start + length - 1), ImmutableList.of((long) start + length - 2));
    }

    @Test
    public void testMoreCornerCases()
    {
        testCustomAggregation(new Long[] {1L, 2L, null, 3L}, 5);
        testInvalidAggregation(new Long[] {1L, 2L, 3L}, 0);
        testInvalidAggregation(new Long[] {1L, 2L, 3L}, -1);
    }

    private void testInvalidAggregation(Long[] x, int n)
    {
        try {
            testAggregation(null, createLongArraysBlock(x), createLongRepeatBlock(n, x.length));
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode().getName(), INVALID_FUNCTION_ARGUMENT.name());
        }
    }

    private void testCustomAggregation(Long[] values, int n)
    {
        PriorityQueue<Long> heap = new PriorityQueue<Long>(n);
        Arrays.stream(values).filter(x -> x != null).forEach(heap::add);
        ImmutableList.Builder<List<Long>> expected = new ImmutableList.Builder<>();
        for (int i = heap.size() - 1; i >= 0; i--) {
            expected.add(ImmutableList.of(heap.remove()));
        }
        testAggregation(Lists.reverse(expected.build()), createLongArraysBlock(values), createLongRepeatBlock(n, values.length));
    }
}
