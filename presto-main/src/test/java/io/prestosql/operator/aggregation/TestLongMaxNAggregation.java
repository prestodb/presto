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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.StandardTypes;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import static io.prestosql.block.BlockAssertions.createLongRepeatBlock;
import static io.prestosql.block.BlockAssertions.createLongSequenceBlock;
import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.testng.Assert.assertEquals;

public class TestLongMaxNAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        return new Block[] {createLongSequenceBlock(start, start + length), createLongRepeatBlock(2, length)};
    }

    @Override
    protected String getFunctionName()
    {
        return "max";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.BIGINT, StandardTypes.BIGINT);
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        if (length == 1) {
            return ImmutableList.of((long) start);
        }
        return ImmutableList.of((long) start + length - 1, (long) start + length - 2);
    }

    @Test
    public void testMoreCornerCases()
    {
        testCustomAggregation(new Long[] {1L, 2L, null, 3L}, 5);
        testInvalidAggregation(new Long[] {1L, 2L, 3L}, 0);
        testInvalidAggregation(new Long[] {1L, 2L, 3L}, -1);
        testInvalidAggregation(new Long[] {1L, 2L, 3L}, 10001);
    }

    private void testInvalidAggregation(Long[] x, int n)
    {
        try {
            testAggregation(new long[] {}, createLongsBlock(x), createLongRepeatBlock(n, x.length));
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode().getName(), INVALID_FUNCTION_ARGUMENT.name());
        }
    }

    private void testCustomAggregation(Long[] values, int n)
    {
        PriorityQueue<Long> heap = new PriorityQueue<>(n);
        Arrays.stream(values).filter(x -> x != null).forEach(heap::add);
        Long[] expected = new Long[heap.size()];
        for (int i = heap.size() - 1; i >= 0; i--) {
            expected[i] = heap.remove();
        }
        testAggregation(Arrays.asList(expected), createLongsBlock(values), createLongRepeatBlock(n, values.length));
    }
}
