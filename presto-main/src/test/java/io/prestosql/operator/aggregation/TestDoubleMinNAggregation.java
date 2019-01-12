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

import static io.prestosql.block.BlockAssertions.createDoubleSequenceBlock;
import static io.prestosql.block.BlockAssertions.createDoublesBlock;
import static io.prestosql.block.BlockAssertions.createLongRepeatBlock;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.testng.Assert.assertEquals;

public class TestDoubleMinNAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        return new Block[] {createDoubleSequenceBlock(start, start + length), createLongRepeatBlock(2, length)};
    }

    @Override
    protected String getFunctionName()
    {
        return "min";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.DOUBLE, StandardTypes.BIGINT);
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        if (length == 1) {
            return ImmutableList.of((double) start);
        }
        return ImmutableList.of((double) start, (double) start + 1);
    }

    @Test
    public void testMoreCornerCases()
    {
        testCustomAggregation(new Double[] {1.0, 2.0, null, 3.0}, 5);
        testInvalidAggregation(new Double[] {1.0, 2.0, 3.0}, 0);
        testInvalidAggregation(new Double[] {1.0, 2.0, 3.0}, -1);
    }

    private void testInvalidAggregation(Double[] x, int n)
    {
        try {
            testAggregation(new long[] {}, createDoublesBlock(x), createLongRepeatBlock(n, x.length));
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode().getName(), INVALID_FUNCTION_ARGUMENT.name());
        }
    }

    private void testCustomAggregation(Double[] values, int n)
    {
        PriorityQueue<Double> heap = new PriorityQueue<>(n, (x, y) -> -Double.compare(x, y));
        Arrays.stream(values).filter(x -> x != null).forEach(heap::add);
        Double[] expected = new Double[heap.size()];
        for (int i = heap.size() - 1; i >= 0; i--) {
            expected[i] = heap.remove();
        }
        testAggregation(Arrays.asList(expected), createDoublesBlock(values), createLongRepeatBlock(n, values.length));
    }
}
