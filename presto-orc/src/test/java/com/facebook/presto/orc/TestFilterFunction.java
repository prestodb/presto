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
package com.facebook.presto.orc;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.relation.Predicate;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFilterFunction
{
    private static final long UNLUCKY = 13;

    @Test
    public void testFilter()
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        FilterFunction filter = new FilterFunction(session.getSqlFunctionProperties(), true, new IsOddPredicate());

        Block numbers = makeNumbers(0, 1000);
        int[] allPositions = makePositions(0, 1000, 1);
        assertFilter(filter, numbers, allPositions, allPositions.length);

        Block dictionaryNumbers = new DictionaryBlock(numbers, allPositions);
        // Sparse coverage of the dictionary values
        int[] sparsePositions = makePositions(1, 300, 3);
        assertFilter(filter, dictionaryNumbers, sparsePositions, sparsePositions.length);

        // Full coverage of the dictionary values
        assertFilter(filter, dictionaryNumbers, allPositions, allPositions.length);

        // Test with a different DictionaryBlock over the same numbers. Results are reused. The DictionaryBlock covers the
        // values sparsely. TheDictionaryBlock itself is accessed sparsely.
        DictionaryBlock otherDictionary = new DictionaryBlock(numbers, makePositions(1, 332, 3));
        int[] otherDictionaryPositions = makePositions(0, 150, 2);
        assertFilter(filter, otherDictionary, otherDictionaryPositions, otherDictionaryPositions.length);

        // Repeat test on a DictionaryBlock over different content to make sure that cached results are not reused.
        assertFilter(filter, new DictionaryBlock(makeNumbers(1, 1001), allPositions), allPositions, allPositions.length);
    }

    private static void assertFilter(FilterFunction filter, Block input, int[] inputPositions, int positionCount)
    {
        // Copy the positions array because filter mutates it.
        int[] positions = Arrays.copyOf(inputPositions, positionCount);
        RuntimeException[] errors = new RuntimeException[inputPositions[positionCount - 1] + 1];
        // Put a pre-existing error in the 1st half of the input.
        int numPreviousErrors = positionCount / 2;
        for (int i = 0; i < numPreviousErrors; i++) {
            errors[i] = new RuntimeException("Pre-existent error at  " + positions[i]);
        }
        int lastErrorPosition = numPreviousErrors > 0 ? positions[numPreviousErrors - 1] : -1;
        int numHits = filter.filter(new Page(positionCount, input), positions, positionCount, errors);
        int hitCounter = 0;
        for (int position : inputPositions) {
            long number = input.getLong(position);
            if (number == UNLUCKY) {
                assertEquals(positions[hitCounter], position);
                assertTrue(errors[hitCounter] instanceof UnluckyError);
                hitCounter++;
            }
            else if ((number & 1) == 1) {
                assertEquals(positions[hitCounter], position);
                if (position <= lastErrorPosition) {
                    assertTrue(errors[hitCounter] instanceof RuntimeException);
                }
                else {
                    assertEquals(errors[hitCounter], null);
                }
                hitCounter++;
            }
        }
        assertEquals(numHits, hitCounter);
    }

    private static class UnluckyError
            extends RuntimeException
    {
    }

    private static int[] makePositions(int from, int count, int step)
    {
        int[] array = new int[count];
        for (int i = 0; i < count; i++) {
            array[i] = from + step * i;
        }
        return array;
    }

    private static Block makeNumbers(int from, int to)
    {
        int count = to - from;
        long[] array = new long[count];
        for (int i = 0; i < count; i++) {
            array[i] = from + i;
        }
        return new LongArrayBlock(count, Optional.empty(), array);
    }

    private static class IsOddPredicate
            implements Predicate
    {
        @Override
        public int[] getInputChannels()
        {
            return new int[] {0};
        }

        @Override
        public boolean evaluate(SqlFunctionProperties properties, Page page, int position)
        {
            long number = page.getBlock(0).getLong(position);
            if (number == UNLUCKY) {
                throw new UnluckyError();
            }
            return (number & 1) == 1;
        }
    }
}
