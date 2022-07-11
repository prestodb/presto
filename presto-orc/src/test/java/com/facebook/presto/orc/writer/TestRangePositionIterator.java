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
package com.facebook.presto.orc.writer;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestRangePositionIterator
{
    private RangePositionIterator it;

    @BeforeMethod
    public void setUp()
    {
        it = new RangePositionIterator();
    }

    @Test
    public void testEmpty()
    {
        assertPositions(it, ImmutableList.of());
    }

    @Test
    public void testSingleRange()
    {
        it.addRange(5, 3);
        assertPositions(it, ImmutableList.of(5, 6, 7));
    }

    @Test
    public void testMultipleRanges()
    {
        it.addRange(1, 4);
        it.addRange(5, 4);
        it.addRange(20, 5);
        assertPositions(it, ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 20, 21, 22, 23, 24));
    }

    @Test
    public void testClear()
    {
        it.clear();
        assertPositions(it, ImmutableList.of());

        it.addRange(1, 4);
        assertPositions(it, ImmutableList.of(1, 2, 3, 4));
        it.clear();
        assertPositions(it, ImmutableList.of());

        it.addRange(1, 4);
        it.addRange(10, 5);
        assertPositions(it, ImmutableList.of(1, 2, 3, 4, 10, 11, 12, 13, 14));
        it.clear();
        assertPositions(it, ImmutableList.of());
    }

    @Test
    public void testGoingOverSize()
    {
        it.addRange(5, 1);
        it.next();
        assertThrows(IndexOutOfBoundsException.class, () -> it.next());

        it.clear();
        assertThrows(IndexOutOfBoundsException.class, () -> it.next());
    }

    // visible for TestArrayPositionIterator
    static void assertPositions(PositionIterator it, List<Integer> expectedValues)
    {
        assertPositionValues(it, expectedValues);

        // repeat after resetting
        it.reset();
        assertPositionValues(it, expectedValues);
    }

    private static void assertPositionValues(PositionIterator it, List<Integer> expectedValues)
    {
        assertEquals(it.getPositionCount(), expectedValues.size());
        ImmutableList.Builder<Integer> actualValues = ImmutableList.builderWithExpectedSize(it.getPositionCount());
        for (int i = 0; i < it.getPositionCount(); i++) {
            actualValues.add(it.next());
        }
        assertEquals(actualValues.build(), expectedValues);
    }
}
