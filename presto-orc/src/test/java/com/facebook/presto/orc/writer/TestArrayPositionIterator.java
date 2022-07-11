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

import static com.facebook.presto.orc.writer.TestRangePositionIterator.assertPositions;
import static org.testng.Assert.assertThrows;

public class TestArrayPositionIterator
{
    private ArrayPositionIterator it;

    @BeforeMethod
    public void setUp()
    {
        it = new ArrayPositionIterator();
    }

    @Test
    public void testEmpty()
    {
        assertPositions(it, ImmutableList.of());
    }

    @Test
    public void testSinglePosition()
    {
        it.add(5);
        assertPositions(it, ImmutableList.of(5));
    }

    @Test
    public void testMultiplePositions()
    {
        it.add(5);
        it.add(7);
        it.add(10);
        assertPositions(it, ImmutableList.of(5, 7, 10));
    }

    @Test
    public void testClear()
    {
        it.clear();
        assertPositions(it, ImmutableList.of());

        it.add(5);
        assertPositions(it, ImmutableList.of(5));
        it.clear();
        assertPositions(it, ImmutableList.of());

        it.add(5);
        it.add(7);
        assertPositions(it, ImmutableList.of(5, 7));
        it.clear();
        assertPositions(it, ImmutableList.of());
    }

    @Test
    public void testGoingOverSize()
    {
        it.add(5);
        it.next();
        assertThrows(IndexOutOfBoundsException.class, () -> it.next());

        it.clear();
        assertThrows(IndexOutOfBoundsException.class, () -> it.next());
    }
}
