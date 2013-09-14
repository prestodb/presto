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
package com.facebook.presto.block;

import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.Tuples;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map.Entry;
import java.util.SortedMap;

import static com.facebook.presto.block.BlockCursorAssertions.assertAdvanceToPosition;
import static com.facebook.presto.block.BlockCursorAssertions.assertCurrentValue;
import static com.facebook.presto.block.BlockCursorAssertions.assertNextPosition;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestBlockCursor
{
    private final SortedMap<Integer, Tuple> expectedValues = BlockCursorAssertions.toTuplesMap(createTestCursor());

    protected abstract Block createExpectedValues();

    protected BlockCursor createTestCursor()
    {
        return createExpectedValues().cursor();
    }

    public final Tuple getExpectedValue(int position)
    {
        return getExpectedValues().get(position);
    }

    public final SortedMap<Integer, Tuple> getExpectedValues()
    {
        return expectedValues;
    }

    @BeforeClass
    public void setUp()
    {
        // verify expected values
        assertEquals(ImmutableList.copyOf(getExpectedValues().keySet()), ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    }

    @Test
    public void testAdvanceToPosition()
    {
        //
        // Note this code will more effective if the values are laid out as follows:
        //
        //   A, A, A, B, B, B, B, B, C, C, D
        //

        BlockCursor cursor = createTestCursor();

        // advance to first position
        assertAdvanceToPosition(cursor, 0);
        assertCurrentValue(cursor, 0, getExpectedValue(0));

        // skip to position in first value
        assertAdvanceToPosition(cursor, 2);
        assertCurrentValue(cursor, 2, getExpectedValue(2));

        // advance to same position
        assertAdvanceToPosition(cursor, 2);
        assertCurrentValue(cursor, 2, getExpectedValue(2));

        // skip to position in next value
        assertAdvanceToPosition(cursor, 4);
        assertCurrentValue(cursor, 4, getExpectedValue(4));

        // skip to position in third value
        assertAdvanceToPosition(cursor, 8);
        assertCurrentValue(cursor, 8, getExpectedValue(8));

        // skip to last position
        assertAdvanceToPosition(cursor, 10);
        assertCurrentValue(cursor, 10, getExpectedValue(10));

        // skip backwards
        try {
            cursor.advanceToPosition(2);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertCurrentValue(cursor, 10, getExpectedValue(10));
        }

        // skip past end
        assertFalse(cursor.advanceToPosition(100));
        assertTrue(cursor.isFinished());
        assertFalse(cursor.isValid());
    }

    @Test
    public void testStates()
    {
        BlockCursor cursor = createTestCursor();

        //
        // We are before the first position, so the cursor is not valid and all get current methods should throw an IllegalStateException
        //
        assertFalse(cursor.isValid());
        assertFalse(cursor.isFinished());

        try {
            cursor.getTuple();
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        try {
            cursor.currentTupleEquals(Tuples.createTuple(0L));
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException expected) {
        }

        //
        // advance to end
        //
        while (cursor.advanceNextPosition()) {
            assertTrue(cursor.isValid());
            assertFalse(cursor.isFinished());
        }

        //
        // We are beyond the last position, so the cursor is not valid, finished and all get next methods should return false
        //

        assertFalse(cursor.isValid());
        assertTrue(cursor.isFinished());

        assertFalse(cursor.advanceNextPosition());

        assertFalse(cursor.isValid());
        assertTrue(cursor.isFinished());
    }

    @Test
    public void testFirstPosition()
    {
        BlockCursor cursor = createTestCursor();
        assertNextPosition(cursor, 0, getExpectedValue(0));
    }

    @Test
    public void testAdvanceNextPosition()
    {
        BlockCursor cursor = createTestCursor();

        for (Entry<Integer, Tuple> entry : getExpectedValues().entrySet()) {
            assertNextPosition(cursor, entry.getKey(), entry.getValue());
        }

        assertFalse(cursor.advanceNextPosition());
    }
}
