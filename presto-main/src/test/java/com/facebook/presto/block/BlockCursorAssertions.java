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

import java.util.SortedMap;
import java.util.TreeMap;

import static java.util.Collections.unmodifiableSortedMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public final class BlockCursorAssertions
{
    private BlockCursorAssertions() {}

    public static void assertAdvanceNextPosition(BlockCursor cursor)
    {
        assertTrue(cursor.advanceNextPosition());
    }

    public static void assertAdvanceToPosition(BlockCursor cursor, int position)
    {
        assertTrue(cursor.advanceToPosition(position));
    }

    public static void assertNextPosition(BlockCursor cursor, int position, Object value)
    {
        assertAdvanceNextPosition(cursor);
        assertCurrentValue(cursor, position, value);
    }

    public static void assertCurrentValue(BlockCursor cursor, int position, Object value)
    {
        assertEquals(cursor.getPosition(), position);
        assertEquals(cursor.getObjectValue(), value);
        assertEquals(cursor.getObjectValue(), value);

        assertEquals(cursor.isNull(), value == null);
        if (cursor.isNull()) {
            return;
        }
        switch (cursor.getTupleInfo().getType()) {
            case BOOLEAN:
                assertEquals(cursor.getBoolean(), value);
                try {
                    cursor.getSlice();
                    fail("Expected IllegalStateException or UnsupportedOperationException");
                }
                catch (IllegalStateException | UnsupportedOperationException expected) {
                }
                try {
                    cursor.getSlice();
                    fail("Expected IllegalStateException or UnsupportedOperationException");
                }
                catch (IllegalStateException | UnsupportedOperationException expected) {
                }
                break;
            case FIXED_INT_64:
                assertEquals(cursor.getLong(), value);
                try {
                    cursor.getDouble();
                    fail("Expected IllegalStateException or UnsupportedOperationException");
                }
                catch (IllegalStateException | UnsupportedOperationException expected) {
                }
                try {
                    cursor.getSlice();
                    fail("Expected IllegalStateException or UnsupportedOperationException");
                }
                catch (IllegalStateException | UnsupportedOperationException expected) {
                }
                break;
            case VARIABLE_BINARY:
                assertEquals(cursor.getSlice().toStringUtf8(), value);
                try {
                    cursor.getDouble();
                    fail("Expected IllegalStateException or UnsupportedOperationException");
                }
                catch (IllegalStateException | UnsupportedOperationException expected) {
                }
                try {
                    cursor.getLong();
                    fail("Expected IllegalStateException or UnsupportedOperationException");
                }
                catch (IllegalStateException | UnsupportedOperationException expected) {
                }
                break;
            case DOUBLE:
                assertEquals(cursor.getDouble(), value);
                try {
                    cursor.getSlice();
                    fail("Expected IllegalStateException or UnsupportedOperationException");
                }
                catch (IllegalStateException | UnsupportedOperationException expected) {
                }
                try {
                    cursor.getSlice();
                    fail("Expected IllegalStateException or UnsupportedOperationException");
                }
                catch (IllegalStateException | UnsupportedOperationException expected) {
                }
                break;
        }
    }

    public static SortedMap<Integer, Object> toValuesMap(BlockCursor cursor)
    {
        SortedMap<Integer, Object> values = new TreeMap<>();
        while (cursor.advanceNextPosition()) {
            values.put(cursor.getPosition(), cursor.getObjectValue());
        }
        return unmodifiableSortedMap(values);
    }
}
