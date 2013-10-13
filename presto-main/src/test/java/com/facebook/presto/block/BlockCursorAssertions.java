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

import com.facebook.presto.type.Type;
import com.facebook.presto.type.Types;

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

        Type type = cursor.getType();
        if (type == Types.BOOLEAN) {
            assertEquals(cursor.getBoolean(), value);
            assertEquals(cursor.getSlice().getByte(0) != 0, value);
            try {
                cursor.getLong();
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                cursor.getDouble();
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
        }
        else if (type == Types.BIGINT) {
            assertEquals(cursor.getLong(), value);
            assertEquals(cursor.getSlice().getLong(0), value);
            try {
                cursor.getBoolean();
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }
            try {
                cursor.getDouble();
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }

        }
        else if (type == Types.DOUBLE) {
            assertEquals(cursor.getDouble(), value);
            assertEquals(cursor.getSlice().getDouble(0), value);
            try {
                cursor.getBoolean();
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

        }
        else if (type == Types.VARCHAR) {
            assertEquals(cursor.getSlice().toStringUtf8(), value);
            try {
                cursor.getBoolean();
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
            try {
                cursor.getDouble();
                fail("Expected IllegalStateException or UnsupportedOperationException");
            }
            catch (IllegalStateException | UnsupportedOperationException expected) {
            }

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
