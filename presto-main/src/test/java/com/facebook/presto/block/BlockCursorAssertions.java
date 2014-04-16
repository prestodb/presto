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

import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;

import java.util.Locale;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.unmodifiableSortedMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public final class BlockCursorAssertions
{
    public static final Session SESSION = new Session("user", "source", "catalog", "schema", UTC_KEY, Locale.ENGLISH, "address", "agent");

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
        assertEquals(cursor.getObjectValue(SESSION), value);
        assertEquals(cursor.getObjectValue(SESSION), value);

        assertEquals(cursor.isNull(), value == null);
        if (cursor.isNull()) {
            return;
        }

        Type type = cursor.getType();
        if (type == BOOLEAN) {
            assertEquals(cursor.getBoolean(), value);
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
        else if (type == BIGINT) {
            assertEquals(cursor.getLong(), value);
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
        else if (type == DOUBLE) {
            assertEquals(cursor.getDouble(), value);
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
        else if (type == VARCHAR) {
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
            values.put(cursor.getPosition(), cursor.getObjectValue(SESSION));
        }
        return unmodifiableSortedMap(values);
    }
}
