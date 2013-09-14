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
package com.facebook.presto.util;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Splitter;
import org.testng.annotations.Test;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.CharStreams.newReaderSupplier;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDelimitedRecordIterable
{
    @Test
    public void testExtraction()
            throws Exception
    {
        DelimitedRecordSet recordIterable = new DelimitedRecordSet(
                newReaderSupplier("apple,fuu,123\nbanana,bar,456"),
                Splitter.on(','),
                new ColumnMetadata("fruit", ColumnType.STRING, 0, false),
                new ColumnMetadata("foo", ColumnType.STRING, 1, false),
                new ColumnMetadata("value", ColumnType.STRING, 2, false));

        RecordCursor cursor = recordIterable.cursor();
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getString(0), "apple".getBytes(UTF_8));
        assertEquals(cursor.getString(1), "fuu".getBytes(UTF_8));
        assertEquals(cursor.getString(2), "123".getBytes(UTF_8));
        assertEquals(cursor.getLong(2), 123L);
        assertEquals(cursor.getDouble(2), 123.0);

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getString(0), "banana".getBytes(UTF_8));
        assertEquals(cursor.getString(1), "bar".getBytes(UTF_8));
        assertEquals(cursor.getString(2), "456".getBytes(UTF_8));
        assertEquals(cursor.getLong(2), 456L);
        assertEquals(cursor.getDouble(2), 456.0);

        assertFalse(cursor.advanceNextPosition());
    }
}
