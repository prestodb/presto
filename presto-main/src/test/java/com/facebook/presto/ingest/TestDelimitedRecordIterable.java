package com.facebook.presto.ingest;

import com.facebook.presto.operator.OperatorStats;
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
    public void testExtraction() throws Exception
    {
        DelimitedRecordSet recordIterable = new DelimitedRecordSet(
                newReaderSupplier("apple,fuu,123\nbanana,bar,456"),
                Splitter.on(','));

        RecordCursor cursor = recordIterable.cursor(new OperatorStats());
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
