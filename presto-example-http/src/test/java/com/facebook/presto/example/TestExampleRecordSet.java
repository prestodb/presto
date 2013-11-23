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
package com.facebook.presto.example;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestExampleRecordSet
{
    private ExampleHttpServer exampleHttpServer;
    private URL dataUrl;

    @Test
    public void testGetColumnTypes()
            throws Exception
    {
        RecordSet recordSet = new ExampleRecordSet(new ExampleSplit("test", "schema", "table", dataUrl.toURI()), ImmutableList.of(
                new ExampleColumnHandle("test", "text", STRING, 0),
                new ExampleColumnHandle("test", "value", LONG, 1)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(STRING, LONG));

        recordSet = new ExampleRecordSet(new ExampleSplit("test", "schema", "table", dataUrl.toURI()), ImmutableList.of(
                new ExampleColumnHandle("test", "value", LONG, 1),
                new ExampleColumnHandle("test", "text", STRING, 0)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(LONG, STRING));

        recordSet = new ExampleRecordSet(new ExampleSplit("test", "schema", "table", dataUrl.toURI()), ImmutableList.of(
                new ExampleColumnHandle("test", "value", LONG, 1),
                new ExampleColumnHandle("test", "value", LONG, 1),
                new ExampleColumnHandle("test", "text", STRING, 0)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(LONG, LONG, STRING));

        recordSet = new ExampleRecordSet(new ExampleSplit("test", "schema", "table", dataUrl.toURI()), ImmutableList.<ExampleColumnHandle>of());
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of());
    }

    @Test
    public void testCursorSimple()
            throws Exception
    {
        RecordSet recordSet = new ExampleRecordSet(new ExampleSplit("test", "schema", "table", dataUrl.toURI()), ImmutableList.of(
                new ExampleColumnHandle("test", "text", STRING, 0),
                new ExampleColumnHandle("test", "value", LONG, 1)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), STRING);
        assertEquals(cursor.getType(1), LONG);

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(new String(cursor.getString(0), UTF_8), cursor.getLong(1));
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
        }
        assertEquals(data, ImmutableMap.<String, Long>builder()
                .put("ten", 10L)
                .put("eleven", 11L)
                .put("twelve", 12L)
                .build());
    }

    @Test
    public void testCursorMixedOrder()
            throws Exception
    {
        RecordSet recordSet = new ExampleRecordSet(new ExampleSplit("test", "schema", "table", dataUrl.toURI()), ImmutableList.of(
                new ExampleColumnHandle("test", "value", LONG, 1),
                new ExampleColumnHandle("test", "value", LONG, 1),
                new ExampleColumnHandle("test", "text", STRING, 0)));
        RecordCursor cursor = recordSet.cursor();

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            assertEquals(cursor.getLong(0), cursor.getLong(1));
            data.put(new String(cursor.getString(2), UTF_8), cursor.getLong(0));
        }
        assertEquals(data, ImmutableMap.<String, Long>builder()
                .put("ten", 10L)
                .put("eleven", 11L)
                .put("twelve", 12L)
                .build());
    }

    //
    // TODO: your code should also have tests for all types that you support and for the state machine of your cursor
    //


    //
    // Start http server for testing
    //

    @BeforeClass
    public void setUp()
            throws Exception
    {
        exampleHttpServer = new ExampleHttpServer();
        dataUrl = exampleHttpServer.getBaseUrl().resolve("/example-data/numbers-2.csv").toURL();
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        if (exampleHttpServer != null) {
            exampleHttpServer.stop();
        }
    }
}
