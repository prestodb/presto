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

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestExampleRecordSetProvider
{
    private ExampleHttpServer exampleHttpServer;
    private URI dataUri;

    @Test
    public void testGetRecordSet()
            throws Exception
    {
        ExampleRecordSetProvider recordSetProvider = new ExampleRecordSetProvider(new ExampleConnectorId("test"));
        RecordSet recordSet = recordSetProvider.getRecordSet(ExampleTransactionHandle.INSTANCE, SESSION, new ExampleSplit("test", "schema", "table", dataUri), ImmutableList.of(
                new ExampleColumnHandle("test", "text", createUnboundedVarcharType(), 0),
                new ExampleColumnHandle("test", "value", BIGINT, 1)));
        assertNotNull(recordSet, "recordSet is null");

        RecordCursor cursor = recordSet.cursor();
        assertNotNull(cursor, "cursor is null");

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(1));
        }
        assertEquals(data, ImmutableMap.<String, Long>builder()
                .put("ten", 10L)
                .put("eleven", 11L)
                .put("twelve", 12L)
                .build());
    }

    //
    // Start http server for testing
    //

    @BeforeClass
    public void setUp()
            throws Exception
    {
        exampleHttpServer = new ExampleHttpServer();
        dataUri = exampleHttpServer.resolve("/example-data/numbers-2.csv");
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
