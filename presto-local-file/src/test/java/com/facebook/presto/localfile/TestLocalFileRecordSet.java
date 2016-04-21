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
package com.facebook.presto.localfile;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.io.Resources;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.localfile.LocalFileTables.HttpRequestLogTable.getSchemaTableName;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestLocalFileRecordSet
{
    private static final LocalFileConnectorId CONNECTOR_ID = new LocalFileConnectorId("connectorId");
    private URI dataUri;
    private HostAddress address;
    private LocalFileMetadata metadata;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        address = HostAddress.fromParts("localhost", 1234);
        dataUri = Resources.getResource("example-data/http-request.log").toURI();
        metadata = new LocalFileMetadata(CONNECTOR_ID, dataUri);
    }

    @Test
    public void testSimpleCursor()
            throws Exception
    {
        List<LocalFileColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, new LocalFileTableHandle(CONNECTOR_ID, getSchemaTableName(), dataUri))
                .values().stream().map(column -> (LocalFileColumnHandle) column)
                .collect(Collectors.toList());

        LocalFileRecordSet recordSet = new LocalFileRecordSet(new LocalFileSplit(CONNECTOR_ID, address, dataUri), columnHandles);
        RecordCursor cursor = recordSet.cursor();

        for (int i = 0; i < columnHandles.size(); i++) {
            assertEquals(cursor.getType(i), columnHandles.get(i).getColumnType());
        }

        // test one row
        cursor.advanceNextPosition();
        assertEquals(cursor.getSlice(0).toStringUtf8(), address.toString());
        assertEquals(cursor.getSlice(2).toStringUtf8(), "127.0.0.1");
        assertEquals(cursor.getSlice(3).toStringUtf8(), "POST");
        assertEquals(cursor.getSlice(4).toStringUtf8(), "/v1/memory");
        assertTrue(cursor.isNull(5));
        assertTrue(cursor.isNull(6));
        assertEquals(cursor.getLong(7), 200);
        assertEquals(cursor.getLong(8), 0);
        assertEquals(cursor.getLong(9), 1000);
        assertEquals(cursor.getLong(10), 10);
        assertTrue(cursor.isNull(11));
    }
}
