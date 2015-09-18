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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import org.testng.annotations.Test;

import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static com.facebook.presto.localfile.LocalFileTables.HttpRequestLogTable.getSchemaTableName;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestLocalFileRecordSet
{
    private static final HostAddress address = HostAddress.fromParts("localhost", 1234);

    @Test
    public void testSimpleCursor()
            throws Exception
    {
        String location = "example-data";
        LocalFileTables localFileTables = new LocalFileTables(new LocalFileConfig().setHttpRequestLogLocation(getResourceFilePath(location)));
        LocalFileMetadata metadata = new LocalFileMetadata(localFileTables);

        assertData(localFileTables, metadata);
    }

    @Test
    public void testGzippedData()
            throws Exception
    {
        String location = "example-gzipped-data";
        LocalFileTables localFileTables = new LocalFileTables(new LocalFileConfig().setHttpRequestLogLocation(getResourceFilePath(location)));
        LocalFileMetadata metadata = new LocalFileMetadata(localFileTables);

        assertData(localFileTables, metadata);
    }

    private static void assertData(LocalFileTables localFileTables, LocalFileMetadata metadata)
    {
        SchemaTableName tableName = getSchemaTableName();
        List<LocalFileColumnHandle> columnHandles = metadata.getColumnHandles(SESSION, new LocalFileTableHandle(tableName, OptionalInt.of(0), OptionalInt.of(-1)))
                .values().stream().map(column -> (LocalFileColumnHandle) column)
                .collect(Collectors.toList());

        LocalFileRecordSet recordSet = new LocalFileRecordSet(localFileTables, new LocalFileSplit(address, tableName, TupleDomain.all()), columnHandles);
        RecordCursor cursor = recordSet.cursor();

        for (int i = 0; i < columnHandles.size(); i++) {
            assertEquals(cursor.getType(i), columnHandles.get(i).getColumnType());
        }

        // test one row
        assertTrue(cursor.advanceNextPosition());
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

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getSlice(0).toStringUtf8(), address.toString());
        assertEquals(cursor.getSlice(2).toStringUtf8(), "127.0.0.1");
        assertEquals(cursor.getSlice(3).toStringUtf8(), "GET");
        assertEquals(cursor.getSlice(4).toStringUtf8(), "/v1/service/presto/general");
        assertEquals(cursor.getSlice(5).toStringUtf8(), "foo");
        assertEquals(cursor.getSlice(6).toStringUtf8(), "ffffffff-ffff-ffff-ffff-ffffffffffff");
        assertEquals(cursor.getLong(7), 200);
        assertEquals(cursor.getLong(8), 0);
        assertEquals(cursor.getLong(9), 37);
        assertEquals(cursor.getLong(10), 1094);
        assertEquals(cursor.getSlice(11).toStringUtf8(), "a7229d56-5cbd-4e23-81ff-312ba6be0f12");
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}
