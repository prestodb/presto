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
package com.facebook.presto.iceberg;

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.iceberg.delete.DeleteFile;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.iceberg.FileContent.POSITION_DELETES;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.FileFormat.PUFFIN;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergInsertTableHandle
{
    private static final ObjectMapper OBJECT_MAPPER = new JsonObjectMapperProvider().get();

    @Test
    public void testLegacyConstructorDefaultsExistingDeletionVectorsToEmptyMap()
    {
        IcebergInsertTableHandle handle = newInsertTableHandle(ImmutableMap.of());

        assertTrue(handle.getExistingDeletionVectors().isEmpty());
    }

    @Test
    public void testExistingDeletionVectorsJsonRoundTrip()
            throws Exception
    {
        DeleteFile deleteFile = new DeleteFile(
                POSITION_DELETES,
                "file:/tmp/table/delete-vector.puffin",
                PUFFIN,
                10,
                1024,
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.of(12L),
                Optional.of(34L),
                Optional.of("file:/tmp/table/data.parquet"),
                56L);
        IcebergInsertTableHandle handle = newInsertTableHandle(ImmutableMap.of("file:/tmp/table/data.parquet", deleteFile));

        String json = OBJECT_MAPPER.writeValueAsString(handle);
        IcebergInsertTableHandle copy = OBJECT_MAPPER.readValue(json, IcebergInsertTableHandle.class);

        assertEquals(copy.getExistingDeletionVectors().size(), 1);
        DeleteFile copyDeleteFile = copy.getExistingDeletionVectors().get("file:/tmp/table/data.parquet");
        assertEquals(copyDeleteFile.path(), "file:/tmp/table/delete-vector.puffin");
        assertEquals(copyDeleteFile.format(), PUFFIN);
        assertEquals(copyDeleteFile.getContentOffset(), Optional.of(12L));
        assertEquals(copyDeleteFile.getContentSizeInBytes(), Optional.of(34L));
        assertEquals(copyDeleteFile.getReferencedDataFile(), Optional.of("file:/tmp/table/data.parquet"));
        assertEquals(copyDeleteFile.getDataSequenceNumber(), 56L);
        assertEquals(copy.getInsertedColumns(), ImmutableList.of("id"));
    }

    private static IcebergInsertTableHandle newInsertTableHandle(ImmutableMap<String, DeleteFile> existingDeletionVectors)
    {
        return new IcebergInsertTableHandle(
                "schema",
                new IcebergTableName("table", DATA, Optional.empty(), Optional.empty(), Optional.empty()),
                emptySchema(),
                new PrestoIcebergPartitionSpec(1, emptySchema(), ImmutableList.of()),
                ImmutableList.of(),
                "file:/tmp/output",
                PARQUET,
                HiveCompressionCodec.NONE,
                ImmutableMap.of(),
                ImmutableList.of(),
                Optional.empty(),
                false,
                ImmutableList.of("id"),
                existingDeletionVectors);
    }

    private static PrestoIcebergSchema emptySchema()
    {
        return new PrestoIcebergSchema(1, ImmutableList.of(), ImmutableMap.of(), null, ImmutableSet.of());
    }
}
