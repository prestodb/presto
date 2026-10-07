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
package com.facebook.presto.delta;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Test {@link DeltaSplit} is created correctly with given arguments and JSON serialization/deserialization works.
 */
public class TestDeltaSplit
{
    private final JsonCodec<DeltaSplit> codec = JsonCodec.jsonCodec(DeltaSplit.class);

    @Test
    public void testJsonRoundTrip()
    {
        DeltaSplit expected = new DeltaSplit(
                "delta",
                "database",
                "table",
                "s3://bucket/path/to/delta/table",
                "file1.parquet",
                0,
                200,
                500,
                ImmutableMap.of("part1", "part1Val"),
                "name",
                NodeSelectionStrategy.NO_PREFERENCE);

        String json = codec.toJson(expected);
        DeltaSplit actual = codec.fromJson(json);

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getTable(), expected.getTable());
        assertEquals(actual.getTableLocation(), expected.getTableLocation());
        assertEquals(actual.getFilePath(), expected.getFilePath());
        assertEquals(actual.getStart(), expected.getStart());
        assertEquals(actual.getLength(), expected.getLength());
        assertEquals(actual.getFileSize(), expected.getFileSize());
        assertEquals(actual.getSplitSizeInBytes(), expected.getSplitSizeInBytes());
        assertEquals(actual.getPartitionValues(), expected.getPartitionValues());
        assertEquals(actual.getColumnMappingMode(), expected.getColumnMappingMode());
    }

    @Test
    public void testColumnMappingModeDefaultsToNone()
    {
        DeltaSplit split = new DeltaSplit(
                "delta",
                "database",
                "table",
                "s3://bucket/path/to/delta/table",
                "file1.parquet",
                0,
                200,
                500,
                ImmutableMap.of(),
                null,
                NodeSelectionStrategy.NO_PREFERENCE);
        // A null or empty value on the wire (older split serializations, or
        // callers that omit the field) resolves to the canonical "none".
        assertEquals(split.getColumnMappingMode(), DeltaTable.COLUMN_MAPPING_MODE_NONE);
    }

    @Test
    public void testColumnMappingModeIdRoundTrip()
    {
        DeltaSplit expected = new DeltaSplit(
                "delta",
                "database",
                "table",
                "s3://bucket/path/to/delta/table",
                "file1.parquet",
                0,
                200,
                500,
                ImmutableMap.of(),
                "id",
                NodeSelectionStrategy.NO_PREFERENCE);
        DeltaSplit actual = codec.fromJson(codec.toJson(expected));
        assertEquals(actual.getColumnMappingMode(), "id");
    }
}
