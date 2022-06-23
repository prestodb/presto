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
                "s3://bucket/path/to/delta/table/file1.parquet",
                0,
                200,
                500,
                ImmutableMap.of("part1", "part1Val"));

        String json = codec.toJson(expected);
        DeltaSplit actual = codec.fromJson(json);

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getTable(), expected.getTable());
        assertEquals(actual.getFilePath(), expected.getFilePath());
        assertEquals(actual.getStart(), expected.getStart());
        assertEquals(actual.getLength(), expected.getLength());
        assertEquals(actual.getFileSize(), expected.getFileSize());
        assertEquals(actual.getSplitSizeInBytes(), expected.getSplitSizeInBytes());
        assertEquals(actual.getPartitionValues(), expected.getPartitionValues());
    }
}
