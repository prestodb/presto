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
package com.facebook.presto.hive;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.SchemaTableName;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.hive.TestHiveMetadataUpdater.TEST_FILE_NAME;
import static org.testng.Assert.assertEquals;

public class TestHiveMetadataUpdateHandle
{
    public static final UUID TEST_REQUEST_ID = UUID.randomUUID();
    public static final String TEST_SCHEMA_NAME = "schema";
    public static final String TEST_TABLE_NAME = "table";
    public static final String TEST_PARTITION_NAME = "partition_name";

    public static final SchemaTableName TEST_SCHEMA_TABLE_NAME = new SchemaTableName(TEST_SCHEMA_NAME, TEST_TABLE_NAME);
    public static final HiveMetadataUpdateHandle TEST_HIVE_METADATA_UPDATE_REQUEST = new HiveMetadataUpdateHandle(TEST_REQUEST_ID, TEST_SCHEMA_TABLE_NAME, Optional.of(TEST_PARTITION_NAME), Optional.empty());

    private final JsonCodec<HiveMetadataUpdateHandle> codec = JsonCodec.jsonCodec(HiveMetadataUpdateHandle.class);

    @Test
    public void testHiveMetadataUpdateRequest()
    {
        testRoundTrip(TEST_HIVE_METADATA_UPDATE_REQUEST);
    }

    @Test
    public void testHiveMetadataUpdateResult()
    {
        HiveMetadataUpdateHandle request = TEST_HIVE_METADATA_UPDATE_REQUEST;
        HiveMetadataUpdateHandle expectedHiveMetadataUpdateResult = new HiveMetadataUpdateHandle(request.getRequestId(), request.getSchemaTableName(), request.getPartitionName(), Optional.of(TEST_FILE_NAME));
        testRoundTrip(expectedHiveMetadataUpdateResult);
    }

    private void testRoundTrip(HiveMetadataUpdateHandle expected)
    {
        String json = codec.toJson(expected);
        HiveMetadataUpdateHandle actual = codec.fromJson(json);

        assertEquals(actual.getRequestId(), expected.getRequestId());
        assertEquals(actual.getSchemaTableName(), expected.getSchemaTableName());
        assertEquals(actual.getPartitionName(), expected.getPartitionName());
        assertEquals(actual.getMetadataUpdate(), expected.getMetadataUpdate());
    }
}
