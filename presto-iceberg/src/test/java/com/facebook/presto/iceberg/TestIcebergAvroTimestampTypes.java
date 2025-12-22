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

import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Tests for Avro timestamp logical types in Iceberg connector.
 * Tests the mapping of:
 * - timestamp-micros with adjust-to-utc: true -> TIMESTAMP_WITH_TIME_ZONE
 * - timestamp-nanos without adjust-to-utc: false -> TIMESTAMP
 */
public class TestIcebergAvroTimestampTypes
{
    @Test
    public void testTimestampMicrosWithAdjustToUtc()
    {
        // timestamp-micros with adjust-to-utc: true should map to TIMESTAMP_WITH_TIME_ZONE
        Types.TimestampType icebergType = Types.TimestampType.withZone();

        Type prestoType = toPrestoType(icebergType, null);

        assertTrue(prestoType instanceof TimestampWithTimeZoneType,
                "timestamp-micros with adjust-to-utc should map to TIMESTAMP_WITH_TIME_ZONE");
        assertEquals(prestoType, TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test
    public void testTimestampNanosWithoutAdjustToUtc()
    {
        // timestamp-nanos without adjust-to-utc: false should map to TIMESTAMP
        Types.TimestampType icebergType = Types.TimestampType.withoutZone();

        Type prestoType = toPrestoType(icebergType, null);

        assertTrue(prestoType instanceof TimestampType,
                "timestamp-nanos without adjust-to-utc should map to TIMESTAMP");
        assertEquals(prestoType, TimestampType.TIMESTAMP);
    }

    @Test
    public void testMixedTimestampTypes()
    {
        // Test schema with both timestamp types
        Schema schema = new Schema(
                Types.NestedField.required(1, "ts_with_tz", Types.TimestampType.withZone()),
                Types.NestedField.required(2, "ts_without_tz", Types.TimestampType.withoutZone()));

        Type typeWithTz = toPrestoType(schema.findField(1).type(), null);
        Type typeWithoutTz = toPrestoType(schema.findField(2).type(), null);

        assertEquals(typeWithTz, TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE,
                "Field with timezone should map to TIMESTAMP_WITH_TIME_ZONE");
        assertEquals(typeWithoutTz, TimestampType.TIMESTAMP,
                "Field without timezone should map to TIMESTAMP");
    }

    @Test
    public void testTimestampTypeConversion()
    {
        // Test that Iceberg timestamp types correctly identify their UTC adjustment
        Types.TimestampType withZone = Types.TimestampType.withZone();
        Types.TimestampType withoutZone = Types.TimestampType.withoutZone();

        assertTrue(withZone.shouldAdjustToUTC(),
                "TimestampType.withZone() should adjust to UTC");
        assertFalse(withoutZone.shouldAdjustToUTC(), "TimestampType.withoutZone() should not adjust to UTC");
    }
}
