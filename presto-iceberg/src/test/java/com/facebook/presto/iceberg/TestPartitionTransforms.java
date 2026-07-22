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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestPartitionTransforms
{
    @Test
    public void testToStringMatchesSpecification()
    {
        assertEquals(Transforms.identity().toString(), "identity");
        assertEquals(Transforms.bucket(13).toString(), "bucket[13]");
        assertEquals(Transforms.truncate(19).toString(), "truncate[19]");
        assertEquals(Transforms.year().toString(), "year");
        assertEquals(Transforms.month().toString(), "month");
        assertEquals(Transforms.day().toString(), "day");
        assertEquals(Transforms.hour().toString(), "hour");
    }

    // Schema with a single timestamp_ntz column (Iceberg TimestampType.withoutZone),
    // matching what Spark writes for timestamp_ntz / TIMESTAMP_MICROSECONDS columns.
    private static final Schema TIMESTAMP_NTZ_SCHEMA = new Schema(
            Types.NestedField.required(1, "ts", Types.TimestampType.withoutZone()));

    private static PartitionField dayField()
    {
        return PartitionSpec.builderFor(TIMESTAMP_NTZ_SCHEMA).day("ts").build().fields().get(0);
    }

    private static PartitionField monthField()
    {
        return PartitionSpec.builderFor(TIMESTAMP_NTZ_SCHEMA).month("ts").build().fields().get(0);
    }

    private static PartitionField yearField()
    {
        return PartitionSpec.builderFor(TIMESTAMP_NTZ_SCHEMA).year("ts").build().fields().get(0);
    }

    private static PartitionField hourField()
    {
        return PartitionSpec.builderFor(TIMESTAMP_NTZ_SCHEMA).hour("ts").build().fields().get(0);
    }

    /**
     * Converts a ZonedDateTime to microseconds since the Unix epoch, which is how
     * TIMESTAMP_MICROSECONDS (Spark timestamp_ntz) stores its values.
     */
    private static long toMicros(ZonedDateTime dt)
    {
        return ChronoUnit.MICROS.between(Instant.EPOCH, dt.toInstant());
    }

    /**
     * −999 µs since epoch = 1969-12-31T23:59:59.999_001Z.
     * Chosen because it is sub-millisecond before the epoch, making the two
     * divisions diverge: −999 / 1000 = 0 (Java truncates toward zero, wrong)
     * vs Math.floorDiv(−999, 1000) = −1 (correct floor toward −∞).
     * This directly catches the truncation-toward-zero bug in the µs→ms conversion.
     */
    private static final long MICROS_JUST_BEFORE_EPOCH = -999L;

    @DataProvider(name = "timestampMicrosecondsTransforms")
    public Object[][] timestampMicrosecondsTransforms()
    {
        ZonedDateTime positiveDt = ZonedDateTime.of(2026, 1, 18, 3, 0, 0, 0, ZoneOffset.UTC);
        long positiveMicros = toMicros(positiveDt);

        return new Object[][] {
                // positive timestamps (post-epoch)
                {dayField(), positiveMicros, (int) ChronoUnit.DAYS.between(Instant.EPOCH, positiveDt.toInstant()), "day/positive"},
                {monthField(), positiveMicros, (int) ((long) (positiveDt.getYear() - 1970) * 12 + (positiveDt.getMonthValue() - 1)), "month/positive"},
                {yearField(), positiveMicros, positiveDt.getYear() - 1970, "year/positive"},
                {hourField(), positiveMicros, (int) ChronoUnit.HOURS.between(Instant.EPOCH, positiveDt.toInstant()), "hour/positive"},
                // negative timestamps (pre-epoch): -999 / 1000 = 0 (wrong); Math.floorDiv(-999, 1000) = -1 (correct)
                {dayField(), MICROS_JUST_BEFORE_EPOCH, -1, "day/negative"},
                {monthField(), MICROS_JUST_BEFORE_EPOCH, -1, "month/negative"},
                {yearField(), MICROS_JUST_BEFORE_EPOCH, -1, "year/negative"},
                {hourField(), MICROS_JUST_BEFORE_EPOCH, -1, "hour/negative"},
        };
    }

    @Test(dataProvider = "timestampMicrosecondsTransforms")
    public void testTimestampMicrosecondsTransform(PartitionField field, long inputMicros, int expectedValue, String description)
    {
        PartitionTransforms.ColumnTransform transform = PartitionTransforms.getColumnTransform(field, TIMESTAMP_MICROSECONDS);

        BlockBuilder builder = TIMESTAMP_MICROSECONDS.createFixedSizeBlockBuilder(1);
        TIMESTAMP_MICROSECONDS.writeLong(builder, inputMicros);
        Block result = transform.getTransform().apply(builder.build());

        assertFalse(result.isNull(0), description + ": result should not be null");
        assertEquals(result.getInt(0), expectedValue, description);
    }
}
