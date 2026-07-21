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

    @Test
    public void testDayTransformOnTimestampMicroseconds()
    {
        ZonedDateTime dt = ZonedDateTime.of(2026, 1, 18, 0, 0, 0, 0, ZoneOffset.UTC);
        long micros = toMicros(dt);
        long expectedDay = ChronoUnit.DAYS.between(Instant.EPOCH, dt.toInstant());

        PartitionTransforms.ColumnTransform transform = PartitionTransforms.getColumnTransform(dayField(), TIMESTAMP_MICROSECONDS);
        BlockBuilder builder = TIMESTAMP_MICROSECONDS.createFixedSizeBlockBuilder(1);
        TIMESTAMP_MICROSECONDS.writeLong(builder, micros);
        Block result = transform.getTransform().apply(builder.build());

        assertFalse(result.isNull(0));
        assertEquals(result.getInt(0), (int) expectedDay);
    }

    @Test
    public void testMonthTransformOnTimestampMicroseconds()
    {
        ZonedDateTime dt = ZonedDateTime.of(2026, 1, 18, 0, 0, 0, 0, ZoneOffset.UTC);
        long micros = toMicros(dt);
        // Iceberg month transform: months since 1970-01-01 = (year-1970)*12 + (month-1)
        long expectedMonth = (long) (dt.getYear() - 1970) * 12 + (dt.getMonthValue() - 1);

        PartitionTransforms.ColumnTransform transform = PartitionTransforms.getColumnTransform(monthField(), TIMESTAMP_MICROSECONDS);
        BlockBuilder builder = TIMESTAMP_MICROSECONDS.createFixedSizeBlockBuilder(1);
        TIMESTAMP_MICROSECONDS.writeLong(builder, micros);
        Block result = transform.getTransform().apply(builder.build());

        assertFalse(result.isNull(0));
        assertEquals(result.getInt(0), (int) expectedMonth);
    }

    @Test
    public void testYearTransformOnTimestampMicroseconds()
    {
        ZonedDateTime dt = ZonedDateTime.of(2026, 1, 18, 0, 0, 0, 0, ZoneOffset.UTC);
        long micros = toMicros(dt);
        // Iceberg year transform: years since 1970
        long expectedYear = dt.getYear() - 1970;

        PartitionTransforms.ColumnTransform transform = PartitionTransforms.getColumnTransform(yearField(), TIMESTAMP_MICROSECONDS);
        BlockBuilder builder = TIMESTAMP_MICROSECONDS.createFixedSizeBlockBuilder(1);
        TIMESTAMP_MICROSECONDS.writeLong(builder, micros);
        Block result = transform.getTransform().apply(builder.build());

        assertFalse(result.isNull(0));
        assertEquals(result.getInt(0), (int) expectedYear);
    }

    @Test
    public void testHourTransformOnTimestampMicroseconds()
    {
        ZonedDateTime dt = ZonedDateTime.of(2026, 1, 18, 3, 0, 0, 0, ZoneOffset.UTC);
        long micros = toMicros(dt);
        long expectedHour = ChronoUnit.HOURS.between(Instant.EPOCH, dt.toInstant());

        PartitionTransforms.ColumnTransform transform = PartitionTransforms.getColumnTransform(hourField(), TIMESTAMP_MICROSECONDS);
        BlockBuilder builder = TIMESTAMP_MICROSECONDS.createFixedSizeBlockBuilder(1);
        TIMESTAMP_MICROSECONDS.writeLong(builder, micros);
        Block result = transform.getTransform().apply(builder.build());

        assertFalse(result.isNull(0));
        assertEquals(result.getInt(0), (int) expectedHour);
    }

    // Negative-timestamp tests — catch the truncation-toward-zero bug where
    // `value / 1000` gives the wrong millisecond bucket for negative µs values.
    //
    // Chosen timestamp: 1969-12-31T23:59:59.999_001Z = −999 µs since epoch.
    // This is sub-millisecond before the epoch, so the two divisions diverge:
    //
    //   −999 / 1000               =  0  ms  (Java truncates toward zero)
    //   Math.floorDiv(−999, 1000) = −1  ms  (correct: floor toward −∞)

    /*
     * −999 µs since epoch = 1969-12-31T23:59:59.999_001Z.
     * Java: −999 / 1000 = 0 (truncates toward zero); Math.floorDiv(−999, 1000) = −1 (correct).
     */
    private static final long MICROS_JUST_BEFORE_EPOCH = -999L;

    @Test
    public void testDayTransformOnTimestampMicrosecondsNegative()
    {
        // −999 µs is 999 µs before epoch: in Dec 31 1969 (day −1), but only 0.999 ms before
        // midnight, so value/1000 = 0 (wrong) while floorDiv = −1 (correct).
        long micros = MICROS_JUST_BEFORE_EPOCH;

        PartitionTransforms.ColumnTransform transform = PartitionTransforms.getColumnTransform(dayField(), TIMESTAMP_MICROSECONDS);
        BlockBuilder builder = TIMESTAMP_MICROSECONDS.createFixedSizeBlockBuilder(1);
        TIMESTAMP_MICROSECONDS.writeLong(builder, micros);
        Block result = transform.getTransform().apply(builder.build());

        assertFalse(result.isNull(0));
        // Dec 31 1969 = epoch day −1
        assertEquals(result.getInt(0), -1);
    }

    @Test
    public void testHourTransformOnTimestampMicrosecondsNegative()
    {
        // Same timestamp: −999 µs is within the hour ending at epoch (hour −1 = 23:00–24:00 on Dec 31 1969).
        // value/1000 = 0 → epochHour = 0 (wrong); floorDiv = −1 → epochHour = −1 (correct).
        long micros = MICROS_JUST_BEFORE_EPOCH;

        PartitionTransforms.ColumnTransform transform = PartitionTransforms.getColumnTransform(hourField(), TIMESTAMP_MICROSECONDS);
        BlockBuilder builder = TIMESTAMP_MICROSECONDS.createFixedSizeBlockBuilder(1);
        TIMESTAMP_MICROSECONDS.writeLong(builder, micros);
        Block result = transform.getTransform().apply(builder.build());

        assertFalse(result.isNull(0));
        // The hour spanning 1969-12-31T23:xx is epoch hour −1
        assertEquals(result.getInt(0), -1);
    }

    @Test
    public void testMonthTransformOnTimestampMicrosecondsNegative()
    {
        // 1969-12-31T23:59:59.999_001Z is in December 1969 = month −1.
        // epochMonth delegates to epochYear/MONTH_OF_YEAR_UTC on the millisecond value,
        // and −999 µs → floorDiv → −1 ms is still Dec 1969, so the month result is −1.
        long micros = MICROS_JUST_BEFORE_EPOCH;

        PartitionTransforms.ColumnTransform transform = PartitionTransforms.getColumnTransform(monthField(), TIMESTAMP_MICROSECONDS);
        BlockBuilder builder = TIMESTAMP_MICROSECONDS.createFixedSizeBlockBuilder(1);
        TIMESTAMP_MICROSECONDS.writeLong(builder, micros);
        Block result = transform.getTransform().apply(builder.build());

        assertFalse(result.isNull(0));
        // Dec 1969 = months since epoch: (1969 − 1970) * 12 + (12 − 1) = −12 + 11 = −1
        assertEquals(result.getInt(0), -1);
    }

    @Test
    public void testYearTransformOnTimestampMicrosecondsNegative()
    {
        // 1969-12-31T23:59:59.999_001Z is in 1969 = year −1 (years since 1970).
        long micros = MICROS_JUST_BEFORE_EPOCH;

        PartitionTransforms.ColumnTransform transform = PartitionTransforms.getColumnTransform(yearField(), TIMESTAMP_MICROSECONDS);
        BlockBuilder builder = TIMESTAMP_MICROSECONDS.createFixedSizeBlockBuilder(1);
        TIMESTAMP_MICROSECONDS.writeLong(builder, micros);
        Block result = transform.getTransform().apply(builder.build());

        assertFalse(result.isNull(0));
        // 1969 = −1 years since 1970
        assertEquals(result.getInt(0), -1);
    }
}
