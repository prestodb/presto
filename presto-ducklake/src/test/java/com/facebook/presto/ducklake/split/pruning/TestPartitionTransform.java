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
package com.facebook.presto.ducklake.split.pruning;

import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static com.facebook.presto.ducklake.split.pruning.PartitionTransform.Kind.BUCKET;
import static com.facebook.presto.ducklake.split.pruning.PartitionTransform.Kind.DAY;
import static com.facebook.presto.ducklake.split.pruning.PartitionTransform.Kind.EPOCH;
import static com.facebook.presto.ducklake.split.pruning.PartitionTransform.Kind.HOUR;
import static com.facebook.presto.ducklake.split.pruning.PartitionTransform.Kind.IDENTITY;
import static com.facebook.presto.ducklake.split.pruning.PartitionTransform.Kind.MONTH;
import static com.facebook.presto.ducklake.split.pruning.PartitionTransform.Kind.UNKNOWN;
import static com.facebook.presto.ducklake.split.pruning.PartitionTransform.Kind.YEAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPartitionTransform
{
    @Test
    public void testParseKnownTransforms()
    {
        assertEquals(PartitionTransform.parse("identity").getKind(), IDENTITY);
        assertEquals(PartitionTransform.parse("year").getKind(), YEAR);
        assertEquals(PartitionTransform.parse("month").getKind(), MONTH);
        assertEquals(PartitionTransform.parse("day").getKind(), DAY);
        assertEquals(PartitionTransform.parse("hour").getKind(), HOUR);
        assertEquals(PartitionTransform.parse("  Identity ").getKind(), IDENTITY);
        assertEquals(PartitionTransform.parse("YEAR").getKind(), YEAR);
    }

    @Test
    public void testParseEpochTransforms()
    {
        assertEquals(PartitionTransform.parse("epoch_year").getKind(), EPOCH);
        assertEquals(PartitionTransform.parse("epoch_month").getKind(), EPOCH);
        assertEquals(PartitionTransform.parse("epoch_day").getKind(), EPOCH);
        assertEquals(PartitionTransform.parse("epoch_hour").getKind(), EPOCH);
    }

    @Test
    public void testParseBucket()
    {
        PartitionTransform bucket16 = PartitionTransform.parse("bucket(16)");
        assertEquals(bucket16.getKind(), BUCKET);
        assertEquals(bucket16.getBucketCount().getAsInt(), 16);

        PartitionTransform bucket4 = PartitionTransform.parse("BUCKET(4)");
        assertEquals(bucket4.getKind(), BUCKET);
        assertEquals(bucket4.getBucketCount().getAsInt(), 4);

        PartitionTransform bareBucket = PartitionTransform.parse("bucket");
        assertEquals(bareBucket.getKind(), BUCKET);
        assertFalse(bareBucket.getBucketCount().isPresent());
    }

    @Test
    public void testParseUnknown()
    {
        assertEquals(PartitionTransform.parse("garbage").getKind(), UNKNOWN);
        assertEquals(PartitionTransform.parse("bucket(abc)").getKind(), UNKNOWN);
        assertEquals(PartitionTransform.parse("").getKind(), UNKNOWN);
    }

    @Test
    public void testIsPrunableIdentity()
    {
        PartitionTransform identity = PartitionTransform.parse("identity");
        assertTrue(identity.isPrunable("int32"));
        assertTrue(identity.isPrunable("int64"));
        assertTrue(identity.isPrunable("varchar"));
        assertTrue(identity.isPrunable("date"));
        assertTrue(identity.isPrunable("boolean"));
        assertFalse(identity.isPrunable("float64"));
        assertFalse(identity.isPrunable("timestamp"));
    }

    @Test
    public void testIsPrunableCalendar()
    {
        PartitionTransform month = PartitionTransform.parse("month");
        assertTrue(month.isPrunable("timestamp_ns"));
        assertTrue(month.isPrunable("timestamp"));
        assertTrue(month.isPrunable("date"));
        assertFalse(month.isPrunable("timestamptz"));
        assertFalse(month.isPrunable("varchar"));
    }

    @Test
    public void testIsPrunableAlwaysFalse()
    {
        assertFalse(PartitionTransform.parse("bucket(16)").isPrunable("int32"));
        assertFalse(PartitionTransform.parse("epoch_month").isPrunable("timestamp"));
        assertFalse(PartitionTransform.parse("garbage").isPrunable("int32"));
    }

    @Test
    public void testApplyCalendarTransforms()
    {
        LocalDateTime value = LocalDateTime.of(2024, 3, 15, 13, 45, 0);
        assertEquals(PartitionTransform.parse("year").apply(value), 2024L);
        assertEquals(PartitionTransform.parse("month").apply(value), 3L);
        assertEquals(PartitionTransform.parse("day").apply(value), 15L);
        assertEquals(PartitionTransform.parse("hour").apply(value), 13L);
    }

    @Test
    public void testApplyOnDateAtStartOfDay()
    {
        LocalDateTime startOfDay = LocalDate.of(2024, 6, 1).atStartOfDay();
        assertEquals(PartitionTransform.parse("year").apply(startOfDay), 2024L);
        assertEquals(PartitionTransform.parse("month").apply(startOfDay), 6L);
        assertEquals(PartitionTransform.parse("day").apply(startOfDay), 1L);
        assertEquals(PartitionTransform.parse("hour").apply(startOfDay), 0L);
    }
}
