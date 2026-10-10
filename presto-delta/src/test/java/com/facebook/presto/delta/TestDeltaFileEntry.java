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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link DeltaFileEntry}.
 */
public class TestDeltaFileEntry
{
    @Test
    public void testConstructorAndGetters()
    {
        Optional<DeltaJsonFileStatistics> stats = DeltaJsonFileStatistics.create(
                "{\"numRecords\":5,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":5},\"nullCount\":{\"id\":0}}");

        DeltaFileEntry entry = new DeltaFileEntry(
                "s3://bucket/table/part-0.parquet",
                1024L,
                1700000000000L,
                ImmutableMap.of("date", "2024-01-01"),
                stats);

        assertEquals(entry.getPath(), "s3://bucket/table/part-0.parquet");
        assertEquals(entry.getSize(), 1024L);
        assertEquals(entry.getModificationTime(), 1700000000000L);
        assertEquals(entry.getPartitionValues(), ImmutableMap.of("date", "2024-01-01"));
        assertTrue(entry.getStats().isPresent());
        assertEquals(entry.getStats().get().getNumRecords(), Optional.of(5L));
    }

    @Test
    public void testEmptyStats()
    {
        DeltaFileEntry entry = new DeltaFileEntry(
                "part-1.parquet",
                512L,
                0L,
                Collections.emptyMap(),
                Optional.empty());

        assertFalse(entry.getStats().isPresent());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullPathThrows()
    {
        new DeltaFileEntry(null, 0L, 0L, Collections.emptyMap(), Optional.empty());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullPartitionValuesThrows()
    {
        new DeltaFileEntry("path", 0L, 0L, null, Optional.empty());
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullStatsThrows()
    {
        new DeltaFileEntry("path", 0L, 0L, Collections.emptyMap(), null);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testPartitionValuesIsImmutable()
    {
        DeltaFileEntry entry = new DeltaFileEntry(
                "path",
                0L,
                0L,
                ImmutableMap.of("k", "v"),
                Optional.empty());

        entry.getPartitionValues().put("new", "value");
    }
}
