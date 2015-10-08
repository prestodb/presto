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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.metadata.ShardMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static org.testng.Assert.assertEquals;

public class TestCompactionSetCreator
{
    private static final long MAX_SHARD_ROWS = 10_000;

    @Test
    public void testNonTemporalCompactionSetSimple()
            throws Exception
    {
        CompactionSetCreator compactionSetCreator = new FileCompactionSetCreator(new DataSize(1, KILOBYTE), MAX_SHARD_ROWS);

        // compact into one shard
        Set<ShardMetadata> inputShards = ImmutableSet.of(
                shardWithSize(100),
                shardWithSize(100),
                shardWithSize(100));

        Set<CompactionSet> compactionSets = compactionSetCreator.createCompactionSets(1L, inputShards);
        assertEquals(compactionSets.size(), 1);
        assertEquals(getOnlyElement(compactionSets).getShardsToCompact(), inputShards);
    }

    @Test
    public void testNonTemporalCompactionSet()
            throws Exception
    {
        CompactionSetCreator compactionSetCreator = new FileCompactionSetCreator(new DataSize(100, BYTE), MAX_SHARD_ROWS);
        long tableId = 1L;

        // compact into two shards
        List<ShardMetadata> inputShards = ImmutableList.of(
                shardWithSize(70),
                shardWithSize(20),
                shardWithSize(30),
                shardWithSize(120));

        Set<CompactionSet> compactionSets = compactionSetCreator.createCompactionSets(tableId, ImmutableSet.copyOf(inputShards));
        assertEquals(compactionSets.size(), 2);
        Set<CompactionSet> expected = ImmutableSet.of(
                new CompactionSet(tableId, ImmutableSet.of(inputShards.get(0), inputShards.get(2))),
                new CompactionSet(tableId, ImmutableSet.of(inputShards.get(1))));
        assertEquals(compactionSets, expected);
    }

    @Test
    public void testTemporalCompactionNoCompactionAcrossDays()
            throws Exception
    {
        CompactionSetCreator compactionSetCreator = new TemporalCompactionSetCreator(new DataSize(100, BYTE), MAX_SHARD_ROWS, TIMESTAMP);
        long tableId = 1L;
        long day1 = Duration.ofDays(Duration.ofNanos(System.nanoTime()).toDays()).toMillis();
        long day2 = Duration.ofDays(Duration.ofMillis(day1).toDays() + 1).toMillis();

        // compact into two shards
        List<ShardMetadata> inputShards = ImmutableList.of(
                shardWithRange(10, day1, day1),
                shardWithRange(10, day2, day2),
                shardWithRange(10, day1, day1));

        Set<CompactionSet> actual = compactionSetCreator.createCompactionSets(tableId, ImmutableSet.copyOf(inputShards));
        assertEquals(actual.size(), 2);
        Set<CompactionSet> expected = ImmutableSet.of(
                new CompactionSet(tableId, ImmutableSet.of(inputShards.get(0), inputShards.get(2))),
                new CompactionSet(tableId, ImmutableSet.of(inputShards.get(1))));
        assertEquals(actual, expected);
    }

    @Test
    public void testTemporalCompactionSpanningDays()
            throws Exception
    {
        CompactionSetCreator compactionSetCreator = new TemporalCompactionSetCreator(new DataSize(100, BYTE), MAX_SHARD_ROWS, TIMESTAMP);
        long tableId = 1L;
        long day1 = Duration.ofDays(Duration.ofNanos(System.nanoTime()).toDays()).toMillis();
        long day2 = Duration.ofDays(Duration.ofMillis(day1).toDays() + 1).toMillis();
        long day3 = Duration.ofDays(Duration.ofMillis(day1).toDays() + 2).toMillis();
        long day4 = Duration.ofDays(Duration.ofMillis(day1).toDays() + 3).toMillis();

        List<ShardMetadata> inputShards = ImmutableList.of(
                shardWithRange(10, day1, day3), // day2
                shardWithRange(10, day2, day2), // day2
                shardWithRange(10, day1, day1), // day1
                shardWithRange(10, day1 + 100, day2 + 100), // day1
                shardWithRange(10, day1 - 100, day2 - 100), // day1
                shardWithRange(10, day2 - 100, day3 - 100),  // day2
                shardWithRange(10, day1, day4) // day2
        );

        Set<CompactionSet> compactionSets = compactionSetCreator.createCompactionSets(tableId, ImmutableSet.copyOf(inputShards));
        assertEquals(compactionSets.size(), 2);
        Set<CompactionSet> expected = ImmutableSet.of(
                new CompactionSet(tableId, ImmutableSet.of(inputShards.get(0), inputShards.get(1), inputShards.get(5), inputShards.get(6))),
                new CompactionSet(tableId, ImmutableSet.of(inputShards.get(2), inputShards.get(3), inputShards.get(4))));
        assertEquals(compactionSets, expected);
    }

    @Test
    public void testTemporalCompactionDate()
            throws Exception
    {
        CompactionSetCreator compactionSetCreator = new TemporalCompactionSetCreator(new DataSize(100, BYTE), MAX_SHARD_ROWS, DATE);
        long tableId = 1L;
        long day1 = Duration.ofNanos(System.nanoTime()).toDays();
        long day2 = day1 + 1;
        long day3 = day1 + 2;

        List<ShardMetadata> inputShards = ImmutableList.of(
                shardWithRange(10, day1, day1),
                shardWithRange(10, day2, day2),
                shardWithRange(10, day3, day3),
                shardWithRange(10, day1, day3),
                shardWithRange(10, day2, day3),
                shardWithRange(10, day1, day2));

        Set<CompactionSet> actual = compactionSetCreator.createCompactionSets(tableId, ImmutableSet.copyOf(inputShards));
        assertEquals(actual.size(), 3);
        Set<CompactionSet> expected = ImmutableSet.of(
                new CompactionSet(tableId, ImmutableSet.of(inputShards.get(0), inputShards.get(3), inputShards.get(5))),
                new CompactionSet(tableId, ImmutableSet.of(inputShards.get(1), inputShards.get(4))),
                new CompactionSet(tableId, ImmutableSet.of(inputShards.get(2))));
        assertEquals(actual, expected);
    }

    private static ShardMetadata shardWithSize(long uncompressedSize)
    {
        return new ShardMetadata(
                1,
                1,
                UUID.randomUUID(),
                10,
                10,
                uncompressedSize,
                OptionalLong.empty(),
                OptionalLong.empty());
    }

    private static ShardMetadata shardWithRange(long uncompressedSize, long rangeStart, long rangeEnd)
    {
        return new ShardMetadata(
                1,
                ThreadLocalRandom.current().nextInt(1, 10),
                UUID.randomUUID(),
                10,
                10,
                uncompressedSize,
                OptionalLong.of(rangeStart),
                OptionalLong.of(rangeEnd));
    }
}
