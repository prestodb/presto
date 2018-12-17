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
package com.facebook.presto.raptorx.storage.organization;

import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCompactionSetCreator
{
    private static final List<ColumnInfo> COLUMNS = ImmutableList.of(
            new ColumnInfo(1, "1", BIGINT, Optional.empty(), 1, OptionalInt.empty(), OptionalInt.empty()));

    private static final List<ColumnInfo> BUCKET_COLUMNS = ImmutableList.of(
            new ColumnInfo(1, "1", BIGINT, Optional.empty(), 1, OptionalInt.of(1), OptionalInt.empty()));

    private static final long MAX_CHHNK_ROWS = 100;
    private static final DataSize MAX_CHUNK_SIZE = new DataSize(100, DataSize.Unit.BYTE);
    private static final TableInfo tableInfo = new TableInfo(1L, "1", 10, 1, OptionalLong.empty(), false, CompressionType.ZSTD, System.currentTimeMillis(), System.currentTimeMillis(), 1, Optional.empty(), COLUMNS);
    private static final TableInfo temporalTableInfo = new TableInfo(2L, "2", 10, 1, OptionalLong.of(1L), false, CompressionType.ZSTD, System.currentTimeMillis(), System.currentTimeMillis(), 1, Optional.empty(), COLUMNS);
    private static final TableInfo bucketedTableInfo = new TableInfo(23L, "3", 10, 1, OptionalLong.empty(), false, CompressionType.ZSTD, System.currentTimeMillis(), System.currentTimeMillis(), 1, Optional.empty(), BUCKET_COLUMNS);
    private static final TableInfo bucketedTemporalTableInfo = new TableInfo(23L, "3", 10, 1, OptionalLong.of(1L), false, CompressionType.ZSTD, System.currentTimeMillis(), System.currentTimeMillis(), 1, Optional.empty(), BUCKET_COLUMNS);

    private final CompactionSetCreator compactionSetCreator = new CompactionSetCreator(new TemporalFunction(UTC), MAX_CHUNK_SIZE, MAX_CHHNK_ROWS);

    @Test
    public void testNonTemporalOrganizationSetSimple()
    {
        List<ChunkIndexInfo> inputChunks = ImmutableList.of(
                chunkWithSize(10, 10),
                chunkWithSize(10, 10),
                chunkWithSize(10, 10));

        Set<OrganizationSet> compactionSets = compactionSetCreator.createCompactionSets(tableInfo, inputChunks);
        assertEquals(compactionSets.size(), 1);
        assertEquals(getOnlyElement(compactionSets).getChunkIds(), extractIndexes(inputChunks, 0, 1, 2));
    }

    @Test
    public void testNonTemporalSizeBasedOrganizationSet()
    {
        List<ChunkIndexInfo> inputChunks = ImmutableList.of(
                chunkWithSize(10, 70),
                chunkWithSize(10, 20),
                chunkWithSize(10, 30),
                chunkWithSize(10, 120));

        Set<OrganizationSet> compactionSets = compactionSetCreator.createCompactionSets(tableInfo, inputChunks);

        Set<Long> actual = new HashSet<>();
        for (OrganizationSet set : compactionSets) {
            actual.addAll(set.getChunkIds());
        }
        assertTrue(extractIndexes(inputChunks, 1, 2).containsAll(actual));
    }

    @Test
    public void testNonTemporalRowCountBasedOrganizationSet()
    {
        List<ChunkIndexInfo> inputChunks = ImmutableList.of(
                chunkWithSize(50, 10),
                chunkWithSize(100, 10),
                chunkWithSize(20, 10),
                chunkWithSize(30, 10));

        Set<OrganizationSet> compactionSets = compactionSetCreator.createCompactionSets(tableInfo, inputChunks);

        Set<Long> actual = new HashSet<>();
        for (OrganizationSet set : compactionSets) {
            actual.addAll(set.getChunkIds());
        }

        assertTrue(extractIndexes(inputChunks, 0, 2, 3).containsAll(actual));
    }

    @Test
    public void testTemporalCompactionNoCompactionAcrossDays()
    {
        long day1 = Duration.ofDays(Duration.ofNanos(System.nanoTime()).toDays()).toMillis();
        long day2 = Duration.ofDays(Duration.ofMillis(day1).toDays() + 1).toMillis();
        long day3 = Duration.ofDays(Duration.ofMillis(day1).toDays() + 2).toMillis();

        List<ChunkIndexInfo> inputChunks = ImmutableList.of(
                chunkWithTemporalRange(TIMESTAMP, day1, day1),
                chunkWithTemporalRange(TIMESTAMP, day2, day2),
                chunkWithTemporalRange(TIMESTAMP, day2, day2),
                chunkWithTemporalRange(TIMESTAMP, day1, day1),
                chunkWithTemporalRange(TIMESTAMP, day3, day3));

        Set<OrganizationSet> actual = compactionSetCreator.createCompactionSets(temporalTableInfo, inputChunks);
        assertEquals(actual.size(), 2);

        Set<OrganizationSet> expected = ImmutableSet.of(
                new OrganizationSet(temporalTableInfo.getTableId(), extractIndexes(inputChunks, 0, 3), 1),
                new OrganizationSet(temporalTableInfo.getTableId(), extractIndexes(inputChunks, 1, 2), 1));
        assertEquals(actual, expected);
    }

    @Test
    public void testTemporalCompactionSpanningDays()
    {
        long day1 = Duration.ofDays(Duration.ofNanos(System.nanoTime()).toDays()).toMillis();
        long day2 = Duration.ofDays(Duration.ofMillis(day1).toDays() + 1).toMillis();
        long day3 = Duration.ofDays(Duration.ofMillis(day1).toDays() + 2).toMillis();
        long day4 = Duration.ofDays(Duration.ofMillis(day1).toDays() + 3).toMillis();

        List<ChunkIndexInfo> inputChunks = ImmutableList.of(
                chunkWithTemporalRange(TIMESTAMP, day1, day3), // day2
                chunkWithTemporalRange(TIMESTAMP, day2, day2), // day2
                chunkWithTemporalRange(TIMESTAMP, day1, day1), // day1
                chunkWithTemporalRange(TIMESTAMP, day1 + 100, day2 + 100), // day1
                chunkWithTemporalRange(TIMESTAMP, day1 - 100, day2 - 100), // day1
                chunkWithTemporalRange(TIMESTAMP, day2 - 100, day3 - 100),  // day2
                chunkWithTemporalRange(TIMESTAMP, day1, day4)); // day2

        long tableId = temporalTableInfo.getTableId();
        Set<OrganizationSet> compactionSets = compactionSetCreator.createCompactionSets(temporalTableInfo, inputChunks);

        assertEquals(compactionSets.size(), 2);

        Set<OrganizationSet> expected = ImmutableSet.of(
                new OrganizationSet(tableId, extractIndexes(inputChunks, 0, 1, 5, 6), 1),
                new OrganizationSet(tableId, extractIndexes(inputChunks, 2, 3, 4), 1));
        assertEquals(compactionSets, expected);
    }

    @Test
    public void testTemporalCompactionDate()
    {
        long day1 = Duration.ofNanos(System.nanoTime()).toDays();
        long day2 = day1 + 1;
        long day3 = day1 + 2;

        List<ChunkIndexInfo> inputChunks = ImmutableList.of(
                chunkWithTemporalRange(DATE, day1, day1),
                chunkWithTemporalRange(DATE, day2, day2),
                chunkWithTemporalRange(DATE, day3, day3),
                chunkWithTemporalRange(DATE, day1, day3),
                chunkWithTemporalRange(DATE, day2, day3),
                chunkWithTemporalRange(DATE, day1, day2));

        long tableId = temporalTableInfo.getTableId();
        Set<OrganizationSet> actual = compactionSetCreator.createCompactionSets(temporalTableInfo, inputChunks);

        assertEquals(actual.size(), 2);

        Set<OrganizationSet> expected = ImmutableSet.of(
                new OrganizationSet(tableId, extractIndexes(inputChunks, 0, 3, 5), 1),
                new OrganizationSet(tableId, extractIndexes(inputChunks, 1, 4), 1));
        assertEquals(actual, expected);
    }

    @Test
    public void testBucketedTableCompaction()
    {
        List<ChunkIndexInfo> inputChunks = ImmutableList.of(
                chunkWithBucket(1),
                chunkWithBucket(2),
                chunkWithBucket(2),
                chunkWithBucket(1),
                chunkWithBucket(2),
                chunkWithBucket(1));

        long tableId = bucketedTableInfo.getTableId();
        Set<OrganizationSet> actual = compactionSetCreator.createCompactionSets(bucketedTableInfo, inputChunks);

        assertEquals(actual.size(), 2);

        Set<OrganizationSet> expected = ImmutableSet.of(
                new OrganizationSet(tableId, extractIndexes(inputChunks, 0, 3, 5), 1),
                new OrganizationSet(tableId, extractIndexes(inputChunks, 1, 2, 4), 2));
        assertEquals(actual, expected);
    }

    static Set<Long> extractIndexes(List<ChunkIndexInfo> inputChunks, int... indexes)
    {
        ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
        for (int index : indexes) {
            builder.add(inputChunks.get(index).getChunkId());
        }
        return builder.build();
    }

    @Test
    public void testBucketedTemporalTableCompaction()
    {
        long day1 = 1;
        long day2 = 2;
        long day3 = 3;
        long day4 = 4;

        List<ChunkIndexInfo> inputChunks = ImmutableList.of(
                chunkWithTemporalBucket(1, DATE, day1, day1),
                chunkWithTemporalBucket(2, DATE, day2, day2),
                chunkWithTemporalBucket(1, DATE, day1, day1),
                chunkWithTemporalBucket(2, DATE, day2, day2),
                chunkWithTemporalBucket(1, DATE, day3, day3),
                chunkWithTemporalBucket(2, DATE, day4, day4));

        long tableId = bucketedTemporalTableInfo.getTableId();
        Set<OrganizationSet> actual = compactionSetCreator.createCompactionSets(bucketedTemporalTableInfo, inputChunks);

        assertEquals(actual.size(), 2);

        Set<OrganizationSet> expected = ImmutableSet.of(
                new OrganizationSet(tableId, extractIndexes(inputChunks, 0, 2), 1),
                new OrganizationSet(tableId, extractIndexes(inputChunks, 1, 3), 2));
        assertEquals(actual, expected);
    }

    private static ChunkIndexInfo chunkWithSize(long rows, long size)
    {
        return new ChunkIndexInfo(
                1,
                1,
                ThreadLocalRandom.current().nextLong(1000),
                rows,
                size,
                Optional.empty(),
                Optional.empty());
    }

    private static ChunkIndexInfo chunkWithTemporalRange(Type type, Long start, Long end)
    {
        return chunkWithTemporalBucket(1, type, start, end);
    }

    private static ChunkIndexInfo chunkWithBucket(int bucketNumber)
    {
        return new ChunkIndexInfo(
                1,
                bucketNumber,
                ThreadLocalRandom.current().nextLong(1000),
                1,
                1,
                Optional.empty(),
                Optional.empty());
    }

    private static ChunkIndexInfo chunkWithTemporalBucket(int bucketNumber, Type type, Long start, Long end)
    {
        if (type.equals(DATE)) {
            return new ChunkIndexInfo(
                    1,
                    bucketNumber,
                    ThreadLocalRandom.current().nextLong(1000),
                    1,
                    1,
                    Optional.empty(),
                    Optional.of(ChunkRange.of(new Tuple(type, start.intValue()), new Tuple(type, end.intValue()))));
        }
        return new ChunkIndexInfo(
                1,
                bucketNumber,
                ThreadLocalRandom.current().nextLong(1000),
                1,
                1,
                Optional.empty(),
                Optional.of(ChunkRange.of(new Tuple(type, start), new Tuple(type, end))));
    }
}
