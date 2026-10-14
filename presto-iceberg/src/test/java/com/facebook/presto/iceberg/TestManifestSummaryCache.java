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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.iceberg.IcebergColumnHandle.primitiveIcebergColumnHandle;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

/**
 * Pins the invariants the cache's safety rests on: that a key names every input the summary was
 * folded from, that a published summary can no longer be mutated, and that the fold runs once per
 * key.
 */
public class TestManifestSummaryCache
{
    private static final SchemaTableName TABLE = new SchemaTableName("schema", "table");
    private static final long SNAPSHOT_ID = 4815162342L;

    private static ManifestSummaryCache newCache()
    {
        return new ManifestSummaryCache(CacheBuilder.newBuilder().recordStats().build());
    }

    private static ManifestSummaryCacheKey key(long snapshotId, List<Integer> columnIds)
    {
        return new ManifestSummaryCacheKey(TABLE, snapshotId, 0, 0, TupleDomain.all(), columnIds);
    }

    /** A summary for one bigint column, folded from a single file. */
    private static Partition summary()
    {
        Map<Integer, Type.PrimitiveType> idToTypeMapping = ImmutableMap.of(1, Types.LongType.get());
        List<Types.NestedField> columns = ImmutableList.of(Types.NestedField.required(1, "c1", Types.LongType.get()));
        return new Partition(
                idToTypeMapping,
                columns,
                new EmptyStruct(),
                10,
                100,
                ImmutableMap.of(1, 1L),
                ImmutableMap.of(1, 9L),
                ImmutableMap.of(1, 0L),
                ImmutableMap.of(1, 8L));
    }

    @Test
    public void testSameKeyFoldsOnceAndReturnsTheSameSummary()
    {
        ManifestSummaryCache cache = newCache();
        AtomicInteger folds = new AtomicInteger();

        Optional<Partition> first = cache.get(key(SNAPSHOT_ID, ImmutableList.of(1)), () -> {
            folds.incrementAndGet();
            return Optional.of(summary());
        });
        Optional<Partition> second = cache.get(key(SNAPSHOT_ID, ImmutableList.of(1)), () -> {
            folds.incrementAndGet();
            return Optional.of(summary());
        });

        assertEquals(folds.get(), 1, "the fold ran again for a key already cached");
        assertTrue(first.isPresent());
        assertSame(second.get(), first.get(), "a hit did not return the cached summary");
    }

    /**
     * A commit produces a new snapshot id, which must produce a new key. This is the whole of the
     * cache's invalidation story, so it is the one property that cannot be allowed to regress.
     */
    @Test
    public void testDifferentSnapshotFoldsAgain()
    {
        ManifestSummaryCache cache = newCache();
        AtomicInteger folds = new AtomicInteger();

        cache.get(key(SNAPSHOT_ID, ImmutableList.of(1)), () -> {
            folds.incrementAndGet();
            return Optional.of(summary());
        });
        cache.get(key(SNAPSHOT_ID + 1, ImmutableList.of(1)), () -> {
            folds.incrementAndGet();
            return Optional.of(summary());
        });

        assertEquals(folds.get(), 2, "a new snapshot was served the previous snapshot's summary");
        assertNotEquals(key(SNAPSHOT_ID, ImmutableList.of(1)), key(SNAPSHOT_ID + 1, ImmutableList.of(1)));
    }

    /**
     * The requested columns bound which columns' metrics the summary carries, so they discriminate;
     * but they are a set, so the order they arrive in must not.
     */
    @Test
    public void testRequestedColumnsDiscriminateButTheirOrderDoesNot()
    {
        assertNotEquals(key(SNAPSHOT_ID, ImmutableList.of(1)), key(SNAPSHOT_ID, ImmutableList.of(1, 2)));
        assertEquals(key(SNAPSHOT_ID, ImmutableList.of(2, 1)), key(SNAPSHOT_ID, ImmutableList.of(1, 2)));
        assertEquals(
                key(SNAPSHOT_ID, ImmutableList.of(2, 1)).hashCode(),
                key(SNAPSHOT_ID, ImmutableList.of(1, 2)).hashCode());
    }

    /**
     * No files matching is a fact about the snapshot and predicate in the key, both immutable, so it
     * is cacheable like any other outcome and must not be re-folded.
     */
    @Test
    public void testEmptySummaryIsCached()
    {
        ManifestSummaryCache cache = newCache();
        AtomicInteger folds = new AtomicInteger();

        for (int attempt = 0; attempt < 2; attempt++) {
            Optional<Partition> summary = cache.get(key(SNAPSHOT_ID, ImmutableList.of(1)), () -> {
                folds.incrementAndGet();
                return Optional.empty();
            });
            assertEquals(summary, Optional.empty());
        }
        assertEquals(folds.get(), 1, "an empty summary was folded twice");
    }

    /**
     * A Partition is a fold accumulator whose getters hand out live maps, so publishing one unguarded
     * would let any caller corrupt every later query that hits the entry. The published copy must
     * reject mutation instead.
     */
    @Test
    public void testPublishedSummaryRejectsMutation()
    {
        ManifestSummaryCache cache = newCache();
        Partition published = cache.get(key(SNAPSHOT_ID, ImmutableList.of(1)), () -> Optional.of(summary())).get();

        assertThrows(UnsupportedOperationException.class, () -> published.getMinValues().put(1, 99L));
        assertThrows(UnsupportedOperationException.class, () -> published.getMaxValues().put(1, 99L));
        assertThrows(UnsupportedOperationException.class, () -> published.getNullCounts().put(1, 99L));
        assertThrows(UnsupportedOperationException.class, () -> published.getCorruptedStats().add(1));
        // The fold itself mutates through getMinValues(), so this is the call that would otherwise
        // have rewritten a shared entry's bounds.
        assertThrows(UnsupportedOperationException.class, () ->
                published.updateMin(ImmutableMap.of(1, -1L), ImmutableMap.of(1, 0L), 10));

        // Mutation is rejected, not silently dropped.
        assertEquals(published.getMinValues().get(1), 1L);
        assertEquals(published.getMaxValues().get(1), 9L);
    }

    @Test
    public void testFoldedFilesCountsMissesOnly()
    {
        ManifestSummaryCache cache = newCache();

        cache.get(key(SNAPSHOT_ID, ImmutableList.of(1)), () -> Optional.of(summary()));
        assertEquals(cache.getFoldedFiles().getTotalCount(), 1);

        cache.get(key(SNAPSHOT_ID, ImmutableList.of(1)), () -> Optional.of(summary()));
        assertEquals(cache.getFoldedFiles().getTotalCount(), 1, "a cache hit was counted as folded work");
    }

    /**
     * The fold reads manifests and fails with the connector's own error. The cache must rethrow that
     * failure as it was thrown: wrapped, it would reach the user under a generic error code, and only
     * when the cache happens to be enabled.
     */
    @Test
    public void testLoaderFailureIsRethrownUnwrapped()
    {
        ManifestSummaryCache cache = newCache();
        PrestoException failure = new PrestoException(ICEBERG_FILESYSTEM_ERROR, "cannot read manifest");

        PrestoException thrown = expectThrows(PrestoException.class, () -> cache.get(key(SNAPSHOT_ID, ImmutableList.of(1)), () -> {
            throw failure;
        }));
        assertSame(thrown, failure);

        // A failure is not cached: the next caller folds again.
        AtomicInteger folds = new AtomicInteger();
        cache.get(key(SNAPSHOT_ID, ImmutableList.of(1)), () -> {
            folds.incrementAndGet();
            return Optional.of(summary());
        });
        assertEquals(folds.get(), 1);
    }

    /**
     * The key holds the predicate, which a long IN list makes far larger than the summary it names.
     * It has to be weighed, or such entries pin memory the cache bound never sees.
     */
    @Test
    public void testKeyEstimatedSizeGrowsWithThePredicate()
    {
        IcebergColumnHandle column = primitiveIcebergColumnHandle(1, "c1", BIGINT, Optional.empty());
        List<Long> values = LongStream.range(0, 1000).boxed().collect(toImmutableList());
        ManifestSummaryCacheKey inList = new ManifestSummaryCacheKey(TABLE, SNAPSHOT_ID, 0, 0,
                TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.multipleValues(BIGINT, values))),
                ImmutableList.of(1));

        assertTrue(inList.getEstimatedSize() > key(SNAPSHOT_ID, ImmutableList.of(1)).getEstimatedSize() * 100,
                "estimated key size does not scale with the number of predicate values");
    }

    /**
     * The cache is bounded by weight rather than entry count, which only helps if the weight
     * actually tracks the column count.
     */
    @Test
    public void testEstimatedSizeGrowsWithColumnCount()
    {
        ImmutableMap.Builder<Integer, Type.PrimitiveType> types = ImmutableMap.builder();
        ImmutableList.Builder<Types.NestedField> columns = ImmutableList.builder();
        ImmutableMap.Builder<Integer, Object> minValues = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, Object> maxValues = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, Long> nullCounts = ImmutableMap.builder();
        for (int column = 1; column <= 100; column++) {
            types.put(column, Types.LongType.get());
            columns.add(Types.NestedField.required(column, "c" + column, Types.LongType.get()));
            minValues.put(column, (long) column);
            maxValues.put(column, (long) column + 1);
            nullCounts.put(column, 0L);
        }
        Partition wide = new Partition(types.build(), columns.build(), new EmptyStruct(), 10, 100,
                minValues.build(), maxValues.build(), nullCounts.build(), ImmutableMap.of());

        assertTrue(wide.getEstimatedSize() > summary().getEstimatedSize() * 10,
                "estimated size does not scale with the number of folded columns");
    }

    private static class EmptyStruct
            implements StructLike
    {
        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public <T> T get(int position, Class<T> javaClass)
        {
            throw new IndexOutOfBoundsException("empty struct has no field " + position);
        }

        @Override
        public <T> void set(int position, T value)
        {
            throw new IndexOutOfBoundsException("empty struct has no field " + position);
        }
    }
}
