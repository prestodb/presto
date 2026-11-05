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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hudi.common.HudiColumnStatsPruner;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.util.collection.Pair;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hudi.common.HudiColumnStatsPruner.MAX_BATCH_SIZE;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestHudiPruningIterator
{
    @Test
    public void testBoundedBatchesAndOriginalOrder()
            throws IOException
    {
        List<HiveFileInfo> files = new ArrayList<>();
        List<HiveFileInfo> expected = new ArrayList<>();
        for (int i = 0; i < 2 * MAX_BATCH_SIZE + 1; i++) {
            HiveFileInfo file = file("file:/table/p/" + i + ".parquet");
            files.add(file);
            if (i % 2 == 0) {
                expected.add(file);
            }
        }
        AtomicInteger calls = new AtomicInteger();
        HudiColumnStatsPruner pruner = new HudiColumnStatsPruner(predicate(), (requested, columns) -> {
            calls.incrementAndGet();
            assertTrue(requested.size() <= MAX_BATCH_SIZE);
            Map<Pair<String, String>, Map<String, Domain>> result = new HashMap<>();
            for (Pair<String, String> key : requested) {
                assertEquals(key.getLeft(), "p");
                int index = Integer.parseInt(key.getRight().replace(".parquet", ""));
                result.put(key, Map.of("x", Domain.singleValue(BIGINT, index % 2 == 0 ? 10L : 1L)));
            }
            return result;
        });
        RuntimeStats stats = new RuntimeStats();
        HudiPruningIterator iterator = new HudiPruningIterator(files.iterator(), pruner, "file:/table", new AtomicBoolean(), stats);
        assertTrue(iterator.hasNext());
        assertEquals(calls.get(), 1);
        assertEquals(ImmutableList.copyOf(iterator), expected);
        assertEquals(calls.get(), 3);
        assertEquals(stats.getMetric("hudiDataSkippingCandidateFiles").getSum(), files.size());
        assertEquals(stats.getMetric("hudiDataSkippingSkippedFiles").getSum(), files.size() - expected.size());
        assertEquals(stats.getMetric("hudiDataSkippingSkippedBytes").getSum(), 100L * (files.size() - expected.size()));
    }

    @Test
    public void testRecoverableFailureDisablesFurtherLookups()
            throws IOException
    {
        AtomicInteger calls = new AtomicInteger();
        HudiColumnStatsPruner pruner = new HudiColumnStatsPruner(predicate(), (files, columns) -> {
            calls.incrementAndGet();
            throw new IOException("metadata unavailable");
        });
        List<HiveFileInfo> files = new ArrayList<>();
        for (int i = 0; i < MAX_BATCH_SIZE + 1; i++) {
            files.add(file("file:/table/" + i + ".parquet"));
        }
        AtomicBoolean disabled = new AtomicBoolean();
        RuntimeStats stats = new RuntimeStats();
        assertEquals(ImmutableList.copyOf(new HudiPruningIterator(files.iterator(), pruner, "file:/table", disabled, stats)), files);
        assertEquals(ImmutableList.copyOf(new HudiPruningIterator(files.iterator(), pruner, "file:/table", disabled, stats)), files);
        assertTrue(disabled.get());
        assertEquals(calls.get(), 1);
        assertEquals(stats.getMetric("hudiDataSkippingFallbacks").getSum(), 1L);
    }

    @Test
    public void testAccessDeniedAndCancellationAreNotFallbacks()
            throws IOException
    {
        HiveFileInfo file = file("file:/table/file.parquet");
        HudiColumnStatsPruner denied = new HudiColumnStatsPruner(predicate(), (files, columns) -> {
            throw new IOException("permission denied");
        });
        HudiPruningIterator iterator = new HudiPruningIterator(List.of(file).iterator(), denied, "file:/table", new AtomicBoolean(), new RuntimeStats());
        assertEquals(expectThrows(PrestoException.class, iterator::hasNext).getErrorCode(), PERMISSION_DENIED.toErrorCode());

        HudiColumnStatsPruner typedDenied = new HudiColumnStatsPruner(predicate(), (files, columns) -> {
            throw new AccessDeniedException("/table/.hoodie/metadata");
        });
        HudiPruningIterator typedDeniedIterator = new HudiPruningIterator(
                List.of(file).iterator(), typedDenied, "file:/table", new AtomicBoolean(), new RuntimeStats());
        assertEquals(expectThrows(PrestoException.class, typedDeniedIterator::hasNext).getErrorCode(), PERMISSION_DENIED.toErrorCode());

        HudiColumnStatsPruner canceled = new HudiColumnStatsPruner(predicate(), (files, columns) -> {
            throw new CancellationException();
        });
        HudiPruningIterator canceledIterator = new HudiPruningIterator(List.of(file).iterator(), canceled, "file:/table", new AtomicBoolean(), new RuntimeStats());
        expectThrows(CancellationException.class, canceledIterator::hasNext);
    }

    @Test
    public void testUnavailableMetadataReaderIsOptional()
            throws IOException
    {
        List<HiveFileInfo> files = List.of(file("file:/table/file.parquet"));
        HudiColumnStatsPruner pruner = new HudiColumnStatsPruner(predicate(), (requested, columns) -> {
            throw new NoClassDefFoundError("optional MDT codec");
        });
        AtomicBoolean disabled = new AtomicBoolean();
        assertEquals(ImmutableList.copyOf(new HudiPruningIterator(
                files.iterator(), pruner, "file:/table", disabled, new RuntimeStats())), files);
        assertTrue(disabled.get());
    }

    @Test
    public void testInterruptionIsPreserved()
            throws IOException
    {
        HudiColumnStatsPruner pruner = new HudiColumnStatsPruner(predicate(), (files, columns) -> {
            throw new InterruptedIOException("interrupted");
        });
        HudiPruningIterator iterator = new HudiPruningIterator(List.of(file("file:/table/file.parquet")).iterator(),
                pruner, "file:/table", new AtomicBoolean(), new RuntimeStats());
        try {
            expectThrows(PrestoException.class, iterator::hasNext);
            assertTrue(Thread.currentThread().isInterrupted());
        }
        finally {
            Thread.interrupted();
        }
    }

    @Test
    public void testListingFailurePropagates()
    {
        Iterator<HiveFileInfo> source = new Iterator<>()
        {
            @Override
            public boolean hasNext()
            {
                throw new IllegalStateException("listing failed");
            }

            @Override
            public HiveFileInfo next()
            {
                throw new AssertionError();
            }
        };
        AtomicBoolean disabled = new AtomicBoolean();
        HudiColumnStatsPruner pruner = new HudiColumnStatsPruner(predicate(), (files, columns) -> {
            throw new AssertionError("Unexpected lookup");
        });
        HudiPruningIterator iterator = new HudiPruningIterator(source, pruner, "file:/table", disabled, new RuntimeStats());
        assertEquals(expectThrows(IllegalStateException.class, iterator::hasNext).getMessage(), "listing failed");
        assertFalse(disabled.get());
    }

    @Test
    public void testExternalPathsAreRetained()
            throws IOException
    {
        List<HiveFileInfo> files = List.of(
                file("file:/table/p/match.parquet"),
                file("file:/other/p/match.parquet"),
                file("file:/table-suffix/p/match.parquet"),
                file("file:/table/p/not-parquet.orc"));
        HudiColumnStatsPruner pruner = new HudiColumnStatsPruner(predicate(), (requested, columns) -> {
            Pair<String, String> expected = Pair.of("p", "match.parquet");
            assertEquals(requested, List.of(expected));
            return Map.of(expected, Map.of("x", Domain.singleValue(BIGINT, 1L)));
        });
        assertEquals(ImmutableList.copyOf(new HudiPruningIterator(
                files.iterator(), pruner, "file:/table", new AtomicBoolean(), new RuntimeStats())), files.subList(1, files.size()));
    }

    private static HiveFileInfo file(String path)
            throws IOException
    {
        FileStatus status = new FileStatus(100, false, 1, 100, 0, new Path(path));
        return HiveFileInfo.createHiveFileInfo(new LocatedFileStatus(status, new BlockLocation[0]), Optional.empty());
    }

    private static TupleDomain<String> predicate()
    {
        return TupleDomain.withColumnDomains(Map.of("x", Domain.singleValue(BIGINT, 10L)));
    }
}
