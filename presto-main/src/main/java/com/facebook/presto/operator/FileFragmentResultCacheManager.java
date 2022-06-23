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
package com.facebook.presto.operator;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.Split.SplitIdentifier;
import com.facebook.presto.spi.PrestoException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.AbstractIterator;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.page.PagesSerdeUtil.readPages;
import static com.facebook.presto.spi.page.PagesSerdeUtil.writePages;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileFragmentResultCacheManager
        implements FragmentResultCacheManager
{
    private static final Logger log = Logger.get(FileFragmentResultCacheManager.class);

    private final Path baseDirectory;
    // Max size for the in-memory buffer.
    private final long maxInFlightBytes;
    // Size limit for every single page.
    private final long maxSinglePagesBytes;
    // Max on-disk size for this fragment result cache.
    private final long maxCacheBytes;
    private final PagesSerdeFactory pagesSerdeFactory;
    private final FragmentCacheStats fragmentCacheStats;
    private final ExecutorService flushExecutor;
    private final ExecutorService removalExecutor;

    private final Cache<CacheKey, CacheEntry> cache;

    // TODO: Decouple CacheKey by encoding PlanNode and SplitIdentifier separately so we don't have to keep too many objects in memory
    @Inject
    public FileFragmentResultCacheManager(
            FileFragmentResultCacheConfig cacheConfig,
            BlockEncodingSerde blockEncodingSerde,
            FragmentCacheStats fragmentCacheStats,
            ExecutorService flushExecutor,
            ExecutorService removalExecutor)
    {
        requireNonNull(cacheConfig, "cacheConfig is null");
        requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");

        this.baseDirectory = Paths.get(cacheConfig.getBaseDirectory());
        this.maxInFlightBytes = cacheConfig.getMaxInFlightSize().toBytes();
        this.maxSinglePagesBytes = cacheConfig.getMaxSinglePagesSize().toBytes();
        this.maxCacheBytes = cacheConfig.getMaxCacheSize().toBytes();
        // pagesSerde is not thread safe
        this.pagesSerdeFactory = new PagesSerdeFactory(blockEncodingSerde, cacheConfig.isBlockEncodingCompressionEnabled());
        this.fragmentCacheStats = requireNonNull(fragmentCacheStats, "fragmentCacheStats is null");
        this.flushExecutor = requireNonNull(flushExecutor, "flushExecutor is null");
        this.removalExecutor = requireNonNull(removalExecutor, "removalExecutor is null");
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(cacheConfig.getMaxCachedEntries())
                .expireAfterAccess(cacheConfig.getCacheTtl().toMillis(), MILLISECONDS)
                .removalListener(new CacheRemovalListener())
                .recordStats()
                .build();

        File target = new File(baseDirectory.toUri());
        if (!target.exists()) {
            try {
                Files.createDirectories(target.toPath());
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "cannot create cache directory " + target, e);
            }
        }
        else {
            File[] files = target.listFiles();
            if (files == null) {
                return;
            }

            this.removalExecutor.submit(() -> Arrays.stream(files).forEach(file -> {
                try {
                    Files.delete(file.toPath());
                }
                catch (IOException e) {
                    // ignore
                }
            }));
        }
    }

    @Override
    public Future<?> put(String serializedPlan, Split split, List<Page> result)
    {
        CacheKey key = new CacheKey(serializedPlan, split.getSplitIdentifier());
        long resultSize = getPagesSize(result);
        if (fragmentCacheStats.getInFlightBytes() + resultSize > maxInFlightBytes ||
                cache.getIfPresent(key) != null ||
                resultSize > maxSinglePagesBytes ||
                // Here we use the logical size resultSize as an estimate for admission control.
                fragmentCacheStats.getCacheSizeInBytes() + resultSize > maxCacheBytes) {
            return immediateFuture(null);
        }

        fragmentCacheStats.addInFlightBytes(resultSize);
        Path path = baseDirectory.resolve(randomUUID().toString().replaceAll("-", "_"));
        return flushExecutor.submit(() -> cachePages(key, path, result, resultSize));
    }

    private static long getPagesSize(List<Page> pages)
    {
        return pages.stream()
                .mapToLong(Page::getSizeInBytes)
                .sum();
    }

    private void cachePages(CacheKey key, Path path, List<Page> pages, long resultSize)
    {
        try {
            Files.createFile(path);
            try (SliceOutput output = new OutputStreamSliceOutput(newOutputStream(path, APPEND))) {
                writePages(pagesSerdeFactory.createPagesSerde(), output, pages.iterator());
                long resultPhysicalBytes = output.size();
                cache.put(key, new CacheEntry(path, resultPhysicalBytes));
                fragmentCacheStats.incrementCacheEntries();
                fragmentCacheStats.addCacheSizeInBytes(resultPhysicalBytes);
            }
            catch (UncheckedIOException | IOException e) {
                log.warn(e, "%s encountered an error while writing to path %s", Thread.currentThread().getName(), path);
                tryDeleteFile(path);
            }
        }
        catch (UncheckedIOException | IOException e) {
            log.warn(e, "%s encountered an error while writing to path %s", Thread.currentThread().getName(), path);
            tryDeleteFile(path);
        }
        finally {
            fragmentCacheStats.addInFlightBytes(-resultSize);
        }
    }

    private static void tryDeleteFile(Path path)
    {
        try {
            File file = new File(path.toUri());
            if (file.exists()) {
                Files.delete(file.toPath());
            }
        }
        catch (IOException e) {
            // ignore
        }
    }

    @Override
    public Optional<Iterator<Page>> get(String serializedPlan, Split split)
    {
        CacheKey key = new CacheKey(serializedPlan, split.getSplitIdentifier());
        CacheEntry cacheEntry = cache.getIfPresent(key);
        if (cacheEntry == null) {
            fragmentCacheStats.incrementCacheMiss();
            return Optional.empty();
        }

        try {
            InputStream inputStream = newInputStream(cacheEntry.getPath());
            Iterator<Page> result = readPages(pagesSerdeFactory.createPagesSerde(), new InputStreamSliceInput(inputStream));
            fragmentCacheStats.incrementCacheHit();
            return Optional.of(closeWhenExhausted(result, inputStream));
        }
        catch (UncheckedIOException | IOException e) {
            log.error(e, "read path %s error", cacheEntry.getPath());
            // there might be a chance the file has been deleted. We would return cache miss in this case.
            fragmentCacheStats.incrementCacheMiss();
            return Optional.empty();
        }
    }

    @Managed
    public void invalidateAllCache()
    {
        cache.invalidateAll();
    }

    private static <T> Iterator<T> closeWhenExhausted(Iterator<T> iterator, Closeable resource)
    {
        requireNonNull(iterator, "iterator is null");
        requireNonNull(resource, "resource is null");

        return new AbstractIterator<T>()
        {
            @Override
            protected T computeNext()
            {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                try {
                    resource.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return endOfData();
            }
        };
    }

    public static class CacheKey
    {
        private final String serializedPlan;
        private final SplitIdentifier splitIdentifier;

        public CacheKey(String serializedPlan, SplitIdentifier splitIdentifier)
        {
            this.serializedPlan = requireNonNull(serializedPlan, "serializedPlan is null");
            this.splitIdentifier = requireNonNull(splitIdentifier, "splitIdentifier is null");
        }

        public String getSerializedPlan()
        {
            return serializedPlan;
        }

        public SplitIdentifier getSplitIdentifier()
        {
            return splitIdentifier;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(serializedPlan, cacheKey.serializedPlan) &&
                    Objects.equals(splitIdentifier, cacheKey.splitIdentifier);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(serializedPlan, splitIdentifier);
        }
    }

    private static class CacheEntry
    {
        private final Path path;
        private final long resultBytes;

        public Path getPath()
        {
            return path;
        }

        public long getResultBytes()
        {
            return resultBytes;
        }

        public CacheEntry(Path path, long resultBytes)
        {
            this.path = requireNonNull(path, "path is null");
            this.resultBytes = resultBytes;
        }
    }

    private class CacheRemovalListener
            implements RemovalListener<CacheKey, CacheEntry>
    {
        @Override
        public void onRemoval(RemovalNotification<CacheKey, CacheEntry> notification)
        {
            CacheEntry cacheEntry = notification.getValue();
            removalExecutor.submit(() -> tryDeleteFile(cacheEntry.getPath()));
            fragmentCacheStats.incrementCacheRemoval();
            fragmentCacheStats.decrementCacheEntries();
            fragmentCacheStats.addCacheSizeInBytes(-cacheEntry.getResultBytes());
        }
    }
}
