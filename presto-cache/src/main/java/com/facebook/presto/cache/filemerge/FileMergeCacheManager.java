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
package com.facebook.presto.cache.filemerge;

import alluxio.collections.ConcurrentHashSet;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.cache.CacheManager;
import com.facebook.presto.cache.CacheResult;
import com.facebook.presto.cache.CacheStats;
import com.facebook.presto.cache.FileReadRequest;
import com.facebook.presto.hive.CacheQuota;
import com.facebook.presto.spi.PrestoException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.Path;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterators.getOnlyElement;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.StrictMath.toIntExact;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

// TODO: Make cache state persistent on disk so we do not need to wipe out cache every time we reboot a server.
@SuppressWarnings("UnstableApiUsage")
public class FileMergeCacheManager
        implements CacheManager
{
    private static final Logger log = Logger.get(FileMergeCacheManager.class);

    private static final String EXTENSION = ".cache";

    private static final int FILE_MERGE_BUFFER_SIZE = toIntExact(new DataSize(8, MEGABYTE).toBytes());

    private final ThreadLocal<byte[]> buffers = ThreadLocal.withInitial(() -> new byte[FILE_MERGE_BUFFER_SIZE]);

    private final ExecutorService cacheFlushExecutor;
    private final ExecutorService cacheRemovalExecutor;
    private final ScheduledExecutorService cacheSizeCalculateExecutor;

    // a mapping from remote file `F` to a range map `M`; the corresponding local cache file for each range in `M` represents the cached chunk of `F`
    private final Map<Path, CacheRange> persistedRanges = new ConcurrentHashMap<>();
    // a local cache only to control the lifecycle of persisted
    // Path and its corresponding cacheScope identifier
    private final Cache<Path, Long> cache;
    // CacheScope identifier to its cached files mapping
    private final Map<Long, Set<Path>> cacheScopeFiles = new ConcurrentHashMap<>();
    private final Map<Long, Long> cacheScopeSizeInBytes = new ConcurrentHashMap<>();

    // stats
    private final CacheStats stats;

    // config
    private final Path baseDirectory;
    private final long maxInflightBytes;

    @Inject
    public FileMergeCacheManager(
            CacheConfig cacheConfig,
            FileMergeCacheConfig fileMergeCacheConfig,
            CacheStats stats,
            ExecutorService cacheFlushExecutor,
            ExecutorService cacheRemovalExecutor,
            ScheduledExecutorService cacheSizeCalculateExecutor)
    {
        requireNonNull(cacheConfig, "directory is null");
        this.cacheFlushExecutor = cacheFlushExecutor;
        this.cacheRemovalExecutor = cacheRemovalExecutor;
        this.cacheSizeCalculateExecutor = cacheSizeCalculateExecutor;
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(fileMergeCacheConfig.getMaxCachedEntries())
                .expireAfterAccess(fileMergeCacheConfig.getCacheTtl().toMillis(), MILLISECONDS)
                .removalListener(new CacheRemovalListener())
                .recordStats()
                .build();
        this.stats = requireNonNull(stats, "stats is null");
        this.baseDirectory = new Path(cacheConfig.getBaseDirectory());
        checkArgument(fileMergeCacheConfig.getMaxInMemoryCacheSize().toBytes() >= 0, "maxInflightBytes is negative");
        this.maxInflightBytes = fileMergeCacheConfig.getMaxInMemoryCacheSize().toBytes();

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

            this.cacheRemovalExecutor.submit(() -> Arrays.stream(files).forEach(file -> {
                try {
                    Files.delete(file.toPath());
                }
                catch (IOException e) {
                    // ignore
                }
            }));
        }

        this.cacheSizeCalculateExecutor.scheduleAtFixedRate(
                () -> {
                    try {
                        cacheScopeFiles.keySet().forEach(cacheIdentifier -> cacheScopeSizeInBytes.put(cacheIdentifier, getCacheScopeSizeInBytes(cacheIdentifier)));
                        cacheScopeSizeInBytes.keySet().removeIf(key -> !cacheScopeFiles.containsKey(key));
                    }
                    catch (Throwable t) {
                        log.error(t, "Error calculating cache size");
                    }
                },
                0,
                15,
                TimeUnit.SECONDS);
    }

    @PreDestroy
    public void destroy()
    {
        cacheFlushExecutor.shutdownNow();
        cacheRemovalExecutor.shutdownNow();
        cacheSizeCalculateExecutor.shutdownNow();
        buffers.remove();
    }

    @Override
    public CacheResult get(FileReadRequest request, byte[] buffer, int offset, CacheQuota cacheQuota)
    {
        boolean result = read(request, buffer, offset);

        if (!result && ifExceedQuota(cacheQuota, request)) {
            stats.incrementQuotaExceed();
            return CacheResult.CACHE_QUOTA_EXCEED;
        }

        try {
            // hint the cache
            cache.get(request.getPath(), cacheQuota::getIdentifier);
        }
        catch (ExecutionException e) {
            // ignore
        }

        if (result) {
            stats.incrementCacheHit();
            return CacheResult.HIT;
        }

        stats.incrementCacheMiss();
        return CacheResult.MISS;
    }

    private boolean ifExceedQuota(CacheQuota cacheQuota, FileReadRequest request)
    {
        DataSize cacheSize = DataSize.succinctBytes(cacheScopeSizeInBytes.getOrDefault(cacheQuota.getIdentifier(), 0L) + request.getLength());
        return cacheQuota.getQuota().map(quota -> cacheSize.compareTo(quota) > 0).orElse(false);
    }

    private long getCacheScopeSizeInBytes(long cacheScopeIdentifier)
    {
        Set<Path> paths = cacheScopeFiles.get(cacheScopeIdentifier);
        if (paths == null) {
            return 0;
        }

        long bytes = 0;
        for (Path path : paths) {
            CacheRange cacheRange = persistedRanges.get(path);
            Lock readLock = cacheRange.getLock().readLock();
            readLock.lock();
            try {
                for (Range<Long> range : cacheRange.getRange().asDescendingMapOfRanges().keySet()) {
                    bytes += range.upperEndpoint() - range.lowerEndpoint();
                }
            }
            finally {
                readLock.unlock();
            }
        }
        return bytes;
    }

    @Override
    public void put(FileReadRequest key, Slice data, CacheQuota cacheQuota)
    {
        if (stats.getInMemoryRetainedBytes() + data.length() >= maxInflightBytes) {
            // cannot accept more requests
            return;
        }

        Set<Path> paths = cacheScopeFiles.computeIfAbsent(cacheQuota.getIdentifier(), k -> new ConcurrentHashSet<>());
        paths.add(key.getPath());

        // make a copy given the input data could be a reusable buffer
        stats.addInMemoryRetainedBytes(data.length());
        byte[] copy = data.getBytes();

        cacheFlushExecutor.submit(() -> {
            Path newFilePath = new Path(baseDirectory.toUri() + "/" + randomUUID() + EXTENSION);
            if (!write(key, copy, newFilePath)) {
                log.warn("%s Fail to persist cache %s with length %s ", Thread.currentThread().getName(), newFilePath, key.getLength());
            }
            stats.addInMemoryRetainedBytes(-copy.length);
        });
    }

    private boolean read(FileReadRequest request, byte[] buffer, int offset)
    {
        if (request.getLength() <= 0) {
            // no-op
            return true;
        }

        // check if the file is cached on local disk
        CacheRange cacheRange = persistedRanges.get(request.getPath());
        if (cacheRange == null) {
            return false;
        }

        LocalCacheFile cacheFile;
        Lock readLock = cacheRange.getLock().readLock();
        readLock.lock();
        try {
            Map<Range<Long>, LocalCacheFile> diskRanges = cacheRange.getRange().subRangeMap(Range.closedOpen(request.getOffset(), request.getLength() + request.getOffset())).asMapOfRanges();
            if (diskRanges.size() != 1) {
                // no range or there is a hole in between
                return false;
            }

            cacheFile = getOnlyElement(diskRanges.entrySet().iterator()).getValue();
        }
        finally {
            readLock.unlock();
        }

        try (RandomAccessFile file = new RandomAccessFile(new File(cacheFile.getPath().toUri()), "r")) {
            file.seek(request.getOffset() - cacheFile.getOffset());
            file.readFully(buffer, offset, request.getLength());
            return true;
        }
        catch (IOException e) {
            // there might be a chance the file has been deleted
            return false;
        }
    }

    private boolean write(FileReadRequest key, byte[] data, Path newFilePath)
    {
        Path targetFile = key.getPath();
        persistedRanges.putIfAbsent(targetFile, new CacheRange());

        LocalCacheFile previousCacheFile;
        LocalCacheFile followingCacheFile;

        CacheRange cacheRange = persistedRanges.get(targetFile);
        if (cacheRange == null) {
            // there is a chance the cache has just expired.
            return false;
        }

        Lock readLock = cacheRange.getLock().readLock();
        readLock.lock();
        try {
            RangeMap<Long, LocalCacheFile> cache = cacheRange.getRange();

            // check if it can be merged with the previous or following range
            previousCacheFile = cache.get(key.getOffset() - 1);
            followingCacheFile = cache.get(key.getOffset() + key.getLength());
        }
        finally {
            readLock.unlock();
        }

        if (previousCacheFile != null && cacheFileEquals(previousCacheFile, followingCacheFile)) {
            log.debug("%s found covered range %s", Thread.currentThread().getName(), previousCacheFile.getPath());
            // this range has already been covered by someone else
            return true;
        }

        long newFileOffset;
        int newFileLength;
        File newFile = new File(newFilePath.toUri());
        try {
            if (previousCacheFile == null) {
                // a new file
                Files.write(newFile.toPath(), data, CREATE_NEW);

                // update new range info
                newFileLength = data.length;
                newFileOffset = key.getOffset();
            }
            else {
                // copy previous file's data to the new file
                int previousFileLength = appendToFile(previousCacheFile, 0, newFile);
                long previousFileOffset = previousCacheFile.getOffset();

                // remove the overlapping part and append the remaining cache data
                int remainingCacheFileOffset = toIntExact(previousFileLength + previousFileOffset - key.getOffset());
                int remainingCacheFileLength = toIntExact((key.getLength() + key.getOffset()) - (previousFileLength + previousFileOffset));

                try (RandomAccessFile randomAccessFile = new RandomAccessFile(newFile, "rw")) {
                    randomAccessFile.seek(randomAccessFile.length());
                    randomAccessFile.write(data, remainingCacheFileOffset, remainingCacheFileLength);
                }

                // update new range info
                newFileLength = previousFileLength + remainingCacheFileLength;
                newFileOffset = previousFileOffset;
            }

            if (followingCacheFile != null) {
                // remove the overlapping part and append the remaining following file data
                newFileLength += appendToFile(followingCacheFile, key.getOffset() + key.getLength() - followingCacheFile.getOffset(), newFile);
            }
        }
        catch (IOException e) {
            log.warn(e, "%s encountered an error while flushing file %s", Thread.currentThread().getName(), newFilePath);
            tryDeleteFile(newFilePath);
            return false;
        }

        // use a flag so that file deletion can be done outside the lock
        boolean updated;
        Set<Path> cacheFilesToDelete = new HashSet<>();

        Lock writeLock = persistedRanges.get(targetFile).getLock().writeLock();
        writeLock.lock();
        try {
            RangeMap<Long, LocalCacheFile> cache = persistedRanges.get(targetFile).getRange();
            // check again if the previous or following range has been updated by someone else
            LocalCacheFile newPreviousCacheFile = cache.get(key.getOffset() - 1);
            LocalCacheFile newFollowingCacheFile = cache.get(key.getOffset() + key.getLength());

            if (!cacheFileEquals(previousCacheFile, newPreviousCacheFile) || !cacheFileEquals(followingCacheFile, newFollowingCacheFile)) {
                // someone else has updated the cache; delete the newly created file
                updated = false;
            }
            else {
                updated = true;

                // remove all the files that can be covered by the current range
                cacheFilesToDelete = cache.subRangeMap(Range.closedOpen(key.getOffset(), key.getOffset() + key.getLength())).asMapOfRanges().values().stream()
                        .map(LocalCacheFile::getPath).collect(Collectors.toSet());

                // update the range
                Range<Long> newRange = Range.closedOpen(newFileOffset, newFileOffset + newFileLength);
                cache.remove(newRange);
                cache.put(newRange, new LocalCacheFile(newFileOffset, newFilePath));
            }
        }
        finally {
            writeLock.unlock();
        }

        // no lock is needed for the following operation
        if (updated) {
            // remove the the previous or following file as well
            if (previousCacheFile != null) {
                cacheFilesToDelete.add(previousCacheFile.getPath());
            }
            if (followingCacheFile != null) {
                cacheFilesToDelete.add(followingCacheFile.getPath());
            }
        }
        else {
            cacheFilesToDelete = ImmutableSet.of(newFilePath);
        }

        cacheFilesToDelete.forEach(FileMergeCacheManager::tryDeleteFile);
        return true;
    }

    private int appendToFile(LocalCacheFile source, long offset, File destination)
            throws IOException
    {
        int totalBytesRead = 0;
        try (FileInputStream fileInputStream = new FileInputStream(new File(source.getPath().toUri()))) {
            fileInputStream.getChannel().position(offset);
            int readBytes;
            byte[] buffer = buffers.get();

            while ((readBytes = fileInputStream.read(buffer)) > 0) {
                if (!Files.exists(destination.toPath())) {
                    Files.createFile(destination.toPath());
                }
                totalBytesRead += readBytes;

                if (readBytes != FILE_MERGE_BUFFER_SIZE) {
                    try (RandomAccessFile randomAccessFile = new RandomAccessFile(destination, "rw")) {
                        randomAccessFile.seek(destination.length());
                        randomAccessFile.write(buffer, 0, readBytes);
                    }
                }
                else {
                    Files.write(destination.toPath(), buffer, APPEND);
                }
            }
        }
        return totalBytesRead;
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

    private static boolean cacheFileEquals(LocalCacheFile left, LocalCacheFile right)
    {
        if (left == null && right == null) {
            return true;
        }

        if (left == null || right == null) {
            return false;
        }

        return left.equals(right);
    }

    private static class LocalCacheFile
    {
        private final long offset;  // the original file offset
        private final Path path;    // the cache location on disk

        public LocalCacheFile(long offset, Path path)
        {
            this.offset = offset;
            this.path = path;
        }

        public long getOffset()
        {
            return offset;
        }

        public Path getPath()
        {
            return path;
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
            LocalCacheFile that = (LocalCacheFile) o;
            return Objects.equals(offset, that.offset) && Objects.equals(path, that.path);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(offset, path);
        }
    }

    private static class CacheRange
    {
        private final RangeMap<Long, LocalCacheFile> range = TreeRangeMap.create();
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        public RangeMap<Long, LocalCacheFile> getRange()
        {
            return range;
        }

        public ReadWriteLock getLock()
        {
            return lock;
        }
    }

    private class CacheRemovalListener
            implements RemovalListener<Path, Long>
    {
        @Override
        public void onRemoval(RemovalNotification<Path, Long> notification)
        {
            Path path = notification.getKey();
            CacheRange cacheRange = persistedRanges.remove(path);
            Set<Path> paths = cacheScopeFiles.get(notification.getValue());
            if (paths != null) {
                paths.remove(path);
                if (paths.isEmpty()) {
                    // ConcurrentHashSet is a custom implementation from Alluxio library.
                    // The equals implementation is not atomic. So  this can remove
                    // non empty set due to race conditions, due to non atomic equals.
                    cacheScopeFiles.remove(notification.getValue(), Collections.emptySet());
                }
            }

            if (cacheRange == null) {
                return;
            }

            cacheRemovalExecutor.submit(() -> {
                Collection<LocalCacheFile> files;
                cacheRange.lock.readLock().lock();
                try {
                    files = cacheRange.getRange().asMapOfRanges().values();
                }
                finally {
                    cacheRange.lock.readLock().unlock();
                }

                // There is a chance of the files to be deleted are being read.
                // We may just fail the cache hit and do it in a simple way given the chance is low.
                for (LocalCacheFile file : files) {
                    try {
                        Files.delete(new File(file.getPath().toUri()).toPath());
                    }
                    catch (IOException e) {
                        // ignore
                    }
                }
            });
        }
    }
}
