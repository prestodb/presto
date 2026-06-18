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
package com.facebook.presto.lance;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.ReadOptions;
import org.lance.WriteParams;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Holds the Lance namespace configuration and provides table management operations.
 * For the "dir" implementation, directly manages a directory-based table store.
 * All tables live under a single "default" schema mapped to the root directory.
 */
public class LanceNamespaceHolder
{
    private static final Logger log = Logger.get(LanceNamespaceHolder.class);
    public static final String DEFAULT_SCHEMA = "default";
    public static final String TABLE_PATH_SUFFIX = ".lance";

    private final BufferAllocator allocator;
    private final String root;
    private final boolean singleLevelNs;
    private final ReadOptions readOptions;
    private final Cache<DatasetCacheKey, Dataset> datasetCache;

    @Inject
    public LanceNamespaceHolder(LanceConfig config)
    {
        this.root = requireNonNull(config.getRootUrl(), "root is null");
        this.singleLevelNs = config.isSingleLevelNs();
        this.readOptions = new ReadOptions.Builder()
                .setIndexCacheSizeBytes(config.getIndexCacheSize().toBytes())
                .setMetadataCacheSizeBytes(config.getMetadataCacheSize().toBytes())
                .build();
        this.datasetCache = CacheBuilder.newBuilder()
                .maximumSize(config.getDatasetCacheMaxEntries())
                .expireAfterAccess(config.getDatasetCacheTtl().toMillis(), MILLISECONDS)
                .removalListener((RemovalListener<DatasetCacheKey, Dataset>) notification -> {
                    try {
                        notification.getValue().close();
                    }
                    catch (Exception e) {
                        log.warn(e, "Error closing cached dataset: %s", notification.getKey());
                    }
                })
                .build();
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        log.debug("LanceNamespaceHolder initialized: root=%s, singleLevelNs=%s", root, singleLevelNs);
    }

    public void shutdown()
    {
        datasetCache.invalidateAll();
        try {
            allocator.close();
        }
        catch (Exception e) {
            log.warn(e, "Error closing Arrow allocator");
        }
    }

    public BufferAllocator getAllocator()
    {
        return allocator;
    }

    public String getRoot()
    {
        return root;
    }

    public boolean isSingleLevelNs()
    {
        return singleLevelNs;
    }

    /**
     * Get ReadOptions with configured cache sizes.
     */
    private ReadOptions getReadOptions()
    {
        return readOptions;
    }

    /**
     * Get a cached or newly opened Dataset for the given table path with optional version.
     * The returned Dataset is managed by the cache — callers must NOT close it.
     *
     * <p>Thread safety: Lance Dataset objects use an internal LockManager for
     * concurrent access. Multiple threads may safely call getFragments(),
     * getSchema(), and newScan() on the same cached Dataset instance.
     *
     * @param tablePath the full table path (URI)
     * @param version the dataset version to open (empty for latest)
     */
    Dataset getCachedDataset(String tablePath, Optional<Long> version)
    {
        DatasetCacheKey cacheKey = new DatasetCacheKey(tablePath, version);
        try {
            return datasetCache.get(cacheKey, () -> {
                if (version.isPresent()) {
                    long v = version.get();
                    checkArgument(v <= Integer.MAX_VALUE,
                            "Dataset version %s exceeds maximum supported version", v);
                    ReadOptions versionedOptions = new ReadOptions.Builder()
                            .setIndexCacheSizeBytes(readOptions.getIndexCacheSizeBytes())
                            .setMetadataCacheSizeBytes(readOptions.getMetadataCacheSizeBytes())
                            .setVersion((int) v)
                            .build();
                    return Dataset.open(tablePath, versionedOptions);
                }
                return Dataset.open(tablePath, readOptions);
            });
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throw new PrestoException(LanceErrorCode.LANCE_ERROR, "Failed to open dataset: " + tablePath, e.getCause());
        }
    }

    /**
     * Get the latest version of a dataset. Opens a fresh Dataset each time
     * (bypasses cache) to ensure the returned version is never stale.
     * The pinned version is then used for all subsequent cached reads.
     */
    public long getLatestVersion(String tableName)
    {
        String tablePath = getTablePath(tableName);
        try (Dataset dataset = Dataset.open(tablePath, readOptions)) {
            return dataset.version();
        }
    }

    /**
     * Get the filesystem path for a table.
     */
    public String getTablePath(String tableName)
    {
        return Paths.get(root, tableName + TABLE_PATH_SUFFIX).toUri().toString();
    }

    /**
     * Check if a table exists on the filesystem.
     */
    public boolean tableExists(String tableName)
    {
        try {
            Path path = Paths.get(root, tableName + TABLE_PATH_SUFFIX);
            return Files.isDirectory(path);
        }
        catch (Exception e) {
            return false;
        }
    }

    /**
     * Get the Arrow schema for a table at an optional version.
     */
    public Schema describeTable(String tableName, Optional<Long> version)
    {
        String tablePath = getTablePath(tableName);
        return getCachedDataset(tablePath, version).getSchema();
    }

    /**
     * List all tables in a schema.
     */
    public List<String> listTables()
    {
        Path rootPath = Paths.get(root);
        if (!Files.isDirectory(rootPath)) {
            return Collections.emptyList();
        }
        List<String> tables = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootPath, "*" + TABLE_PATH_SUFFIX)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    String fileName = entry.getFileName().toString();
                    tables.add(fileName.substring(0, fileName.length() - TABLE_PATH_SUFFIX.length()));
                }
            }
        }
        catch (IOException e) {
            log.warn(e, "Failed to list tables in %s", root);
        }
        return tables;
    }

    /**
     * Create an empty table with the given schema.
     */
    public void createTable(String tableName, Schema arrowSchema)
    {
        String tablePath = getTablePath(tableName);
        WriteParams params = new WriteParams.Builder().build();
        Dataset.create(allocator, tablePath, arrowSchema, params).close();
    }

    /**
     * Drop a table.
     */
    public void dropTable(String tableName)
    {
        String tablePath = getTablePath(tableName);
        invalidateByTablePath(tablePath);
        Path fsPath = Paths.get(root, tableName + TABLE_PATH_SUFFIX);
        if (Files.exists(fsPath)) {
            try {
                MoreFiles.deleteRecursively(fsPath, RecursiveDeleteOption.ALLOW_INSECURE);
            }
            catch (IOException e) {
                throw new PrestoException(LanceErrorCode.LANCE_ERROR, "Failed to delete table " + tableName, e);
            }
        }
    }

    /**
     * Commit fragments to a table (append operation).
     */
    public void commitAppend(String tableName, List<FragmentMetadata> fragments)
    {
        String tablePath = getTablePath(tableName);
        try (Dataset dataset = Dataset.open(tablePath, getReadOptions())) {
            FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
            Dataset.commit(allocator, tablePath, appendOp, Optional.of(dataset.version()), Collections.emptyMap()).close();
        }
        // Invalidate all cache entries for this table path
        invalidateByTablePath(tablePath);
    }

    private void invalidateByTablePath(String tablePath)
    {
        List<DatasetCacheKey> keysToInvalidate = datasetCache.asMap().keySet().stream()
                .filter(key -> key.matchesTablePath(tablePath))
                .collect(toImmutableList());
        datasetCache.invalidateAll(keysToInvalidate);
    }

    /**
     * Get fragments for a table at an optional version.
     */
    public List<Fragment> getFragments(String tableName, Optional<Long> version)
    {
        String tablePath = getTablePath(tableName);
        return getCachedDataset(tablePath, version).getFragments();
    }

    /**
     * Cache key that includes table path and version for snapshot isolation.
     */
    private static class DatasetCacheKey
    {
        private final String tablePath;
        private final Optional<Long> version;

        DatasetCacheKey(String tablePath, Optional<Long> version)
        {
            this.tablePath = requireNonNull(tablePath, "tablePath is null");
            this.version = requireNonNull(version, "version is null");
        }

        boolean matchesTablePath(String path)
        {
            return tablePath.equals(path);
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
            DatasetCacheKey that = (DatasetCacheKey) o;
            return Objects.equals(tablePath, that.tablePath) &&
                    Objects.equals(version, that.version);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tablePath, version);
        }

        @Override
        public String toString()
        {
            return "DatasetCacheKey{" +
                    "tablePath='" + tablePath + '\'' +
                    ", version=" + version +
                    '}';
        }
    }
}
