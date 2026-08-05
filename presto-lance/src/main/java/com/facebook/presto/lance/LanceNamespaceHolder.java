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
import com.google.common.collect.ImmutableMap;
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
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.ErrorCode;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.CreateEmptyTableRequest;
import org.lance.namespace.model.CreateEmptyTableResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;

import javax.inject.Inject;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Holds the Lance namespace and provides table management operations.
 * Delegates to the LanceNamespace API for table discovery and lifecycle,
 * supporting pluggable namespace implementations (dir, rest, etc.).
 */
public class LanceNamespaceHolder
{
    private static final Logger log = Logger.get(LanceNamespaceHolder.class);
    public static final String DEFAULT_SCHEMA = "default";

    // Upper bound on off-heap Arrow allocation for this catalog. Fixed at 8 GB;
    // there is no catalog property to tune it.
    private static final long ALLOCATOR_MAX_BYTES = 8L * 1024 * 1024 * 1024;

    private final BufferAllocator allocator;
    private final LanceNamespace namespace;
    private final boolean singleLevelNs;
    private final Optional<List<String>> parentPrefix;
    private final Map<String, String> namespaceStorageOptions;
    private final ReadOptions readOptions;
    private final Cache<DatasetCacheKey, Dataset> datasetCache;

    @Inject
    public LanceNamespaceHolder(LanceConfig config, @LanceNamespaceProperties Map<String, String> namespaceProperties)
    {
        this.allocator = new RootAllocator(ALLOCATOR_MAX_BYTES);
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

        // Parse namespace properties from catalog config
        String impl = config.getImpl();
        Map<String, String> properties = new HashMap<>();
        Map<String, String> storageOpts = new HashMap<>();
        for (Map.Entry<String, String> entry : namespaceProperties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("lance.")) {
                String strippedKey = key.substring(6);
                properties.put(strippedKey, entry.getValue());
                if (strippedKey.startsWith("storage.")) {
                    storageOpts.put(strippedKey.substring(8), entry.getValue());
                }
            }
        }
        this.namespaceStorageOptions = ImmutableMap.copyOf(storageOpts);

        // Validate that 'root' is set for directory namespace
        if ("dir".equals(impl) && !properties.containsKey("root")) {
            throw new PrestoException(LanceErrorCode.LANCE_ERROR,
                    "lance.root must be set when using lance.impl=dir");
        }

        // For DirectoryNamespace, ensure default settings are applied
        if ("dir".equals(impl)) {
            properties.putIfAbsent("manifest_enabled", "true");
            properties.putIfAbsent("dir_listing_enabled", "true");
        }

        // Initialize namespace
        this.namespace = LanceNamespace.connect(impl, properties, allocator);

        // Initialize namespace level handling
        this.singleLevelNs = config.isSingleLevelNs();
        String parent = config.getParent();
        if (parent != null && !parent.isEmpty()) {
            // Parent uses '$' as delimiter to avoid conflicts with common path separators ('/' and '.')
            // and namespace-level separators. Example: "org$warehouse" -> ["org", "warehouse"]
            this.parentPrefix = Optional.of(Arrays.asList(parent.split("\\$")));
        }
        else {
            this.parentPrefix = Optional.empty();
        }

        log.debug("LanceNamespaceHolder initialized: impl=%s, singleLevelNs=%s", impl, singleLevelNs);
    }

    public void shutdown()
    {
        datasetCache.invalidateAll();
        if (namespace instanceof Closeable) {
            try {
                ((Closeable) namespace).close();
            }
            catch (Exception e) {
                log.warn(e, "Error closing namespace");
            }
        }
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

    public LanceNamespace getNamespace()
    {
        return namespace;
    }

    public boolean isSingleLevelNs()
    {
        return singleLevelNs;
    }

    // ================== Namespace Utilities ==================

    /**
     * Get a cached or newly opened Dataset for the given table path with optional version.
     * The returned Dataset is managed by the cache — callers must NOT close it.
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
     */
    public long getLatestVersion(String tablePath)
    {
        try (Dataset dataset = Dataset.open(tablePath, readOptions)) {
            return dataset.version();
        }
    }

    /**
     * Transform Presto schema name to Lance namespace identifier.
     * In single-level mode, maps to empty (root).
     * Otherwise, adds parent prefix if configured.
     */
    List<String> prestoSchemaToLanceNamespace(String schema)
    {
        if (singleLevelNs) {
            return Collections.emptyList();
        }
        List<String> namespaceId = Collections.singletonList(schema);
        return addParentPrefix(namespaceId);
    }

    /**
     * Add parent prefix for 3+ level namespaces.
     */
    List<String> addParentPrefix(List<String> namespaceId)
    {
        if (!parentPrefix.isPresent()) {
            return namespaceId;
        }
        List<String> result = new ArrayList<>(parentPrefix.get());
        result.addAll(namespaceId);
        return result;
    }

    /**
     * Convert a Presto SchemaTableName to a Lance table identifier.
     */
    public List<String> getTableId(String schemaName, String tableName)
    {
        List<String> tableId = new ArrayList<>();
        if (parentPrefix.isPresent()) {
            tableId.addAll(parentPrefix.get());
        }
        if (!singleLevelNs) {
            tableId.add(schemaName);
        }
        tableId.add(tableName);
        return tableId;
    }

    // ================== Schema/Namespace Operations ==================

    /**
     * List schema names (namespaces).
     */
    public List<String> listSchemaNames()
    {
        if (singleLevelNs) {
            return Collections.singletonList(DEFAULT_SCHEMA);
        }

        return drainPages(
                pageToken -> {
                    ListNamespacesRequest request = new ListNamespacesRequest();
                    parentPrefix.ifPresent(request::setId);
                    if (pageToken != null) {
                        request.setPageToken(pageToken);
                    }
                    return namespace.listNamespaces(request);
                },
                ListNamespacesResponse::getNamespaces,
                ListNamespacesResponse::getPageToken);
    }

    /**
     * Check if a schema (namespace) exists.
     */
    public boolean schemaExists(String schema)
    {
        if (singleLevelNs && DEFAULT_SCHEMA.equals(schema)) {
            return true;
        }
        if (singleLevelNs) {
            return false;
        }
        try {
            NamespaceExistsRequest request = new NamespaceExistsRequest();
            request.setId(prestoSchemaToLanceNamespace(schema));
            namespace.namespaceExists(request);
            return true;
        }
        catch (NamespaceNotFoundException e) {
            return false;
        }
        catch (LanceNamespaceException e) {
            if (e.getErrorCode() == ErrorCode.NAMESPACE_NOT_FOUND) {
                return false;
            }
            // A namespace that is unreachable, throttled, or rejecting our credentials is not
            // the same as one that is absent. Surfacing it as "absent" makes CREATE SCHEMA and
            // SHOW SCHEMAS silently wrong, so fail loudly instead.
            throw new PrestoException(LanceErrorCode.LANCE_ERROR,
                    format("Failed to check whether schema %s exists", schema), e);
        }
        catch (RuntimeException e) {
            // The directory namespace calls into native code, which reports every failure --
            // including a plain "namespace not found" -- as an untyped RuntimeException. With no
            // error code to inspect, absence is indistinguishable from a transient fault here, so
            // keep the historical "absent" answer rather than guess from the message text.
            log.debug(e, "namespaceExists failed for %s; treating as absent", schema);
            return false;
        }
    }

    // ================== Table Operations ==================

    /**
     * Get the storage path for a table via namespace API.
     * Returns null if table does not exist.
     */
    public String getTablePath(String schemaName, String tableName)
    {
        if (singleLevelNs && !DEFAULT_SCHEMA.equals(schemaName)) {
            return null;
        }
        try {
            List<String> tableId = getTableId(schemaName, tableName);
            DescribeTableRequest request = new DescribeTableRequest()
                    .id(tableId);
            DescribeTableResponse response = namespace.describeTable(request);
            return response.getLocation();
        }
        catch (TableNotFoundException | NamespaceNotFoundException e) {
            return null;
        }
        catch (LanceNamespaceException e) {
            if (e.getErrorCode() == ErrorCode.TABLE_NOT_FOUND || e.getErrorCode() == ErrorCode.NAMESPACE_NOT_FOUND) {
                return null;
            }
            // Reporting a transient namespace failure as "table absent" turns a retryable error
            // into a confusing "table does not exist", so only absence maps to null.
            throw new PrestoException(LanceErrorCode.LANCE_ERROR,
                    format("Failed to describe table %s.%s", schemaName, tableName), e);
        }
        catch (RuntimeException e) {
            // See schemaExists: the native directory namespace has no typed errors, and callers
            // such as tableExists and getTableHandle rely on null meaning "not found".
            log.debug(e, "describeTable failed for %s.%s; treating as not found", schemaName, tableName);
            return null;
        }
    }

    /**
     * Check if a table exists.
     */
    public boolean tableExists(String schemaName, String tableName)
    {
        return getTablePath(schemaName, tableName) != null;
    }

    /**
     * Get storage options for a table.
     */
    public Map<String, String> getStorageOptionsForTable(List<String> tableId)
    {
        try {
            DescribeTableRequest request = new DescribeTableRequest().id(tableId);
            DescribeTableResponse response = namespace.describeTable(request);
            Map<String, String> storageOptions = response.getStorageOptions();
            if (storageOptions != null && !storageOptions.isEmpty()) {
                return ImmutableMap.copyOf(storageOptions);
            }
        }
        catch (Exception e) {
+            log.debug(e, "Failed to get storage options from describeTable for %s", tableId);
+        }

        if (!namespaceStorageOptions.isEmpty()) {
            return namespaceStorageOptions;
        }

        return ImmutableMap.of();
    }

    /**
     * Get the Arrow schema for a table at an optional version.
     */
    public Schema describeTable(String tablePath, Optional<Long> version)
    {
        return getCachedDataset(tablePath, version).getSchema();
    }

    /**
     * List all tables in a schema.
     */
    public List<String> listTables(String schemaName)
    {
        List<String> namespaceId = prestoSchemaToLanceNamespace(schemaName);
        return drainPages(
                pageToken -> {
                    ListTablesRequest request = new ListTablesRequest();
                    request.setId(namespaceId);
                    if (pageToken != null) {
                        request.setPageToken(pageToken);
                    }
                    return namespace.listTables(request);
                },
                ListTablesResponse::getTables,
                ListTablesResponse::getPageToken);
    }

    /**
     * Drains a paginated listing, following page tokens until the server stops
     * returning one. Results accumulate in encounter order, and entries repeated
     * across pages collapse.
     * <p>
     * Paging also stops if the server echoes back the token that was just
     * requested, so a misbehaving server cannot spin the coordinator in a hot loop.
     *
     * @param fetchPage fetches one page; receives null for the first request
     */
    private static <T> List<String> drainPages(
            Function<String, T> fetchPage,
            Function<T, Set<String>> getItems,
            Function<T, String> getPageToken)
    {
        Set<String> items = new LinkedHashSet<>();
        String requestedToken = null;
        while (true) {
            T page = fetchPage.apply(requestedToken);
            Set<String> pageItems = getItems.apply(page);
            if (pageItems != null) {
                items.addAll(pageItems);
            }
            String responseToken = getPageToken.apply(page);
            if (responseToken == null || responseToken.isEmpty() || responseToken.equals(requestedToken)) {
                return items.stream().collect(toImmutableList());
            }
            requestedToken = responseToken;
        }
    }

    /**
     * Create an empty table with the given schema.
     * Returns the table path assigned by the namespace.
     */
    public String createTable(String schemaName, String tableName, Schema arrowSchema)
    {
        List<String> tableId = getTableId(schemaName, tableName);
        CreateEmptyTableRequest createRequest = new CreateEmptyTableRequest()
                .id(tableId);
        CreateEmptyTableResponse createResponse = namespace.createEmptyTable(createRequest);
        String tablePath = createResponse.getLocation();

        try {
            WriteParams params = new WriteParams.Builder().build();
            Dataset.create(allocator, tablePath, arrowSchema, params).close();
        }
        catch (Exception e) {
            // Clean up the namespace entry if physical dataset creation fails
            try {
                dropTable(tableId);
            }
            catch (Exception dropException) {
                log.warn(dropException, "Failed to clean up namespace entry for %s after Dataset.create failure", tableId);
            }
            throw new PrestoException(LanceErrorCode.LANCE_ERROR,
                    "Failed to create dataset at " + tablePath + ": " + e.getMessage(), e);
        }

        return tablePath;
    }

    /**
     * Drop a table.
     */
    public void dropTable(List<String> tableId)
    {
        DropTableRequest dropRequest = new DropTableRequest()
                .id(tableId);
        namespace.dropTable(dropRequest);
    }

    /**
     * Commit fragments to a table (append operation).
     */
    public void commitAppend(String tablePath, List<FragmentMetadata> fragments)
    {
        try (Dataset dataset = Dataset.open(tablePath, readOptions)) {
            FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
            Dataset.commit(allocator, tablePath, appendOp, Optional.of(dataset.version()), Collections.emptyMap()).close();
        }
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
    public List<Fragment> getFragments(String tablePath, Optional<Long> version)
    {
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
