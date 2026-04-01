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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Holds the Lance namespace and provides table management operations.
 * Delegates to the LanceNamespace API for table discovery and lifecycle,
 * supporting pluggable namespace implementations (dir, rest, etc.).
 */
public class LanceNamespaceHolder
{
    private static final Logger log = Logger.get(LanceNamespaceHolder.class);
    public static final String DEFAULT_SCHEMA = "default";

    private final BufferAllocator allocator;
    private final LanceNamespace namespace;
    private final boolean singleLevelNs;
    private final Optional<List<String>> parentPrefix;
    private final Map<String, String> namespaceStorageOptions;

    @Inject
    public LanceNamespaceHolder(LanceConfig config, @LanceNamespaceProperties Map<String, String> namespaceProperties)
    {
        this.allocator = new RootAllocator(Long.MAX_VALUE);

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
        this.namespaceStorageOptions = storageOpts;

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
            this.parentPrefix = Optional.of(Arrays.asList(parent.split("\\$")));
        }
        else {
            this.parentPrefix = Optional.empty();
        }

        log.debug("LanceNamespaceHolder initialized: impl=%s, singleLevelNs=%s", impl, singleLevelNs);
    }

    public void shutdown()
    {
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
     * Transform Presto schema name to Lance namespace identifier.
     * In single-level mode, maps to empty (root).
     * Otherwise, adds parent prefix if configured.
     */
    public List<String> prestoSchemaToLanceNamespace(String schema)
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
    public List<String> addParentPrefix(List<String> namespaceId)
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

        ListNamespacesRequest request = new ListNamespacesRequest();
        if (parentPrefix.isPresent()) {
            request.setId(parentPrefix.get());
        }
        ListNamespacesResponse response = namespace.listNamespaces(request);
        Set<String> namespaces = response.getNamespaces();
        if (namespaces == null || namespaces.isEmpty()) {
            return Collections.emptyList();
        }
        return namespaces.stream().collect(toImmutableList());
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
        catch (Exception e) {
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
        catch (Exception e) {
            log.debug("Failed to describe table %s.%s: %s", schemaName, tableName, e.getMessage());
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
                return storageOptions;
            }
        }
        catch (Exception e) {
            log.debug("Failed to get storage options from describeTable for %s: %s", tableId, e.getMessage());
        }

        if (!namespaceStorageOptions.isEmpty()) {
            return new HashMap<>(namespaceStorageOptions);
        }

        return new HashMap<>();
    }

    /**
     * Get the Arrow schema for a table.
     */
    public Schema describeTable(String tablePath)
    {
        try (Dataset dataset = Dataset.open(tablePath, new ReadOptions.Builder().build())) {
            return dataset.getSchema();
        }
    }

    /**
     * List all tables in a schema.
     */
    public List<String> listTables(String schemaName)
    {
        List<String> namespaceId = prestoSchemaToLanceNamespace(schemaName);
        ListTablesRequest request = new ListTablesRequest();
        request.setId(namespaceId);

        ListTablesResponse response = namespace.listTables(request);
        Set<String> tables = response.getTables();
        if (tables == null || tables.isEmpty()) {
            return Collections.emptyList();
        }
        return tables.stream().collect(toImmutableList());
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

        WriteParams params = new WriteParams.Builder().build();
        Dataset.create(allocator, tablePath, arrowSchema, params).close();

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
        try (Dataset dataset = Dataset.open(tablePath, new ReadOptions.Builder().build())) {
            FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
            Dataset.commit(allocator, tablePath, appendOp, Optional.of(dataset.version()), Collections.emptyMap()).close();
        }
    }

    /**
     * Get fragments for a table.
     */
    public List<Fragment> getFragments(String tablePath)
    {
        try (Dataset dataset = Dataset.open(tablePath, new ReadOptions.Builder().build())) {
            return dataset.getFragments();
        }
    }
}
