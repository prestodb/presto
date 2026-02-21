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

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Holds the Lance namespace configuration and provides table management operations.
 * For the initial "dir" implementation, directly manages a directory-based table store.
 */
public class LanceNamespaceHolder
{
    private static final Logger log = Logger.get(LanceNamespaceHolder.class);
    public static final String DEFAULT_SCHEMA = "default";
    public static final String TABLE_PATH_SUFFIX = ".lance";

    private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    private final String root;
    private final boolean singleLevelNs;

    @Inject
    public LanceNamespaceHolder(LanceConfig config)
    {
        this.root = requireNonNull(config.getRootUrl(), "root is null");
        this.singleLevelNs = config.isSingleLevelNs();
        log.debug("LanceNamespaceHolder initialized: root=%s, singleLevelNs=%s", root, singleLevelNs);
    }

    public static BufferAllocator getAllocator()
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
     * Get the filesystem path for a table.
     */
    public String getTablePath(String schemaName, String tableName)
    {
        return Paths.get(root, tableName + TABLE_PATH_SUFFIX).toUri().toString();
    }

    /**
     * Check if a table exists on the filesystem.
     */
    public boolean tableExists(String schemaName, String tableName)
    {
        try {
            Path path = Paths.get(root, tableName + TABLE_PATH_SUFFIX);
            return Files.exists(path) && Files.isDirectory(path);
        }
        catch (Exception e) {
            return false;
        }
    }

    /**
     * Get the Arrow schema for a table.
     */
    public Schema describeTable(String schemaName, String tableName)
    {
        String tablePath = getTablePath(schemaName, tableName);
        try (Dataset dataset = Dataset.open(tablePath, new ReadOptions.Builder().build())) {
            return dataset.getSchema();
        }
    }

    /**
     * List all tables in a schema.
     */
    public List<String> listTables(String schemaName)
    {
        Path rootPath = Paths.get(root);
        if (!Files.exists(rootPath) || !Files.isDirectory(rootPath)) {
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
    public void createTable(String schemaName, String tableName, Schema arrowSchema)
    {
        String tablePath = getTablePath(schemaName, tableName);
        WriteParams params = new WriteParams.Builder().build();
        Dataset.create(allocator, tablePath, arrowSchema, params).close();
    }

    /**
     * Drop a table.
     */
    public void dropTable(String schemaName, String tableName)
    {
        Path tablePath = Paths.get(root, tableName + TABLE_PATH_SUFFIX);
        if (Files.exists(tablePath)) {
            deleteRecursively(tablePath.toFile());
        }
    }

    private static void deleteRecursively(File file)
    {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        if (!file.delete()) {
            log.warn("Failed to delete %s", file);
        }
    }

    /**
     * Open a dataset for reading.
     */
    public Dataset openDataset(String schemaName, String tableName)
    {
        String tablePath = getTablePath(schemaName, tableName);
        return Dataset.open(tablePath, new ReadOptions.Builder().build());
    }

    /**
     * Get the version of a table.
     */
    public long getTableVersion(String schemaName, String tableName)
    {
        try (Dataset dataset = openDataset(schemaName, tableName)) {
            return dataset.version();
        }
    }

    /**
     * Commit fragments to a table (append operation).
     */
    public void commitAppend(String schemaName, String tableName, List<FragmentMetadata> fragments)
    {
        String tablePath = getTablePath(schemaName, tableName);
        try (Dataset dataset = Dataset.open(tablePath, new ReadOptions.Builder().build())) {
            FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
            Dataset.commit(allocator, tablePath, appendOp, Optional.of(dataset.version()), Collections.emptyMap()).close();
        }
    }

    /**
     * Get fragments for a table.
     */
    public List<Fragment> getFragments(String schemaName, String tableName)
    {
        try (Dataset dataset = openDataset(schemaName, tableName)) {
            return dataset.getFragments();
        }
    }
}
