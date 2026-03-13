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
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
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
import java.util.Optional;

import static java.util.Objects.requireNonNull;

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
    private final long indexCacheSizeBytes;
    private final long metadataCacheSizeBytes;

    @Inject
    public LanceNamespaceHolder(LanceConfig config)
    {
        this.root = requireNonNull(config.getRootUrl(), "root is null");
        this.singleLevelNs = config.isSingleLevelNs();
        this.indexCacheSizeBytes = config.getIndexCacheSizeBytes();
        this.metadataCacheSizeBytes = config.getMetadataCacheSizeBytes();
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        log.debug("LanceNamespaceHolder initialized: root=%s, singleLevelNs=%s", root, singleLevelNs);
    }

    public void shutdown()
    {
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
     * Build ReadOptions with configured cache sizes.
     */
    public ReadOptions buildReadOptions()
    {
        return new ReadOptions.Builder()
                .setIndexCacheSizeBytes(indexCacheSizeBytes)
                .setMetadataCacheSizeBytes(metadataCacheSizeBytes)
                .build();
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
     * Get the Arrow schema for a table.
     */
    public Schema describeTable(String tableName)
    {
        String tablePath = getTablePath(tableName);
        try (Dataset dataset = Dataset.open(tablePath, buildReadOptions())) {
            return dataset.getSchema();
        }
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
        Path tablePath = Paths.get(root, tableName + TABLE_PATH_SUFFIX);
        if (Files.exists(tablePath)) {
            try {
                MoreFiles.deleteRecursively(tablePath, RecursiveDeleteOption.ALLOW_INSECURE);
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to delete table " + tableName, e);
            }
        }
    }

    /**
     * Commit fragments to a table (append operation).
     */
    public void commitAppend(String tableName, List<FragmentMetadata> fragments)
    {
        String tablePath = getTablePath(tableName);
        try (Dataset dataset = Dataset.open(tablePath, buildReadOptions())) {
            FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments);
            Dataset.commit(allocator, tablePath, appendOp, Optional.of(dataset.version()), Collections.emptyMap()).close();
        }
    }

    /**
     * Get fragments for a table.
     */
    public List<Fragment> getFragments(String tableName)
    {
        String tablePath = getTablePath(tableName);
        try (Dataset dataset = Dataset.open(tablePath, buildReadOptions())) {
            return dataset.getFragments();
        }
    }
}
