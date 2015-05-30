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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.spi.PrestoException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Locale.ENGLISH;

public class FileStorageService
        implements StorageService
{
    private final File baseStorageDir;
    private final File baseStagingDir;

    @Inject
    public FileStorageService(StorageManagerConfig config)
    {
        this(config.getDataDirectory());
    }

    public FileStorageService(File dataDirectory)
    {
        File baseDataDir = checkNotNull(dataDirectory, "dataDirectory is null");
        this.baseStorageDir = new File(baseDataDir, "storage");
        this.baseStagingDir = new File(baseDataDir, "staging");
    }

    @Override
    @PostConstruct
    public void start()
            throws IOException
    {
        deleteDirectory(baseStagingDir);
        createParents(baseStagingDir);
        createParents(baseStorageDir);
    }

    @PreDestroy
    public void stop()
            throws IOException
    {
        deleteDirectory(baseStagingDir);
    }

    @Override
    public File getStorageFile(UUID shardUuid)
    {
        return getFileSystemPath(baseStorageDir, shardUuid);
    }

    @Override
    public File getStagingFile(UUID shardUuid)
    {
        String name = getFileSystemPath(new File("/"), shardUuid).getName();
        return new File(baseStagingDir, name);
    }

    @Override
    public void createParents(File file)
    {
        File dir = file.getParentFile();
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new PrestoException(RAPTOR_ERROR, "Failed creating directories: " + dir);
        }
    }

    /**
     * Generate a file system path for a shard UUID.
     * This creates a three level deep directory structure where the first
     * two levels each contain two hex digits (lowercase) of the UUID
     * and the final level contains the full UUID. Example:
     * <pre>
     * UUID: 701e1a79-74f7-4f56-b438-b41e8e7d019d
     * Path: /base/70/1e/701e1a79-74f7-4f56-b438-b41e8e7d019d.orc
     * </pre>
     * This ensures that files are spread out evenly through the tree
     * while a path can still be easily navigated by a human being.
     */
    public static File getFileSystemPath(File base, UUID shardUuid)
    {
        String uuid = shardUuid.toString().toLowerCase(ENGLISH);
        return base.toPath()
                .resolve(uuid.substring(0, 2))
                .resolve(uuid.substring(2, 4))
                .resolve(uuid + ".orc")
                .toFile();
    }

    private static void deleteDirectory(File dir)
            throws IOException
    {
        if (!dir.exists()) {
            return;
        }
        File[] files = dir.listFiles();
        if (files == null) {
            throw new IOException("Failed to list directory: " + dir);
        }
        for (File file : files) {
            Files.delete(file.toPath());
        }
        Files.delete(dir.toPath());
    }
}
