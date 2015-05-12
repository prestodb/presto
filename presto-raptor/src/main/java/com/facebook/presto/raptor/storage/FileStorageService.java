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
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ENGLISH;

public class FileStorageService
        implements StorageService
{
    private final File baseStorageDir;
    private final File baseStagingDir;
    private final Optional<File> baseBackupDir;

    @Inject
    public FileStorageService(StorageManagerConfig config)
    {
        this(config.getDataDirectory(), Optional.ofNullable(config.getBackupDirectory()));
    }

    public FileStorageService(File dataDirectory, Optional<File> backupDirectory)
    {
        File baseDataDir = checkNotNull(dataDirectory, "dataDirectory is null");
        this.baseBackupDir = checkNotNull(backupDirectory, "backupDirectory is null");

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

        if (baseBackupDir.isPresent()) {
            createParents(baseBackupDir.get());
        }
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
    public File getBackupFile(UUID shardUuid)
    {
        checkState(baseBackupDir.isPresent(), "backup directory not set");
        return getFileSystemPath(baseBackupDir.get(), shardUuid);
    }

    @Override
    public void createParents(File file)
    {
        File dir = file.getParentFile();
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new PrestoException(RAPTOR_ERROR, "Failed creating directories: " + dir);
        }
    }

    @Override
    public boolean isBackupAvailable(UUID shardUuid)
    {
        return isBackupAvailable() && getBackupFile(shardUuid).exists();
    }

    @Override
    public boolean isBackupAvailable()
    {
        return baseBackupDir.isPresent();
    }

    /**
     * Generate a file system path for a shard UUID.
     * <p/>
     * This creates a three level deep directory structure where the first two levels each contain three hex digits (lowercase) of the UUID and the final level contains the full UUID. Example:
     * <p/>
     * <pre>
     * UUID: 701e1a79-74f7-4f56-b438-b41e8e7d019d
     * Path: /base/701/e1a/701e1a79-74f7-4f56-b438-b41e8e7d019d.orc
     * </pre>
     * <p/>
     * This ensures that files are spread out evenly through the tree while a path can still be easily navigated by a human being.
     */
    private static File getFileSystemPath(File base, UUID shardUuid)
    {
        String uuid = shardUuid.toString().toLowerCase(ENGLISH);
        return base.toPath()
                .resolve(uuid.substring(0, 3))
                .resolve(uuid.substring(3, 6))
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
