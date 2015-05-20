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
package com.facebook.presto.raptor.backup;

import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.OptionalLong;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.storage.FileStorageService.getFileSystemPath;
import static com.facebook.presto.raptor.util.FileUtil.copyFile;
import static java.nio.file.Files.readAttributes;
import static java.util.Objects.requireNonNull;

public class FileBackupStore
        implements BackupStore
{
    private final File baseDir;

    @Inject
    public FileBackupStore(FileBackupConfig config)
    {
        this(config.getBackupDirectory());
    }

    public FileBackupStore(File baseDir)
    {
        this.baseDir = requireNonNull(baseDir, "baseDir is null");
    }

    @PostConstruct
    public void start()
    {
        createDirectories(baseDir);
    }

    @Override
    public void backupShard(UUID uuid, File source)
    {
        File backupFile = getBackupFile(uuid);
        createDirectories(backupFile.getParentFile());

        try {
            copyFile(source.toPath(), backupFile.toPath());
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to create backup shard file", e);
        }
    }

    @Override
    public void restoreShard(UUID uuid, File target)
    {
        try {
            copyFile(getBackupFile(uuid).toPath(), target.toPath());
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to copy backup shard: " + uuid, e);
        }
    }

    @Override
    public OptionalLong shardSize(UUID uuid)
    {
        Path path = getBackupFile(uuid).toPath();
        try {
            BasicFileAttributes attributes = readAttributes(path, BasicFileAttributes.class);
            if (!attributes.isRegularFile()) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(attributes.size());
        }
        catch (IOException e) {
            return OptionalLong.empty();
        }
    }

    @VisibleForTesting
    public File getBackupFile(UUID uuid)
    {
        return getFileSystemPath(baseDir, uuid);
    }

    private static void createDirectories(File dir)
    {
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new PrestoException(RAPTOR_ERROR, "Failed creating directories: " + dir);
        }
    }
}
