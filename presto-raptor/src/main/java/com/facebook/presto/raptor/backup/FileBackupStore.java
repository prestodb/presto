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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_BACKUP_ERROR;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_BACKUP_NOT_FOUND;
import static com.facebook.presto.raptor.storage.FileStorageService.getFileSystemPath;
import static java.nio.file.Files.deleteIfExists;
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
            copyFile(source, backupFile);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to create backup shard file", e);
        }
    }

    @Override
    public void restoreShard(UUID uuid, File target)
    {
        try {
            copyFile(getBackupFile(uuid), target);
        }
        catch (FileNotFoundException e) {
            throw new PrestoException(RAPTOR_BACKUP_NOT_FOUND, "Backup shard not found: " + uuid, e);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to copy backup shard: " + uuid, e);
        }
    }

    @Override
    public boolean deleteShard(UUID uuid)
    {
        try {
            return deleteIfExists(getBackupFile(uuid).toPath());
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to delete backup shard: " + uuid, e);
        }
    }

    @Override
    public boolean shardExists(UUID uuid)
    {
        return getBackupFile(uuid).isFile();
    }

    @VisibleForTesting
    public File getBackupFile(UUID uuid)
    {
        return getFileSystemPath(baseDir, uuid);
    }

    private static void createDirectories(File dir)
    {
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed creating directories: " + dir);
        }
    }

    private static void copyFile(File source, File target)
            throws IOException
    {
        try (InputStream in = new FileInputStream(source);
                FileOutputStream out = new FileOutputStream(target)) {
            byte[] buffer = new byte[128 * 1024];
            while (true) {
                int n = in.read(buffer);
                if (n == -1) {
                    break;
                }
                out.write(buffer, 0, n);
            }
            out.flush();
            out.getFD().sync();
        }
    }
}
