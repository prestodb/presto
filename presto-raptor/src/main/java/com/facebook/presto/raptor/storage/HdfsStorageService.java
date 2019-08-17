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
import io.airlift.log.Logger;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

// TODO: this only works for hard file affinity; but good for a prototype
// TODO: need to handle race condition on all workers
public class HdfsStorageService
        implements StorageService
{
    private static final Logger log = Logger.get(HdfsStorageService.class);

    private static final String FILE_EXTENSION = ".orc";

    private final FileSystem fileSystem;

    private final Path baseStorageDir;
    private final Path baseStagingDir;
    private final Path baseQuarantineDir;

    @Inject
    public HdfsStorageService(OrcDataEnvironment environment, Path baseLocation)
    {
        this.fileSystem = requireNonNull(environment, "requireNonNull is null").getFileSystem();
        Path baseDataDir = requireNonNull(baseLocation, "baseLocation is null");
        this.baseStorageDir = new Path(baseDataDir, "storage");
        this.baseStagingDir = new Path(baseDataDir, "staging");
        this.baseQuarantineDir = new Path(baseDataDir, "quarantine");
    }

    @Override
    @PostConstruct
    public void start()
    {
        deleteStagingFilesAsync();
        createDirectory(baseStagingDir);
        createDirectory(baseStorageDir);
        createDirectory(baseQuarantineDir);
    }

    @Override
    public long getAvailableBytes()
    {
        return Long.MAX_VALUE;
    }

    @PreDestroy
    public void stop()
            throws IOException
    {
        fileSystem.delete(baseStagingDir, true);
    }

    @Override
    public Path getStorageFile(UUID shardUuid)
    {
        return getFileSystemPath(baseStorageDir, shardUuid);
    }

    @Override
    public Path getStagingFile(UUID shardUuid)
    {
        return getFileSystemPath(baseStagingDir, shardUuid);
    }

    @Override
    public Path getQuarantineFile(UUID shardUuid)
    {
        return getFileSystemPath(baseQuarantineDir, shardUuid);
    }

    @Override
    public Set<UUID> getStorageShards()
    {
        throw new UnsupportedOperationException("HDFS storage does not support list directory on purpose");
    }

    @Override
    public void createParents(Path file)
    {
        createDirectory(file.getParent());
    }

    private static Path getFileSystemPath(Path base, UUID shardUuid)
    {
        String uuid = shardUuid.toString().toLowerCase(ENGLISH);
        return new Path(base, uuid + FILE_EXTENSION);
    }

    private void deleteStagingFilesAsync()
    {
        FileStatus[] files;
        try {
            files = fileSystem.listStatus(baseStagingDir);
        }
        catch (IOException e) {
            log.warn(e, "Failed to list director " + baseStagingDir);
            return;
        }

        if (files.length > 0) {
            new Thread(() -> {
                for (FileStatus file : files) {
                    try {
                        fileSystem.delete(file.getPath(), false);
                    }
                    catch (IOException e) {
                        log.warn(e, "Failed to delete file: %s", file);
                    }
                }
            }, "background-staging-delete").start();
        }
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
            Files.deleteIfExists(file.toPath());
        }
        Files.deleteIfExists(dir.toPath());
    }

    private void createDirectory(Path directory)
    {
        boolean madeDirectory;
        try {
            madeDirectory = fileSystem.mkdirs(directory) && fileSystem.isDirectory(directory);
        }
        catch (IOException e) {
            madeDirectory = false;
        }

        if (!madeDirectory) {
            throw new PrestoException(RAPTOR_ERROR, "Failed creating directories: " + directory);
        }
    }

    private static Optional<UUID> uuidFromString(String value)
    {
        try {
            return Optional.of(UUID.fromString(value));
        }
        catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
