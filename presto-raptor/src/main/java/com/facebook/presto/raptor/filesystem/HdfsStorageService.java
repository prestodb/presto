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
package com.facebook.presto.raptor.filesystem;

import com.facebook.presto.raptor.storage.OrcDataEnvironment;
import com.facebook.presto.raptor.storage.StorageService;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_FILE_SYSTEM_ERROR;
import static com.facebook.presto.raptor.filesystem.FileSystemUtil.DEFAULT_RAPTOR_CONTEXT;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

// TODO: this only works for hard file affinity; but good for a prototype
// TODO: need to handle race condition on all workers
// TODO: Staging dir and storage dir are the same in this StorageService implementation
public class HdfsStorageService
        implements StorageService
{
    private static final String FILE_EXTENSION = ".orc";

    private final Path baseStorageDir;
    private final Path baseQuarantineDir;
    private final OrcDataEnvironment environment;

    @Inject
    public HdfsStorageService(OrcDataEnvironment environment, Path baseLocation)
    {
        requireNonNull(baseLocation, "baseLocation is null");
        this.baseStorageDir = new Path(baseLocation, "storage");
        this.baseQuarantineDir = new Path(baseLocation, "quarantine");
        this.environment = requireNonNull(environment, "environment is null");
    }

    @Override
    @PostConstruct
    public void start()
    {
        createDirectory(baseStorageDir);
        createDirectory(baseQuarantineDir);
    }

    @Override
    public long getAvailableBytes()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public Path getStorageFile(UUID shardUuid)
    {
        return getFileSystemPath(baseStorageDir, shardUuid);
    }

    @Override
    public Path getStagingFile(UUID shardUuid)
    {
        // Deliberately returned storage file path because we don't have a stage directory here
        return getStorageFile(shardUuid);
    }

    @Override
    public Path getQuarantineFile(UUID shardUuid)
    {
        throw new PrestoException(RAPTOR_ERROR, "Possible data corruption is detected in metadata or remote storage");
    }

    @Override
    public Set<UUID> getStorageShards()
    {
        // Bug: This prevent ShardCleaner from cleaning up any unused shards so currently storage will grow indefinitely
        // This can only be solved until we re-design the metadata layout for disagg Raptor
        throw new UnsupportedOperationException("HDFS storage does not support list directory on purpose");
    }

    @Override
    public void createParents(Path file)
    {
        checkArgument(file.getParent().equals(baseStorageDir) || file.getParent().equals(baseQuarantineDir));
        // No need to create parent as we only have 2 levels of directory now
        // TODO: This may change based on metadata redesign
    }

    @Override
    public void promoteFromStagingToStorage(UUID shardUuid)
    {
        // Nothing to do as we don't have staging directory
    }

    private static Path getFileSystemPath(Path base, UUID shardUuid)
    {
        String uuid = shardUuid.toString().toLowerCase(ENGLISH);
        return new Path(base, uuid + FILE_EXTENSION);
    }

    private void createDirectory(Path directory)
    {
        boolean madeDirectory;
        try {
            FileSystem fileSystem = environment.getFileSystem(DEFAULT_RAPTOR_CONTEXT);
            madeDirectory = fileSystem.mkdirs(directory) && fileSystem.isDirectory(directory);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_FILE_SYSTEM_ERROR, "Failed creating directories: " + directory, e);
        }

        if (!madeDirectory) {
            throw new PrestoException(RAPTOR_FILE_SYSTEM_ERROR, "Failed creating directories: " + directory);
        }
    }
}
