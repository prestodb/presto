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
package com.facebook.presto.cache.alluxio;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.Mode;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class AlluxioCachingClientFileSystem
        implements alluxio.client.file.FileSystem
{
    private final AlluxioConfiguration alluxioConfiguration;
    private final ExtendedFileSystem fileSystem;

    public AlluxioCachingClientFileSystem(
            ExtendedFileSystem fileSystem,
            AlluxioConfiguration alluxioConfiguration)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.alluxioConfiguration = requireNonNull(alluxioConfiguration, "alluxioConfiguration is null");
    }

    @Override
    public void createDirectory(AlluxioURI alluxioURI, CreateDirectoryPOptions options)
            throws IOException
    {
        FsPermission permission = new FsPermission(Mode.fromProto(options.getMode()).toShort());
        fileSystem.mkdirs(toPath(alluxioURI), permission);
    }

    @Override
    public FileInStream openFile(AlluxioURI alluxioURI, OpenFilePOptions options)
            throws IOException
    {
        return new AlluxioCachingInputStream(fileSystem.open(toPath(alluxioURI)));
    }

    @Override
    public FileInStream openFile(URIStatus uriStatus, OpenFilePOptions options)
            throws IOException
    {
        // URIStatus is the mechanism to pass the hiveFileContext to the source filesystem
        // hiveFileContext is critical to use to open file.
        checkState(uriStatus instanceof AlluxioURIStatus);
        HiveFileContext hiveFileContext = ((AlluxioURIStatus) uriStatus).getHiveFileContext();
        try {
            return new AlluxioCachingInputStream(fileSystem.openFile(new Path(uriStatus.getPath()), hiveFileContext));
        }
        catch (Exception e) {
            throw new IOException("Failed to open file", e);
        }
    }

    @Override
    public FileOutStream createFile(AlluxioURI alluxioURI, CreateFilePOptions options)
    {
        throw new UnsupportedOperationException("CreateFile operation is not supported");
    }

    @Override
    public void delete(AlluxioURI alluxioURI, DeletePOptions options)
    {
        throw new UnsupportedOperationException("Delete operation is not supported");
    }

    @Override
    public boolean exists(AlluxioURI alluxioURI, ExistsPOptions options)
    {
        throw new UnsupportedOperationException("Exists operation is not supported");
    }

    @Override
    public AlluxioConfiguration getConf()
    {
        return alluxioConfiguration;
    }

    @Override
    public URIStatus getStatus(AlluxioURI alluxioURI, GetStatusPOptions options)
            throws IOException
    {
        return toUriStatus(fileSystem.getFileStatus(toPath(alluxioURI)));
    }

    @Override
    public List<URIStatus> listStatus(AlluxioURI alluxioURI, ListStatusPOptions options)
            throws IOException
    {
        return Arrays.stream(fileSystem.listStatus(toPath(alluxioURI))).map(this::toUriStatus).collect(Collectors.toList());
    }

    @Override
    public boolean isClosed()
    {
        throw new UnsupportedOperationException("isClosed is not supported");
    }

    @Override
    public void free(AlluxioURI alluxioURI, FreePOptions options)
    {
        throw new UnsupportedOperationException("Free is not supported.");
    }

    @Override
    public List<BlockLocationInfo> getBlockLocations(AlluxioURI alluxioURI)
    {
        throw new UnsupportedOperationException("GetBlockLocations is not supported.");
    }

    @Override
    public void mount(AlluxioURI alluxioURI1, AlluxioURI alluxioURI2, MountPOptions options)
    {
        throw new UnsupportedOperationException("Mount is not supported.");
    }

    @Override
    public void updateMount(AlluxioURI alluxioURI, MountPOptions options)
    {
        throw new UnsupportedOperationException("UpdateMount is not supported.");
    }

    @Override
    public Map<String, MountPointInfo> getMountTable()
    {
        throw new UnsupportedOperationException("GetMountTable is not supported.");
    }

    @Override
    public List<SyncPointInfo> getSyncPathList()
    {
        throw new UnsupportedOperationException("GetSyncPathList is not supported.");
    }

    @Override
    public void persist(AlluxioURI alluxioURI, ScheduleAsyncPersistencePOptions options)
    {
        throw new UnsupportedOperationException("Persist is not supported.");
    }

    @Override
    public void rename(AlluxioURI source, AlluxioURI destination, RenamePOptions options)
    {
        throw new UnsupportedOperationException("ReverseResolve is not supported.");
    }

    @Override
    public AlluxioURI reverseResolve(AlluxioURI alluxioURI)
    {
        throw new UnsupportedOperationException("ReverseResolve is not supported.");
    }

    @Override
    public void setAcl(AlluxioURI alluxioURI, SetAclAction action, List<AclEntry> entries, SetAclPOptions options)
    {
        throw new UnsupportedOperationException("SetAcl is not supported.");
    }

    @Override
    public void startSync(AlluxioURI alluxioURI)
    {
        throw new UnsupportedOperationException("StartSync is not supported.");
    }

    @Override
    public void stopSync(AlluxioURI alluxioURI)
    {
        throw new UnsupportedOperationException("StopSync is not supported.");
    }

    @Override
    public void setAttribute(AlluxioURI alluxioURI, SetAttributePOptions options)
    {
        throw new UnsupportedOperationException("SetAttribute is not supported.");
    }

    @Override
    public void unmount(AlluxioURI alluxioURI, UnmountPOptions options)
    {
        throw new UnsupportedOperationException("unmount is not supported.");
    }

    @Override
    public void close()
            throws IOException
    {
        fileSystem.close();
    }

    private Path toPath(AlluxioURI alluxioURI)
    {
        return new Path(alluxioURI.toString());
    }

    private URIStatus toUriStatus(FileStatus status)
    {
        //FilePath is a unique identifier for a file, however it can be a long string
        //hence using md5 hash of the file path as the identifier in the cache.
        FileInfo info = new FileInfo();
        info.setFileIdentifier(md5().hashString(status.getPath().toString(), UTF_8).toString());
        info.setLength(status.getLen()).setPath(status.getPath().toString());
        info.setFolder(status.isDirectory()).setBlockSizeBytes(status.getBlockSize());
        info.setLastModificationTimeMs(status.getModificationTime()).setLastAccessTimeMs(status.getAccessTime());
        info.setOwner(status.getOwner()).setGroup(status.getGroup());
        return new URIStatus(info);
    }
}
