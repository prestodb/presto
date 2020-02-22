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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;

public class AlluxioCachingClientFileSystem
        implements alluxio.client.file.FileSystem
{
    private final AlluxioConfiguration alluxioConfiguration;
    protected final org.apache.hadoop.fs.FileSystem fileSystem;

    public AlluxioCachingClientFileSystem(FileSystem fileSystem, AlluxioConfiguration alluxioConfiguration)
    {
        this.fileSystem = fileSystem;
        this.alluxioConfiguration = alluxioConfiguration;
    }

    @Override
    public void createDirectory(AlluxioURI alluxioURI, CreateDirectoryPOptions options)
            throws IOException
    {
        FsPermission fsPermission = new FsPermission(Mode.fromProto(options.getMode()).toShort());
        fileSystem.mkdirs(toPath(alluxioURI), fsPermission);
    }

    @Override
    public FileInStream openFile(AlluxioURI alluxioURI, OpenFilePOptions options)
            throws IOException
    {
        FSDataInputStream inputStream = fileSystem.open(toPath(alluxioURI));
        return new AlluxioCachingInputStream(inputStream);
    }

    @Override
    public FileInStream openFile(URIStatus uriStatus, OpenFilePOptions options)
            throws IOException
    {
        return openFile(new AlluxioURI(uriStatus.getPath()), options);
    }

    @Override
    public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
    {
        return null;
    }

    @Override
    public void delete(AlluxioURI alluxioURI, DeletePOptions options)
            throws IOException
    {
        fileSystem.delete(toPath(alluxioURI), options.getRecursive());
    }

    @Override
    public boolean exists(AlluxioURI alluxioURI, ExistsPOptions options)
            throws IOException
    {
        return fileSystem.exists(toPath(alluxioURI));
    }

    @Override
    public AlluxioConfiguration getConf()
    {
        return alluxioConfiguration;
    }

    @Override
    public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
            throws IOException
    {
        return toUriStatus(fileSystem.getFileStatus(toPath(path)));
    }

    @Override
    public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
            throws IOException
    {
        List<URIStatus> statuses = Arrays.stream(fileSystem.listStatus(toPath(path))).map(this::toUriStatus).collect(Collectors.toList());
        return statuses;
    }

    @Override
    public boolean isClosed()
    {
        throw new UnsupportedOperationException("isClosed is not supported");
    }

    @Override
    public void free(AlluxioURI path, FreePOptions options)
    {
        throw new UnsupportedOperationException("Free is not supported.");
    }

    private BlockLocationInfo toBlockLocationInfo(BlockLocation blockLocation)
    {
        throw new UnsupportedOperationException("toBlockLocationInfo is not supported.");
    }

    @Override
    public List<BlockLocationInfo> getBlockLocations(AlluxioURI alluxioURI)
            throws IOException
    {
        Path path = toPath(alluxioURI);
        BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(path, 0, path.toString().length());
        Arrays.stream(fileBlockLocations).map(this::toBlockLocationInfo).collect(Collectors.toList());

        throw new UnsupportedOperationException("GetBlockLocations is not supported.");
    }

    @Override
    public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
    {
        throw new UnsupportedOperationException("Mount is not supported.");
    }

    @Override
    public void updateMount(AlluxioURI alluxioPath, MountPOptions options)
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
    public void persist(AlluxioURI path, ScheduleAsyncPersistencePOptions options)
    {
        throw new UnsupportedOperationException("Persist is not supported.");
    }

    @Override
    public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
    {
        throw new UnsupportedOperationException("ReverseResolve is not supported.");
    }

    @Override
    public AlluxioURI reverseResolve(AlluxioURI ufsUri)
    {
        throw new UnsupportedOperationException("ReverseResolve is not supported.");
    }

    @Override
    public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries, SetAclPOptions options)
    {
        throw new UnsupportedOperationException("SetAcl is not supported.");
    }

    @Override
    public void startSync(AlluxioURI path)
    {
        throw new UnsupportedOperationException("StartSync is not supported.");
    }

    @Override
    public void stopSync(AlluxioURI path)
    {
        throw new UnsupportedOperationException("StopSync is not supported.");
    }

    @Override
    public void setAttribute(AlluxioURI alluxioURI, SetAttributePOptions options)
    {
        throw new UnsupportedOperationException("SetAttribute is not supported.");
    }

    @Override
    public void unmount(AlluxioURI path, UnmountPOptions options)
    {
        throw new UnsupportedOperationException("unmount is not supported.");
    }

    @Override
    public void close()
            throws IOException
    {
        fileSystem.close();
    }

    private Path toPath(AlluxioURI uri)
    {
        return new Path(uri.toString());
    }

    private URIStatus toUriStatus(FileStatus status)
    {
        FileInfo info = new FileInfo();
        info.setFileIdentifier(md5().hashString(status.getPath().toString(), UTF_8).toString());
        info.setLength(status.getLen()).setPath(status.getPath().toString());
        info.setFolder(status.isDirectory()).setBlockSizeBytes(status.getBlockSize());
        info.setLastModificationTimeMs(status.getModificationTime()).setLastAccessTimeMs(status.getAccessTime());
        info.setOwner(status.getOwner()).setGroup(status.getGroup());
        return new URIStatus(info);
    }
}
