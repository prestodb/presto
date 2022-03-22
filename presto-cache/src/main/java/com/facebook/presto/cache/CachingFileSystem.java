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
package com.facebook.presto.cache;

import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveFileInfo;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public abstract class CachingFileSystem
        extends ExtendedFileSystem
{
    protected final ExtendedFileSystem dataTier;
    protected final URI uri;

    public CachingFileSystem(ExtendedFileSystem dataTier, URI uri)
    {
        this.dataTier = requireNonNull(dataTier, "dataTier is null");
        this.uri = requireNonNull(uri, "uri is null");
    }

    public ExtendedFileSystem getDataTier()
    {
        return dataTier;
    }

    @Override
    public String getScheme()
    {
        return dataTier.getScheme();
    }

    @Override
    public URI getUri()
    {
        return uri;
    }

    @Override
    public void initialize(URI uri, Configuration configuration)
            throws IOException
    {
        dataTier.initialize(uri, configuration);
    }

    @Override
    public Configuration getConf()
    {
        return dataTier.getConf();
    }

    @Override
    public Path getWorkingDirectory()
    {
        return dataTier.getWorkingDirectory();
    }

    @Override
    public void setWorkingDirectory(Path workingDirectory)
    {
        dataTier.setWorkingDirectory(workingDirectory);
    }

    @Override
    public long getDefaultBlockSize()
    {
        return dataTier.getDefaultBlockSize();
    }

    @Override
    public short getDefaultReplication()
    {
        return dataTier.getDefaultReplication();
    }

    @Override
    public Path getHomeDirectory()
    {
        return dataTier.getHomeDirectory();
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus fileStatus, long offset, long length)
            throws IOException
    {
        return dataTier.getFileBlockLocations(fileStatus, offset, length);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path path, long offset, long length)
            throws IOException
    {
        return dataTier.getFileBlockLocations(path, offset, length);
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum)
    {
        dataTier.setVerifyChecksum(verifyChecksum);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException
    {
        return dataTier.open(path, bufferSize);
    }

    // HiveFileContext contains caching info, which must be used while overriding this method.
    @Override
    public abstract FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext)
            throws Exception;

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progressable)
            throws IOException
    {
        return dataTier.append(path, bufferSize, progressable);
    }

    @Override
    public FSDataOutputStream create(
            Path path,
            FsPermission permission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progressable)
            throws IOException
    {
        return dataTier.create(path, permission, overwrite, bufferSize, replication, blockSize, progressable);
    }

    @Override
    public FSDataOutputStream create(
            Path path,
            FsPermission permission,
            EnumSet<CreateFlag> createFlags,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progressable,
            Options.ChecksumOpt checksumOption)
            throws IOException
    {
        return dataTier.create(path, permission, createFlags, bufferSize, replication, blockSize, progressable, checksumOption);
    }

    @Override
    public boolean setReplication(Path path, short replication)
            throws IOException
    {
        return dataTier.setReplication(path, replication);
    }

    @Override
    public void concat(Path targetPath, Path[] sourcePaths)
            throws IOException
    {
        dataTier.concat(targetPath, sourcePaths);
    }

    @Override
    public boolean rename(Path source, Path destination)
            throws IOException
    {
        return dataTier.rename(source, destination);
    }

    @Override
    public boolean truncate(Path path, long length)
            throws IOException
    {
        return dataTier.truncate(path, length);
    }

    @Override
    public boolean delete(Path path, boolean recursive)
            throws IOException
    {
        return dataTier.delete(path, recursive);
    }

    @Override
    public ContentSummary getContentSummary(Path path)
            throws IOException
    {
        return dataTier.getContentSummary(path);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path path)
            throws IOException
    {
        return dataTier.listLocatedStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        return dataTier.listStatus(path);
    }

    @Override
    public RemoteIterator<FileStatus> listStatusIterator(Path path)
            throws IOException
    {
        return dataTier.listStatusIterator(path);
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission)
            throws IOException
    {
        return dataTier.mkdirs(path, permission);
    }

    @Override
    public void close()
            throws IOException
    {
        dataTier.close();
    }

    @Override
    public FsStatus getStatus(Path path)
            throws IOException
    {
        return dataTier.getStatus(path);
    }

    @Override
    public RemoteIterator<Path> listCorruptFileBlocks(Path path)
            throws IOException
    {
        return dataTier.listCorruptFileBlocks(path);
    }

    @Override
    public FsServerDefaults getServerDefaults()
            throws IOException
    {
        return dataTier.getServerDefaults();
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        return dataTier.getFileStatus(path);
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent)
            throws IOException
    {
        dataTier.createSymlink(target, link, createParent);
    }

    @Override
    public boolean supportsSymlinks()
    {
        return dataTier.supportsSymlinks();
    }

    @Override
    public FileStatus getFileLinkStatus(Path path)
            throws IOException
    {
        return dataTier.getFileLinkStatus(path);
    }

    @Override
    public Path getLinkTarget(Path path)
            throws IOException
    {
        return dataTier.getLinkTarget(path);
    }

    @Override
    public FileChecksum getFileChecksum(Path path)
            throws IOException
    {
        return dataTier.getFileChecksum(path);
    }

    @Override
    public FileChecksum getFileChecksum(Path path, long length)
            throws IOException
    {
        return dataTier.getFileChecksum(path, length);
    }

    @Override
    public void setPermission(Path path, FsPermission permission)
            throws IOException
    {
        dataTier.setPermission(path, permission);
    }

    @Override
    public void setOwner(Path path, String user, String group)
            throws IOException
    {
        dataTier.setOwner(path, user, group);
    }

    @Override
    public void setTimes(Path path, long modificationTime, long accessTime)
            throws IOException
    {
        dataTier.setTimes(path, modificationTime, accessTime);
    }

    @Override
    public Token<?> getDelegationToken(String renewer)
            throws IOException
    {
        return dataTier.getDelegationToken(renewer);
    }

    @Override
    public String getCanonicalServiceName()
    {
        return dataTier.getCanonicalServiceName();
    }

    @Override
    public Path createSnapshot(Path path, String snapshotName)
            throws IOException
    {
        return dataTier.createSnapshot(path, snapshotName);
    }

    @Override
    public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName)
            throws IOException
    {
        dataTier.renameSnapshot(path, snapshotOldName, snapshotNewName);
    }

    @Override
    public void deleteSnapshot(Path snapshotDirectory, String snapshotName)
            throws IOException
    {
        dataTier.deleteSnapshot(snapshotDirectory, snapshotName);
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclEntries)
            throws IOException
    {
        dataTier.modifyAclEntries(path, aclEntries);
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclEntries)
            throws IOException
    {
        dataTier.removeAclEntries(path, aclEntries);
    }

    @Override
    public void removeDefaultAcl(Path path)
            throws IOException
    {
        dataTier.removeDefaultAcl(path);
    }

    @Override
    public void removeAcl(Path path)
            throws IOException
    {
        dataTier.removeAcl(path);
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclEntries)
            throws IOException
    {
        dataTier.setAcl(path, aclEntries);
    }

    @Override
    public AclStatus getAclStatus(Path path)
            throws IOException
    {
        return dataTier.getAclStatus(path);
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> xAttrSetFlags)
            throws IOException
    {
        dataTier.setXAttr(path, name, value, xAttrSetFlags);
    }

    @Override
    public byte[] getXAttr(Path path, String name)
            throws IOException
    {
        return dataTier.getXAttr(path, name);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path)
            throws IOException
    {
        return dataTier.getXAttrs(path);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path, List<String> names)
            throws IOException
    {
        return dataTier.getXAttrs(path, names);
    }

    @Override
    public List<String> listXAttrs(Path path)
            throws IOException
    {
        return dataTier.listXAttrs(path);
    }

    @Override
    public void removeXAttr(Path path, String name)
            throws IOException
    {
        dataTier.removeXAttr(path, name);
    }

    @Override
    public void access(Path path, FsAction mode)
            throws IOException
    {
        dataTier.access(path, mode);
    }

    @Override
    public Token<?>[] addDelegationTokens(String renewer, Credentials credentials)
            throws IOException
    {
        return dataTier.addDelegationTokens(renewer, credentials);
    }

    @Override
    public Path makeQualified(Path path)
    {
        return dataTier.makeQualified(path);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listDirectory(Path path)
            throws IOException
    {
        return dataTier.listDirectory(path);
    }

    @Override
    public RemoteIterator<HiveFileInfo> listFiles(Path path)
            throws IOException
    {
        return dataTier.listFiles(path);
    }

    @Override
    public ListenableFuture<Void> renameFileAsync(Path source, Path destination)
            throws IOException
    {
        return dataTier.renameFileAsync(source, destination);
    }
}
