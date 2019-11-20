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

import com.facebook.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class CachingFileSystem
        extends FileSystem
{
    private static final Logger log = Logger.get(CachingFileSystem.class);
    private final URI uri;
    private final CacheManager cacheManager;
    private final FileSystem dataTier;
    private final boolean cacheValidationEnabled;

    public CachingFileSystem(
            URI uri,
            Configuration configuration,
            CacheManager cacheManager,
            FileSystem dataTier,
            boolean cacheValidationEnabled)
    {
        requireNonNull(configuration, "configuration is null");
        this.uri = requireNonNull(uri, "uri is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.dataTier = requireNonNull(dataTier, "dataTier is null");
        this.cacheValidationEnabled = cacheValidationEnabled;

        setConf(configuration);

        //noinspection AssignmentToSuperclassField
        statistics = getStatistics(this.uri.getScheme(), getClass());
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
        this.dataTier.initialize(uri, configuration);
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
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException
    {
        return dataTier.getFileBlockLocations(file, start, len);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path p, final long start, final long len)
            throws IOException
    {
        return dataTier.getFileBlockLocations(p, start, len);
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
        return new CachingInputStream(dataTier.open(path, bufferSize), cacheManager, path, cacheValidationEnabled);
    }

    //TODO: use CachingOutputStream
    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
            throws IOException
    {
        return dataTier.append(path, bufferSize, progress);
    }

    @Override
    public FSDataOutputStream create(
            Path f,
            FsPermission permission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress)
            throws IOException
    {
        return dataTier.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(
            final Path f,
            final FsPermission permission,
            final EnumSet<CreateFlag> cflags,
            final int bufferSize,
            final short replication,
            final long blockSize,
            final Progressable progress,
            final Options.ChecksumOpt checksumOpt)
            throws IOException
    {
        return dataTier.create(f, permission, cflags, bufferSize, replication, blockSize, progress, checksumOpt);
    }

    @Override
    public boolean setReplication(Path src, final short replication)
            throws IOException
    {
        return dataTier.setReplication(src, replication);
    }

    @Override
    public void concat(Path trg, Path[] psrcs)
            throws IOException
    {
        dataTier.concat(trg, psrcs);
    }

    @Override
    public boolean rename(Path source, Path destination)
            throws IOException
    {
        return dataTier.rename(source, destination);
    }

    @Override
    public boolean truncate(Path f, final long newLength)
            throws IOException
    {
        return dataTier.truncate(f, newLength);
    }

    @Override
    public boolean delete(Path path, boolean recursive)
            throws IOException
    {
        return dataTier.delete(path, recursive);
    }

    @Override
    public ContentSummary getContentSummary(Path f)
            throws IOException
    {
        return dataTier.getContentSummary(f);
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        return dataTier.listStatus(path);
    }

    @Override
    public RemoteIterator<FileStatus> listStatusIterator(final Path p)
            throws IOException
    {
        return dataTier.listStatusIterator(p);
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
    public String toString()
    {
        return dataTier.toString();
    }

    @Override
    public FsStatus getStatus(Path p)
            throws IOException
    {
        return dataTier.getStatus(p);
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
    public void createSymlink(final Path target, final Path link, final boolean createParent)
            throws AccessControlException,
            FileAlreadyExistsException, FileNotFoundException,
            ParentNotDirectoryException, UnsupportedFileSystemException,
            IOException
    {
        dataTier.createSymlink(target, link, createParent);
    }

    @Override
    public boolean supportsSymlinks()
    {
        return dataTier.supportsSymlinks();
    }

    @Override
    public FileStatus getFileLinkStatus(final Path f)
            throws AccessControlException, FileNotFoundException,
            UnsupportedFileSystemException, IOException
    {
        return dataTier.getFileLinkStatus(f);
    }

    @Override
    public Path getLinkTarget(final Path f)
            throws AccessControlException,
            FileNotFoundException, UnsupportedFileSystemException, IOException
    {
        return dataTier.getLinkTarget(f);
    }

    @Override
    public FileChecksum getFileChecksum(Path f)
            throws IOException
    {
        return dataTier.getFileChecksum(f);
    }

    @Override
    public FileChecksum getFileChecksum(Path f, final long length)
            throws IOException
    {
        return dataTier.getFileChecksum(f, length);
    }

    @Override
    public void setPermission(Path p, final FsPermission permission)
            throws IOException
    {
        dataTier.setPermission(p, permission);
    }

    @Override
    public void setOwner(Path p, final String username, final String groupname)
            throws IOException
    {
        dataTier.setOwner(p, username, groupname);
    }

    @Override
    public void setTimes(Path p, final long mtime, final long atime)
            throws IOException
    {
        dataTier.setTimes(p, mtime, atime);
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
    public Path createSnapshot(final Path path, final String snapshotName)
            throws IOException
    {
        return dataTier.createSnapshot(path, snapshotName);
    }

    @Override
    public void renameSnapshot(final Path path, final String snapshotOldName, final String snapshotNewName)
            throws IOException
    {
        dataTier.renameSnapshot(path, snapshotOldName, snapshotNewName);
    }

    @Override
    public void deleteSnapshot(final Path snapshotDir, final String snapshotName)
            throws IOException
    {
        dataTier.deleteSnapshot(snapshotDir, snapshotName);
    }

    @Override
    public void modifyAclEntries(Path path, final List<AclEntry> aclSpec)
            throws IOException
    {
        dataTier.modifyAclEntries(path, aclSpec);
    }

    @Override
    public void removeAclEntries(Path path, final List<AclEntry> aclSpec)
            throws IOException
    {
        dataTier.removeAclEntries(path, aclSpec);
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
    public void setAcl(Path path, final List<AclEntry> aclSpec)
            throws IOException
    {
        dataTier.setAcl(path, aclSpec);
    }

    @Override
    public AclStatus getAclStatus(Path path)
            throws IOException
    {
        return dataTier.getAclStatus(path);
    }

    @Override
    public void setXAttr(Path path, final String name, final byte[] value, final EnumSet<XAttrSetFlag> flag)
            throws IOException
    {
        dataTier.setXAttr(path, name, value, flag);
    }

    @Override
    public byte[] getXAttr(Path path, final String name)
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
    public Map<String, byte[]> getXAttrs(Path path, final List<String> names)
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
    public void removeXAttr(Path path, final String name)
            throws IOException
    {
        dataTier.removeXAttr(path, name);
    }

    @Override
    public void access(Path path, final FsAction mode)
            throws IOException
    {
        dataTier.access(path, mode);
    }

    @Override
    public Token<?>[] addDelegationTokens(final String renewer, Credentials credentials)
            throws IOException
    {
        return dataTier.addDelegationTokens(renewer, credentials);
    }

    @Override
    public Path makeQualified(Path path)
    {
        return dataTier.makeQualified(path);
    }

    public FileSystem getDataTier()
    {
        return dataTier;
    }

    public boolean isCacheValidationEnabled()
    {
        return cacheValidationEnabled;
    }
}
