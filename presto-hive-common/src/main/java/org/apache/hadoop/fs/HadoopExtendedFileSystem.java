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
package org.apache.hadoop.fs;

import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.Futures.immediateFuture;

public class HadoopExtendedFileSystem
        extends ExtendedFileSystem
{
    private FileSystem fs;
    private String swapScheme;

    public HadoopExtendedFileSystem(FileSystem fs)
    {
        this.fs = fs;
        this.statistics = fs.statistics;
    }

    @Override
    public void initialize(URI name, Configuration conf)
            throws IOException
    {
        super.initialize(name, conf);
        // this is less than ideal, but existing filesystems sometimes neglect
        // to initialize the embedded filesystem
        if (fs.getConf() == null) {
            fs.initialize(name, conf);
        }
        String scheme = name.getScheme();
        if (!scheme.equals(fs.getUri().getScheme())) {
            swapScheme = scheme;
        }
    }

    public FileSystem getRawFileSystem()
    {
        return fs;
    }

    @Override
    public URI getUri()
    {
        return fs.getUri();
    }

    @Override
    protected URI getCanonicalUri()
    {
        return fs.getCanonicalUri();
    }

    @Override
    protected URI canonicalizeUri(URI uri)
    {
        return fs.canonicalizeUri(uri);
    }

    @Override
    public Path makeQualified(Path path)
    {
        Path fqPath = fs.makeQualified(path);
        // swap in our scheme if the filtered fs is using a different scheme
        if (swapScheme != null) {
            try {
                // NOTE: should deal with authority, but too much other stuff is broken
                fqPath = new Path(new URI(swapScheme, fqPath.toUri().getSchemeSpecificPart(), null));
            }
            catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return fqPath;
    }

    @Override
    protected void checkPath(Path path)
    {
        fs.checkPath(path);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
            long len)
            throws IOException
    {
        return fs.getFileBlockLocations(file, start, len);
    }

    @Override
    public Path resolvePath(final Path p)
            throws IOException
    {
        return fs.resolvePath(p);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize)
            throws IOException
    {
        return fs.open(f, bufferSize);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
            Progressable progress)
            throws IOException
    {
        return fs.append(f, bufferSize, progress);
    }

    @Override
    public void concat(Path f, Path[] psrcs)
            throws IOException
    {
        fs.concat(f, psrcs);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
            boolean overwrite, int bufferSize, short replication, long blockSize,
            Progressable progress)
            throws IOException
    {
        return fs.create(f, permission,
                overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f,
            FsPermission permission,
            EnumSet<CreateFlag> flags,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress,
            Options.ChecksumOpt checksumOpt)
            throws IOException
    {
        return fs.create(f, permission,
                flags, bufferSize, replication, blockSize, progress, checksumOpt);
    }

    @Override
    @Deprecated
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
            EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
            Progressable progress)
            throws IOException
    {
        return fs.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress);
    }

    @Override
    public boolean setReplication(Path src, short replication)
            throws IOException
    {
        return fs.setReplication(src, replication);
    }

    @Override
    public boolean rename(Path src, Path dst)
            throws IOException
    {
        return fs.rename(src, dst);
    }

    @Override
    public ListenableFuture<Void> renameFileAsync(Path src, Path dst)
            throws IOException
    {
        fs.rename(src, dst);
        return immediateFuture(null);
    }

    @Override
    protected void rename(Path src, Path dst, Options.Rename... options)
            throws IOException
    {
        fs.rename(src, dst, options);
    }

    @Override
    public boolean truncate(Path f, final long newLength)
            throws IOException
    {
        return fs.truncate(f, newLength);
    }

    @Override
    public boolean delete(Path f, boolean recursive)
            throws IOException
    {
        return fs.delete(f, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path f)
            throws IOException
    {
        return fs.listStatus(f);
    }

    @Override
    public RemoteIterator<Path> listCorruptFileBlocks(Path path)
            throws IOException
    {
        return fs.listCorruptFileBlocks(path);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
            throws IOException
    {
        return fs.listLocatedStatus(f);
    }

    @Override
    public RemoteIterator<FileStatus> listStatusIterator(Path f)
            throws IOException
    {
        return fs.listStatusIterator(f);
    }

    @Override
    public Path getHomeDirectory()
    {
        return fs.getHomeDirectory();
    }

    @Override
    public void setWorkingDirectory(Path newDir)
    {
        fs.setWorkingDirectory(newDir);
    }

    @Override
    public Path getWorkingDirectory()
    {
        return fs.getWorkingDirectory();
    }

    @Override
    protected Path getInitialWorkingDirectory()
    {
        return fs.getInitialWorkingDirectory();
    }

    @Override
    public FsStatus getStatus(Path p)
            throws IOException
    {
        return fs.getStatus(p);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
            throws IOException
    {
        return fs.mkdirs(f, permission);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException
    {
        fs.copyFromLocalFile(delSrc, src, dst);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite,
            Path[] srcs, Path dst)
            throws IOException
    {
        fs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite,
            Path src, Path dst)
            throws IOException
    {
        fs.copyFromLocalFile(delSrc, overwrite, src, dst);
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException
    {
        fs.copyToLocalFile(delSrc, src, dst);
    }

    @Override
    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
            throws IOException
    {
        return fs.startLocalOutput(fsOutputFile, tmpLocalFile);
    }

    @Override
    public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
            throws IOException
    {
        fs.completeLocalOutput(fsOutputFile, tmpLocalFile);
    }

    @Override
    public long getUsed()
            throws IOException
    {
        return fs.getUsed();
    }

    @Override
    public long getDefaultBlockSize()
    {
        return fs.getDefaultBlockSize();
    }

    @Override
    public short getDefaultReplication()
    {
        return fs.getDefaultReplication();
    }

    @Override
    public FsServerDefaults getServerDefaults()
            throws IOException
    {
        return fs.getServerDefaults();
    }

    // path variants delegate to underlying filesystem
    @Override
    public long getDefaultBlockSize(Path f)
    {
        return fs.getDefaultBlockSize(f);
    }

    @Override
    public short getDefaultReplication(Path f)
    {
        return fs.getDefaultReplication(f);
    }

    @Override
    public FsServerDefaults getServerDefaults(Path f)
            throws IOException
    {
        return fs.getServerDefaults(f);
    }

    @Override
    public FileStatus getFileStatus(Path f)
            throws IOException
    {
        return fs.getFileStatus(f);
    }

    @Override
    public void access(Path path, FsAction mode)
            throws IOException
    {
        fs.access(path, mode);
    }

    public void createSymlink(final Path target, final Path link,
            final boolean createParent)
            throws IOException
    {
        fs.createSymlink(target, link, createParent);
    }

    public FileStatus getFileLinkStatus(final Path f)
            throws IOException
    {
        return fs.getFileLinkStatus(f);
    }

    public boolean supportsSymlinks()
    {
        return fs.supportsSymlinks();
    }

    public Path getLinkTarget(Path f)
            throws IOException
    {
        return fs.getLinkTarget(f);
    }

    protected Path resolveLink(Path f)
            throws IOException
    {
        return fs.resolveLink(f);
    }

    @Override
    public FileChecksum getFileChecksum(Path f)
            throws IOException
    {
        return fs.getFileChecksum(f);
    }

    @Override
    public FileChecksum getFileChecksum(Path f, long length)
            throws IOException
    {
        return fs.getFileChecksum(f, length);
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum)
    {
        fs.setVerifyChecksum(verifyChecksum);
    }

    @Override
    public void setWriteChecksum(boolean writeChecksum)
    {
        fs.setWriteChecksum(writeChecksum);
    }

    @Override
    public Configuration getConf()
    {
        return fs.getConf();
    }

    @Override
    public void close()
            throws IOException
    {
        super.close();
        fs.close();
    }

    @Override
    public void setOwner(Path path, String user, String group)
            throws IOException
    {
        fs.setOwner(path, user, group);
    }

    @Override
    public void setTimes(Path p, long mtime, long atime)
            throws IOException
    {
        fs.setTimes(p, mtime, atime);
    }

    @Override
    public void setPermission(Path p, FsPermission permission)
            throws IOException
    {
        fs.setPermission(p, permission);
    }

    @Override
    protected FSDataOutputStream primitiveCreate(Path f,
            FsPermission absolutePermission, EnumSet<CreateFlag> flag,
            int bufferSize, short replication, long blockSize,
            Progressable progress, Options.ChecksumOpt checksumOpt)
            throws IOException
    {
        return fs.primitiveCreate(f, absolutePermission, flag,
                bufferSize, replication, blockSize, progress, checksumOpt);
    }

    @Override
    @SuppressWarnings("deprecation")
    protected boolean primitiveMkdir(Path f, FsPermission abdolutePermission)
            throws IOException
    {
        return fs.primitiveMkdir(f, abdolutePermission);
    }

    @Override // FileSystem
    public FileSystem[] getChildFileSystems()
    {
        return new FileSystem[] {fs};
    }

    @Override // FileSystem
    public Path createSnapshot(Path path, String snapshotName)
            throws IOException
    {
        return fs.createSnapshot(path, snapshotName);
    }

    @Override // FileSystem
    public void renameSnapshot(Path path, String snapshotOldName,
            String snapshotNewName)
            throws IOException
    {
        fs.renameSnapshot(path, snapshotOldName, snapshotNewName);
    }

    @Override // FileSystem
    public void deleteSnapshot(Path path, String snapshotName)
            throws IOException
    {
        fs.deleteSnapshot(path, snapshotName);
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
            throws IOException
    {
        fs.modifyAclEntries(path, aclSpec);
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec)
            throws IOException
    {
        fs.removeAclEntries(path, aclSpec);
    }

    @Override
    public void removeDefaultAcl(Path path)
            throws IOException
    {
        fs.removeDefaultAcl(path);
    }

    @Override
    public void removeAcl(Path path)
            throws IOException
    {
        fs.removeAcl(path);
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec)
            throws IOException
    {
        fs.setAcl(path, aclSpec);
    }

    @Override
    public AclStatus getAclStatus(Path path)
            throws IOException
    {
        return fs.getAclStatus(path);
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value)
            throws IOException
    {
        fs.setXAttr(path, name, value);
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value,
            EnumSet<XAttrSetFlag> flag)
            throws IOException
    {
        fs.setXAttr(path, name, value, flag);
    }

    @Override
    public byte[] getXAttr(Path path, String name)
            throws IOException
    {
        return fs.getXAttr(path, name);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path)
            throws IOException
    {
        return fs.getXAttrs(path);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path, List<String> names)
            throws IOException
    {
        return fs.getXAttrs(path, names);
    }

    @Override
    public List<String> listXAttrs(Path path)
            throws IOException
    {
        return fs.listXAttrs(path);
    }

    @Override
    public void removeXAttr(Path path, String name)
            throws IOException
    {
        fs.removeXAttr(path, name);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listDirectory(Path path)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext)
            throws Exception
    {
        return fs.open(path);
    }
}
