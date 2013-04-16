package com.facebook.presto.hive;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.OpenFileInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

public abstract class ForwardingFileSystem
        extends FileSystem
{
    private final FileSystem fileSystem;

    public ForwardingFileSystem(FileSystem fileSystem)
    {
        this.fileSystem = Preconditions.checkNotNull(fileSystem, "fileSystem is null");
    }

    @Override
    public void initialize(URI name, Configuration conf)
            throws IOException
    {
        fileSystem.initialize(name, conf);
    }

    @Override
    public URI getUri()
    {
        return fileSystem.getUri();
    }

    @Override
    public String getName()
    {
        return fileSystem.getName();
    }

    @Override
    public Path makeQualified(Path path)
    {
        return fileSystem.makeQualified(path);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException
    {
        return fileSystem.getFileBlockLocations(file, start, len);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize)
            throws IOException
    {
        return fileSystem.open(f, bufferSize);
    }

    @Override
    public FSDataInputStream open(Path f)
            throws IOException
    {
        return fileSystem.open(f);
    }

    @Override
    public FSDataOutputStream create(Path f)
            throws IOException
    {
        return fileSystem.create(f);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite)
            throws IOException
    {
        return fileSystem.create(f, overwrite);
    }

    @Override
    public FSDataOutputStream create(Path f, Progressable progress)
            throws IOException
    {
        return fileSystem.create(f, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, short replication)
            throws IOException
    {
        return fileSystem.create(f, replication);
    }

    @Override
    public FSDataOutputStream create(Path f, short replication, Progressable progress)
            throws IOException
    {
        return fileSystem.create(f, replication, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize)
            throws IOException
    {
        return fileSystem.create(f, overwrite, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress)
            throws IOException
    {
        return fileSystem.create(f, overwrite, bufferSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)
            throws IOException
    {
        return fileSystem.create(f, overwrite, bufferSize, replication, blockSize);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        return fileSystem.create(f, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        return fileSystem.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, int bytesPerChecksum, Progressable progress)
            throws IOException
    {
        return fileSystem.create(f, permission, overwrite, bufferSize, replication, blockSize, bytesPerChecksum, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, int bytesPerChecksum, Progressable progress, boolean forceSync)
            throws IOException
    {
        return fileSystem.create(f, permission, overwrite, bufferSize, replication, blockSize, bytesPerChecksum, progress, forceSync);
    }

    @Override
    @Deprecated
    public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        return fileSystem.createNonRecursive(f, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    @Deprecated
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        return fileSystem.createNonRecursive(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    @Deprecated
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress, boolean forceSync, boolean doParallelWrites)
            throws IOException
    {
        return fileSystem.createNonRecursive(f, permission, overwrite, bufferSize, replication, blockSize, progress, forceSync, doParallelWrites);
    }

    @Override
    public boolean createNewFile(Path f)
            throws IOException
    {
        return fileSystem.createNewFile(f);
    }

    @Override
    public FSDataOutputStream append(Path f)
            throws IOException
    {
        return fileSystem.append(f);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize)
            throws IOException
    {
        return fileSystem.append(f, bufferSize);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
            throws IOException
    {
        return fileSystem.append(f, bufferSize, progress);
    }

    @Override
    @Deprecated
    public short getReplication(Path src)
            throws IOException
    {
        return fileSystem.getReplication(src);
    }

    @Override
    public boolean setReplication(Path src, short replication)
            throws IOException
    {
        return fileSystem.setReplication(src, replication);
    }

    @Override
    public boolean hardLink(Path src, Path dst)
            throws IOException
    {
        return fileSystem.hardLink(src, dst);
    }

    @Override
    public String[] getHardLinkedFiles(Path src)
            throws IOException
    {
        return fileSystem.getHardLinkedFiles(src);
    }

    @Override
    public boolean rename(Path src, Path dst)
            throws IOException
    {
        return fileSystem.rename(src, dst);
    }

    @Override
    @Deprecated
    public boolean delete(Path f)
            throws IOException
    {
        return fileSystem.delete(f);
    }

    @Override
    public boolean delete(Path f, boolean recursive)
            throws IOException
    {
        return fileSystem.delete(f, recursive);
    }

    @Override
    public boolean delete(Path f, boolean recursive, boolean skipTrash)
            throws IOException
    {
        return fileSystem.delete(f, recursive, skipTrash);
    }

    @Override
    public boolean undelete(Path f, String userName)
            throws IOException
    {
        return fileSystem.undelete(f, userName);
    }

    @Override
    public boolean deleteOnExit(Path f)
            throws IOException
    {
        return fileSystem.deleteOnExit(f);
    }

    @Override
    public boolean exists(Path f)
            throws IOException
    {
        return fileSystem.exists(f);
    }

    @Override
    @Deprecated
    public boolean isDirectory(Path f)
            throws IOException
    {
        return fileSystem.isDirectory(f);
    }

    @Override
    public boolean isFile(Path f)
            throws IOException
    {
        return fileSystem.isFile(f);
    }

    @Override
    @Deprecated
    public long getLength(Path f)
            throws IOException
    {
        return fileSystem.getLength(f);
    }

    @Override
    public ContentSummary getContentSummary(Path f)
            throws IOException
    {
        return fileSystem.getContentSummary(f);
    }

    @Override
    public FileStatus[] listStatus(Path f)
            throws IOException
    {
        return fileSystem.listStatus(f);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
            throws FileNotFoundException, IOException
    {
        return fileSystem.listLocatedStatus(f);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f, PathFilter filter)
            throws FileNotFoundException, IOException
    {
        return fileSystem.listLocatedStatus(f, filter);
    }

    @Override
    public RemoteIterator<Path> listCorruptFileBlocks(Path path)
            throws IOException
    {
        return fileSystem.listCorruptFileBlocks(path);
    }

    @Override
    public FileStatus[] listStatus(Path f, PathFilter filter)
            throws IOException
    {
        return fileSystem.listStatus(f, filter);
    }

    @Override
    public FileStatus[] listStatus(Path[] files)
            throws IOException
    {
        return fileSystem.listStatus(files);
    }

    @Override
    public FileStatus[] listStatus(Path[] files, PathFilter filter)
            throws IOException
    {
        return fileSystem.listStatus(files, filter);
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern)
            throws IOException
    {
        return fileSystem.globStatus(pathPattern);
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
            throws IOException
    {
        return fileSystem.globStatus(pathPattern, filter);
    }

    @Override
    public Path getHomeDirectory()
    {
        return fileSystem.getHomeDirectory();
    }

    @Override
    public Path getHomeDirectory(String userName)
    {
        return fileSystem.getHomeDirectory(userName);
    }

    @Override
    public void setWorkingDirectory(Path new_dir)
    {
        fileSystem.setWorkingDirectory(new_dir);
    }

    @Override
    public Path getWorkingDirectory()
    {
        return fileSystem.getWorkingDirectory();
    }

    @Override
    public boolean mkdirs(Path f)
            throws IOException
    {
        return fileSystem.mkdirs(f);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
            throws IOException
    {
        return fileSystem.mkdirs(f, permission);
    }

    @Override
    @Deprecated
    public void copyFromLocalFile(Path src, Path dst)
            throws IOException
    {
        fileSystem.copyFromLocalFile(src, dst);
    }

    @Override
    public void moveFromLocalFile(Path[] srcs, Path dst)
            throws IOException
    {
        fileSystem.moveFromLocalFile(srcs, dst);
    }

    @Override
    public void moveFromLocalFile(Path src, Path dst)
            throws IOException
    {
        fileSystem.moveFromLocalFile(src, dst);
    }

    @Override
    @Deprecated
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException
    {
        fileSystem.copyFromLocalFile(delSrc, src, dst);
    }

    @Override
    @Deprecated
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst)
            throws IOException
    {
        fileSystem.copyFromLocalFile(delSrc, overwrite, srcs, dst);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, boolean validate, Path[] srcs, Path dst)
            throws IOException
    {
        fileSystem.copyFromLocalFile(delSrc, overwrite, validate, srcs, dst);
    }

    @Override
    @Deprecated
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)
            throws IOException
    {
        fileSystem.copyFromLocalFile(delSrc, overwrite, src, dst);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, boolean validate, Path src, Path dst)
            throws IOException
    {
        fileSystem.copyFromLocalFile(delSrc, overwrite, validate, src, dst);
    }

    @Override
    @Deprecated
    public void copyToLocalFile(Path src, Path dst)
            throws IOException
    {
        fileSystem.copyToLocalFile(src, dst);
    }

    @Override
    public void moveToLocalFile(Path src, Path dst)
            throws IOException
    {
        fileSystem.moveToLocalFile(src, dst);
    }

    @Override
    @Deprecated
    public void copyToLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException
    {
        fileSystem.copyToLocalFile(delSrc, src, dst);
    }

    @Override
    public void copyToLocalFile(boolean delSrc, boolean validate, Path src, Path dst)
            throws IOException
    {
        fileSystem.copyToLocalFile(delSrc, validate, src, dst);
    }

    @Override
    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
            throws IOException
    {
        return fileSystem.startLocalOutput(fsOutputFile, tmpLocalFile);
    }

    @Override
    public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
            throws IOException
    {
        fileSystem.completeLocalOutput(fsOutputFile, tmpLocalFile);
    }

    @Override
    public void close()
            throws IOException
    {
        fileSystem.close();
    }

    @Override
    public OpenFileInfo[] iterativeGetOpenFiles(Path prefix, int millis, String start)
            throws IOException
    {
        return fileSystem.iterativeGetOpenFiles(prefix, millis, start);
    }

    @Override
    public long getUsed()
            throws IOException
    {
        return fileSystem.getUsed();
    }

    @Override
    @Deprecated
    public long getBlockSize(Path f)
            throws IOException
    {
        return fileSystem.getBlockSize(f);
    }

    @Override
    public long getDefaultBlockSize()
    {
        return fileSystem.getDefaultBlockSize();
    }

    @Override
    public short getDefaultReplication()
    {
        return fileSystem.getDefaultReplication();
    }

    @Override
    public FileStatus getFileStatus(Path f)
            throws IOException
    {
        return fileSystem.getFileStatus(f);
    }

    @Override
    public FileChecksum getFileChecksum(Path f)
            throws IOException
    {
        return fileSystem.getFileChecksum(f);
    }

    @Override
    public int getFileCrc(Path f)
            throws IOException
    {
        return fileSystem.getFileCrc(f);
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum)
    {
        fileSystem.setVerifyChecksum(verifyChecksum);
    }

    @Override
    public void clearOsBuffer(boolean clearOsBuffer)
    {
        fileSystem.clearOsBuffer(clearOsBuffer);
    }

    @Override
    public void setPermission(Path p, FsPermission permission)
            throws IOException
    {
        fileSystem.setPermission(p, permission);
    }

    @Override
    public void setOwner(Path p, String username, String groupname)
            throws IOException
    {
        fileSystem.setOwner(p, username, groupname);
    }

    @Override
    public void setTimes(Path p, long mtime, long atime)
            throws IOException
    {
        fileSystem.setTimes(p, mtime, atime);
    }
}
