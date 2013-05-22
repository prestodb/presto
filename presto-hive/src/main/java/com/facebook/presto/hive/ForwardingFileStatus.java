package com.facebook.presto.hive;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class ForwardingFileStatus
        extends FileStatus
{
    private final FileStatus fileStatus;

    public ForwardingFileStatus(FileStatus fileStatus)
    {
        this.fileStatus = Preconditions.checkNotNull(fileStatus, "fileStatus is null");
    }

    @Override
    public long getLen()
    {
        return fileStatus.getLen();
    }

    @Override
    public boolean isDir()
    {
        return fileStatus.isDir();
    }

    @Override
    public long getBlockSize()
    {
        return fileStatus.getBlockSize();
    }

    @Override
    public short getReplication()
    {
        return fileStatus.getReplication();
    }

    @Override
    public long getModificationTime()
    {
        return fileStatus.getModificationTime();
    }

    @Override
    public long getAccessTime()
    {
        return fileStatus.getAccessTime();
    }

    @Override
    public FsPermission getPermission()
    {
        return fileStatus.getPermission();
    }

    @Override
    public String getOwner()
    {
        return fileStatus.getOwner();
    }

    @Override
    public String getGroup()
    {
        return fileStatus.getGroup();
    }

    @Override
    public Path getPath()
    {
        return fileStatus.getPath();
    }

    @Override
    public void write(DataOutput out)
            throws IOException
    {
        fileStatus.write(out);
    }

    @Override
    public void readFields(DataInput in)
            throws IOException
    {
        fileStatus.readFields(in);
    }

    @Override
    public int compareTo(Object o)
    {
        return fileStatus.compareTo(o);
    }

    @Override
    public boolean equals(Object o)
    {
        return fileStatus.equals(o);
    }

    @Override
    public int hashCode()
    {
        return fileStatus.hashCode();
    }

    @Override
    public String toString()
    {
        return fileStatus.toString();
    }
}
