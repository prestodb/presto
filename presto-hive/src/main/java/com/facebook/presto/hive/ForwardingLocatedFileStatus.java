package com.facebook.presto.hive;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class ForwardingLocatedFileStatus
        extends LocatedFileStatus
{
    private final LocatedFileStatus locatedFileStatus;

    public ForwardingLocatedFileStatus(LocatedFileStatus locatedFileStatus)
            throws IOException
    {
        super(checkNotNull(locatedFileStatus, "locatedFileStatus is null"), null);
        this.locatedFileStatus = locatedFileStatus;
    }

    @Override
    public BlockLocation[] getBlockLocations()
    {
        return locatedFileStatus.getBlockLocations();
    }

    @Override
    public int compareTo(Object o)
    {
        return locatedFileStatus.compareTo(o);
    }

    @Override
    public boolean equals(Object o)
    {
        return locatedFileStatus.equals(o);
    }

    @Override
    public int hashCode()
    {
        return locatedFileStatus.hashCode();
    }

    @Override
    public long getLen()
    {
        return locatedFileStatus.getLen();
    }

    @Override
    public long getChildrenCount()
    {
        return locatedFileStatus.getChildrenCount();
    }

    @Override
    public boolean isDir()
    {
        return locatedFileStatus.isDir();
    }

    @Override
    public long getBlockSize()
    {
        return locatedFileStatus.getBlockSize();
    }

    @Override
    public short getReplication()
    {
        return locatedFileStatus.getReplication();
    }

    @Override
    public long getModificationTime()
    {
        return locatedFileStatus.getModificationTime();
    }

    @Override
    public long getAccessTime()
    {
        return locatedFileStatus.getAccessTime();
    }

    @Override
    public FsPermission getPermission()
    {
        return locatedFileStatus.getPermission();
    }

    @Override
    public String getOwner()
    {
        return locatedFileStatus.getOwner();
    }

    @Override
    public String getGroup()
    {
        return locatedFileStatus.getGroup();
    }

    @Override
    public Path getPath()
    {
        return locatedFileStatus.getPath();
    }

    @Override
    public void makeQualified(FileSystem fs)
    {
        locatedFileStatus.makeQualified(fs);
    }

    @Override
    public void write(DataOutput out)
            throws IOException
    {
        locatedFileStatus.write(out);
    }

    @Override
    public void readFields(DataInput in)
            throws IOException
    {
        locatedFileStatus.readFields(in);
    }

    @Override
    public boolean compareFull(Object o, boolean closedFile)
    {
        return locatedFileStatus.compareFull(o, closedFile);
    }

    @Override
    public String toString()
    {
        return locatedFileStatus.toString();
    }
}
