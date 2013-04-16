package com.facebook.presto.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class ForwardingPath
        extends Path
{
    private final Path path;

    public ForwardingPath(Path path)
    {
        super(checkNotNull(path, "path is null").toString());
        this.path = path;
    }

    @Override
    public URI toUri()
    {
        return path.toUri();
    }

    @Override
    public FileSystem getFileSystem(Configuration conf)
            throws IOException
    {
        return path.getFileSystem(conf);
    }

    @Override
    public boolean isAbsolute()
    {
        return path.isAbsolute();
    }

    @Override
    public String getName()
    {
        return path.getName();
    }

    @Override
    public Path getParent()
    {
        return path.getParent();
    }

    @Override
    public Path suffix(String suffix)
    {
        return path.suffix(suffix);
    }

    @Override
    public String toString()
    {
        return path.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        return path.equals(o);
    }

    @Override
    public int hashCode()
    {
        return path.hashCode();
    }

    @Override
    public int compareTo(Object o)
    {
        return path.compareTo(o);
    }

    @Override
    public int depth()
    {
        return path.depth();
    }

    @Override
    public Path makeQualified(FileSystem fs)
    {
        return path.makeQualified(fs);
    }
}
