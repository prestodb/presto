package com.facebook.presto.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
                                                             
public class CachingPath
    extends Path
{
    private final FileSystemCache fileSystemCache;
    
    public CachingPath(String pathString, FileSystemCache fileSystemCache)
    {
        super(pathString);
        this.fileSystemCache = fileSystemCache;
    }

    @Override
    public FileSystem getFileSystem(Configuration conf)
            throws IOException
    {
        // Assumes that the provided configuration will be default and empty
        return fileSystemCache.getFileSystem(this);
    }
}
