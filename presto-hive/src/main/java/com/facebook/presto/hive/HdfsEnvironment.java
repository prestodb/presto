package com.facebook.presto.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HdfsEnvironment
{
    private final HdfsConfiguration hdfsConfiguration;
    private final FileSystemWrapper fileSystemWrapper;

    @Inject
    public HdfsEnvironment(HdfsConfiguration hdfsConfiguration, FileSystemWrapper fileSystemWrapper)
    {
        this.hdfsConfiguration = checkNotNull(hdfsConfiguration, "hdfsConfiguration is null");
        this.fileSystemWrapper = checkNotNull(fileSystemWrapper, "fileSystemWrapper is null");
    }

    public Configuration getConfiguration(Path path)
    {
        String host = path.toUri().getHost();
        checkArgument(host != null, "path host is null: %s", path);
        return hdfsConfiguration.getConfiguration(host);
    }

    public FileSystemWrapper getFileSystemWrapper()
    {
        return fileSystemWrapper;
    }
}
