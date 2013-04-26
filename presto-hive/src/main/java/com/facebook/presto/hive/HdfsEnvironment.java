package com.facebook.presto.hive;

import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

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

    public HdfsEnvironment()
    {
        this.hdfsConfiguration = new HdfsConfiguration();
        this.fileSystemWrapper = FileSystemWrapper.identity();
    }

    public Configuration getConfiguration()
    {
        return hdfsConfiguration.getConfiguration();
    }

    public FileSystemWrapper getFileSystemWrapper()
    {
        return fileSystemWrapper;
    }
}
