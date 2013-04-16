package com.facebook.presto.hive;

import com.google.common.base.Functions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;

import javax.inject.Inject;
import javax.inject.Provider;

import static com.google.common.base.Preconditions.checkNotNull;

public class FileSystemWrapperProvider
    implements Provider<FileSystemWrapper>
{
    private final FileSystemCache fileSystemCache;

    @Inject
    public FileSystemWrapperProvider(FileSystemCache fileSystemCache)
    {
        this.fileSystemCache = checkNotNull(fileSystemCache, "fileSystemCache is null");
    }

    @Override
    public FileSystemWrapper get()
    {
        return new FileSystemWrapper(Functions.<FileSystem>identity(), fileSystemCache.createPathWrapper(), Functions.<FileStatus>identity(), Functions.<LocatedFileStatus>identity());
    }
}
