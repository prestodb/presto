package com.facebook.presto.hive;

import com.google.common.base.Functions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import javax.inject.Inject;
import javax.inject.Provider;

import static com.google.common.base.Preconditions.checkNotNull;

public class FileSystemWrapperProvider
    implements Provider<FileSystemWrapper>
{
    private final FileSystemCache fileSystemCache;
    private final SlowDatanodeSwitcher slowDatanodeSwitcher;
    private final boolean slowDatanodeSwitcherEnabled;

    @Inject
    public FileSystemWrapperProvider(FileSystemCache fileSystemCache, SlowDatanodeSwitcher slowDatanodeSwitcher, HiveClientConfig config)
    {
        this.fileSystemCache = checkNotNull(fileSystemCache, "fileSystemCache is null");
        this.slowDatanodeSwitcher = checkNotNull(slowDatanodeSwitcher, "slowDatanodeSwitcher is null");
        slowDatanodeSwitcherEnabled = checkNotNull(config, "config is null").getSlowDatanodeSwitchingEnabled();
    }

    @Override
    public FileSystemWrapper get()
    {
        return new FileSystemWrapper(
                slowDatanodeSwitcherEnabled && SlowDatanodeSwitcher.isSupported() ? slowDatanodeSwitcher.createFileSystemWrapper() : Functions.<FileSystem>identity(),
                fileSystemCache.createPathWrapper(),
                Functions.<FileStatus>identity()
        );
    }
}
