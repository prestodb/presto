package com.facebook.presto.hive;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class FileSystemWrapperProvider
        implements Provider<FileSystemWrapper>
{
    private final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
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
        Function<FileSystem, FileSystem> fileSystemWrapper = createThreadContextClassLoaderWrapper();
        if (slowDatanodeSwitcherEnabled && SlowDatanodeSwitcher.isSupported()) {
            fileSystemWrapper = Functions.compose(fileSystemWrapper, slowDatanodeSwitcher.createFileSystemWrapper());
        }
        return new FileSystemWrapper(
                fileSystemWrapper,
                fileSystemCache.createPathWrapper(),
                Functions.<FileStatus>identity()
        );
    }

    private Function<FileSystem, FileSystem> createThreadContextClassLoaderWrapper()
    {
        return new Function<FileSystem, FileSystem>()
        {
            @Override
            public FileSystem apply(FileSystem fileSystem)
            {
                return new ForwardingFileSystem(fileSystem)
                {
                    @Override
                    public FSDataInputStream open(Path f, int bufferSize)
                            throws IOException
                    {
                        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
                            return super.open(f, bufferSize);
                        }
                    }

                    @Override
                    public FSDataInputStream open(Path f)
                            throws IOException
                    {
                        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
                            return super.open(f);
                        }
                    }
                };
            }
        };
    }
}
