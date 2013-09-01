package com.facebook.presto.hive.util;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;

public class AsyncRecursiveWalker
{
    private final FileSystem fileSystem;
    private final Executor executor;

    public AsyncRecursiveWalker(FileSystem fileSystem, Executor executor)
    {
        this.fileSystem = checkNotNull(fileSystem, "fileSystem is null");
        this.executor = checkNotNull(executor, "executor is null");
    }

    public ListenableFuture<Void> beginWalk(Path path, FileStatusCallback callback)
    {
        SettableFuture<Void> settableFuture = SettableFuture.create();
        recursiveWalk(path, callback, new AtomicLong(), settableFuture);
        return settableFuture;
    }

    private void recursiveWalk(final Path path, final FileStatusCallback callback, final AtomicLong taskCount, final SettableFuture<Void> settableFuture)
    {
        taskCount.incrementAndGet();
        executor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    RemoteIterator<LocatedFileStatus> iter = fileSystem.listLocatedStatus(path);
                    while (iter.hasNext()) {
                        LocatedFileStatus status = iter.next();
                        if (isDirectory(status)) {
                            recursiveWalk(status.getPath(), callback, taskCount, settableFuture);
                        }
                        else {
                            callback.process(status, status.getBlockLocations());
                        }
                    }
                }
                catch (Exception e) {
                    if (e instanceof FileNotFoundException) {
                        e = new FileNotFoundException("Partition location does not exist: " + path);
                    }
                    settableFuture.setException(e);
                }
                finally {
                    if (taskCount.decrementAndGet() == 0) {
                        settableFuture.set(null);
                    }
                }
            }
        });
    }

    @SuppressWarnings("deprecation")
    private static boolean isDirectory(FileStatus status)
    {
        // older versions of Hadoop only have this method
        return status.isDir();
    }
}
