/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.util;

import com.facebook.presto.hive.DirectoryLister;
import com.facebook.presto.hive.NamenodeStats;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.stats.TimeStat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.hadoop.HadoopFileStatus.isDirectory;
import static com.google.common.base.Preconditions.checkNotNull;

public class AsyncWalker
{
    private final FileSystem fileSystem;
    private final Executor executor;
    private final DirectoryLister directoryLister;
    private final NamenodeStats namenodeStats;
    private final boolean recursive;

    public AsyncWalker(
        FileSystem fileSystem,
        Executor executor,
        DirectoryLister directoryLister,
        NamenodeStats namenodeStats,
        boolean recursive)
    {
        this.fileSystem = checkNotNull(fileSystem, "fileSystem is null");
        this.executor = checkNotNull(executor, "executor is null");
        this.directoryLister = checkNotNull(directoryLister, "directoryLister is null");
        this.namenodeStats = checkNotNull(namenodeStats, "namenodeStats is null");
        this.recursive = recursive;
    }

    public ListenableFuture<Void> beginWalk(Path path, FileStatusCallback callback)
    {
        SettableFuture<Void> future = SettableFuture.create();
        recursiveWalk(path, callback, new AtomicLong(), future);
        return future;
    }

    private void recursiveWalk(final Path path, final FileStatusCallback callback, final AtomicLong taskCount, final SettableFuture<Void> future)
    {
        taskCount.incrementAndGet();
        try {
            executor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    doWalk(path, callback, taskCount, future);
                }
            });
        }
        catch (Throwable t) {
            future.setException(t);
        }
    }

    private void doWalk(Path path, FileStatusCallback callback, AtomicLong taskCount, SettableFuture<Void> future)
    {
        try (SetThreadName ignored = new SetThreadName("HiveHdfsWalker")) {
            RemoteIterator<LocatedFileStatus> iterator = getLocatedFileStatusRemoteIterator(path);

            while (iterator.hasNext()) {
                LocatedFileStatus status = getLocatedFileStatus(iterator);

                // ignore hidden files. Hive ignores files starting with _ and . as well.
                String fileName = status.getPath().getName();
                if (fileName.startsWith("_") || fileName.startsWith(".")) {
                    continue;
                }
                if (!isDirectory(status)) {
                    callback.process(status, status.getBlockLocations());
                }
                else if (recursive) {
                    recursiveWalk(status.getPath(), callback, taskCount, future);
                }
                if (future.isDone()) {
                    return;
                }
            }
        }
        catch (FileNotFoundException e) {
            future.setException(new FileNotFoundException("Partition location does not exist: " + path));
        }
        catch (Throwable t) {
            future.setException(t);
        }
        finally {
            if (taskCount.decrementAndGet() == 0) {
                future.set(null);
            }
        }
    }

    private RemoteIterator<LocatedFileStatus> getLocatedFileStatusRemoteIterator(Path path)
            throws IOException
    {
        try (TimeStat.BlockTimer timer = namenodeStats.getListLocatedStatus().time()) {
            return directoryLister.list(fileSystem, path);
        }
        catch (IOException | RuntimeException e) {
            namenodeStats.getListLocatedStatus().recordException(e);
            throw e;
        }
    }

    private LocatedFileStatus getLocatedFileStatus(RemoteIterator<LocatedFileStatus> iterator)
            throws IOException
    {
        try (TimeStat.BlockTimer timer = namenodeStats.getRemoteIteratorNext().time()) {
            return iterator.next();
        }
        catch (IOException | RuntimeException e) {
            namenodeStats.getRemoteIteratorNext().recordException(e);
            throw e;
        }
    }
}
