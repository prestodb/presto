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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.hive.util.DirectoryLister.listDirectory;
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
        try {
            for (DirectoryEntry entry : listDirectory(fileSystem, path)) {
                if (entry.isDirectory()) {
                    recursiveWalk(entry.getFileStatus().getPath(), callback, taskCount, future);
                }
                else {
                    callback.process(entry.getFileStatus(), entry.getBlockLocations());
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
}
