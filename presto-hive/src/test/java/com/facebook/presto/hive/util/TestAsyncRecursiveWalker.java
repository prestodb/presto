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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TestAsyncRecursiveWalker
{
    @Test
    public void testSanity()
            throws Exception
    {
        ImmutableMap<String, List<FileStatus>> paths = ImmutableMap.<String, List<FileStatus>>builder()
                .put("/", ImmutableList.of(fileStatus("/a", true), fileStatus("/file1", false)))
                .put("/a", ImmutableList.of(fileStatus("/a/file2", false), fileStatus("/a/file3", false)))
                .build();

        AsyncRecursiveWalker walker = new AsyncRecursiveWalker(createMockFileSystem(paths), MoreExecutors.sameThreadExecutor());

        MockFileStatusCallback callback = new MockFileStatusCallback();
        ListenableFuture<Void> listenableFuture = walker.beginWalk(new Path("/"), callback);

        Assert.assertTrue(listenableFuture.isDone());
        Assert.assertEquals(ImmutableSet.copyOf(callback.getProcessedFiles()), ImmutableSet.of("/file1", "/a/file2", "/a/file3"));

        // Should not have an exception
        listenableFuture.get();
    }

    @Test
    public void testEmptyPath()
            throws Exception
    {
        ImmutableMap<String, List<FileStatus>> paths = ImmutableMap.<String, List<FileStatus>>builder()
                .put("/", ImmutableList.<FileStatus>of())
                .build();

        AsyncRecursiveWalker walker = new AsyncRecursiveWalker(createMockFileSystem(paths), MoreExecutors.sameThreadExecutor());

        MockFileStatusCallback callback = new MockFileStatusCallback();
        ListenableFuture<Void> listenableFuture = walker.beginWalk(new Path("/"), callback);

        Assert.assertTrue(listenableFuture.isDone());
        Assert.assertTrue(callback.getProcessedFiles().isEmpty());

        // Should not have an exception
        listenableFuture.get();
    }

    @Test
    public void testDoubleIOException()
            throws Exception
    {
        AsyncRecursiveWalker walker = new AsyncRecursiveWalker(new StubFileSystem()
        {
            @Override
            public FileStatus[] listStatus(Path f)
                    throws IOException
            {
                throw new IOException();
            }
        }, MoreExecutors.sameThreadExecutor());

        MockFileStatusCallback callback = new MockFileStatusCallback();
        ListenableFuture<Void> listenableFuture1 = walker.beginWalk(new Path("/"), callback);
        ListenableFuture<Void> listenableFuture2 = walker.beginWalk(new Path("/"), callback);

        ListenableFuture<List<Void>> listenableFuture = Futures.allAsList(ImmutableList.of(listenableFuture1, listenableFuture2));

        Assert.assertTrue(listenableFuture.isDone());
        Futures.addCallback(listenableFuture, new FutureCallback<List<Void>>()
        {
            @Override
            public void onSuccess(List<Void> result)
            {
                // Should not succeed
                throw new IllegalStateException();
            }

            @Override
            public void onFailure(Throwable t)
            {
                Assert.assertTrue(t instanceof IOException);
            }
        });
    }

    private static FileSystem createMockFileSystem(final Map<String, List<FileStatus>> paths)
    {
        return new StubFileSystem()
        {
            @Override
            public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
                    throws IOException
            {
                ImmutableList.Builder<LocatedFileStatus> list = ImmutableList.builder();
                for (FileStatus status : paths.get(f.toString())) {
                    list.add(new LocatedFileStatus(status, new BlockLocation[0]));
                }
                return remoteIterator(list.build().iterator());
            }

            @Override
            public FileStatus[] listStatus(Path f)
                    throws IOException
            {
                List<FileStatus> fileStatuses = paths.get(f.toString());
                return fileStatuses.toArray(new FileStatus[fileStatuses.size()]);
            }
        };
    }

    private static FileStatus fileStatus(String path, boolean directory)
    {
        return new FileStatus(0, directory, 0, 0, 0, new Path(path));
    }

    private static class MockFileStatusCallback
            implements FileStatusCallback
    {
        private final List<String> processedFiles = new ArrayList<>();

        @Override
        public void process(FileStatus fileStatus, BlockLocation[] blockLocations)
        {
            processedFiles.add(fileStatus.getPath().toString());
        }

        public List<String> getProcessedFiles()
        {
            return ImmutableList.copyOf(processedFiles);
        }
    }

    @SuppressWarnings("deprecation")
    private static class StubFileSystem
            extends FileSystem
    {
        @Override
        public URI getUri()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rename(Path src, Path dst)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean delete(Path f)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean delete(Path f, boolean recursive)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(Path f)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWorkingDirectory(Path new_dir)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Path getWorkingDirectory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mkdirs(Path f, FsPermission permission)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path f)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }
    }

    private static <E> RemoteIterator<E> remoteIterator(final Iterator<E> iterator)
    {
        return new RemoteIterator<E>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public E next()
            {
                return iterator.next();
            }
        };
    }
}
