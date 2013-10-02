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

    @Inject
    public FileSystemWrapperProvider(FileSystemCache fileSystemCache)
    {
        this.fileSystemCache = checkNotNull(fileSystemCache, "fileSystemCache is null");
    }

    @Override
    public FileSystemWrapper get()
    {
        return new FileSystemWrapper(
                createFileSystemWrapper(),
                createPathWrapper(),
                createFileStatusWrapper()
        );
    }

    Function<FileSystem, FileSystem> createFileSystemWrapper()
    {
        return createThreadContextClassLoaderWrapper();
    }

    Function<Path, Path> createPathWrapper()
    {
        return fileSystemCache.createPathWrapper();
    }

    Function<FileStatus, FileStatus> createFileStatusWrapper()
    {
        return Functions.identity();
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
