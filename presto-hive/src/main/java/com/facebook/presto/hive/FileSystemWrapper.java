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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;

/**
 * FileSystemWrapper provides a framework for intercepting HDFS FileSystem type objects.
 * <p/>
 * The class encapsulates the relationship between HDFS FileSystem objects as they get
 * converted between each other (e.g. FileSystem produces Path, which produces FileStatus).
 * As each one of these types are returned through the API, the framework will guarantee
 * that they will first be intercepted by the Function objects specified through the
 * constructor and provide an opportunity to read, modify, or wrap these objects as they
 * pass through. Use the FileSystemWrapper to call wrap() on any of the supported HDFS types
 * to initialize this functionality.
 */
public class FileSystemWrapper
{
    private final Function<FileSystem, FileSystem> fileSystemFunction;
    private final Function<Path, Path> pathFunction;
    private final Function<FileStatus, FileStatus> fileStatusFunction;

    public FileSystemWrapper(Function<FileSystem, FileSystem> fileSystemFunction, Function<Path, Path> pathFunction, Function<FileStatus, FileStatus> fileStatusFunction)
    {
        this.fileSystemFunction = fileSystemFunction;
        this.pathFunction = pathFunction;
        this.fileStatusFunction = fileStatusFunction;
    }

    public static FileSystemWrapper identity()
    {
        return new FileSystemWrapper(Functions.<FileSystem>identity(), Functions.<Path>identity(), Functions.<FileStatus>identity());
    }

    public FileSystem wrap(FileSystem fileSystem)
    {
        return wrapFileSystem(fileSystemFunction.apply(fileSystem));
    }

    public Path wrap(Path path)
    {
        return wrapPath(pathFunction.apply(path));
    }

    public FileStatus wrap(FileStatus fileStatus)
    {
        return wrapFileStatus(fileStatusFunction.apply(fileStatus));
    }

    private Path wrapPath(Path path)
    {
        return new ForwardingPath(path)
        {
            @Override
            public FileSystem getFileSystem(Configuration conf)
                    throws IOException
            {
                return wrap(super.getFileSystem(conf));
            }

            @Override
            public Path getParent()
            {
                return wrap(super.getParent());
            }

            @Override
            public Path suffix(String suffix)
            {
                return wrap(super.suffix(suffix));
            }

            @Override
            public Path makeQualified(FileSystem fs)
            {
                return wrap(super.makeQualified(fs));
            }
        };
    }

    private FileStatus wrapFileStatus(FileStatus fileStatus)
    {
        return new ForwardingFileStatus(fileStatus)
        {
            @Override
            public Path getPath()
            {
                return wrap(super.getPath());
            }
        };
    }

    private FileSystem wrapFileSystem(FileSystem fileSystem)
    {
        return new ForwardingFileSystem(fileSystem)
        {
            private FileStatus[] wrapFileStatuses(FileStatus[] fileStatuses)
            {
                if (fileStatuses == null) {
                    return null;
                }
                FileStatus[] newFileStatuses = new FileStatus[fileStatuses.length];
                for (int i = 0; i < fileStatuses.length; i++) {
                    newFileStatuses[i] = wrap(fileStatuses[i]);
                }
                return newFileStatuses;
            }

            @Override
            public Path makeQualified(Path path)
            {
                return wrap(super.makeQualified(path));
            }

            @Override
            public FileStatus[] listStatus(Path f)
                    throws IOException
            {
                return wrapFileStatuses(super.listStatus(f));
            }

            @Override
            public FileStatus[] listStatus(Path f, PathFilter filter)
                    throws IOException
            {
                return wrapFileStatuses(super.listStatus(f, filter));
            }

            @Override
            public FileStatus[] listStatus(Path[] files)
                    throws IOException
            {
                return wrapFileStatuses(super.listStatus(files));
            }

            @Override
            public FileStatus[] listStatus(Path[] files, PathFilter filter)
                    throws IOException
            {
                return wrapFileStatuses(super.listStatus(files, filter));
            }

            @Override
            public FileStatus[] globStatus(Path pathPattern)
                    throws IOException
            {
                return wrapFileStatuses(super.globStatus(pathPattern));
            }

            @Override
            public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
                    throws IOException
            {
                return wrapFileStatuses(super.globStatus(pathPattern, filter));
            }

            @Override
            public Path getHomeDirectory()
            {
                return wrap(super.getHomeDirectory());
            }

            @Override
            public Path getWorkingDirectory()
            {
                return wrap(super.getWorkingDirectory());
            }

            @Override
            public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
                    throws IOException
            {
                return wrap(super.startLocalOutput(fsOutputFile, tmpLocalFile));
            }

            @Override
            public FileStatus getFileStatus(Path f)
                    throws IOException
            {
                return wrap(super.getFileStatus(f));
            }
        };
    }
}
