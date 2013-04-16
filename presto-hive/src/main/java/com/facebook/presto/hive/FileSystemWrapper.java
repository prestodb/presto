package com.facebook.presto.hive;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * FileSystemWrapper provides a framework for intercepting HDFS FileSystem type objects.
 *
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
    private final Function<LocatedFileStatus, LocatedFileStatus> locatedFileStatusFunction;

    public FileSystemWrapper(Function<FileSystem, FileSystem> fileSystemFunction, Function<Path, Path> pathFunction, Function<FileStatus, FileStatus> fileStatusFunction, Function<LocatedFileStatus, LocatedFileStatus> locatedFileStatusFunction)
    {
        this.fileSystemFunction = fileSystemFunction;
        this.pathFunction = pathFunction;
        this.fileStatusFunction = fileStatusFunction;
        this.locatedFileStatusFunction = locatedFileStatusFunction;
    }

    public static FileSystemWrapper identity()
    {
        return new FileSystemWrapper(Functions.<FileSystem>identity(), Functions.<Path>identity(), Functions.<FileStatus>identity(), Functions.<LocatedFileStatus>identity());
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

    public LocatedFileStatus wrap(LocatedFileStatus locatedFileStatus)
            throws IOException
    {
        return wrapLocatedFileStatus(locatedFileStatusFunction.apply(locatedFileStatus));
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

    private LocatedFileStatus wrapLocatedFileStatus(LocatedFileStatus locatedFileStatus)
            throws IOException
    {
        return new ForwardingLocatedFileStatus(locatedFileStatus)
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
        return new ForwardingFileSystem(fileSystem) {
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
            public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
                    throws FileNotFoundException, IOException
            {
                return new ForwardingRemoteIterator<LocatedFileStatus>(super.listLocatedStatus(f)) {
                    @Override
                    public LocatedFileStatus next()
                            throws IOException
                    {
                        return wrap(super.next());
                    }
                };
            }

            @Override
            public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f, PathFilter filter)
                    throws FileNotFoundException, IOException
            {
                return new ForwardingRemoteIterator<LocatedFileStatus>(super.listLocatedStatus(f, filter)) {
                    @Override
                    public LocatedFileStatus next()
                            throws IOException
                    {
                        return wrap(super.next());
                    }
                };
            }

            @Override
            public RemoteIterator<Path> listCorruptFileBlocks(Path path)
                    throws IOException
            {
                return new ForwardingRemoteIterator<Path>(super.listCorruptFileBlocks(path)) {
                    @Override
                    public Path next()
                            throws IOException
                    {
                        return wrap(super.next());
                    }
                };
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
            public Path getHomeDirectory(String userName)
            {
                return wrap(super.getHomeDirectory(userName));
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
