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
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.AbstractIterator;
import io.airlift.stats.TimeStat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;

import static com.facebook.presto.hadoop.HadoopFileStatus.isDirectory;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class HiveFileIterator
        extends AbstractIterator<LocatedFileStatus>
{
    public enum NestedDirectoryPolicy
    {
        IGNORED,
        RECURSE,
        FAIL
    }

    private final Deque<Path> paths = new ArrayDeque<>();
    private final Table table;
    private final FileSystem fileSystem;
    private final DirectoryLister directoryLister;
    private final NamenodeStats namenodeStats;
    private final NestedDirectoryPolicy nestedDirectoryPolicy;

    private Iterator<LocatedFileStatus> remoteIterator = Collections.emptyIterator();

    public HiveFileIterator(
            Table table,
            Path path,
            FileSystem fileSystem,
            DirectoryLister directoryLister,
            NamenodeStats namenodeStats,
            NestedDirectoryPolicy nestedDirectoryPolicy)
    {
        paths.addLast(requireNonNull(path, "path is null"));
        this.table = requireNonNull(table, "table is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
        this.namenodeStats = requireNonNull(namenodeStats, "namenodeStats is null");
        this.nestedDirectoryPolicy = requireNonNull(nestedDirectoryPolicy, "nestedDirectoryPolicy is null");
    }

    @Override
    protected LocatedFileStatus computeNext()
    {
        while (true) {
            while (remoteIterator.hasNext()) {
                LocatedFileStatus status = getLocatedFileStatus(remoteIterator);

                // Ignore hidden files and directories. Hive ignores files starting with _ and . as well.
                String fileName = status.getPath().getName();
                if (fileName.startsWith("_") || fileName.startsWith(".")) {
                    continue;
                }

                if (isDirectory(status)) {
                    switch (nestedDirectoryPolicy) {
                        case IGNORED:
                            continue;
                        case RECURSE:
                            paths.add(status.getPath());
                            continue;
                        case FAIL:
                            throw new NestedDirectoryNotAllowedException();
                    }
                }

                return status;
            }

            if (paths.isEmpty()) {
                return endOfData();
            }
            remoteIterator = getLocatedFileStatusRemoteIterator(paths.removeFirst());
        }
    }

    private Iterator<LocatedFileStatus> getLocatedFileStatusRemoteIterator(Path path)
    {
        try (TimeStat.BlockTimer ignored = namenodeStats.getListLocatedStatus().time()) {
            return new FileStatusIterator(table, path, fileSystem, directoryLister, namenodeStats);
        }
    }

    private LocatedFileStatus getLocatedFileStatus(Iterator<LocatedFileStatus> iterator)
    {
        try (TimeStat.BlockTimer ignored = namenodeStats.getRemoteIteratorNext().time()) {
            return iterator.next();
        }
    }

    private static class FileStatusIterator
            implements Iterator<LocatedFileStatus>
    {
        private final Path path;
        private final NamenodeStats namenodeStats;
        private final RemoteIterator<LocatedFileStatus> fileStatusIterator;

        private FileStatusIterator(Table table, Path path, FileSystem fileSystem, DirectoryLister directoryLister, NamenodeStats namenodeStats)
        {
            this.path = path;
            this.namenodeStats = namenodeStats;
            try {
                this.fileStatusIterator = directoryLister.list(fileSystem, table, path);
            }
            catch (IOException e) {
                throw processException(e);
            }
        }

        @Override
        public boolean hasNext()
        {
            try {
                return fileStatusIterator.hasNext();
            }
            catch (IOException e) {
                throw processException(e);
            }
        }

        @Override
        public LocatedFileStatus next()
        {
            try {
                return fileStatusIterator.next();
            }
            catch (IOException e) {
                throw processException(e);
            }
        }

        private PrestoException processException(IOException exception)
        {
            namenodeStats.getRemoteIteratorNext().recordException(exception);
            if (exception instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_FILE_NOT_FOUND, "Partition location does not exist: " + path);
            }
            return new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed to list directory: " + path, exception);
        }
    }

    public static class NestedDirectoryNotAllowedException
            extends RuntimeException
    {
        public NestedDirectoryNotAllowedException()
        {
            super("Nested sub-directories are not allowed");
        }
    }
}
