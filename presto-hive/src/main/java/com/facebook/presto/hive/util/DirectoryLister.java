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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.util.DirectoryEntry.entryForDirectory;
import static com.facebook.presto.hive.util.DirectoryEntry.entryForFile;
import static com.facebook.presto.hive.util.FileStatusUtil.isDirectory;

/**
 * Shim to work around old versions of Hadoop not having LocatedFileStatus.
 */
final class DirectoryLister
{
    private static final Lister LISTER = createLister();

    private DirectoryLister() {}

    public static List<DirectoryEntry> listDirectory(FileSystem fileSystem, Path path)
            throws IOException
    {
        return LISTER.list(fileSystem, path);
    }

    private static Lister createLister()
    {
        try {
            // this fails on old versions of Hadoop that don't have LocatedFileStatus
            return new FastLister();
        }
        catch (Throwable t) {
            return new SlowLister();
        }
    }

    private interface Lister
    {
        List<DirectoryEntry> list(FileSystem fileSystem, Path path)
                throws IOException;
    }

    private static class FastLister
            implements Lister
    {
        @Override
        public List<DirectoryEntry> list(FileSystem fileSystem, Path path)
                throws IOException
        {
            ImmutableList.Builder<DirectoryEntry> list = ImmutableList.builder();
            RemoteIterator<LocatedFileStatus> iter = fileSystem.listLocatedStatus(path);
            while (iter.hasNext()) {
                LocatedFileStatus status = iter.next();
                if (isDirectory(status)) {
                    list.add(entryForDirectory(status));
                }
                else {
                    list.add(entryForFile(status, status.getBlockLocations()));
                }
            }
            return list.build();
        }
    }

    private static class SlowLister
            implements Lister
    {
        @Override
        public List<DirectoryEntry> list(FileSystem fileSystem, Path path)
                throws IOException
        {
            FileStatus[] listing = fileSystem.listStatus(path);
            if (listing == null) {
                return ImmutableList.of();
            }

            ImmutableList.Builder<DirectoryEntry> list = ImmutableList.builder();
            for (FileStatus status : listing) {
                if (isDirectory(status)) {
                    list.add(entryForDirectory(status));
                }
                else {
                    // old versions of Hadoop require fetching block locations for every file individually
                    list.add(entryForFile(status, fileSystem.getFileBlockLocations(status, 0, Long.MAX_VALUE)));
                }
            }
            return list.build();
        }
    }
}
