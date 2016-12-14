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
package com.facebook.presto.hdfs.fs;

import com.facebook.presto.hdfs.HDFSConfig;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystemNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public final class FSFactory
{
    private static Configuration conf = new Configuration();
    private static Logger log = Logger.get(FSFactory.class.getName());

    private FSFactory()
    {
    }

    public static Optional<FileSystem> getFS()
    {
        return getFS(HDFSConfig.getMetaserverStore());
    }

    public static Optional<FileSystem> getFS(String basePath)
    {
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        try {
            return Optional.of(FileSystem.get(new URI(basePath), conf));
        }
        catch (IOException | URISyntaxException e) {
            log.error(e);
            return Optional.empty();
        }
    }

    public static List<Path> listFiles(Path dirPath)
    {
        List<Path> files = new ArrayList<>();
        if (!getFS().isPresent()) {
            throw new FileSystemNotFoundException("");
        }
        FileStatus[] fileStatuses = new FileStatus[0];
        try {
            fileStatuses = getFS().get().listStatus(dirPath);
        }
        catch (IOException e) {
            log.error(e);
        }
        for (FileStatus f : fileStatuses) {
            if (f.isFile()) {
                files.add(f.getPath());
            }
        }
        return files;
    }

    // assume that a file contains only a block
    public static List<HostAddress> getBlockLocations(Path file, long start, long len)
    {
        Set<HostAddress> addresses = new HashSet<>();
        if (!getFS().isPresent()) {
            throw new FileSystemNotFoundException("");
        }
        BlockLocation[] locations = new BlockLocation[0];
        try {
            locations = getFS().get().getFileBlockLocations(file, start, len);
        }
        catch (IOException e) {
            log.error(e);
        }
        assert locations.length <= 1;
        for (BlockLocation location : locations) {
            try {
                addresses.addAll(toHostAddress(location.getHosts()));
            }
            catch (IOException e) {
                log.error(e);
            }
        }
        return new ArrayList<>(addresses);
    }

    private static List<HostAddress> toHostAddress(String[] hosts)
    {
        ImmutableList.Builder<HostAddress> builder = ImmutableList.builder();
        for (String host : hosts) {
            builder.add(HostAddress.fromString(host));
        }
        return builder.build();
    }
}
