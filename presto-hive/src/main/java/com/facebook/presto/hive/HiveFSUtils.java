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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class HiveFSUtils
{
    private static final Logger log = Logger.get(HiveFSUtils.class);

    private static Configuration conf;

    private HiveFSUtils()
    {
        //Prevent Instantiation
    }

    public static void initialize(Configuration config)
    {
        conf = config;
    }

    public static boolean pathExists(Path path)
    {
        try {
            return getFileSystem(path).exists(path);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed checking path: " + path, e);
        }
    }

    public static boolean isDirectory(Path path)
    {
        try {
            return getFileSystem(path).isDirectory(path);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed checking path: " + path, e);
        }
    }

    public static void createDirectories(Path path)
    {
        try {
            if (!getFileSystem(path).mkdirs(path)) {
                throw new IOException("mkdirs returned false");
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to create directory: " + path, e);
        }
    }

    public static FileSystem getFileSystem(Path path)
            throws IOException
    {
        checkNotNull(conf, "HiveFSUtils used before being initialized");
        return path.getFileSystem(conf);
    }

    public static void rename(Path source, Path target)
    {
        try {
            if (!getFileSystem(source).rename(source, target)) {
                throw new IOException("rename returned false");
            }
        }
        catch (IOException e) {
            throw new RuntimeException(format("Failed to rename %s to %s", source, target), e);
        }
    }

    public static void delete(Path source, boolean recursive) throws IOException
    {
        if (!getFileSystem(source).delete(source, recursive)) {
            throw new IOException(String.format("delete on '%s' returned false", source));
        }
    }

    public static List<String> getPartitionValues(String partitionName)
    {
        String[] cols = partitionName.split(Path.SEPARATOR);
        List<String> values = new ArrayList<String>();
        for (String col : cols) {
            values.add(col.split("=")[1]);
        }
        return values;
    }
}
