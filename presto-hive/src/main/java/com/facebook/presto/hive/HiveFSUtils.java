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

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.HiveStatsUtils;

import com.facebook.presto.spi.PrestoException;

public final class HiveFSUtils
{
    private static HdfsEnvironment hdfsEnvironment;

    private HiveFSUtils()
    {
        //Prevent Instantiation
    }

    public static void initialize(HdfsEnvironment env)
    {
        hdfsEnvironment = env;
    }

    public static FileStatus[] listFiles(Path path) throws PrestoException
    {
        FileSystem fs;
        FileStatus[] fss = null;
        try {
            fs = getFileSystem(path);
            fss = fs.listStatus(path);
        }
        catch (IOException e) {
            throw new PrestoException(HiveErrorCode.HIVE_FILE_NOT_FOUND.toErrorCode(), e);
        }
        return fss;
    }

    public static FileStatus[] listPathsAtLevel(Path basePath, int levels) throws PrestoException
    {
        FileSystem fs;
        FileStatus[] fss = null;
        try {
            fs = getFileSystem(basePath);
            fss = HiveStatsUtils.getFileStatusRecurse(basePath, levels, fs);
        }
        catch (IOException e) {
            throw new PrestoException(HiveErrorCode.HIVE_FILE_NOT_FOUND.toErrorCode(), e);
        }

        return fss;
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
        checkNotNull(hdfsEnvironment, "HiveFSUtils used before being initialized");
        return path.getFileSystem(hdfsEnvironment.getConfiguration(path));
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
}
