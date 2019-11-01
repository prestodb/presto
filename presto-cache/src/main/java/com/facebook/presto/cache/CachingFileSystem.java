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
package com.facebook.presto.cache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;

import static java.util.Objects.requireNonNull;

public final class CachingFileSystem
        extends FileSystem
{
    private final URI workingDirectory;
    private final CacheManager cacheManager;
    private final FileSystem dataTier;
    private final boolean cacheValidationEnabled;

    public CachingFileSystem(
            URI workingDirectory,
            Configuration configuration,
            CacheManager cacheManager,
            FileSystem dataTier,
            boolean cacheValidationEnabled)
    {
        requireNonNull(workingDirectory, "uri is null");
        requireNonNull(configuration, "configuration is null");

        this.workingDirectory = URI.create(workingDirectory.getScheme() + "://" + workingDirectory.getAuthority());
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.dataTier = requireNonNull(dataTier, "dataTier is null");
        this.cacheValidationEnabled = cacheValidationEnabled;

        setConf(configuration);

        //noinspection AssignmentToSuperclassField
        statistics = getStatistics(workingDirectory.getScheme(), getClass());
    }

    @Override
    public void initialize(URI uri, Configuration configuration)
    {
        // make unique instances
        throw new UnsupportedOperationException();
    }

    @Override
    public URI getUri()
    {
        return workingDirectory;
    }

    @Override
    public void setWorkingDirectory(Path workingDirectory)
    {
        dataTier.setWorkingDirectory(workingDirectory);
    }

    @Override
    public Path getWorkingDirectory()
    {
        return dataTier.getWorkingDirectory();
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission)
            throws IOException
    {
        return dataTier.mkdirs(path, permission);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException
    {
        return new CachingInputStream(dataTier.open(path, bufferSize), cacheManager, path, cacheValidationEnabled);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        return dataTier.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
            throws IOException
    {
        return dataTier.append(path, bufferSize, progress);
    }

    @Override
    public boolean rename(Path source, Path destination)
            throws IOException
    {
        return dataTier.rename(source, destination);
    }

    @Override
    public boolean delete(Path path, boolean recursive)
            throws IOException
    {
        return dataTier.delete(path, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        return dataTier.listStatus(path);
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        return dataTier.getFileStatus(path);
    }
}
