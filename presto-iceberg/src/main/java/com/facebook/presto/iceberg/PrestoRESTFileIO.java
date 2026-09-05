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
package com.facebook.presto.iceberg;

import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * REST-catalog counterpart of {@link HdfsFileIO}: routes metadata I/O through the Presto
 * {@link HdfsEnvironment} so that per-identity
 * {@link com.facebook.presto.hive.DynamicConfigurationProvider}s, such as the AWS S3 security
 * mapping, resolve with the real path and the querying user. Built per table load by the
 * {@code ioBuilder}, so an instance is never shared between identities.
 * <p>
 * Manifests are cached by Presto's {@link ManifestFileCache}, which is keyed by path and so is
 * unaffected by a FileIO being built per load, rather than by Iceberg's {@code ContentCache}, which
 * is keyed on the FileIO instance and would never be hit.
 */
public class PrestoRESTFileIO
        implements DelegateFileIO, SupportsStorageCredentials
{
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final ManifestFileCache manifestFileCache;
    private final Map<String, String> properties;

    private volatile List<StorageCredential> storageCredentials = ImmutableList.of();

    public PrestoRESTFileIO(HdfsEnvironment hdfsEnvironment, HdfsContext hdfsContext, ManifestFileCache manifestFileCache, Map<String, String> properties)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
        this.manifestFileCache = requireNonNull(manifestFileCache, "manifestFileCache is null");
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
    }

    @Override
    public Map<String, String> properties()
    {
        return properties;
    }

    @Override
    public void setCredentials(List<StorageCredential> credentials)
    {
        this.storageCredentials = ImmutableList.copyOf(requireNonNull(credentials, "credentials is null"));
    }

    @Override
    public List<StorageCredential> credentials()
    {
        return storageCredentials;
    }

    @Override
    public InputFile newInputFile(String location)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return new HdfsInputFile(new Path(location), hdfsEnvironment, hdfsContext);
        }
    }

    @Override
    public InputFile newInputFile(String location, long length)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return new HdfsInputFile(new Path(location), hdfsEnvironment, hdfsContext, Optional.of(length));
        }
    }

    @Override
    public InputFile newInputFile(ManifestFile manifest)
    {
        checkArgument(
                manifest.keyMetadata() == null,
                "Cannot decrypt manifest: %s (use EncryptingFileIO)",
                manifest.path());
        InputFile inputFile = newInputFile(manifest.path(), manifest.length());
        return manifestFileCache.isEnabled() ?
                new HdfsCachedInputFile(inputFile, new ManifestFileCacheKey(manifest.path()), manifestFileCache) :
                inputFile;
    }

    @Override
    public OutputFile newOutputFile(String location)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return new HdfsOutputFile(new Path(location), hdfsEnvironment, hdfsContext);
        }
    }

    @Override
    public void deleteFile(String location)
    {
        Path path = new Path(location);
        try {
            hdfsEnvironment.doAs(hdfsContext.getIdentity().getUser(), () -> fileSystem(path).delete(path, false));
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "Failed to delete file: " + path, e);
        }
    }

    @Override
    public void deleteFiles(Iterable<String> pathsToDelete)
            throws BulkDeletionFailureException
    {
        for (String location : pathsToDelete) {
            deleteFile(location);
        }
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix)
    {
        Path path = new Path(prefix);
        return () -> {
            try {
                RemoteIterator<LocatedFileStatus> files = hdfsEnvironment.doAs(
                        hdfsContext.getIdentity().getUser(),
                        () -> fileSystem(path).listFiles(path, true));
                return new Iterator<FileInfo>()
                {
                    @Override
                    public boolean hasNext()
                    {
                        try {
                            return files.hasNext();
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }

                    @Override
                    public FileInfo next()
                    {
                        try {
                            if (!files.hasNext()) {
                                throw new NoSuchElementException();
                            }
                            LocatedFileStatus status = files.next();
                            return new FileInfo(status.getPath().toString(), status.getLen(), status.getModificationTime());
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                };
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to list prefix: " + prefix, e);
            }
        };
    }

    @Override
    public void deletePrefix(String prefix)
    {
        for (FileInfo file : listPrefix(prefix)) {
            deleteFile(file.location());
        }
    }

    private FileSystem fileSystem(Path path)
            throws IOException
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return hdfsEnvironment.getFileSystem(hdfsContext, path);
        }
    }
}
