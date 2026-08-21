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
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.util.SerializableSupplier;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.facebook.presto.common.Utils.checkArgument;

/**
 * Iceberg FileIO implementation that routes metadata reads through the Presto
 * {@link HdfsEnvironment} for credential-aware per-query S3 access.
 */
public class PrestoRESTFileIO
        implements DelegateFileIO, HadoopConfigurable, SupportsStorageCredentials
{
    private volatile Map<String, String> properties = ImmutableMap.of();
    private volatile Configuration hadoopConf;
    private volatile List<StorageCredential> storageCredentials = ImmutableList.of();
    private volatile HdfsEnvironment hdfsEnvironment;
    private volatile HdfsContext hdfsContext;
    private final AtomicReference<HadoopFileIO> hadoopFileIO = new AtomicReference<>();

    /** No-arg constructor required by {@code CatalogUtil.loadFileIO()} reflection. */
    public PrestoRESTFileIO() {}

    public void setHdfsEnvironmentAndContext(HdfsEnvironment environment, HdfsContext context)
    {
        this.hdfsEnvironment = environment;
        this.hdfsContext = context;
    }

    @Override
    public void setConf(Configuration conf)
    {
        this.hadoopConf = conf;
    }

    @Override
    public Configuration getConf()
    {
        return hadoopConf;
    }

    @Override
    public void serializeConfWith(Function<Configuration, SerializableSupplier<Configuration>> confSerializer)
    {
        //no-op.
    }

    @Override
    public void initialize(Map<String, String> newProperties)
    {
        this.properties = ImmutableMap.copyOf(newProperties);
    }

    @Override
    public void setCredentials(List<StorageCredential> credentials)
    {
        checkArgument(credentials != null, "Invalid storage credentials: null");
        this.storageCredentials = ImmutableList.copyOf(credentials);
    }

    @Override
    public List<StorageCredential> credentials()
    {
        return storageCredentials;
    }

    @Override
    public InputFile newInputFile(String location)
    {
        Path path = new Path(location);
        if (hdfsEnvironment != null) {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
                return new HdfsInputFile(path, hdfsEnvironment, hdfsContext);
            }
        }
        return hadoopFileIO().newInputFile(location);
    }

    @Override
    public InputFile newInputFile(String location, long length)
    {
        Path path = new Path(location);
        if (hdfsEnvironment != null) {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
                return new HdfsInputFile(path, hdfsEnvironment, hdfsContext, Optional.of(length));
            }
        }
        return hadoopFileIO().newInputFile(location, length);
    }

    @Override
    public OutputFile newOutputFile(String location)
    {
        Path path = new Path(location);
        if (hdfsEnvironment != null) {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
                return new HdfsOutputFile(path, hdfsEnvironment, hdfsContext);
            }
        }
        return hadoopFileIO().newOutputFile(location);
    }

    @Override
    public void deleteFile(String location)
    {
        Path path = new Path(location);
        try {
            FileSystem fs;
            if (hdfsEnvironment != null) {
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
                    fs = hdfsEnvironment.getFileSystem(hdfsContext, path);
                }
            }
            else {
                fs = path.getFileSystem(hadoopConf);
            }
            fs.delete(path, false);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to delete file: " + location, e);
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
        if (hdfsEnvironment == null) {
            return hadoopFileIO().listPrefix(prefix);
        }
        Path path = new Path(prefix);
        return () -> {
            try {
                FileSystem fs;
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
                    fs = hdfsEnvironment.getFileSystem(hdfsContext, path);
                }
                RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, true);
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
                            LocatedFileStatus s = files.next();
                            return new FileInfo(s.getPath().toString(), s.getLen(), s.getModificationTime());
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
        if (hdfsEnvironment == null) {
            hadoopFileIO().deletePrefix(prefix);
            return;
        }
        for (FileInfo file : listPrefix(prefix)) {
            deleteFile(file.location());
        }
    }

    private HadoopFileIO hadoopFileIO()
    {
        Configuration conf = hadoopConf;
        HadoopFileIO io = hadoopFileIO.get();
        if (io == null || io.getConf() != conf) {
            io = new HadoopFileIO();
            io.setConf(conf);
            hadoopFileIO.set(io);
        }
        return io;
    }

    @Override
    public Map<String, String> properties()
    {
        return properties;
    }
}
