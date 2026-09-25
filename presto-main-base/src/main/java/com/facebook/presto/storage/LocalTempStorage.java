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
package com.facebook.presto.storage;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.storage.StorageCapabilities;
import com.facebook.presto.spi.storage.TempDataOperationContext;
import com.facebook.presto.spi.storage.TempDataSink;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.spi.storage.TempStorageHandle;
import com.facebook.presto.spiller.TempStorageSpillerUtil;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import jakarta.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.OUT_OF_TEMP_STORAGE_SPACE;
import static com.facebook.presto.spiller.TempStorageSpillerConstants.SPILL_FILE_PREFIX;
import static com.facebook.presto.spiller.TempStorageSpillerConstants.SPILL_FILE_SUFFIX;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.getFileStore;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.Objects.requireNonNull;

public class LocalTempStorage
        implements TempStorage
{
    public static final String NAME = "local";
    public static final String TEMP_STORAGE_PATH = "temp-storage.path";

    private static final Logger log = Logger.get(LocalTempStorage.class);

    private final List<Path> tempStoragePaths;
    private final double maxUsedSpaceThreshold;

    @GuardedBy("this")
    private int roundRobinIndex;

    @Inject
    public LocalTempStorage(LocalTempStorageConfig config)
    {
        requireNonNull(config, "config is null");

        String configPaths = config.getTempStoragePath();
        List<String> pathsSplit = ImmutableList.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().split(configPaths));
        this.tempStoragePaths = ImmutableList.copyOf(
                pathsSplit.stream()
                        .map(Paths::get)
                        .collect(toImmutableList()));

        this.maxUsedSpaceThreshold = config.getMaxUsedSpaceThreshold();
        initialize();
    }

    public LocalTempStorage(List<Path> tempStoragePaths, double maxUsedSpaceThreshold)
    {
        this.tempStoragePaths = ImmutableList.copyOf(requireNonNull(tempStoragePaths, "tempStoragePaths is null"));
        this.maxUsedSpaceThreshold = maxUsedSpaceThreshold;
        initialize();
    }

    private void initialize()
    {
        tempStoragePaths.forEach(path -> {
            try {
                createDirectories(path);
            }
            catch (IOException e) {
                throw new IllegalArgumentException(
                        format("could not create temp storage path %s; adjust temp-storage.path config property or filesystem permissions", path), e);
            }
            if (!path.toFile().canWrite()) {
                throw new IllegalArgumentException(
                        format("temp storage path %s is not writable; adjust temp-storage.path config property or filesystem permissions", path));
            }
        });

        // Clean up stale spill files in temp storage
        tempStoragePaths.forEach(TempStorageSpillerUtil::cleanupOldSpillFiles);
    }

    @Override
    public TempDataSink create(TempDataOperationContext context)
            throws IOException
    {
        Path path = Files.createTempFile(getNextTempStoragePath(), SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX);
        return new LocalTempDataSink(path);
    }

    @Override
    public TempDataSink create(TempDataOperationContext context, TempStorageHandle handle, boolean createFile)
            throws IOException
    {
        Path path = ((LocalTempStorageHandle) handle).getFilePath();
        if (createFile) {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);

                Files.newOutputStream(path, CREATE, TRUNCATE_EXISTING).close();
            }
        }
        return new LocalTempDataSink(path);
    }

    @Override
    public InputStream open(TempDataOperationContext context, TempStorageHandle handle)
            throws IOException
    {
        return Files.newInputStream(((LocalTempStorageHandle) handle).getFilePath());
    }

    @Override
    public void remove(TempDataOperationContext context, TempStorageHandle handle)
            throws IOException
    {
        Files.delete(((LocalTempStorageHandle) handle).getFilePath());
    }

    @Override
    public boolean exists(TempDataOperationContext context, TempStorageHandle handle)
    {
        return Files.exists(((LocalTempStorageHandle) handle).getFilePath());
    }

    @Override
    public TempStorageHandle getRootDirectoryHandle()
    {
        return new LocalTempStorageHandle(getNextTempStoragePath());
    }

    @Override
    public byte[] serializeHandle(TempStorageHandle storageHandle)
    {
        return LocalTempStorage.serializeHandleStatic(storageHandle);
    }

    public static byte[] serializeHandleStatic(TempStorageHandle storageHandle)
    {
        URI uri = ((LocalTempStorageHandle) storageHandle).getFilePath().toUri();
        return uri.toString().getBytes(UTF_8);
    }

    @Override
    public TempStorageHandle deserialize(byte[] serializedStorageHandle)
    {
        return LocalTempStorage.deserializeStatic(serializedStorageHandle);
    }

    public static LocalTempStorageHandle deserializeStatic(byte[] serializedStorageHandle)
    {
        String uriString = new String(serializedStorageHandle, UTF_8);
        try {
            return new LocalTempStorageHandle(Paths.get(new URI(uriString)));
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URI: " + uriString, e);
        }
    }

    @Override
    public List<StorageCapabilities> getStorageCapabilities()
    {
        return ImmutableList.of();
    }

    public synchronized Path getNextTempStoragePath()
    {
        int tempStoragePathsCount = tempStoragePaths.size();
        for (int i = 0; i < tempStoragePathsCount; ++i) {
            int pathIndex = (roundRobinIndex + i) % tempStoragePathsCount;
            Path path = tempStoragePaths.get(pathIndex);
            if (hasEnoughDiskSpace(path)) {
                roundRobinIndex = (roundRobinIndex + i + 1) % tempStoragePathsCount;
                return path;
            }
        }
        if (tempStoragePaths.isEmpty()) {
            throw new PrestoException(OUT_OF_TEMP_STORAGE_SPACE, "No temp storage paths configured");
        }
        throw new PrestoException(OUT_OF_TEMP_STORAGE_SPACE, "No free space available for temp storage");
    }

    private boolean hasEnoughDiskSpace(Path path)
    {
        try {
            FileStore fileStore = getFileStore(path);
            return fileStore.getUsableSpace() > fileStore.getTotalSpace() * (1.0 - maxUsedSpaceThreshold);
        }
        catch (IOException e) {
            throw new PrestoException(OUT_OF_TEMP_STORAGE_SPACE, "Cannot determine free space for temp storage", e);
        }
    }

    public static class LocalTempStorageHandle
            implements TempStorageHandle
    {
        private final Path filePath;

        public LocalTempStorageHandle(Path filePath)
        {
            this.filePath = requireNonNull(filePath, "filePath is null");
        }

        public Path getFilePath()
        {
            return filePath;
        }

        @Override
        public String getPathAsString()
        {
            return filePath.toString();
        }

        @Override
        public String toString()
        {
            return filePath.toString();
        }
    }

    private static class LocalTempDataSink
            implements TempDataSink
    {
        private final DataSink sink;
        private final Path path;

        public LocalTempDataSink(Path path)
                throws IOException
        {
            this.path = requireNonNull(path, "path is null");
            this.sink = new OutputStreamDataSink(Files.newOutputStream(path, APPEND));
        }

        @Override
        public TempStorageHandle commit()
                throws IOException
        {
            sink.close();
            return new LocalTempStorageHandle(path);
        }

        @Override
        public void rollback()
                throws IOException
        {
            this.commit();
            Files.delete(path);
        }

        @Override
        public long size()
        {
            return sink.size();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return sink.getRetainedSizeInBytes();
        }

        @Override
        public void write(List<DataOutput> outputData)
                throws IOException
        {
            sink.write(outputData);
        }

        @Override
        public void close()
                throws IOException
        {
            sink.close();
        }
    }
}
