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
package com.facebook.presto.spiller;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.storage.StorageCapabilities;
import com.facebook.presto.spi.storage.TempDataOperationContext;
import com.facebook.presto.spi.storage.TempDataSink;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.spi.storage.TempStorageContext;
import com.facebook.presto.spi.storage.TempStorageFactory;
import com.facebook.presto.spi.storage.TempStorageHandle;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.OUT_OF_SPILL_SPACE;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.getFileStore;
import static java.nio.file.Files.newDirectoryStream;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Objects.requireNonNull;

public class LocalTempStorage
        implements TempStorage
{
    public static final String NAME = "local";
    public static final String TEMP_STORAGE_PATH = "temp-storage.path";

    private static final Logger log = Logger.get(LocalTempStorage.class);

    private static final String SPILL_FILE_PREFIX = "spill";
    private static final String SPILL_FILE_SUFFIX = ".bin";
    private static final String SPILL_FILE_GLOB = "spill*.bin";

    private final List<Path> spillPaths;
    private final double maxUsedSpaceThreshold;

    @GuardedBy("this")
    private int roundRobinIndex;

    public LocalTempStorage(List<Path> spillPaths, double maxUsedSpaceThreshold)
    {
        this.spillPaths = ImmutableList.copyOf(requireNonNull(spillPaths, "spillPaths is null"));
        this.maxUsedSpaceThreshold = maxUsedSpaceThreshold;
        initialize();
    }

    private void initialize()
    {
        // From FileSingleStreamSpillerFactory constructor
        spillPaths.forEach(path -> {
            try {
                createDirectories(path);
            }
            catch (IOException e) {
                throw new IllegalArgumentException(
                        format("could not create spill path %s; adjust experimental.spiller-spill-path config property or filesystem permissions", path), e);
            }
            if (!path.toFile().canWrite()) {
                throw new IllegalArgumentException(
                        format("spill path %s is not writable; adjust experimental.spiller-spill-path config property or filesystem permissions", path));
            }
        });

        // From FileSingleStreamSpillerFactory#cleanupOldSpillFiles
        spillPaths.forEach(LocalTempStorage::cleanupOldSpillFiles);
    }

    @Override
    public TempDataSink create(TempDataOperationContext context)
            throws IOException
    {
        Path path = Files.createTempFile(getNextSpillPath(), SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX);
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
    public byte[] serializeHandle(TempStorageHandle storageHandle)
    {
        URI uri = ((LocalTempStorageHandle) storageHandle).getFilePath().toUri();
        return uri.toString().getBytes(UTF_8);
    }

    @Override
    public TempStorageHandle deserialize(byte[] serializedStorageHandle)
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

    private static void cleanupOldSpillFiles(Path path)
    {
        try (DirectoryStream<Path> stream = newDirectoryStream(path, SPILL_FILE_GLOB)) {
            stream.forEach(spillFile -> {
                try {
                    log.info("Deleting old spill file: " + spillFile);
                    delete(spillFile);
                }
                catch (Exception e) {
                    log.warn("Could not cleanup old spill file: " + spillFile);
                }
            });
        }
        catch (IOException e) {
            log.warn(e, "Error cleaning spill files");
        }
    }

    private synchronized Path getNextSpillPath()
    {
        int spillPathsCount = spillPaths.size();
        for (int i = 0; i < spillPathsCount; ++i) {
            int pathIndex = (roundRobinIndex + i) % spillPathsCount;
            Path path = spillPaths.get(pathIndex);
            if (hasEnoughDiskSpace(path)) {
                roundRobinIndex = (roundRobinIndex + i + 1) % spillPathsCount;
                return path;
            }
        }
        if (spillPaths.isEmpty()) {
            throw new PrestoException(OUT_OF_SPILL_SPACE, "No spill paths configured");
        }
        throw new PrestoException(OUT_OF_SPILL_SPACE, "No free space available for spill");
    }

    private boolean hasEnoughDiskSpace(Path path)
    {
        try {
            FileStore fileStore = getFileStore(path);
            return fileStore.getUsableSpace() > fileStore.getTotalSpace() * (1.0 - maxUsedSpaceThreshold);
        }
        catch (IOException e) {
            throw new PrestoException(OUT_OF_SPILL_SPACE, "Cannot determine free space for spill", e);
        }
    }

    private static class LocalTempStorageHandle
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

    public static class Factory
            implements TempStorageFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public TempStorage create(Map<String, String> config, TempStorageContext context)
        {
            String configPaths = config.get(TEMP_STORAGE_PATH);
            checkState(configPaths != null, "Local temp storage configuration must contain the '%s' property", TEMP_STORAGE_PATH);

            List<String> pathsSplit = ImmutableList.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().split(config.get(TEMP_STORAGE_PATH)));
            List<Path> tempStoragePaths = pathsSplit.stream()
                    .map(Paths::get)
                    .collect(toImmutableList());

            // TODO: make maxUsedSpaceThreshold configurable
            return new LocalTempStorage(tempStoragePaths, 1.0);
        }
    }
}
