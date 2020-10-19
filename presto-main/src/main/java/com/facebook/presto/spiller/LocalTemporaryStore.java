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
import com.facebook.presto.spi.storage.TemporaryDataSink;
import com.facebook.presto.spi.storage.TemporaryStore;
import com.facebook.presto.spi.storage.TemporaryStoreHandle;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.OUT_OF_SPILL_SPACE;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.getFileStore;
import static java.nio.file.Files.newDirectoryStream;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Objects.requireNonNull;

public class LocalTemporaryStore
        implements TemporaryStore
{
    private static final Logger log = Logger.get(LocalTemporaryStore.class);

    private static final String SPILL_FILE_PREFIX = "spill";
    private static final String SPILL_FILE_SUFFIX = ".bin";
    private static final String SPILL_FILE_GLOB = "spill*.bin";

    private final List<Path> spillPaths;
    private final double maxUsedSpaceThreshold;

    @GuardedBy("this")
    private int roundRobinIndex;

    public LocalTemporaryStore(List<Path> spillPaths, double maxUsedSpaceThreshold)
    {
        this.spillPaths = ImmutableList.copyOf(requireNonNull(spillPaths, "spillPaths is null"));
        this.maxUsedSpaceThreshold = maxUsedSpaceThreshold;
    }

    @Override
    public void initialize()
            throws IOException
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
        spillPaths.forEach(LocalTemporaryStore::cleanupOldSpillFiles);
    }

    @Override
    public TemporaryDataSink create()
            throws IOException
    {
        Path path = Files.createTempFile(getNextSpillPath(), SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX);
        return new LocalTemporaryDataSink(path);
    }

    @Override
    public InputStream open(TemporaryStoreHandle handle)
            throws IOException
    {
        return Files.newInputStream(((LocalTemporaryStoreHandle) handle).getFilePath());
    }

    @Override
    public void remove(TemporaryStoreHandle handle)
            throws IOException
    {
        Files.delete(((LocalTemporaryStoreHandle) handle).getFilePath());
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

    private static class LocalTemporaryStoreHandle
            implements TemporaryStoreHandle
    {
        private final Path filePath;

        public LocalTemporaryStoreHandle(Path filePath)
        {
            this.filePath = requireNonNull(filePath, "filePath is null");
        }

        public Path getFilePath()
        {
            return filePath;
        }
    }

    private static class LocalTemporaryDataSink
            implements TemporaryDataSink
    {
        private final DataSink sink;
        private final Path path;

        public LocalTemporaryDataSink(Path path)
                throws IOException
        {
            this.path = requireNonNull(path, "path is null");
            this.sink = new OutputStreamDataSink(Files.newOutputStream(path, APPEND));
        }

        @Override
        public TemporaryStoreHandle commit()
                throws IOException
        {
            sink.close();
            return new LocalTemporaryStoreHandle(path);
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
