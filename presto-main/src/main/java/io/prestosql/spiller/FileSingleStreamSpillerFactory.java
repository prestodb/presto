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
package io.prestosql.spiller;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.execution.buffer.PagesSerdeFactory;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.SpillContext;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.FeaturesConfig;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.spi.StandardErrorCode.OUT_OF_SPILL_SPACE;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.getFileStore;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class FileSingleStreamSpillerFactory
        implements SingleStreamSpillerFactory
{
    private static final Logger log = Logger.get(FileSingleStreamSpillerFactory.class);

    @VisibleForTesting
    static final String SPILL_FILE_PREFIX = "spill";
    @VisibleForTesting
    static final String SPILL_FILE_SUFFIX = ".bin";
    private static final String SPILL_FILE_GLOB = "spill*.bin";

    private final ListeningExecutorService executor;
    private final PagesSerdeFactory serdeFactory;
    private final List<Path> spillPaths;
    private final SpillerStats spillerStats;
    private final double maxUsedSpaceThreshold;
    private int roundRobinIndex;

    @Inject
    public FileSingleStreamSpillerFactory(BlockEncodingSerde blockEncodingSerde, SpillerStats spillerStats, FeaturesConfig featuresConfig)
    {
        this(
                listeningDecorator(newFixedThreadPool(
                        requireNonNull(featuresConfig, "featuresConfig is null").getSpillerThreads(),
                        daemonThreadsNamed("binary-spiller-%s"))),
                blockEncodingSerde,
                spillerStats,
                requireNonNull(featuresConfig, "featuresConfig is null").getSpillerSpillPaths(),
                requireNonNull(featuresConfig, "featuresConfig is null").getSpillMaxUsedSpaceThreshold());
    }

    @VisibleForTesting
    public FileSingleStreamSpillerFactory(
            ListeningExecutorService executor,
            BlockEncodingSerde blockEncodingSerde,
            SpillerStats spillerStats,
            List<Path> spillPaths,
            double maxUsedSpaceThreshold)
    {
        this.serdeFactory = new PagesSerdeFactory(requireNonNull(blockEncodingSerde, "blockEncodingSerde is null"), false);
        this.executor = requireNonNull(executor, "executor is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats can not be null");
        requireNonNull(spillPaths, "spillPaths is null");
        this.spillPaths = ImmutableList.copyOf(spillPaths);
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
        this.maxUsedSpaceThreshold = maxUsedSpaceThreshold;
        this.roundRobinIndex = 0;
    }

    @PostConstruct
    public void cleanupOldSpillFiles()
    {
        spillPaths.forEach(FileSingleStreamSpillerFactory::cleanupOldSpillFiles);
    }

    @PreDestroy
    public void destroy()
    {
        executor.shutdownNow();
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

    @Override
    public SingleStreamSpiller create(List<Type> types, SpillContext spillContext, LocalMemoryContext memoryContext)
    {
        return new FileSingleStreamSpiller(serdeFactory.createPagesSerde(), executor, getNextSpillPath(), spillerStats, spillContext, memoryContext);
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
}
