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

import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.SpillContext;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.spiller.SpillCipher;
import com.facebook.presto.spi.spiller.TempFileStore;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;

import javax.annotation.PreDestroy;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class FileSingleStreamSpillerFactory
        implements SingleStreamSpillerFactory
{
    @VisibleForTesting
    static final String SPILL_FILE_PREFIX = "spill";
    @VisibleForTesting
    static final String SPILL_FILE_SUFFIX = ".bin";
    private static final String SPILL_FILE_GLOB = "spill*.bin";

    private final TempFileStore fileStore;
    private final ListeningExecutorService executor;
    private final PagesSerdeFactory serdeFactory;
    private final SpillerStats spillerStats;
    private final boolean spillEncryptionEnabled;

    @Inject
    public FileSingleStreamSpillerFactory(
            BlockEncodingSerde blockEncodingSerde,
            SpillerStats spillerStats,
            FeaturesConfig featuresConfig,
            NodeSpillConfig nodeSpillConfig)
    {
        this(
                listeningDecorator(newFixedThreadPool(
                        requireNonNull(featuresConfig, "featuresConfig is null").getSpillerThreads(),
                        daemonThreadsNamed("binary-spiller-%s"))),
                blockEncodingSerde,
                spillerStats,
                requireNonNull(featuresConfig, "featuresConfig is null").getSpillerSpillPaths(),
                requireNonNull(featuresConfig, "featuresConfig is null").getSpillMaxUsedSpaceThreshold(),
                requireNonNull(nodeSpillConfig, "nodeSpillConfig is null").isSpillCompressionEnabled(),
                requireNonNull(nodeSpillConfig, "nodeSpillConfig is null").isSpillEncryptionEnabled());
    }

    @VisibleForTesting
    public FileSingleStreamSpillerFactory(
            ListeningExecutorService executor,
            BlockEncodingSerde blockEncodingSerde,
            SpillerStats spillerStats,
            List<Path> spillPaths,
            double maxUsedSpaceThreshold,
            boolean spillCompressionEnabled,
            boolean spillEncryptionEnabled)
    {
        this.serdeFactory = new PagesSerdeFactory(requireNonNull(blockEncodingSerde, "blockEncodingSerde is null"), spillCompressionEnabled);
        this.executor = requireNonNull(executor, "executor is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats can not be null");
        requireNonNull(spillPaths, "spillPaths is null");

        this.fileStore = new LocalTempFileStore(spillPaths, maxUsedSpaceThreshold);

        // This is going to be tricky when file store is configurable
        // When TempFileStore is configurable, a FileStoreManager will be provided here (similar to AccessManager)
        // However, at injection time, plugin registration is not completed, thus we cannot fetch the TempFileStore underlying
        // Probably need to lazily initialize it in `create`
        fileStore.initialize();

        this.spillEncryptionEnabled = spillEncryptionEnabled;
    }

    @PreDestroy
    public void destroy()
    {
        executor.shutdownNow();
    }

    @Override
    public SingleStreamSpiller create(List<Type> types, SpillContext spillContext, LocalMemoryContext memoryContext)
    {
        Optional<SpillCipher> spillCipher = Optional.empty();
        if (spillEncryptionEnabled) {
            spillCipher = Optional.of(new AesSpillCipher());
        }
        PagesSerde serde = serdeFactory.createPagesSerdeForSpill(spillCipher);
        return new FileSingleStreamSpiller(
                fileStore,
                serde,
                executor,
                spillerStats,
                spillContext,
                memoryContext,
                spillCipher);
    }
}
