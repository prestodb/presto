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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.spiller.SpillCipher;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.storage.TempStorageManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;

import javax.annotation.PreDestroy;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class TempStorageSingleStreamSpillerFactory
        implements SingleStreamSpillerFactory
{
    private final TempStorageManager tempStorageManager;
    private final ListeningExecutorService executor;
    private final PagesSerdeFactory serdeFactory;
    private final SpillerStats spillerStats;
    private final boolean spillEncryptionEnabled;
    private final int bufferSizeInBytes;
    private final String tempStorageName;

    private volatile TempStorage tempStorage;

    @Inject
    public TempStorageSingleStreamSpillerFactory(
            TempStorageManager tempStorageManager,
            BlockEncodingSerde blockEncodingSerde,
            SpillerStats spillerStats,
            FeaturesConfig featuresConfig,
            NodeSpillConfig nodeSpillConfig)
    {
        this(
                tempStorageManager,
                listeningDecorator(newFixedThreadPool(
                        requireNonNull(featuresConfig, "featuresConfig is null").getSpillerThreads(),
                        daemonThreadsNamed("binary-spiller-%s"))),
                blockEncodingSerde,
                spillerStats,
                requireNonNull(nodeSpillConfig, "nodeSpillConfig is null").isSpillCompressionEnabled(),
                requireNonNull(nodeSpillConfig, "nodeSpillConfig is null").isSpillEncryptionEnabled(),
                toIntExact(requireNonNull(nodeSpillConfig, "nodeSpillConfig is null").getTempStorageBufferSize().toBytes()),
                requireNonNull(featuresConfig, "featuresConfig is null").getSpillerTempStorage());
    }

    @VisibleForTesting
    TempStorageSingleStreamSpillerFactory(
            TempStorageManager tempStorageManager,
            ListeningExecutorService executor,
            BlockEncodingSerde blockEncodingSerde,
            SpillerStats spillerStats,
            boolean spillCompressionEnabled,
            boolean spillEncryptionEnabled,
            int bufferSizeInBytes,
            String tempStorageName)
    {
        this.tempStorageManager = requireNonNull(tempStorageManager, "tempStorageManager is null");
        this.serdeFactory = new PagesSerdeFactory(requireNonNull(blockEncodingSerde, "blockEncodingSerde is null"), spillCompressionEnabled);
        this.executor = requireNonNull(executor, "executor is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats can not be null");
        this.spillEncryptionEnabled = spillEncryptionEnabled;
        this.bufferSizeInBytes = bufferSizeInBytes;
        this.tempStorageName = requireNonNull(tempStorageName, "tempStorageName is null");
    }

    @PreDestroy
    public void destroy()
    {
        executor.shutdownNow();
    }

    @Override
    public SingleStreamSpiller create(List<Type> types, SpillContext spillContext, LocalMemoryContext memoryContext)
    {
        if (tempStorage == null) {
            initialize();
        }

        Optional<SpillCipher> spillCipher = Optional.empty();
        if (spillEncryptionEnabled) {
            spillCipher = Optional.of(new AesSpillCipher());
        }
        PagesSerde serde = serdeFactory.createPagesSerdeForSpill(spillCipher);
        return new TempStorageSingleStreamSpiller(
                tempStorage,
                serde,
                executor,
                spillerStats,
                spillContext,
                memoryContext,
                spillCipher,
                bufferSizeInBytes);
    }

    private synchronized void initialize()
    {
        if (this.tempStorage != null) {
            return;
        }

        TempStorage tempStorage = tempStorageManager.getTempStorage(tempStorageName);
        try {
            tempStorage.initialize();
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to initialize temp storage", e);
        }

        this.tempStorage = tempStorage;
    }
}
