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

import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import javax.inject.Inject;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class BinarySpillerFactory
        extends SpillerFactoryWithStats
{
    public static final String SPILLER_THREAD_NAME_PREFIX = "binary-spiller";

    private final ListeningExecutorService executor;
    private final BlockEncodingSerde blockEncodingSerde;
    private final Path spillPath;

    @Inject
    public BinarySpillerFactory(BlockEncodingSerde blockEncodingSerde, FeaturesConfig featuresConfig)
    {
        this(createExecutorServiceOfSize(requireNonNull(featuresConfig, "featuresConfig is null").getSpillerThreads()),
                blockEncodingSerde,
                requireNonNull(featuresConfig, "featuresConfig is null").getSpillerSpillPath());
    }

    public BinarySpillerFactory(ListeningExecutorService executor, BlockEncodingSerde blockEncodingSerde, Path spillPath)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.spillPath = requireNonNull(spillPath, "spillPath is null");
        this.spillPath.toFile().mkdirs();
    }

    private static ListeningExecutorService createExecutorServiceOfSize(int nThreads)
    {
        ThreadFactory threadFactory = daemonThreadsNamed(SPILLER_THREAD_NAME_PREFIX + "-%s");
        ExecutorService executorService = newFixedThreadPool(nThreads, threadFactory);
        return MoreExecutors.listeningDecorator(executorService);
    }

    @Override
    public Spiller create(List<Type> types)
    {
        return new BinaryFileSpiller(blockEncodingSerde, executor, spillPath, spilledBytes);
    }
}
