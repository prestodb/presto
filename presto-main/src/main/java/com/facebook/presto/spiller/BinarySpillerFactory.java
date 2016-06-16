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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class BinarySpillerFactory
        implements SpillerFactory
{
    public static final Path SPILL_PATH = Paths.get("/tmp/spills");

    private final ListeningExecutorService executor;
    private final BlockEncodingSerde blockEncodingSerde;

    @Inject
    public BinarySpillerFactory(BlockEncodingSerde blockEncodingSerde)
    {
        this(blockEncodingSerde, MoreExecutors.listeningDecorator(newFixedThreadPool(4, daemonThreadsNamed("binary-spiller-%s"))));
    }

    public BinarySpillerFactory(BlockEncodingSerde blockEncodingSerde, ListeningExecutorService executor)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.executor = requireNonNull(executor, "executor is null");

        SPILL_PATH.toFile().mkdirs();
    }

    @Override
    public Spiller create(List<Type> types)
    {
        return new BinaryFileSpiller(blockEncodingSerde, executor);
    }
}
