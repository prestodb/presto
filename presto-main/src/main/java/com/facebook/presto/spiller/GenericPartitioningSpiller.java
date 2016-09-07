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

import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.concurrent.MoreFutures;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class GenericPartitioningSpiller
        implements PartitioningSpiller
{
    private final List<Type> types;
    private final int partitionsCount;
    private final boolean[] doNotSpillPartitions;
    private final PageBuilder[] pageBuilders;
    private final LocalPartitionGenerator partitionGenerator;
    private final SingleStreamSpiller[] spillers;
    private boolean readingStarted;

    public GenericPartitioningSpiller(
            List<Type> types,
            LocalPartitionGenerator partitionGenerator,
            int partitionsCount,
            Set<Integer> doNotSpillPartitions,
            SpillerFactory spillerFactory)
    {
        this.partitionsCount = partitionsCount;
        this.types = requireNonNull(types, "types is null");
        this.spillers = new SingleStreamSpiller[partitionsCount];
        this.doNotSpillPartitions = new boolean[partitionsCount];
        this.pageBuilders = new PageBuilder[partitionsCount];
        this.partitionGenerator = requireNonNull(partitionGenerator, "partitionGenerator is null");

        for (int partition = 0; partition < partitionsCount; partition++) {
            this.doNotSpillPartitions[partition] = doNotSpillPartitions.contains(partition);
            pageBuilders[partition] = new PageBuilder(types);
            spillers[partition] = spillerFactory.createSingleStreamSpiller(types);
        }
    }

    @Override
    public synchronized Iterator<Page> getSpilledPages(int partition)
    {
        readingStarted = true;
        checkArgument(partition < partitionsCount);
        getFutureValue(flush(partition));
        return spillers[partition].getSpilledPages();
    }

    @Override
    public synchronized PartitioningSpillResult partitionAndSpill(Page page)
    {
        checkState(!readingStarted);
        IntArrayList unspilledPositions = partitionPage(page);
        CompletableFuture<?> future = flush();

        return new PartitioningSpillResult(future, unspilledPositions);
    }

    private synchronized IntArrayList partitionPage(Page page)
    {
        IntArrayList unspilledPositions = new IntArrayList();

        for (int position = 0; position < page.getPositionCount(); position++) {
            int partition = partitionGenerator.getPartition(position, page);

            if (doNotSpillPartitions[partition]) {
                unspilledPositions.add(position);
                continue;
            }

            PageBuilder pageBuilder = pageBuilders[partition];
            pageBuilder.declarePosition();
            for (int channel = 0; channel < types.size(); channel++) {
                Type type = types.get(channel);
                type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
            }
        }

        return unspilledPositions;
    }

    private synchronized CompletableFuture<?> flush()
    {
        ImmutableList.Builder<CompletableFuture<?>> futures = ImmutableList.builder();

        for (int partition = 0; partition < partitionsCount; partition++) {
            PageBuilder pageBuilder = pageBuilders[partition];
            if (pageBuilder.isFull()) {
                futures.add(flush(partition));
            }
        }

        return MoreFutures.allAsList(futures.build());
    }

    private synchronized CompletableFuture<?> flush(int partition)
    {
        PageBuilder pageBuilder = pageBuilders[partition];
        if (pageBuilder.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return writePage(partition, page);
    }

    private synchronized CompletableFuture<?> writePage(int partition, Page page)
    {
        return spillers[partition].spill(page);
    }

    @Override
    public synchronized void close()
    {
        for (SingleStreamSpiller spiller : spillers) {
            spiller.close();
        }
    }
}
