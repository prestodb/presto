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

import com.facebook.presto.memory.AggregatedMemoryContext;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.SpillContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntPredicate;

import static com.facebook.presto.spi.Page.mask;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class GenericPartitioningSpiller
        implements PartitioningSpiller
{
    private final List<Type> types;
    private final PartitionFunction partitionFunction;
    private final Closer closer = Closer.create();
    private final SingleStreamSpillerFactory spillerFactory;
    private final SpillContext spillContext;
    private final AggregatedMemoryContext memoryContext;

    private final PageBuilder[] pageBuilders;
    private final Optional<SingleStreamSpiller>[] spillers;

    private boolean readingStarted;
    private Set<Integer> spilledPartitions = new HashSet<>();

    public GenericPartitioningSpiller(
            List<Type> types,
            PartitionFunction partitionFunction,
            SpillContext spillContext,
            AggregatedMemoryContext memoryContext,
            SingleStreamSpillerFactory spillerFactory)
    {
        requireNonNull(spillContext, "spillContext is null");

        this.types = requireNonNull(types, "types is null");
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.spillContext = closer.register(requireNonNull(spillContext, "spillContext is null"));
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        closer.register(memoryContext::close);

        int partitionCount = partitionFunction.getPartitionCount();
        this.pageBuilders = new PageBuilder[partitionCount];
        //noinspection unchecked
        this.spillers = new Optional[partitionCount];

        for (int partition = 0; partition < partitionCount; partition++) {
            pageBuilders[partition] = new PageBuilder(types);
            spillers[partition] = Optional.empty();
        }
    }

    @Override
    public synchronized Iterator<Page> getSpilledPages(int partition)
    {
        readingStarted = true;
        getFutureValue(flush(partition));
        spilledPartitions.remove(partition);
        return getSpiller(partition).getSpilledPages();
    }

    @Override
    public synchronized void verifyAllPartitionsRead()
    {
        verify(spilledPartitions.isEmpty(), "Some partitions were spilled but not read: %s", spilledPartitions);
    }

    @Override
    public synchronized PartitioningSpillResult partitionAndSpill(Page page, IntPredicate spillPartitionMask)
    {
        requireNonNull(page, "page is null");
        requireNonNull(spillPartitionMask, "spillPartitionMask is null");
        checkArgument(page.getChannelCount() == types.size(), "Wrong page channel count, expected %s but got %s", types.size(), page.getChannelCount());

        checkState(!readingStarted, "reading already started");
        IntArrayList unspilledPositions = partitionPage(page, spillPartitionMask);
        ListenableFuture<?> future = flush();

        return new PartitioningSpillResult(future, mask(page, unspilledPositions.toIntArray()));
    }

    private synchronized IntArrayList partitionPage(Page page, IntPredicate spillPartitionMask)
    {
        IntArrayList unspilledPositions = new IntArrayList();

        for (int position = 0; position < page.getPositionCount(); position++) {
            int partition = partitionFunction.getPartition(page, position);

            if (!spillPartitionMask.test(partition)) {
                unspilledPositions.add(position);
                continue;
            }

            spilledPartitions.add(partition);
            PageBuilder pageBuilder = pageBuilders[partition];
            pageBuilder.declarePosition();
            for (int channel = 0; channel < types.size(); channel++) {
                Type type = types.get(channel);
                type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
            }
        }

        return unspilledPositions;
    }

    private synchronized ListenableFuture<?> flush()
    {
        ImmutableList.Builder<ListenableFuture<?>> futures = ImmutableList.builder();

        for (int partition = 0; partition < spillers.length; partition++) {
            PageBuilder pageBuilder = pageBuilders[partition];
            if (pageBuilder.isFull()) {
                futures.add(flush(partition));
            }
        }

        return Futures.allAsList(futures.build());
    }

    private synchronized ListenableFuture<?> flush(int partition)
    {
        PageBuilder pageBuilder = pageBuilders[partition];
        if (pageBuilder.isEmpty()) {
            return Futures.immediateFuture(null);
        }
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return getSpiller(partition).spill(page);
    }

    private synchronized SingleStreamSpiller getSpiller(int partition)
    {
        Optional<SingleStreamSpiller> spiller = spillers[partition];
        if (!spiller.isPresent()) {
            spiller = Optional.of(closer.register(
                    spillerFactory.create(types, spillContext.newLocalSpillContext(), memoryContext.newLocalMemoryContext())
            ));
            spillers[partition] = spiller;
        }
        return spiller.get();
    }

    @Override
    public synchronized void close()
            throws IOException
    {
        closer.close();
    }
}
