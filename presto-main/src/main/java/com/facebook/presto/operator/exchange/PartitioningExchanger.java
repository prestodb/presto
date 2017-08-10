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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.PrecomputedHashGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

class PartitioningExchanger
        implements Exchanger
{
    private final List<Consumer<PageReference>> buffers;
    private final LongConsumer memoryTracker;
    private final LocalPartitionGenerator partitionGenerator;
    private final List<PageBuilder> pageBuilders;
    private final List<? extends Type> types;

    public PartitioningExchanger(
            List<Consumer<PageReference>> partitions,
            LongConsumer memoryTracker,
            List<? extends Type> types,
            List<Integer> partitionChannels,
            Optional<Integer> hashChannel)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        this.memoryTracker = requireNonNull(memoryTracker, "memoryTracker is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

        HashGenerator hashGenerator;
        if (hashChannel.isPresent()) {
            hashGenerator = new PrecomputedHashGenerator(hashChannel.get());
        }
        else {
            List<Type> partitionChannelTypes = partitionChannels.stream()
                    .map(types::get)
                    .collect(toImmutableList());
            hashGenerator = new InterpretedHashGenerator(partitionChannelTypes, Ints.toArray(partitionChannels));
        }
        partitionGenerator = new LocalPartitionGenerator(hashGenerator, buffers.size());

        pageBuilders = IntStream.range(0, buffers.size())
                .mapToObj(i -> new PageBuilder(types))
                .collect(toImmutableList());
    }

    @Override
    public synchronized void exchange(Page page)
    {
        long initialRetainedSize = getRetainedSizeInBytes();
        for (int position = 0; position < page.getPositionCount(); position++) {
            int partition = partitionGenerator.getPartition(position, page);
            PageBuilder pageBuilder = pageBuilders.get(partition);
            appendRow(pageBuilder, page, position);
        }
        flush(false);
        long finalRetainedSize = getRetainedSizeInBytes();
        memoryTracker.accept(finalRetainedSize - initialRetainedSize);
    }

    @Override
    public void flush()
    {
        long initialRetainedSize = getRetainedSizeInBytes();
        flush(true);
        long finalRetainedSize = getRetainedSizeInBytes();
        memoryTracker.accept(finalRetainedSize - initialRetainedSize);
    }

    private void appendRow(PageBuilder pageBuilder, Page page, int position)
    {
        pageBuilder.declarePosition();

        for (int channel = 0; channel < types.size(); channel++) {
            Type type = types.get(channel);
            type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
        }
    }

    private void flush(boolean force)
    {
        for (int partition = 0; partition < pageBuilders.size(); partition++) {
            PageBuilder partitionPageBuilder = pageBuilders.get(partition);
            if (!partitionPageBuilder.isEmpty() && (force || partitionPageBuilder.isFull())) {
                Page pageSplit = partitionPageBuilder.build();
                partitionPageBuilder.reset();
                memoryTracker.accept(pageSplit.getRetainedSizeInBytes());
                buffers.get(partition).accept(new PageReference(pageSplit, 1, () -> memoryTracker.accept(-pageSplit.getRetainedSizeInBytes())));
            }
        }
    }

    private long getRetainedSizeInBytes()
    {
        return pageBuilders.stream()
                .mapToLong(PageBuilder::getRetainedSizeInBytes)
                .sum();
    }
}
