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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

class PartitioningExchanger
        implements Consumer<Page>
{
    private final List<Consumer<PageReference>> buffers;
    private final LongConsumer memoryTracker;
    private final LocalPartitionGenerator partitionGenerator;
    private final IntList[] partitionAssignments;

    public PartitioningExchanger(
            List<Consumer<PageReference>> partitions,
            LongConsumer memoryTracker,
            List<? extends Type> types,
            List<Integer> partitionChannels,
            Optional<Integer> hashChannel)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        this.memoryTracker = requireNonNull(memoryTracker, "memoryTracker is null");

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

        partitionAssignments = new IntList[partitions.size()];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
        }
    }

    @Override
    public synchronized void accept(Page page)
    {
        // reset the assignment lists
        for (IntList partitionAssignment : partitionAssignments) {
            partitionAssignment.clear();
        }

        // assign each row to a partition
        for (int position = 0; position < page.getPositionCount(); position++) {
            int partition = partitionGenerator.getPartition(position, page);
            partitionAssignments[partition].add(position);
        }

        // build a page for each partition
        Block[] sourceBlocks = page.getBlocks();
        Block[] outputBlocks = new Block[sourceBlocks.length];
        for (int partition = 0; partition < buffers.size(); partition++) {
            List<Integer> positions = partitionAssignments[partition];
            if (!positions.isEmpty()) {
                for (int i = 0; i < sourceBlocks.length; i++) {
                    outputBlocks[i] = sourceBlocks[i].copyPositions(positions);
                }

                Page pageSplit = new Page(positions.size(), outputBlocks);
                memoryTracker.accept(pageSplit.getSizeInBytes());
                buffers.get(partition).accept(new PageReference(pageSplit, 1, () -> memoryTracker.accept(-pageSplit.getSizeInBytes())));
            }
        }
    }
}
