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
package io.prestosql.operator.exchange;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.operator.HashGenerator;
import io.prestosql.operator.InterpretedHashGenerator;
import io.prestosql.operator.PrecomputedHashGenerator;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

class PartitioningExchanger
        implements LocalExchanger
{
    private final List<Consumer<PageReference>> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final LocalPartitionGenerator partitionGenerator;
    private final IntArrayList[] partitionAssignments;

    public PartitioningExchanger(
            List<Consumer<PageReference>> partitions,
            LocalExchangeMemoryManager memoryManager,
            List<? extends Type> types,
            List<Integer> partitionChannels,
            Optional<Integer> hashChannel)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");

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

        partitionAssignments = new IntArrayList[partitions.size()];
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
            int partition = partitionGenerator.getPartition(page, position);
            partitionAssignments[partition].add(position);
        }

        // build a page for each partition
        Block[] outputBlocks = new Block[page.getChannelCount()];
        for (int partition = 0; partition < buffers.size(); partition++) {
            IntArrayList positions = partitionAssignments[partition];
            if (!positions.isEmpty()) {
                for (int i = 0; i < page.getChannelCount(); i++) {
                    outputBlocks[i] = page.getBlock(i).copyPositions(positions.elements(), 0, positions.size());
                }

                Page pageSplit = new Page(positions.size(), outputBlocks);
                memoryManager.updateMemoryUsage(pageSplit.getRetainedSizeInBytes());
                buffers.get(partition).accept(new PageReference(pageSplit, 1, () -> memoryManager.updateMemoryUsage(-pageSplit.getRetainedSizeInBytes())));
            }
        }
    }

    @Override
    public ListenableFuture<?> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
