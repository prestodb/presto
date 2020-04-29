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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.operator.PartitionFunction;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

class PartitioningExchanger
        implements LocalExchanger
{
    private final List<Consumer<PageReference>> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final PartitionFunction partitionFunction;
    private final List<Integer> partitioningChannels;
    private final Optional<Integer> hashChannel;
    private final IntArrayList[] partitionAssignments;

    public PartitioningExchanger(
            List<Consumer<PageReference>> partitions,
            LocalExchangeMemoryManager memoryManager,
            PartitionFunction partitionFunction,
            List<Integer> partitioningChannels,
            Optional<Integer> hashChannel)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.partitioningChannels = ImmutableList.copyOf(requireNonNull(partitioningChannels, "partitioningChannels is null"));
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");

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
        Page partitioningChannelsPage = extractPartitioningChannels(page);
        for (int position = 0; position < partitioningChannelsPage.getPositionCount(); position++) {
            int partition = partitionFunction.getPartition(partitioningChannelsPage, position);
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

    private Page extractPartitioningChannels(Page inputPage)
    {
        // hash value is pre-computed, only needs to extract that channel
        if (hashChannel.isPresent()) {
            return new Page(inputPage.getBlock(hashChannel.get()));
        }

        // extract partitioning channels
        Block[] blocks = new Block[partitioningChannels.size()];
        for (int i = 0; i < partitioningChannels.size(); i++) {
            blocks[i] = inputPage.getBlock(partitioningChannels.get(i));
        }
        return new Page(inputPage.getPositionCount(), blocks);
    }

    @Override
    public ListenableFuture<?> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
