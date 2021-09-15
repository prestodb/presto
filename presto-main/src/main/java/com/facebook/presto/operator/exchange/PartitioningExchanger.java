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
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.exchange.PageReference.PageReleasedListener;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayList;

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
    private final int[] partitioningChannels;
    private final int hashChannel; // when >= 0, this is the precomputed raw hash column to partition on
    private final IntArrayList[] partitionAssignments;
    private final PageReleasedListener onPageReleased;

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
        this.partitioningChannels = Ints.toArray(requireNonNull(partitioningChannels, "partitioningChannels is null"));
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null").orElse(-1);
        this.onPageReleased = PageReleasedListener.forLocalExchangeMemoryManager(memoryManager);

        partitionAssignments = new IntArrayList[partitions.size()];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
        }
    }

    @Override
    public void accept(Page page)
    {
        // extract the partitioning channel before entering the critical section
        partitionPage(page, extractPartitioningChannels(page));
    }

    private synchronized void partitionPage(Page page, Page partitioningChannelsPage)
    {
        // assign each row to a partition. The assignments lists are all expected to cleared by the previous iterations
        for (int position = 0; position < partitioningChannelsPage.getPositionCount(); position++) {
            int partition = partitionFunction.getPartition(partitioningChannelsPage, position);
            partitionAssignments[partition].add(position);
        }

        // build a page for each partition
        for (int partition = 0; partition < partitionAssignments.length; partition++) {
            IntArrayList positions = partitionAssignments[partition];
            int partitionSize = positions.size();
            if (partitionSize == 0) {
                continue;
            }
            Page pageSplit;
            if (partitionSize == page.getPositionCount()) {
                pageSplit = page; // entire page will be sent to this partition, no copies necessary
            }
            else {
                pageSplit = page.copyPositions(positions.elements(), 0, partitionSize);
            }
            // clear the assigned positions list for the next iteration to start empty
            positions.clear();
            memoryManager.updateMemoryUsage(pageSplit.getRetainedSizeInBytes());
            buffers.get(partition).accept(new PageReference(pageSplit, 1, onPageReleased));
        }
    }

    private Page extractPartitioningChannels(Page inputPage)
    {
        // hash value is pre-computed, only needs to extract that channel
        if (hashChannel >= 0) {
            return inputPage.extractChannel(hashChannel);
        }

        // extract partitioning channels
        return inputPage.extractChannels(partitioningChannels);
    }

    @Override
    public ListenableFuture<?> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
