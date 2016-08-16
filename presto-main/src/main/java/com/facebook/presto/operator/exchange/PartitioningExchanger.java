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

import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;

import static java.util.Objects.requireNonNull;

class PartitioningExchanger
        implements Consumer<Page>
{
    private final List<Consumer<PageReference>> buffers;
    private final LongConsumer memoryTracker;
    private final PartitionFunction partitionFunction;
    private final Function<Page, Page> functionArgumentExtractor;
    private final IntList[] partitionAssignments;

    public PartitioningExchanger(
            List<Consumer<PageReference>> partitions,
            LongConsumer memoryTracker,
            PartitionFunction partitionFunction,
            Function<Page, Page> functionArgumentExtractor)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        this.memoryTracker = requireNonNull(memoryTracker, "memoryTracker is null");

        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.functionArgumentExtractor = requireNonNull(functionArgumentExtractor, "functionArgumentExtractor is null");

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
        Page functionArguments = functionArgumentExtractor.apply(page);
        for (int position = 0; position < functionArguments.getPositionCount(); position++) {
            int partition = partitionFunction.getPartition(functionArguments, position);
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
                memoryTracker.accept(pageSplit.getRetainedSizeInBytes());
                buffers.get(partition).accept(new PageReference(pageSplit, 1, () -> memoryTracker.accept(-pageSplit.getRetainedSizeInBytes())));
            }
        }
    }
}
