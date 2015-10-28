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
package com.facebook.presto.operator;

import com.facebook.presto.execution.SharedBuffer;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static java.util.Objects.requireNonNull;

public class PartitionedOutputOperator
        implements Operator
{
    public static class PartitionedOutputFactory
            implements OutputFactory
    {
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final SharedBuffer sharedBuffer;
        private final OptionalInt nullChannel;

        public PartitionedOutputFactory(
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                OptionalInt nullChannel,
                SharedBuffer sharedBuffer)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.sharedBuffer = requireNonNull(sharedBuffer, "sharedBuffer is null");
        }

        @Override
        public OperatorFactory createOutputOperator(int operatorId, List<Type> sourceTypes)
        {
            return new PartitionedOutputOperatorFactory(operatorId, sourceTypes, partitionFunction, partitionChannels, nullChannel, sharedBuffer);
        }
    }

    public static class PartitionedOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Type> sourceTypes;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final OptionalInt nullChannel;
        private final SharedBuffer sharedBuffer;

        public PartitionedOutputOperatorFactory(
                int operatorId,
                List<Type> sourceTypes,
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                OptionalInt nullChannel,
                SharedBuffer sharedBuffer)
        {
            this.operatorId = operatorId;
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.sharedBuffer = requireNonNull(sharedBuffer, "sharedBuffer is null");
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, PartitionedOutputOperator.class.getSimpleName());
            return new PartitionedOutputOperator(operatorContext, sourceTypes, partitionFunction, partitionChannels, nullChannel, sharedBuffer);
        }

        @Override
        public void close()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PartitionedOutputOperatorFactory(operatorId, sourceTypes, partitionFunction, partitionChannels, nullChannel, sharedBuffer);
        }
    }

    private final OperatorContext operatorContext;
    private final ListenableFuture<PagePartitioner> partitionFunction;
    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private boolean finished;

    public PartitionedOutputOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            PartitionFunction partitionFunction,
            List<Integer> partitionChannels,
            OptionalInt nullChannel,
            SharedBuffer sharedBuffer)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.partitionFunction = Futures.immediateFuture(new PagePartitioner(partitionFunction, partitionChannels, nullChannel, sharedBuffer, sourceTypes));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.of();
    }

    @Override
    public void finish()
    {
        finished = true;
        blocked = getUnchecked(partitionFunction).flush(true);
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!partitionFunction.isDone()) {
            return partitionFunction;
        }
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(isBlocked().isDone(), "output is already blocked");

        if (page.getPositionCount() == 0) {
            return;
        }

        blocked = getUnchecked(partitionFunction).partitionPage(page);

        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    private static class PagePartitioner
    {
        private final SharedBuffer sharedBuffer;
        private final List<Type> sourceTypes;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<PageBuilder> pageBuilders;
        private final OptionalInt nullChannel; // when present, send the position to every partition if this channel is null.

        public PagePartitioner(
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                OptionalInt nullChannel,
                SharedBuffer sharedBuffer,
                List<Type> sourceTypes)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.sharedBuffer = requireNonNull(sharedBuffer, "sharedBuffer is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");

            ImmutableList.Builder<PageBuilder> pageBuilders = ImmutableList.builder();
            for (int i = 0; i < partitionFunction.getPartitionCount(); i++) {
                pageBuilders.add(new PageBuilder(sourceTypes));
            }
            this.pageBuilders = pageBuilders.build();
        }

        public ListenableFuture<?> partitionPage(Page page)
        {
            requireNonNull(page, "page is null");

            Page partitionFunctionArgs = getPartitionFunctionArguments(page);
            for (int position = 0; position < page.getPositionCount(); position++) {
                if (nullChannel.isPresent() && page.getBlock(nullChannel.getAsInt()).isNull(position)) {
                    for (PageBuilder pageBuilder : pageBuilders) {
                        pageBuilder.declarePosition();

                        for (int channel = 0; channel < sourceTypes.size(); channel++) {
                            Type type = sourceTypes.get(channel);
                            type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
                        }
                    }
                }
                else {
                    int partition = partitionFunction.getPartition(partitionFunctionArgs, position);

                    PageBuilder pageBuilder = pageBuilders.get(partition);
                    pageBuilder.declarePosition();

                    for (int channel = 0; channel < sourceTypes.size(); channel++) {
                        Type type = sourceTypes.get(channel);
                        type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
                    }
                }
            }
            return flush(false);
        }

        private Page getPartitionFunctionArguments(Page page)
        {
            Block[] blocks = new Block[partitionChannels.size()];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = page.getBlock(partitionChannels.get(i));
            }
            return new Page(page.getPositionCount(), blocks);
        }

        public ListenableFuture<?> flush(boolean force)
        {
            // add all full pages to output buffer
            List<ListenableFuture<?>> blockedFutures = new ArrayList<>();
            for (int partition = 0; partition < pageBuilders.size(); partition++) {
                PageBuilder partitionPageBuilder = pageBuilders.get(partition);
                if (!partitionPageBuilder.isEmpty() && (force || partitionPageBuilder.isFull())) {
                    Page pagePartition = partitionPageBuilder.build();
                    partitionPageBuilder.reset();

                    blockedFutures.add(sharedBuffer.enqueue(partition, pagePartition));
                }
            }
            ListenableFuture<?> future = Futures.allAsList(blockedFutures);
            if (future.isDone()) {
                return NOT_BLOCKED;
            }
            return future;
        }
    }
}
