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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ParallelHashBuilder
{
    private final List<Integer> hashChannels;
    private final Optional<Integer> hashChannel;
    private final int expectedPositions;
    private final List<SettableFuture<PagesIndex>> pagesIndexFutures;
    private final List<SettableFuture<SharedLookupSource>> lookupSourceFutures;
    private final LookupSourceSupplier lookupSourceSupplier;
    private final List<Type> types;

    public ParallelHashBuilder(
            List<Type> types,
            List<Integer> hashChannels,
            Optional<Integer> hashChannel,
            int expectedPositions,
            int partitionCount)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        checkArgument(expectedPositions >= 0, "expectedPositions is negative");
        this.expectedPositions = expectedPositions;

        checkArgument(Integer.bitCount(partitionCount) == 1, "partitionCount must be a power of 2");
        ImmutableList.Builder<SettableFuture<PagesIndex>> pagesIndexFutures = ImmutableList.builder();
        ImmutableList.Builder<SettableFuture<SharedLookupSource>> lookupSourceFutures = ImmutableList.builder();
        for (int i = 0; i < partitionCount; i++) {
            pagesIndexFutures.add(SettableFuture.create());
            lookupSourceFutures.add(SettableFuture.create());
        }
        this.pagesIndexFutures = pagesIndexFutures.build();
        this.lookupSourceFutures = lookupSourceFutures.build();

        lookupSourceSupplier = new ParallelLookupSourceSupplier(types, hashChannels, this.lookupSourceFutures);
    }

    public OperatorFactory getCollectOperatorFactory(int operatorId, PlanNodeId planNodeId)
    {
        return new ParallelHashCollectOperatorFactory(
                operatorId,
                planNodeId,
                pagesIndexFutures,
                types,
                hashChannels,
                hashChannel,
                expectedPositions);
    }

    public OperatorFactory getBuildOperatorFactory(PlanNodeId planNodeId)
    {
        return new ParallelHashBuilderOperatorFactory(
                0,
                planNodeId,
                types,
                pagesIndexFutures,
                lookupSourceFutures,
                hashChannels,
                hashChannel);
    }

    public LookupSourceSupplier getLookupSourceSupplier()
    {
        return lookupSourceSupplier;
    }

    private static class ParallelHashCollectOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<SettableFuture<PagesIndex>> partitionFutures;
        private final List<Type> types;
        private final List<Integer> hashChannels;
        private final Optional<Integer> hashChannel;

        private final int expectedPositions;
        private boolean closed;

        public ParallelHashCollectOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<SettableFuture<PagesIndex>> partitionFutures,
                List<Type> types,
                List<Integer> hashChannels,
                Optional<Integer> hashChannel,
                int expectedPositions)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.partitionFutures = partitionFutures;
            this.types = types;
            this.hashChannels = hashChannels;
            this.hashChannel = hashChannel;
            this.expectedPositions = expectedPositions;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, ParallelHashBuilder.class.getSimpleName());
            return new ParallelHashCollectOperator(
                    operatorContext,
                    partitionFutures,
                    types,
                    hashChannels,
                    hashChannel,
                    expectedPositions);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel hash collector can not be duplicated");
        }
    }

    private static class ParallelHashCollectOperator
            implements Operator
    {
        private final OperatorContext operatorContext;
        private final List<SettableFuture<PagesIndex>> partitionFutures;

        private final HashGenerator hashGenerator;
        private final int parallelStreamMask;
        private final PagesIndex[] partitions;
        private final List<Type> types;

        private boolean finished;

        public ParallelHashCollectOperator(
                OperatorContext operatorContext,
                List<SettableFuture<PagesIndex>> partitionFutures,
                List<Type> types,
                List<Integer> hashChannels,
                Optional<Integer> hashChannel,
                int expectedPositions)
        {
            this.operatorContext = operatorContext;
            this.partitionFutures = partitionFutures;

            this.types = types;

            if (hashChannel.isPresent()) {
                this.hashGenerator = new PrecomputedHashGenerator(hashChannel.get());
            }
            else {
                ImmutableList.Builder<Type> hashChannelTypes = ImmutableList.builder();
                for (int channel : hashChannels) {
                    hashChannelTypes.add(types.get(channel));
                }
                this.hashGenerator = new InterpretedHashGenerator(hashChannelTypes.build(), Ints.toArray(hashChannels));
            }

            parallelStreamMask = partitionFutures.size() - 1;
            partitions = new PagesIndex[partitionFutures.size()];
            for (int partition = 0; partition < partitions.length; partition++) {
                this.partitions[partition] = new PagesIndex(types, expectedPositions, partitions.length);
            }
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public void finish()
        {
            if (finished) {
                return;
            }

            for (int partition = 0; partition < partitions.length; partition++) {
                partitionFutures.get(partition).set(partitions[partition]);
            }

            finished = true;
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public boolean needsInput()
        {
            return !finished;
        }

        @Override
        public void addInput(Page page)
        {
            requireNonNull(page, "page is null");
            checkState(!isFinished(), "Operator is already finished");

            // build a block containing the partition id of each position
            BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), page.getPositionCount());
            for (int position = 0; position < page.getPositionCount(); position++) {
                int rawHash = hashGenerator.hashPosition(position, page);
                int partition = murmurHash3(rawHash) & parallelStreamMask;
                BIGINT.writeLong(blockBuilder, partition);
            }
            Block partitionIds = blockBuilder.build();

            long size = 0;
            for (int partition = 0; partition < partitions.length; partition++) {
                PagesIndex index = partitions[partition];
                index.addPage(page, partition, partitionIds);
                size += index.getEstimatedSize().toBytes();
            }

            operatorContext.setMemoryReservation(size);
            operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
        }

        @Override
        public Page getOutput()
        {
            return null;
        }
    }

    private static class ParallelHashBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> types;
        private final List<ListenableFuture<PagesIndex>> partitionFutures;
        private final List<SettableFuture<SharedLookupSource>> lookupSourceFutures;
        private final List<Integer> hashChannels;
        private final Optional<Integer> hashChannel;

        private int partition;
        private boolean closed;

        public ParallelHashBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                List<? extends ListenableFuture<PagesIndex>> partitionFutures,
                List<SettableFuture<SharedLookupSource>> lookupSourceFutures,
                List<Integer> hashChannels,
                Optional<Integer> hashChannel)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.types = types;
            this.partitionFutures = ImmutableList.copyOf(partitionFutures);
            this.lookupSourceFutures = lookupSourceFutures;

            this.hashChannels = hashChannels;
            this.hashChannel = hashChannel;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            checkState(partition < lookupSourceFutures.size(), "All operators already created");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, ParallelHashBuilder.class.getSimpleName());
            ParallelHashBuilderOperator parallelHashBuilderOperator = new ParallelHashBuilderOperator(
                    operatorContext,
                    types,
                    partitionFutures.get(partition),
                    lookupSourceFutures.get(partition),
                    hashChannels,
                    hashChannel);

            partition++;

            return parallelHashBuilderOperator;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel hash collector can not be duplicated");
        }
    }

    private static class ParallelHashBuilderOperator
            implements Operator
    {
        private final OperatorContext operatorContext;
        private final List<Type> types;
        private final ListenableFuture<PagesIndex> pagesIndexFuture;
        private final SettableFuture<SharedLookupSource> lookupSourceFuture;
        private final List<Integer> hashChannels;
        private final Optional<Integer> hashChannel;

        private boolean finished;

        public ParallelHashBuilderOperator(
                OperatorContext operatorContext,
                List<Type> types,
                ListenableFuture<PagesIndex> pagesIndexFuture,
                SettableFuture<SharedLookupSource> lookupSourceFuture,
                List<Integer> hashChannels,
                Optional<Integer> hashChannel)
        {
            this.operatorContext = operatorContext;
            this.types = types;
            this.pagesIndexFuture = pagesIndexFuture;
            this.lookupSourceFuture = lookupSourceFuture;

            this.hashChannels = hashChannels;
            this.hashChannel = hashChannel;
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public ListenableFuture<?> isBlocked()
        {
            if (pagesIndexFuture.isDone()) {
                return NOT_BLOCKED;
            }
            return pagesIndexFuture;
        }

        @Override
        public void finish()
        {
            if (finished) {
                return;
            }

            PagesIndex pagesIndex = Futures.getUnchecked(pagesIndexFuture);
            // After this point the SharedLookupSource will take over our memory reservation, and ours will be zero
            SharedLookupSource sharedLookupSource = new SharedLookupSource(pagesIndex.createLookupSource(hashChannels, hashChannel), operatorContext);

            if (!lookupSourceFuture.set(sharedLookupSource)) {
                sharedLookupSource.freeMemory();
                sharedLookupSource.close();
            }

            finished = true;
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public boolean needsInput()
        {
            return false;
        }

        @Override
        public void addInput(Page page)
        {
            throw new UnsupportedOperationException(getClass().getName() + " can not take input");
        }

        @Override
        public Page getOutput()
        {
            return null;
        }
    }
}
