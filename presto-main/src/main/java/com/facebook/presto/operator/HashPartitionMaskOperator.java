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
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.XxHash64;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.abs;

public class HashPartitionMaskOperator
        implements Operator
{
    public static class HashPartitionMaskOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final int partitionCount;
        private final Optional<Integer> hashChannel;
        private final int[] maskChannels;
        private final int[] partitionChannels;
        private final List<Type> types;
        private int partition;
        private boolean closed;

        public HashPartitionMaskOperatorFactory(
                int operatorId,
                int partitionCount,
                List<? extends Type> sourceTypes,
                Collection<Integer> maskChannels,
                Collection<Integer> partitionChannels,
                Optional<Integer> hashChannel)
        {
            this.operatorId = operatorId;
            checkArgument(partitionCount > 1, "partition count must be greater than 1");
            this.partitionCount = partitionCount;
            checkNotNull(maskChannels, "maskChannels is null");
            this.maskChannels = Ints.toArray(maskChannels);

            checkNotNull(partitionChannels, "partitionChannels is null");
            checkArgument(!partitionChannels.isEmpty(), "partitionChannels is empty");
            this.partitionChannels = Ints.toArray(partitionChannels);

            this.hashChannel = checkNotNull(hashChannel, "hashChannel is null");

            this.types = ImmutableList.<Type>builder()
                    .addAll(sourceTypes)
                    .add(BOOLEAN)
                    .build();
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        public int getDefaultMaskChannel()
        {
            // default mask is in the last channel
            return types.size() - 1;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            checkState(partition < partitionCount, "All operators already created");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, MarkDistinctOperator.class.getSimpleName());
            return new HashPartitionMaskOperator(operatorContext, partition++, partitionCount, types, maskChannels, partitionChannels, hashChannel);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final int partition;
    private final int partitionCount;
    private final List<Type> types;
    private final int[] maskChannels;
    private final HashGenerator hashGenerator;

    private Page outputPage;
    private boolean finishing;

    public HashPartitionMaskOperator(OperatorContext operatorContext,
            int partition,
            int partitionCount,
            List<Type> types,
            int[] maskChannels,
            int[] partitionChannels,
            Optional<Integer> hashChannel)
    {
        this.partition = partition;
        this.partitionCount = partitionCount;
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.maskChannels = maskChannels;

        checkNotNull(hashChannel, "hashChannel is null");

        ImmutableList.Builder<Type> distinctTypes = ImmutableList.builder();
        for (int channel : partitionChannels) {
            distinctTypes.add(types.get(channel));
        }

        ImmutableList.Builder<Type> partitionChannelTypes = ImmutableList.builder();
        for (int channel : partitionChannels) {
            partitionChannelTypes.add(types.get(channel));
        }

        if (hashChannel.isPresent()) {
            this.hashGenerator = new PrecomputedHashGenerator(hashChannel.get());
        }
        else {
            this.hashGenerator = new InterpretedHashGenerator(partitionChannelTypes.build(), partitionChannels);
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
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && outputPage == null;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing || outputPage != null) {
            return false;
        }
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(outputPage == null, "Operator still has pending output");

        BlockBuilder activePositions = BOOLEAN.createBlockBuilder(new BlockBuilderStatus(), page.getPositionCount());
        BlockBuilder[] maskBuilders = new BlockBuilder[maskChannels.length];
        for (int i = 0; i < maskBuilders.length; i++) {
            maskBuilders[i] = BOOLEAN.createBlockBuilder(new BlockBuilderStatus(), page.getPositionCount());
        }
        for (int position = 0; position < page.getPositionCount(); position++) {
            int rawHash = hashGenerator.hashPosition(position, page);
            // mix the bits so we don't use the same hash used to distribute between stages
            rawHash = abs((int) XxHash64.hash(Integer.reverse(rawHash)));

            boolean active = (rawHash % partitionCount == partition);
            BOOLEAN.writeBoolean(activePositions, active);

            for (int i = 0; i < maskBuilders.length; i++) {
                Block maskBlock = page.getBlock(maskChannels[i]);
                if (maskBlock.isNull(position)) {
                    maskBuilders[i].appendNull();
                }
                else {
                    boolean maskValue = active && BOOLEAN.getBoolean(maskBlock, position);
                    BOOLEAN.writeBoolean(maskBuilders[i], maskValue);
                }
            }
        }

        // build output page
        Block[] sourceBlocks = page.getBlocks();
        Block[] outputBlocks = new Block[sourceBlocks.length + 1]; // +1 for the single boolean output channel
        System.arraycopy(sourceBlocks, 0, outputBlocks, 0, sourceBlocks.length);

        // add the new boolean column to the page
        outputBlocks[sourceBlocks.length] = activePositions.build();

        // replace mask blocks
        for (int i = 0; i < maskBuilders.length; i++) {
            outputBlocks[maskChannels[i]] = maskBuilders[i].build();
        }

        outputPage = new Page(outputBlocks);
    }

    @Override
    public Page getOutput()
    {
        Page result = outputPage;
        outputPage = null;
        return result;
    }
}
