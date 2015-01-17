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
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class MarkDistinctOperator
        implements Operator
{
    public static class MarkDistinctOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final Optional<Integer> hashChannel;
        private final int[] markDistinctChannels;
        private final List<Type> types;
        private boolean closed;

        public MarkDistinctOperatorFactory(int operatorId, List<? extends Type> sourceTypes, Collection<Integer> markDistinctChannels, Optional<Integer> hashChannel)
        {
            this.operatorId = operatorId;
            checkNotNull(markDistinctChannels, "markDistinctChannels is null");
            checkArgument(!markDistinctChannels.isEmpty(), "markDistinctChannels is empty");
            this.markDistinctChannels = Ints.toArray(markDistinctChannels);
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

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, MarkDistinctOperator.class.getSimpleName());
            return new MarkDistinctOperator(operatorContext, types, markDistinctChannels, hashChannel);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final MarkDistinctHash markDistinctHash;

    private Page outputPage;
    private boolean finishing;

    public MarkDistinctOperator(OperatorContext operatorContext, List<Type> types, int[] markDistinctChannels, Optional<Integer> hashChannel)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        checkArgument(markDistinctChannels.length >= 0, "markDistinctChannels is empty");
        checkNotNull(hashChannel, "hashChannel is null");

        ImmutableList.Builder<Type> distinctTypes = ImmutableList.builder();
        for (int channel : markDistinctChannels) {
            distinctTypes.add(types.get(channel));
        }
        this.markDistinctHash = new MarkDistinctHash(distinctTypes.build(), markDistinctChannels, hashChannel);
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
        operatorContext.setMemoryReservation(markDistinctHash.getEstimatedSize());
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
        operatorContext.setMemoryReservation(markDistinctHash.getEstimatedSize());

        Block markerBlock = markDistinctHash.markDistinctRows(page);

        // add the new boolean column to the page
        Block[] sourceBlocks = page.getBlocks();
        Block[] outputBlocks = new Block[sourceBlocks.length + 1]; // +1 for the single boolean output channel

        System.arraycopy(sourceBlocks, 0, outputBlocks, 0, sourceBlocks.length);
        outputBlocks[sourceBlocks.length] = markerBlock;

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
