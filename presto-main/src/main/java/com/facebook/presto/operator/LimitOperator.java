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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockBuilderStatus;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.type.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.presto.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class LimitOperator
        implements Operator
{
    public static class LimitOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Type> types;
        private final long limit;
        private final Optional<Integer> sampleWeightChannel;
        private boolean closed;

        public LimitOperatorFactory(int operatorId, List<? extends Type> types, long limit, Optional<Integer> sampleWeightChannel)
        {
            this.operatorId = operatorId;
            this.types = ImmutableList.copyOf(types);
            this.limit = limit;
            this.sampleWeightChannel = sampleWeightChannel;
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, LimitOperator.class.getSimpleName());
            return new LimitOperator(operatorContext, types, limit, sampleWeightChannel);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final Optional<Integer> sampleWeightChannel;
    private Page nextPage;
    private long remainingLimit;

    public LimitOperator(OperatorContext operatorContext, List<Type> types, long limit, Optional<Integer> sampleWeightChannel)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.types = checkNotNull(types, "types is null");

        checkArgument(limit >= 0, "limit must be at least zero");
        this.remainingLimit = limit;
        this.sampleWeightChannel = checkNotNull(sampleWeightChannel, "sampleWeightChannel is null");
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
        remainingLimit = 0;
    }

    @Override
    public boolean isFinished()
    {
        return remainingLimit == 0 && nextPage == null;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return remainingLimit > 0 && nextPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput());

        if (sampleWeightChannel.isPresent()) {
            addInputWithSampling(page, sampleWeightChannel.get());
        }
        else {
            addInputWithoutSampling(page);
        }
    }

    private void addInputWithoutSampling(Page page)
    {
        if (page.getPositionCount() <= remainingLimit) {
            remainingLimit -= page.getPositionCount();
            nextPage = page;
        }
        else {
            Block[] blocks = new Block[page.getChannelCount()];
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                blocks[channel] = block.getRegion(0, (int) remainingLimit);
            }
            nextPage = new Page((int) remainingLimit, blocks);
            remainingLimit = 0;
        }
    }

    private void addInputWithSampling(Page page, int sampleWeightChannel)
    {
        BlockCursor cursor = page.getBlock(sampleWeightChannel).cursor();
        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus());

        int rowsToCopy = 0;
        // Build the sample weight block, and count how many rows of data to copy
        while (remainingLimit > 0 && cursor.advanceNextPosition()) {
            rowsToCopy++;
            long sampleWeight = cursor.getLong();
            if (sampleWeight <= remainingLimit) {
                builder.append(sampleWeight);
            }
            else {
                builder.append(remainingLimit);
            }
            remainingLimit -= sampleWeight;
        }

        if (remainingLimit >= 0 && rowsToCopy == page.getPositionCount()) {
            nextPage = page;
        }
        else {
            Block[] blocks = new Block[page.getChannelCount()];
            blocks[sampleWeightChannel] = builder.build();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                if (channel == sampleWeightChannel) {
                    continue;
                }
                Block block = page.getBlock(channel);
                blocks[channel] = block.getRegion(0, rowsToCopy);
            }
            nextPage = new Page(rowsToCopy, blocks);
            remainingLimit = 0;
        }
    }

    @Override
    public Page getOutput()
    {
        Page page = nextPage;
        nextPage = null;
        return page;
    }
}
