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
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DistinctLimitOperator
        implements Operator
{
    public static class DistinctLimitOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Integer> distinctChannels;
        private final List<Type> types;
        private final long limit;
        private final Optional<Integer> hashChannel;
        private boolean closed;

        public DistinctLimitOperatorFactory(int operatorId, List<? extends Type> types, List<Integer> distinctChannels, long limit, Optional<Integer> hashChannel)
        {
            this.operatorId = operatorId;
            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
            this.distinctChannels = checkNotNull(distinctChannels, "distinctChannels is null");

            checkArgument(limit >= 0, "limit must be at least zero");
            this.limit = limit;
            this.hashChannel = checkNotNull(hashChannel, "hashChannel is null");
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, DistinctLimitOperator.class.getSimpleName());
            return new DistinctLimitOperator(operatorContext, types, distinctChannels, limit, hashChannel);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;

    private final PageBuilder pageBuilder;
    private Page outputPage;
    private long remainingLimit;

    private boolean finishing;

    private final GroupByHash groupByHash;
    private long nextDistinctId;

    public DistinctLimitOperator(OperatorContext operatorContext, List<Type> types, List<Integer> distinctChannels, long limit, Optional<Integer> hashChannel)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        checkNotNull(distinctChannels, "distinctChannels is null");
        checkArgument(limit >= 0, "limit must be at least zero");
        checkNotNull(hashChannel, "hashChannel is null");

        ImmutableList.Builder<Type> distinctTypes = ImmutableList.builder();
        for (int channel : distinctChannels) {
            distinctTypes.add(types.get(channel));
        }
        this.groupByHash = new GroupByHash(distinctTypes.build(), Ints.toArray(distinctChannels), hashChannel, Math.min((int) limit, 10_000));
        this.pageBuilder = new PageBuilder(types);
        remainingLimit = limit;
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
        pageBuilder.reset();
    }

    @Override
    public boolean isFinished()
    {
        return (finishing && outputPage == null) || (remainingLimit == 0 && outputPage == null);
    }

    @Override
    public boolean needsInput()
    {
        operatorContext.setMemoryReservation(groupByHash.getEstimatedSize());
        return !finishing && remainingLimit > 0 && outputPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput());
        operatorContext.setMemoryReservation(groupByHash.getEstimatedSize());

        pageBuilder.reset();

        GroupByIdBlock ids = groupByHash.getGroupIds(page);
        for (int position = 0; position < ids.getPositionCount(); position++) {
            if (ids.getGroupId(position) == nextDistinctId) {
                pageBuilder.declarePosition();
                for (int channel = 0; channel < types.size(); channel++) {
                    Type type = types.get(channel);
                    type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
                }
                remainingLimit--;
                nextDistinctId++;
                if (remainingLimit == 0) {
                    break;
                }
            }
        }
        if (!pageBuilder.isEmpty()) {
            outputPage = pageBuilder.build();
        }
    }

    @Override
    public Page getOutput()
    {
        Page result = outputPage;
        outputPage = null;
        return result;
    }
}
