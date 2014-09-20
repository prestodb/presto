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
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RowNumberLimitOperator
        implements Operator
{
    public static class RowNumberLimitOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final List<Integer> partitionChannels;
        private final int expectedPositions;
        private final List<Type> types;
        private boolean closed;

        private final List<Type> partitionTypes;
        private final int maxRowCountPerPartition;

        public RowNumberLimitOperatorFactory(
                int operatorId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<Integer> partitionChannels,
                List<? extends Type> partitionTypes,
                int expectedPositions,
                int maxRowCountPerPartition)
        {
            this.operatorId = operatorId;
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(checkNotNull(outputChannels, "outputChannels is null"));
            this.partitionChannels = ImmutableList.copyOf(checkNotNull(partitionChannels, "partitionChannels is null"));
            this.partitionTypes = ImmutableList.copyOf(checkNotNull(partitionTypes, "partitionTypes is null"));

            checkArgument(expectedPositions > 0, "expectedPositions < 0");
            this.expectedPositions = expectedPositions;
            checkArgument(maxRowCountPerPartition > 0, "maxRowCountPerPartition < 0");
            this.maxRowCountPerPartition = maxRowCountPerPartition;
            this.types = toTypes(sourceTypes, outputChannels);
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

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, RowNumberLimitOperator.class.getSimpleName());
            return new RowNumberLimitOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    partitionChannels,
                    partitionTypes,
                    expectedPositions,
                    maxRowCountPerPartition);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private boolean finishing;

    private final PageBuilder pageBuilder;
    private final int[] outputChannels;
    private final List<Type> types;

    private GroupByIdBlock partitionIds;
    private final GroupByHash groupByHash;

    private Page inputPage;
    private int currentPosition;
    private final int maxRowCountPerPartition;
    private final LongBigArray partitionRowCount;

    public RowNumberLimitOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            List<Integer> partitionChannels,
            List<Type> partitionTypes,
            int expectedPositions,
            int maxRowCountPerPartition)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.outputChannels = Ints.toArray(outputChannels);
        this.maxRowCountPerPartition = maxRowCountPerPartition;

        this.partitionRowCount = new LongBigArray(0);
        this.groupByHash = new GroupByHash(partitionTypes, Ints.toArray(partitionChannels), expectedPositions);
        this.types = toTypes(sourceTypes, outputChannels);
        this.pageBuilder = new PageBuilder(types);
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
        return finishing && pageBuilder.isEmpty();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && inputPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkNotNull(page, "page is null");
        checkState(inputPage == null);
        inputPage = page;
        partitionIds = groupByHash.getGroupIds(inputPage);
        partitionRowCount.ensureCapacity(partitionIds.getGroupCount());
        currentPosition = 0;
    }

    @Override
    public Page getOutput()
    {
        if (inputPage == null) {
            return null;
        }

        while (!pageBuilder.isFull() && currentPosition < inputPage.getPositionCount()) {
            long partitionId = partitionIds.getGroupId(currentPosition);
            long rowCount = partitionRowCount.get(partitionId);
            if (rowCount < maxRowCountPerPartition) {
                for (int i = 0; i < outputChannels.length; i++) {
                    int channel = outputChannels[i];
                    Type type = types.get(channel);
                    type.appendTo(inputPage.getBlock(channel), currentPosition, pageBuilder.getBlockBuilder(i));
                }
                BIGINT.writeLong(pageBuilder.getBlockBuilder(types.size() - 1), rowCount + 1);
                partitionRowCount.set(partitionId, rowCount + 1);
            }
            currentPosition++;
        }
        if (currentPosition == inputPage.getPositionCount()) {
            inputPage = null;
            currentPosition = 0;
        }

        if (pageBuilder.isEmpty()) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private static List<Type> toTypes(List<? extends Type> sourceTypes, List<Integer> outputChannels)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (int channel : outputChannels) {
            types.add(sourceTypes.get(channel));
        }
        types.add(BIGINT);
        return types.build();
    }
}
