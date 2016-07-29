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
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class EnforceUniqueColumnsOperator
        implements Operator
{
    private static final long VALUES_PER_REQUEST = 1 << 20;

    public static class UuidOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> types;
        private boolean closed;
        private final AtomicLong valuePool = new AtomicLong();

        public UuidOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> types)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.types = ImmutableList.copyOf(types);
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

            OperatorContext operatorContext = driverContext.addOperatorContext(
                    operatorId,
                    planNodeId,
                    EnforceUniqueColumnsOperator.class.getSimpleName());
            return new EnforceUniqueColumnsOperator(operatorContext, types, valuePool);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new UuidOperatorFactory(operatorId, planNodeId, types);
        }
    }

    private final OperatorContext operatorContext;
    private boolean finishing;
    private final int taskId;
    private final AtomicLong valuePool;
    private final List<Type> types;
    private final Block taskIdBlock;

    private Page inputPage;
    private long nextValue;
    private long lastValue;

    public EnforceUniqueColumnsOperator(
            OperatorContext operatorContext,
            List<Type> types,
            AtomicLong valuePool)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(types);
        this.taskId = operatorContext.getDriverContext().getTaskId().getId();
        this.valuePool = requireNonNull(valuePool, "valuePool is null");

        BlockBuilder taskIdBlockBuilder = BIGINT.createFixedSizeBlockBuilder(1);
        BIGINT.writeLong(taskIdBlockBuilder, taskId);
        taskIdBlock = taskIdBlockBuilder.build();

        requestValues();
    }

    private void requestValues()
    {
        lastValue = valuePool.addAndGet(VALUES_PER_REQUEST);
        nextValue = lastValue - VALUES_PER_REQUEST;
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
        return finishing && inputPage == null;
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
        requireNonNull(page, "page is null");
        checkState(inputPage == null);
        inputPage = page;
    }

    @Override
    public Page getOutput()
    {
        if (inputPage == null) {
            return null;
        }

        Page outputPage = getRowsWithUniqueColumns();
        inputPage = null;
        return outputPage;
    }

    private Page getRowsWithUniqueColumns()
    {
        int inputPageChannelCount = inputPage.getChannelCount();
        Block[] outputBlocks = new Block[inputPageChannelCount + 2]; // +2 for the uuid columns
        for (int i = 0; i < inputPageChannelCount; i++) {
            outputBlocks[i] = inputPage.getBlock(i);
        }

        outputBlocks[inputPageChannelCount] = new RunLengthEncodedBlock(taskIdBlock, inputPage.getPositionCount());
        outputBlocks[inputPageChannelCount + 1] = createBlockWithValuesFromPool();

        return new Page(inputPage.getPositionCount(), outputBlocks);
    }

    private Block createBlockWithValuesFromPool()
    {
        BlockBuilder leastSigBitsBlock = BIGINT.createFixedSizeBlockBuilder(inputPage.getPositionCount());
        for (int currentPosition = 0; currentPosition < inputPage.getPositionCount(); currentPosition++) {
            if (nextValue >= lastValue) {
                requestValues();
            }
            BIGINT.writeLong(leastSigBitsBlock, nextValue++);
        }
        return leastSigBitsBlock.build();
    }
}
