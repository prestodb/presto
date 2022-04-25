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
package com.facebook.presto.operator.unnest;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.plan.PlanNodeId;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class UnnestArrayConstant
        implements Operator
{
    private final OperatorContext operatorContext;

    public static class UnnestArrayConstantFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final int channelcount;
        private final Type elementType;
        private final Block block;
        private final boolean needsOrdinal;
        private boolean closed;

        public UnnestArrayConstantFactory(int operatorId, PlanNodeId planNodeId, int channelCount, Type elementType, Block block, boolean needsOrdinal)
        {
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.channelcount = channelCount;
            this.elementType = elementType;
            this.block = block;
            this.needsOrdinal = needsOrdinal;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, UnnestArrayConstant.class.getSimpleName());
            return new UnnestArrayConstant(operatorContext, channelcount, elementType, block, needsOrdinal);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new UnnestArrayConstantFactory(operatorId, planNodeId, channelcount, elementType, block, needsOrdinal);
        }
    }

    private final Type elementType;
    private final Block unnestBlock;
    private final Block[] columnsToAppend;
    private final boolean ordinalNeeded;
    private final int[] channelsToCopy;

    private Page outputPage;
    private boolean finishing;
    private int currentBlockSize;
    private int unnestIndex;

    public UnnestArrayConstant(OperatorContext operatorContext, int channelCount, Type elementType, Block unnestBlock, boolean ordinalNeeded)
    {
        this.operatorContext = operatorContext;
        this.elementType = elementType;
        this.unnestBlock = unnestBlock;
        this.columnsToAppend = new Block[unnestBlock.getPositionCount()];
        this.ordinalNeeded = ordinalNeeded;
        this.channelsToCopy = new int[channelCount];
        for (int i = 0; i < channelCount; i++) {
            channelsToCopy[i] = i;
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
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
        return !finishing && outputPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(outputPage == null, "Operator still has pending output");

        if (currentBlockSize < page.getPositionCount()) {
            // need to resize
            for (int i = 0; i < columnsToAppend.length; i++) {
                BlockBuilder blockBuilder = elementType.createBlockBuilder(null, page.getPositionCount());
                if (unnestBlock.isNull(i)) {
                    for (int j = 0; j < page.getPositionCount(); j++) {
                        blockBuilder.appendNull();
                    }
                }
                else {
                    for (int j = 0; j < page.getPositionCount(); j++) {
                        elementType.appendTo(unnestBlock, i, blockBuilder);
                    }
                }
                columnsToAppend[i] = blockBuilder.build();
            }
            currentBlockSize = page.getPositionCount();
        }

        outputPage = page;
    }

    @Override
    public Page getOutput()
    {
        if (outputPage != null && unnestIndex < columnsToAppend.length) {
            Page result;
            if (currentBlockSize > outputPage.getPositionCount()) {
                result = outputPage.getLoadedPage(channelsToCopy).appendColumn(columnsToAppend[unnestIndex].getRegion(0, outputPage.getPositionCount()));
            }
            else {
                result = outputPage.getLoadedPage(channelsToCopy).appendColumn(columnsToAppend[unnestIndex]);
            }
            unnestIndex++;
            return result;
        }

        unnestIndex = 0;
        outputPage = null;
        return null;
    }
}
