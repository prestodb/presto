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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.plan.PlanNodeId;

import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class AssignUniqueIdOperator
        implements Operator
{
    private static final long ROW_IDS_PER_REQUEST = 1L << 20L;
    private static final long MAX_ROW_ID = 1L << 40L;

    public static class AssignUniqueIdOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private boolean closed;
        private final AtomicLong valuePool = new AtomicLong();

        public AssignUniqueIdOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(
                    operatorId,
                    planNodeId,
                    AssignUniqueIdOperator.class.getSimpleName());
            return new AssignUniqueIdOperator(operatorContext, valuePool);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new AssignUniqueIdOperatorFactory(operatorId, planNodeId);
        }
    }

    private final OperatorContext operatorContext;
    private boolean finishing;
    private final AtomicLong rowIdPool;
    private final long uniqueValueMask;

    private Page inputPage;
    private long rowIdCounter;
    private long maxRowIdCounterValue;

    public AssignUniqueIdOperator(OperatorContext operatorContext, AtomicLong rowIdPool)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.rowIdPool = requireNonNull(rowIdPool, "rowIdPool is null");

        TaskId fullTaskId = operatorContext.getDriverContext().getTaskId();
        uniqueValueMask = (((long) fullTaskId.getStageExecutionId().getStageId().getId()) << 54) | (((long) fullTaskId.getId()) << 40);

        requestValues();
    }

    private void requestValues()
    {
        rowIdCounter = rowIdPool.getAndAdd(ROW_IDS_PER_REQUEST);
        maxRowIdCounterValue = Math.min(rowIdCounter + ROW_IDS_PER_REQUEST, MAX_ROW_ID);
        checkState(rowIdCounter < MAX_ROW_ID, "Unique row id exceeds a limit: %s", MAX_ROW_ID);
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

        Page outputPage = processPage();
        inputPage = null;
        return outputPage;
    }

    private Page processPage()
    {
        return inputPage.appendColumn(generateIdColumn());
    }

    private Block generateIdColumn()
    {
        BlockBuilder block = BIGINT.createFixedSizeBlockBuilder(inputPage.getPositionCount());
        for (int currentPosition = 0; currentPosition < inputPage.getPositionCount(); currentPosition++) {
            if (rowIdCounter >= maxRowIdCounterValue) {
                requestValues();
            }
            long rowId = rowIdCounter++;
            verify((rowId & uniqueValueMask) == 0, "RowId and uniqueValue mask overlaps");
            BIGINT.writeLong(block, uniqueValueMask | rowId);
        }
        return block.build();
    }
}
