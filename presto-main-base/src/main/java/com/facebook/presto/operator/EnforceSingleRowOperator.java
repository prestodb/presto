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
import com.facebook.presto.common.block.ByteArrayBlock;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNodeId;

import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class EnforceSingleRowOperator
        implements Operator
{
    public static class EnforceSingleRowOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private boolean closed;

        public EnforceSingleRowOperatorFactory(int operatorId, PlanNodeId planNodeId)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, EnforceSingleRowOperator.class.getSimpleName());
            return new EnforceSingleRowOperator(operatorContext);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new EnforceSingleRowOperatorFactory(operatorId, planNodeId);
        }
    }

    private static final Page SINGLE_NULL_VALUE_PAGE = new Page(1, new ByteArrayBlock(1, Optional.of(new boolean[] {true}), new byte[1]));

    private final OperatorContext operatorContext;
    private boolean finishing;
    private Page page;

    public EnforceSingleRowOperator(OperatorContext operatorContext)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (!finishing && page == null) {
            this.page = SINGLE_NULL_VALUE_PAGE;
        }
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && page == null;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(needsInput(), "Operator did not expect any more data");
        if (page.getPositionCount() == 0) {
            return;
        }
        if (this.page != null || page.getPositionCount() > 1) {
            throw new PrestoException(SUBQUERY_MULTIPLE_ROWS, "Scalar sub-query has returned multiple rows");
        }
        this.page = page;
    }

    @Override
    public Page getOutput()
    {
        if (!finishing) {
            return null;
        }
        checkState(page != null, "Operator is already done");

        Page pageToReturn = page;
        page = null;
        return pageToReturn;
    }
}
