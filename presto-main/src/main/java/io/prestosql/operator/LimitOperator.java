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
package io.prestosql.operator;

import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.sql.planner.plan.PlanNodeId;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LimitOperator
        implements Operator
{
    public static class LimitOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final long limit;
        private boolean closed;

        public LimitOperatorFactory(int operatorId, PlanNodeId planNodeId, long limit)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.limit = limit;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LimitOperator.class.getSimpleName());
            return new LimitOperator(operatorContext, limit);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new LimitOperatorFactory(operatorId, planNodeId, limit);
        }
    }

    private final OperatorContext operatorContext;
    private Page nextPage;
    private long remainingLimit;

    public LimitOperator(OperatorContext operatorContext, long limit)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        checkArgument(limit >= 0, "limit must be at least zero");
        this.remainingLimit = limit;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
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
    public boolean needsInput()
    {
        return remainingLimit > 0 && nextPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput());

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

    @Override
    public Page getOutput()
    {
        Page page = nextPage;
        nextPage = null;
        return page;
    }
}
