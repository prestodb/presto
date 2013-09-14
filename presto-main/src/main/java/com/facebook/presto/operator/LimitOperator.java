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
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

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
        private final List<TupleInfo> tupleInfos;
        private final long limit;
        private boolean closed;

        public LimitOperatorFactory(int operatorId, List<TupleInfo> tupleInfos, long limit)
        {
            this.operatorId = operatorId;
            this.tupleInfos = tupleInfos;
            this.limit = limit;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, LimitOperator.class.getSimpleName());
            return new LimitOperator(operatorContext, tupleInfos, limit);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<TupleInfo> tupleInfos;
    private Page nextPage;
    private long remainingLimit;

    public LimitOperator(OperatorContext operatorContext, List<TupleInfo> tupleInfos, long limit)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.tupleInfos = checkNotNull(tupleInfos, "tupleInfos is null");

        checkArgument(limit >= 0, "limit must be at least zero");
        this.remainingLimit = limit;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
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
            remainingLimit = 0;
            nextPage = new Page(blocks);
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
