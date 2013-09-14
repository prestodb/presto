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

import com.facebook.presto.spi.Split;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ExchangeOperator
        implements SourceOperator
{
    public static class ExchangeOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final Supplier<ExchangeClient> exchangeClientSupplier;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public ExchangeOperatorFactory(int operatorId, PlanNodeId sourceId, Supplier<ExchangeClient> exchangeClientSupplier, List<TupleInfo> tupleInfos)
        {
            this.operatorId = operatorId;
            this.sourceId = sourceId;
            this.exchangeClientSupplier = exchangeClientSupplier;
            this.tupleInfos = tupleInfos;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, ExchangeOperator.class.getSimpleName());
            return new ExchangeOperator(
                    operatorContext,
                    tupleInfos,
                    sourceId,
                    exchangeClientSupplier.get());
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final ExchangeClient exchangeClient;
    private final List<TupleInfo> tupleInfos;

    public ExchangeOperator(
            OperatorContext operatorContext,
            List<TupleInfo> tupleInfos,
            PlanNodeId sourceId,
            final ExchangeClient exchangeClient)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.sourceId = checkNotNull(sourceId, "sourceId is null");
        this.exchangeClient = checkNotNull(exchangeClient, "exchangeClient is null");
        this.tupleInfos = checkNotNull(tupleInfos, "tupleInfos is null");

        operatorContext.setInfoSupplier(new Supplier<Object>()
        {
            @Override
            public Object get()
            {
                return exchangeClient.getStatus();
            }
        });
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public void addSplit(Split split)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof RemoteSplit, "split is not a remote split");

        URI location = ((RemoteSplit) split).getLocation();
        exchangeClient.addLocation(location);
    }

    @Override
    public void noMoreSplits()
    {
        exchangeClient.noMoreLocations();
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
        exchangeClient.close();
    }

    @Override
    public boolean isFinished()
    {
        return exchangeClient.isClosed();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = exchangeClient.isBlocked();
        if (blocked.isDone()) {
            return NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        Page page = exchangeClient.pollPage();
        if (page != null) {
            operatorContext.recordGeneratedInput(page.getDataSize(), page.getPositionCount());
        }
        return page;
    }
}
