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
import com.facebook.presto.spi.type.Type;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class InMemoryExchangeSinkOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final InMemoryExchange inMemoryExchange;
    private boolean finished;

    InMemoryExchangeSinkOperator(OperatorContext operatorContext, InMemoryExchange inMemoryExchange)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.inMemoryExchange = requireNonNull(inMemoryExchange, "inMemoryExchange is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return inMemoryExchange.getTypes();
    }

    @Override
    public void finish()
    {
        if (!finished) {
            finished = true;
            inMemoryExchange.sinkFinished();
        }
    }

    @Override
    public boolean isFinished()
    {
        if (!finished) {
            finished = inMemoryExchange.isFinishing();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = inMemoryExchange.waitForWriting();
        if (blocked.isDone()) {
            return NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        return !isFinished() && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finished, "Already finished");
        inMemoryExchange.addPage(page);
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void close()
            throws Exception
    {
        finish();
    }
}
