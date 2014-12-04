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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ValuesOperator
        implements Operator
{
    public static class ValuesOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Type> types;
        private final List<Page> pages;
        private boolean closed;

        public ValuesOperatorFactory(int operatorId, List<Type> types, List<Page> pages)
        {
            this.operatorId = operatorId;
            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
            this.pages = ImmutableList.copyOf(checkNotNull(pages, "pages is null"));
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, ValuesOperator.class.getSimpleName());
            return new ValuesOperator(operatorContext, types, pages);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final ImmutableList<Type> types;
    private final Iterator<Page> pages;

    public ValuesOperator(OperatorContext operatorContext, List<Type> types, List<Page> pages)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));

        checkNotNull(pages, "pages is null");
        checkArgument(!pages.isEmpty(), "pages is empty");

        this.pages = ImmutableList.copyOf(pages).iterator();
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
        Iterators.size(pages);
    }

    @Override
    public boolean isFinished()
    {
        return !pages.hasNext();
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (!pages.hasNext()) {
            return null;
        }
        Page page = pages.next();
        if (page != null) {
            operatorContext.recordGeneratedInput(page.getSizeInBytes(), page.getPositionCount());
        }
        return page;
    }
}
