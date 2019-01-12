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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.prestosql.spi.Page;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ValuesOperator
        implements Operator
{
    public static class ValuesOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Page> pages;
        private boolean closed;

        public ValuesOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Page> pages)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.pages = ImmutableList.copyOf(requireNonNull(pages, "pages is null"));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, ValuesOperator.class.getSimpleName());
            return new ValuesOperator(operatorContext, pages);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new ValuesOperatorFactory(operatorId, planNodeId, pages);
        }
    }

    private final OperatorContext operatorContext;
    private final Iterator<Page> pages;

    public ValuesOperator(OperatorContext operatorContext, List<Page> pages)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        requireNonNull(pages, "pages is null");

        this.pages = ImmutableList.copyOf(pages).iterator();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
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
            operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
        }
        return page;
    }
}
