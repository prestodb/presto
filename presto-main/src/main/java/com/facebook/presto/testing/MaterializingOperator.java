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
package com.facebook.presto.testing;

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MaterializingOperator
        implements Operator
{
    public static class MaterializingOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private boolean closed;

        public MaterializingOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = sourceTypes;
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public MaterializingOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, MaterializingOperator.class.getSimpleName());
            return new MaterializingOperator(operatorContext, sourceTypes);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new MaterializingOperatorFactory(operatorId, planNodeId, sourceTypes);
        }
    }

    private final OperatorContext operatorContext;
    private final MaterializedResult.Builder resultBuilder;
    private boolean finished;
    private boolean closed;

    public MaterializingOperator(OperatorContext operatorContext, List<Type> sourceTypes)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        resultBuilder = MaterializedResult.resultBuilder(operatorContext.getSession(), sourceTypes);
    }

    public MaterializingOperator(OperatorContext operatorContext, MaterializedResult.Builder resultBuilder)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.resultBuilder = requireNonNull(resultBuilder, "resultBuilder is null");
    }

    public boolean isClosed()
    {
        return closed;
    }

    public MaterializedResult getMaterializedResult()
    {
        return resultBuilder.build();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.of();
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finished, "operator finished");

        resultBuilder.page(page);
        operatorContext.recordGeneratedOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
