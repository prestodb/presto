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
package io.prestosql.testing;

import io.prestosql.execution.buffer.PagesSerdeFactory;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.OutputFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class NullOutputOperator
        implements Operator
{
    public static class NullOutputFactory
            implements OutputFactory
    {
        @Override
        public OperatorFactory createOutputOperator(int operatorId, PlanNodeId planNodeId, List<Type> types, Function<Page, Page> pagePreprocessor, PagesSerdeFactory serdeFactory)
        {
            return new NullOutputOperatorFactory(operatorId, planNodeId);
        }
    }

    public static class NullOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;

        public NullOutputOperatorFactory(int operatorId, PlanNodeId planNodeId)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, NullOutputOperator.class.getSimpleName());
            return new NullOutputOperator(operatorContext);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new NullOutputOperatorFactory(operatorId, planNodeId);
        }
    }

    private final OperatorContext operatorContext;
    private boolean finished;

    public NullOutputOperator(OperatorContext operatorContext)
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
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
