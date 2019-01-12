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
import io.prestosql.sql.planner.plan.PlanNodeId;

import static java.util.Objects.requireNonNull;

public class DevNullOperator
        implements Operator
{
    public static class DevNullOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;

        public DevNullOperatorFactory(int operatorId, PlanNodeId planNodeId)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            return new DevNullOperator(driverContext.addOperatorContext(operatorId, planNodeId, DevNullOperator.class.getSimpleName()));
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new DevNullOperatorFactory(operatorId, planNodeId);
        }
    }

    private final OperatorContext context;
    private boolean finished;

    public DevNullOperator(OperatorContext context)
    {
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return context;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
    }

    @Override
    public Page getOutput()
    {
        return null;
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
}
