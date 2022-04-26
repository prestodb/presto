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
package com.facebook.presto.operator.index;

import com.facebook.presto.common.Page;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.plan.PlanNodeId;

import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PagesIndexBuilderOperator
        implements Operator
{
    public static class PagesIndexBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final IndexSnapshotBuilder indexSnapshotBuilder;
        private final String operatorType;
        private boolean closed;

        public PagesIndexBuilderOperatorFactory(int operatorId, PlanNodeId planNodeId, IndexSnapshotBuilder indexSnapshotBuilder, String operatorType)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.indexSnapshotBuilder = requireNonNull(indexSnapshotBuilder, "indexSnapshotBuilder is null");
            this.operatorType = requireNonNull(operatorType, "operatorType is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, operatorType);
            return new PagesIndexBuilderOperator(operatorContext, indexSnapshotBuilder);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PagesIndexBuilderOperatorFactory(operatorId, planNodeId, indexSnapshotBuilder, operatorType);
        }
    }

    private final OperatorContext operatorContext;
    private final IndexSnapshotBuilder indexSnapshotBuilder;

    private boolean finished;

    public PagesIndexBuilderOperator(OperatorContext operatorContext, IndexSnapshotBuilder indexSnapshotBuilder)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.indexSnapshotBuilder = requireNonNull(indexSnapshotBuilder, "indexSnapshotBuilder is null");
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
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        if (!indexSnapshotBuilder.tryAddPage(page)) {
            finish();
            return;
        }
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
