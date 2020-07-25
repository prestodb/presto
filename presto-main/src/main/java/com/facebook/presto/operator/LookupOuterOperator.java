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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LookupOuterOperator
        implements Operator
{
    public static class LookupOuterOperatorFactory
            implements OperatorFactory
    {
        private enum State
        {
            NOT_CREATED, CREATED, CLOSED
        }

        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> probeOutputTypes;
        private final List<Type> buildOutputTypes;
        private final JoinBridgeManager<?> joinBridgeManager;

        private final Set<Lifespan> createdLifespans = new HashSet<>();
        private boolean closed;

        public LookupOuterOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> probeOutputTypes,
                List<Type> buildOutputTypes,
                JoinBridgeManager<?> joinBridgeManager)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.probeOutputTypes = ImmutableList.copyOf(requireNonNull(probeOutputTypes, "probeOutputTypes is null"));
            this.buildOutputTypes = ImmutableList.copyOf(requireNonNull(buildOutputTypes, "buildOutputTypes is null"));
            this.joinBridgeManager = joinBridgeManager;
        }

        public int getOperatorId()
        {
            return operatorId;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "LookupOuterOperatorFactory is closed");
            Lifespan lifespan = driverContext.getLifespan();
            if (createdLifespans.contains(lifespan)) {
                throw new IllegalStateException("Only one outer operator can be created per Lifespan");
            }
            createdLifespans.add(lifespan);

            ListenableFuture<OuterPositionIterator> outerPositionsFuture = joinBridgeManager.getOuterPositionsFuture(lifespan);
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LookupOuterOperator.class.getSimpleName());
            joinBridgeManager.outerOperatorCreated(lifespan);
            return new LookupOuterOperator(operatorContext, outerPositionsFuture, probeOutputTypes, buildOutputTypes, () -> joinBridgeManager.outerOperatorClosed(lifespan));
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
            joinBridgeManager.outerOperatorFactoryClosed(lifespan);
        }

        @Override
        public void noMoreOperators()
        {
            if (closed) {
                return;
            }
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Source operator factories can not be duplicated");
        }
    }

    private final OperatorContext operatorContext;
    private final ListenableFuture<OuterPositionIterator> outerPositionsFuture;

    private final List<Type> probeOutputTypes;
    private final Runnable onClose;

    private final PageBuilder pageBuilder;

    private OuterPositionIterator outerPositions;
    private boolean closed;

    public LookupOuterOperator(
            OperatorContext operatorContext,
            ListenableFuture<OuterPositionIterator> outerPositionsFuture,
            List<Type> probeOutputTypes,
            List<Type> buildOutputTypes,
            Runnable onClose)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outerPositionsFuture = requireNonNull(outerPositionsFuture, "outerPositionsFuture is null");

        List<Type> types = ImmutableList.<Type>builder()
                .addAll(requireNonNull(probeOutputTypes, "probeOutputTypes is null"))
                .addAll(requireNonNull(buildOutputTypes, "buildOutputTypes is null"))
                .build();
        this.probeOutputTypes = ImmutableList.copyOf(probeOutputTypes);
        this.pageBuilder = new PageBuilder(types);
        this.onClose = requireNonNull(onClose, "onClose is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return outerPositionsFuture;
    }

    @Override
    public void finish()
    {
        // this is a source operator, so we can just terminate the output now
        close();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
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
        if (outerPositions == null) {
            outerPositions = tryGetFutureValue(outerPositionsFuture).orElse(null);
            if (outerPositions == null) {
                return null;
            }
        }

        boolean outputPositionsFinished = false;
        while (!pageBuilder.isFull()) {
            // write build columns
            outputPositionsFinished = !outerPositions.appendToNext(pageBuilder, probeOutputTypes.size());
            if (outputPositionsFinished) {
                break;
            }
            pageBuilder.declarePosition();

            // write nulls into probe columns
            // todo use RLE blocks
            for (int probeChannel = 0; probeChannel < probeOutputTypes.size(); probeChannel++) {
                pageBuilder.getBlockBuilder(probeChannel).appendNull();
            }
        }

        // only flush full pages unless we are done
        Page page = null;
        if (pageBuilder.isFull() || (outputPositionsFinished && !pageBuilder.isEmpty())) {
            page = pageBuilder.build();
            pageBuilder.reset();
        }

        if (outputPositionsFinished) {
            close();
        }
        return page;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        pageBuilder.reset();
        onClose.run();
    }
}
