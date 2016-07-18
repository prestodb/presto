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

import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.operator.LookupOuterOperator.LookupOuterOperatorFactory;
import com.facebook.presto.operator.LookupSource.OuterPositionIterator;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.INNER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperatorFactory
        implements JoinOperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final List<Type> probeTypes;
    private final List<Type> buildTypes;
    private final JoinType joinType;
    private final LookupSourceSupplier lookupSourceSupplier;
    private final JoinProbeFactory joinProbeFactory;
    private final Optional<OperatorFactory> outerOperatorFactory;
    private final ReferenceCount referenceCount;
    private boolean closed;

    public LookupJoinOperatorFactory(int operatorId,
            PlanNodeId planNodeId,
            LookupSourceSupplier lookupSourceSupplier,
            List<Type> probeTypes,
            JoinType joinType,
            JoinProbeFactory joinProbeFactory)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.lookupSourceSupplier = requireNonNull(lookupSourceSupplier, "lookupSourceSupplier is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.buildTypes = ImmutableList.copyOf(lookupSourceSupplier.getTypes());
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");

        this.referenceCount = new ReferenceCount();

        if (joinType == INNER || joinType == PROBE_OUTER) {
            // when all join operators finish, destroy the lookup source (freeing the memory)
            this.referenceCount.getFreeFuture().addListener(lookupSourceSupplier::destroy, directExecutor());
            this.outerOperatorFactory = Optional.empty();
        }
        else {
            // when all join operators finish, set the outer position future to start the outer operator
            SettableFuture<OuterPositionIterator> outerPositionsFuture = SettableFuture.create();
            this.referenceCount.getFreeFuture().addListener(() -> {
                // lookup source may not be finished yet, so add a listener
                Futures.addCallback(
                        lookupSourceSupplier.getLookupSource(),
                        new OnSuccessFutureCallback<>(lookupSource -> outerPositionsFuture.set(lookupSource.getOuterPositionIterator())));
            }, directExecutor());

            // when output operator finishes, destroy the lookup source
            Runnable onOperatorClose = () -> {
                // lookup source may not be finished yet, so add a listener, to free the memory
                lookupSourceSupplier.getLookupSource().addListener(lookupSourceSupplier::destroy, directExecutor());
            };
            this.outerOperatorFactory = Optional.of(new LookupOuterOperatorFactory(operatorId, planNodeId, outerPositionsFuture, probeTypes, buildTypes, onOperatorClose));
        }
    }

    private LookupJoinOperatorFactory(LookupJoinOperatorFactory other)
    {
        requireNonNull(other, "other is null");
        operatorId = other.operatorId;
        planNodeId = other.planNodeId;
        probeTypes = other.probeTypes;
        buildTypes = other.buildTypes;
        joinType = other.joinType;
        lookupSourceSupplier = other.lookupSourceSupplier;
        joinProbeFactory = other.joinProbeFactory;
        referenceCount = other.referenceCount;
        outerOperatorFactory = other.outerOperatorFactory;

        referenceCount.retain();
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.<Type>builder()
                .addAll(probeTypes)
                .addAll(buildTypes)
                .build();
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LookupJoinOperator.class.getSimpleName());

        lookupSourceSupplier.setTaskContext(driverContext.getPipelineContext().getTaskContext());

        referenceCount.retain();
        return new LookupJoinOperator(
                operatorContext,
                getTypes(),
                joinType,
                lookupSourceSupplier.getLookupSource(),
                joinProbeFactory,
                referenceCount::release);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        referenceCount.release();
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new LookupJoinOperatorFactory(this);
    }

    @Override
    public Optional<OperatorFactory> createOuterOperatorFactory()
    {
        return outerOperatorFactory;
    }

    // We use a public class to avoid access problems with the isolated class loaders
    public static class OnSuccessFutureCallback<T>
            implements FutureCallback<T>
    {
        private final Consumer<T> onSuccess;

        public OnSuccessFutureCallback(Consumer<T> onSuccess)
        {
            this.onSuccess = onSuccess;
        }

        @Override
        public void onSuccess(T result)
        {
            onSuccess.accept(result);
        }

        @Override
        public void onFailure(Throwable t)
        {
        }
    }
}
