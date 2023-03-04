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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.operator.JoinProbe.JoinProbeFactory;
import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.operator.LookupOuterOperator.LookupOuterOperatorFactory;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.INNER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperatorFactory
        implements JoinOperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final List<Type> probeTypes;
    private final List<Type> buildOutputTypes;
    private final JoinType joinType;
    private final JoinProbeFactory joinProbeFactory;
    private final Optional<OuterOperatorFactoryResult> outerOperatorFactoryResult;
    private final JoinBridgeManager<? extends LookupSourceFactory> joinBridgeManager;
    private final OptionalInt totalOperatorsCount;
    private final HashGenerator probeHashGenerator;
    private final PartitioningSpillerFactory partitioningSpillerFactory;
    private final boolean optimizeProbeForEmptyBuild;

    private boolean closed;

    public LookupJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
            List<Type> probeTypes,
            List<Type> probeOutputTypes,
            List<Type> buildOutputTypes,
            JoinType joinType,
            JoinProbeFactory joinProbeFactory,
            OptionalInt totalOperatorsCount,
            List<Integer> probeJoinChannels,
            OptionalInt probeHashChannel,
            PartitioningSpillerFactory partitioningSpillerFactory,
            boolean optimizeProbeForEmptyBuild)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.buildOutputTypes = ImmutableList.copyOf(requireNonNull(buildOutputTypes, "buildOutputTypes is null"));
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");

        this.joinBridgeManager = lookupSourceFactoryManager;
        joinBridgeManager.incrementProbeFactoryCount();

        if (joinType == INNER || joinType == PROBE_OUTER) {
            this.outerOperatorFactoryResult = Optional.empty();
        }
        else {
            this.outerOperatorFactoryResult = Optional.of(new OuterOperatorFactoryResult(
                    new LookupOuterOperatorFactory(
                            operatorId,
                            planNodeId,
                            probeOutputTypes,
                            buildOutputTypes,
                            lookupSourceFactoryManager),
                    lookupSourceFactoryManager.getBuildExecutionStrategy()));
        }
        this.totalOperatorsCount = requireNonNull(totalOperatorsCount, "totalOperatorsCount is null");

        requireNonNull(probeHashChannel, "probeHashChannel is null");
        if (probeHashChannel.isPresent()) {
            this.probeHashGenerator = new PrecomputedHashGenerator(probeHashChannel.getAsInt());
        }
        else {
            requireNonNull(probeJoinChannels, "probeJoinChannels is null");
            List<Type> hashTypes = probeJoinChannels.stream()
                    .map(probeTypes::get)
                    .collect(toImmutableList());
            this.probeHashGenerator = new InterpretedHashGenerator(hashTypes, probeJoinChannels);
        }

        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
        this.optimizeProbeForEmptyBuild = optimizeProbeForEmptyBuild;
    }

    private LookupJoinOperatorFactory(LookupJoinOperatorFactory other)
    {
        requireNonNull(other, "other is null");
        checkArgument(!other.closed, "cannot duplicated closed OperatorFactory");

        operatorId = other.operatorId;
        planNodeId = other.planNodeId;
        probeTypes = other.probeTypes;
        buildOutputTypes = other.buildOutputTypes;
        joinType = other.joinType;
        joinProbeFactory = other.joinProbeFactory;
        joinBridgeManager = other.joinBridgeManager;
        outerOperatorFactoryResult = other.outerOperatorFactoryResult;
        totalOperatorsCount = other.totalOperatorsCount;
        probeHashGenerator = other.probeHashGenerator;
        partitioningSpillerFactory = other.partitioningSpillerFactory;
        optimizeProbeForEmptyBuild = other.optimizeProbeForEmptyBuild;

        closed = false;
        joinBridgeManager.incrementProbeFactoryCount();
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        LookupSourceFactory lookupSourceFactory = joinBridgeManager.getJoinBridge(driverContext.getLifespan());

        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LookupJoinOperator.class.getSimpleName());

        lookupSourceFactory.setTaskContext(driverContext.getPipelineContext().getTaskContext());

        joinBridgeManager.probeOperatorCreated(driverContext.getLifespan());
        return new LookupJoinOperator(
                operatorContext,
                probeTypes,
                buildOutputTypes,
                joinType,
                lookupSourceFactory,
                joinProbeFactory,
                () -> joinBridgeManager.probeOperatorClosed(driverContext.getLifespan()),
                totalOperatorsCount,
                probeHashGenerator,
                partitioningSpillerFactory,
                optimizeProbeForEmptyBuild);
    }

    @Override
    public void noMoreOperators()
    {
        checkState(!closed);
        closed = true;
        joinBridgeManager.probeOperatorFactoryClosedForAllLifespans();
    }

    @Override
    public void noMoreOperators(Lifespan lifespan)
    {
        joinBridgeManager.probeOperatorFactoryClosed(lifespan);
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new LookupJoinOperatorFactory(this);
    }

    @Override
    public Optional<OuterOperatorFactoryResult> createOuterOperatorFactory()
    {
        return outerOperatorFactoryResult;
    }
}
