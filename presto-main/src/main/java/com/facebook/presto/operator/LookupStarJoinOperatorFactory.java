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
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LookupStarJoinOperatorFactory
        implements JoinOperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final List<Type> probeTypes;
    private final List<Type> buildOutputTypes;
    private final LookupJoinOperators.JoinType joinType;
    private final JoinProbeFactory joinProbeFactory;
    private final Optional<OuterOperatorFactoryResult> outerOperatorFactoryResult;
    private final List<JoinBridgeManager<? extends LookupSourceFactory>> joinBridgeManager;
    private final OptionalInt totalOperatorsCount;
    private final HashGenerator probeHashGenerator;
    private final PartitioningSpillerFactory partitioningSpillerFactory;

    private boolean closed;

    public LookupStarJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            List<JoinBridgeManager<? extends LookupSourceFactory>> lookupSourceFactoryManager,
            List<Type> probeTypes,
            List<Type> probeOutputTypes,
            List<Type> buildOutputTypes,
            LookupJoinOperators.JoinType joinType,
            JoinProbeFactory joinProbeFactory,
            OptionalInt totalOperatorsCount,
            List<Integer> probeJoinChannels,
            OptionalInt probeHashChannel,
            PartitioningSpillerFactory partitioningSpillerFactory)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.buildOutputTypes = ImmutableList.copyOf(requireNonNull(buildOutputTypes, "buildOutputTypes is null"));
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");

        this.joinBridgeManager = ImmutableList.copyOf(lookupSourceFactoryManager);
        joinBridgeManager.forEach(JoinBridgeManager::incrementProbeFactoryCount);

        checkState(joinType == LookupJoinOperators.JoinType.INNER || joinType == LookupJoinOperators.JoinType.PROBE_OUTER);
        this.outerOperatorFactoryResult = Optional.empty();
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
    }

    private LookupStarJoinOperatorFactory(LookupStarJoinOperatorFactory other)
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

        closed = false;
        joinBridgeManager.forEach(JoinBridgeManager::incrementProbeFactoryCount);
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        List<LookupSourceFactory> lookupSourceFactory = joinBridgeManager.stream().map(x -> x.getJoinBridge(driverContext.getLifespan())).collect(toImmutableList());

        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LookupStarJoinOperator.class.getSimpleName());

        lookupSourceFactory.forEach(x -> x.setTaskContext(driverContext.getPipelineContext().getTaskContext()));

        joinBridgeManager.forEach(x -> x.probeOperatorCreated(driverContext.getLifespan()));
        return new LookupStarJoinOperator(
                operatorContext,
                probeTypes,
                buildOutputTypes,
                joinType,
                lookupSourceFactory,
                joinProbeFactory,
                () -> joinBridgeManager.forEach(x -> x.probeOperatorClosed(driverContext.getLifespan())),
                totalOperatorsCount,
                probeHashGenerator,
                partitioningSpillerFactory);
    }

    @Override
    public void noMoreOperators()
    {
        checkState(!closed);
        closed = true;
        joinBridgeManager.forEach(x -> x.probeOperatorFactoryClosedForAllLifespans());
    }

    @Override
    public void noMoreOperators(Lifespan lifespan)
    {
        joinBridgeManager.forEach(x -> x.probeOperatorFactoryClosed(lifespan));
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new LookupStarJoinOperatorFactory(this);
    }

    @Override
    public Optional<OuterOperatorFactoryResult> createOuterOperatorFactory()
    {
        return outerOperatorFactoryResult;
    }
}
