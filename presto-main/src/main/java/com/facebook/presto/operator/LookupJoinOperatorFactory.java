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

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.operator.JoinProbe.JoinProbeFactory;
import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.facebook.presto.operator.LookupOuterOperator.LookupOuterOperatorFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.INNER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
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
    private final Optional<OperatorFactory> outerOperatorFactory;
    private final PerLifespanDataManager perLifespanDataManager;
    private final OptionalInt totalOperatorsCount;
    private final HashGenerator probeHashGenerator;
    private final PartitioningSpillerFactory partitioningSpillerFactory;

    private boolean closed;

    public LookupJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            LookupSourceFactoryManager lookupSourceFactoryManager,
            List<Type> probeTypes,
            List<Type> probeOutputTypes,
            List<Type> buildOutputTypes,
            JoinType joinType,
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

        this.perLifespanDataManager = new PerLifespanDataManager(joinType, lookupSourceFactoryManager);

        if (joinType == INNER || joinType == PROBE_OUTER) {
            this.outerOperatorFactory = Optional.empty();
        }
        else {
            this.outerOperatorFactory = Optional.of(new LookupOuterOperatorFactory(
                    operatorId,
                    planNodeId,
                    perLifespanDataManager::getOuterPositionsFuture,
                    probeOutputTypes,
                    buildOutputTypes,
                    perLifespanDataManager::getLookupSourceFactoryUsersCount));
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
    }

    private LookupJoinOperatorFactory(LookupJoinOperatorFactory other)
    {
        requireNonNull(other, "other is null");
        operatorId = other.operatorId;
        planNodeId = other.planNodeId;
        probeTypes = other.probeTypes;
        buildOutputTypes = other.buildOutputTypes;
        joinType = other.joinType;
        joinProbeFactory = other.joinProbeFactory;
        // Invokes .duplicate on perLifespanDataManager
        perLifespanDataManager = other.perLifespanDataManager.duplicate();
        outerOperatorFactory = other.outerOperatorFactory;
        totalOperatorsCount = other.totalOperatorsCount;
        probeHashGenerator = other.probeHashGenerator;
        partitioningSpillerFactory = other.partitioningSpillerFactory;
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        LookupSourceFactory lookupSourceFactory = perLifespanDataManager.getLookupSourceFactory(driverContext.getLifespan());
        ReferenceCount probeReferenceCount = perLifespanDataManager.getProbeReferenceCount(driverContext.getLifespan());

        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LookupJoinOperator.class.getSimpleName());

        lookupSourceFactory.setTaskContext(driverContext.getPipelineContext().getTaskContext());

        probeReferenceCount.retain();
        return new LookupJoinOperator(
                operatorContext,
                probeTypes,
                buildOutputTypes,
                joinType,
                lookupSourceFactory,
                joinProbeFactory,
                probeReferenceCount::release,
                totalOperatorsCount,
                probeHashGenerator,
                partitioningSpillerFactory);
    }

    @Override
    public void noMoreOperators()
    {
        checkState(!closed);
        closed = true;
        perLifespanDataManager.noMoreLifespan();
    }

    @Override
    public void noMoreOperators(Lifespan lifespan)
    {
        perLifespanDataManager.getProbeReferenceCount(lifespan).release();
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

    public static class PerLifespanDataManager
    {
        private final JoinType joinType;
        private final FreezeOnReadCounter factoryCount;
        private final LookupSourceFactoryManager lookupSourceFactoryManager;

        private final Map<Lifespan, PerLifespanData> dataByLifespan;
        private final ReferenceCount probeFactoryReferenceCount;

        private boolean closed;

        public PerLifespanDataManager(JoinType joinType, LookupSourceFactoryManager lookupSourceFactoryManager)
        {
            this.joinType = joinType;
            this.lookupSourceFactoryManager = lookupSourceFactoryManager;
            this.dataByLifespan = new ConcurrentHashMap<>();

            this.factoryCount = new FreezeOnReadCounter();
            this.factoryCount.increment();

            // Each LookupJoinOperatorFactory count as 1 probe, until noMoreOperators() is called.
            this.probeFactoryReferenceCount = new ReferenceCount(1);
            this.probeFactoryReferenceCount.getFreeFuture().addListener(lookupSourceFactoryManager::noMoreLookupSourceFactory, directExecutor());
        }

        private PerLifespanDataManager(PerLifespanDataManager other)
        {
            joinType = other.joinType;
            factoryCount = other.factoryCount;
            lookupSourceFactoryManager = other.lookupSourceFactoryManager;
            dataByLifespan = other.dataByLifespan;
            probeFactoryReferenceCount = other.probeFactoryReferenceCount;

            factoryCount.increment();
            probeFactoryReferenceCount.retain();
        }

        public PerLifespanDataManager duplicate()
        {
            return new PerLifespanDataManager(this);
        }

        public void noMoreLifespan()
        {
            checkState(!closed);
            closed = true;
            probeFactoryReferenceCount.release();
        }

        public LookupSourceFactory getLookupSourceFactory(Lifespan lifespan)
        {
            return data(lifespan).getLookupSourceFactory();
        }

        public ReferenceCount getProbeReferenceCount(Lifespan lifespan)
        {
            return data(lifespan).getProbeReferenceCount();
        }

        public ReferenceCount getLookupSourceFactoryUsersCount(Lifespan lifespan)
        {
            return data(lifespan).getLookupSourceFactoryUsersCount();
        }

        public ListenableFuture<OuterPositionIterator> getOuterPositionsFuture(Lifespan lifespan)
        {
            return data(lifespan).getOuterPositionsFuture();
        }

        private PerLifespanData data(Lifespan lifespan)
        {
            return dataByLifespan.computeIfAbsent(
                    lifespan,
                    id -> {
                        checkState(!closed);
                        return new PerLifespanData(joinType, factoryCount.get(), lookupSourceFactoryManager.forLifespan(id));
                    });
        }
    }

    public static class PerLifespanData
    {
        private final LookupSourceFactory lookupSourceFactory;
        private final ReferenceCount probeReferenceCount;
        private final ReferenceCount lookupSourceFactoryUsersCount;
        private final ListenableFuture<OuterPositionIterator> outerPositionsFuture;

        public PerLifespanData(JoinType joinType, int factoryCount, LookupSourceFactory lookupSourceFactory)
        {
            this.lookupSourceFactory = lookupSourceFactory;

            // When all probe and build-outer operators finish, destroy the lookup source (freeing the memory)
            // * Whole probe side (operator and operatorFactory) is counted as 1 lookup source factory user
            // * Each LookupOuterOperatorFactory count as 1 lookup source factory user, until noMoreOperators(LifeSpan) is called.
            //   * There is at most 1 LookupOuterOperatorFactory
            // * Each LookupOuterOperator count as 1 lookup source factory user, until close() is called.
            lookupSourceFactoryUsersCount = new ReferenceCount(1);
            lookupSourceFactoryUsersCount.getFreeFuture().addListener(lookupSourceFactory::destroy, directExecutor());

            // * Each LookupJoinOperatorFactory count as 1 probe, until noMoreOperators(LifeSpan) is called.
            // * Each LookupJoinOperator count as 1 probe, until close() is called.
            probeReferenceCount = new ReferenceCount(factoryCount);
            probeReferenceCount.getFreeFuture().addListener(lookupSourceFactoryUsersCount::release, directExecutor());

            if (joinType == INNER || joinType == PROBE_OUTER) {
                outerPositionsFuture = null;
            }
            else {
                // increment the user count by 1 to account for the build-outer factory
                lookupSourceFactoryUsersCount.retain();

                // Set the outer position future to start the outer operator:
                // 1. when all probe operators finish, and
                // 2. when lookup source is ready
                ListenableFuture<LookupSourceProvider> lookupSourceProviderAfterProbeFinished = transformAsync(
                        probeReferenceCount.getFreeFuture(),
                        ignored -> lookupSourceFactory.createLookupSourceProvider());
                outerPositionsFuture = transform(lookupSourceProviderAfterProbeFinished, lookupSourceProvider -> {
                    // Close the lookupSourceProvider we just created.
                    // The only reason we created it is to wait until lookup source is ready.
                    lookupSourceProvider.close();
                    return lookupSourceFactory.getOuterPositionIterator();
                });
            }
        }

        public LookupSourceFactory getLookupSourceFactory()
        {
            return lookupSourceFactory;
        }

        public ReferenceCount getProbeReferenceCount()
        {
            return probeReferenceCount;
        }

        public ReferenceCount getLookupSourceFactoryUsersCount()
        {
            return lookupSourceFactoryUsersCount;
        }

        public ListenableFuture<OuterPositionIterator> getOuterPositionsFuture()
        {
            return outerPositionsFuture;
        }
    }

    public static class FreezeOnReadCounter
    {
        private int count;
        private boolean freezed;

        public synchronized void increment()
        {
            checkState(!freezed, "Counter has been read");
            count++;
        }

        public synchronized int get()
        {
            freezed = true;
            return count;
        }
    }
}
