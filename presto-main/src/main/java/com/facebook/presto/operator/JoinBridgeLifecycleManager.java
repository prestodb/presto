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
import com.facebook.presto.operator.LookupJoinOperators.JoinType;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.INNER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class JoinBridgeLifecycleManager
{
    private final JoinType joinType;
    private final FreezeOnReadCounter factoryCount;
    private final JoinBridgeDataManager<LookupSourceFactory> lookupSourceFactoryManager;

    private final Map<Lifespan, PerLifespanData> dataByLifespan;
    private final ReferenceCount probeFactoryReferenceCount;

    private boolean closed;

    public JoinBridgeLifecycleManager(JoinType joinType, JoinBridgeDataManager<LookupSourceFactory> lookupSourceFactoryManager)
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

    private JoinBridgeLifecycleManager(JoinBridgeLifecycleManager other)
    {
        joinType = other.joinType;
        factoryCount = other.factoryCount;
        lookupSourceFactoryManager = other.lookupSourceFactoryManager;
        dataByLifespan = other.dataByLifespan;
        probeFactoryReferenceCount = other.probeFactoryReferenceCount;

        factoryCount.increment();
        probeFactoryReferenceCount.retain();
    }

    public JoinBridgeLifecycleManager duplicate()
    {
        return new JoinBridgeLifecycleManager(this);
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
