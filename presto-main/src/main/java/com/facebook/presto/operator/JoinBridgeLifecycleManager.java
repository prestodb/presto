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
import java.util.function.Consumer;
import java.util.function.Function;

import static com.facebook.presto.operator.LookupJoinOperators.JoinType.INNER;
import static com.facebook.presto.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class JoinBridgeLifecycleManager<T>
{
    public static JoinBridgeLifecycleManager<LookupSourceFactory> lookup(
            JoinType joinType,
            JoinBridgeDataManager<LookupSourceFactory> lookupSourceFactoryManager)
    {
        return new JoinBridgeLifecycleManager<>(
                joinType,
                lookupSourceFactoryManager,
                LookupSourceFactory::destroy,
                lookupSourceFactory -> transform(
                        lookupSourceFactory.createLookupSourceProvider(),
                        lookupSourceProvider -> {
                            // Close the lookupSourceProvider we just created.
                            // The only reason we created it is to wait until lookup source is ready.
                            lookupSourceProvider.close();
                            return lookupSourceFactory.getOuterPositionIterator();
                        },
                        directExecutor()));
    }

    public static JoinBridgeLifecycleManager<NestedLoopJoinPagesBridge> nestedLoop(
            JoinType joinType,
            JoinBridgeDataManager<NestedLoopJoinPagesBridge> nestedLoopJoinPagesSupplierManager)
    {
        checkArgument(joinType == INNER);
        return new JoinBridgeLifecycleManager<>(
                joinType,
                nestedLoopJoinPagesSupplierManager,
                NestedLoopJoinPagesBridge::destroy,
                joinBridge -> {
                    throw new UnsupportedOperationException();
                });
    }

    private final JoinType joinType;
    private final FreezeOnReadCounter factoryCount;
    private final JoinBridgeDataManager<T> joinBridgeDataManager;
    private final Consumer<T> destroy;
    private final Function<T, ListenableFuture<OuterPositionIterator>> outerPositionIteratorFutureFunction;

    private final Map<Lifespan, PerLifespanData<T>> dataByLifespan;
    private final ReferenceCount probeFactoryReferenceCount;

    private boolean closed;

    public JoinBridgeLifecycleManager(
            JoinType joinType,
            JoinBridgeDataManager<T> joinBridgeDataManager,
            Consumer<T> destroy,
            Function<T, ListenableFuture<OuterPositionIterator>> outerPositionIteratorFutureFunction)
    {
        this.joinType = joinType;
        this.joinBridgeDataManager = joinBridgeDataManager;
        this.destroy = destroy;
        this.outerPositionIteratorFutureFunction = outerPositionIteratorFutureFunction;

        this.dataByLifespan = new ConcurrentHashMap<>();

        this.factoryCount = new FreezeOnReadCounter();
        this.factoryCount.increment();

        // Each LookupJoinOperatorFactory count as 1 probe, until noMoreOperators() is called.
        this.probeFactoryReferenceCount = new ReferenceCount(1);
        this.probeFactoryReferenceCount.getFreeFuture().addListener(joinBridgeDataManager::noMoreJoinBridge, directExecutor());
    }

    private JoinBridgeLifecycleManager(JoinBridgeLifecycleManager<T> other)
    {
        joinType = other.joinType;
        factoryCount = other.factoryCount;
        joinBridgeDataManager = other.joinBridgeDataManager;
        destroy = other.destroy;
        outerPositionIteratorFutureFunction = other.outerPositionIteratorFutureFunction;
        dataByLifespan = other.dataByLifespan;
        probeFactoryReferenceCount = other.probeFactoryReferenceCount;

        // closed is intentionally not copied.
        closed = false;

        factoryCount.increment();
        probeFactoryReferenceCount.retain();
    }

    public JoinBridgeLifecycleManager<T> duplicate()
    {
        return new JoinBridgeLifecycleManager<>(this);
    }

    public void noMoreLifespan()
    {
        checkState(!closed);
        closed = true;
        probeFactoryReferenceCount.release();
    }

    public T getJoinBridge(Lifespan lifespan)
    {
        return data(lifespan).getJoinBridge();
    }

    public ReferenceCount getProbeReferenceCount(Lifespan lifespan)
    {
        return data(lifespan).getProbeReferenceCount();
    }

    public ReferenceCount getJoinBridgeUsersCount(Lifespan lifespan)
    {
        return data(lifespan).getJoinBridgeUsersCount();
    }

    public ListenableFuture<OuterPositionIterator> getOuterPositionsFuture(Lifespan lifespan)
    {
        return data(lifespan).getOuterPositionsFuture();
    }

    private PerLifespanData<T> data(Lifespan lifespan)
    {
        return dataByLifespan.computeIfAbsent(
                lifespan,
                id -> {
                    checkState(!closed);
                    return new PerLifespanData<>(
                            joinType,
                            factoryCount.get(),
                            joinBridgeDataManager.forLifespan(id),
                            destroy,
                            outerPositionIteratorFutureFunction);
                });
    }

    public static class PerLifespanData<T>
    {
        private final T joinBridge;
        private final ReferenceCount probeReferenceCount;
        private final ReferenceCount joinBridgeUsersCount;
        private final ListenableFuture<OuterPositionIterator> outerPositionsFuture;

        /**
         * @param outerPositionIteratorFutureFunction The function returns a Future that will present OuterPositionIterator when ready.
         *   The function is invoked once probe has finished. However, the build might not have finished at the time of invocation.
         */
        public PerLifespanData(JoinType joinType, int factoryCount, T joinBridge, Consumer<T> destroy, Function<T, ListenableFuture<OuterPositionIterator>> outerPositionIteratorFutureFunction)
        {
            this.joinBridge = joinBridge;

            // When all probe and lookup-outer operators finish, destroy the join bridge (freeing the memory)
            // * Whole probe side (operator and operatorFactory) is counted as 1 join bridge user
            // * Each LookupOuterOperatorFactory count as 1 join bridge user, until noMoreOperators(LifeSpan) is called.
            //   * There is at most 1 LookupOuterOperatorFactory
            // * Each LookupOuterOperator count as 1 join bridge user, until close() is called.
            joinBridgeUsersCount = new ReferenceCount(1);
            joinBridgeUsersCount.getFreeFuture().addListener(() -> destroy.accept(joinBridge), directExecutor());

            // * Each LookupJoinOperatorFactory count as 1 probe, until noMoreOperators(LifeSpan) is called.
            // * Each LookupJoinOperator count as 1 probe, until close() is called.
            probeReferenceCount = new ReferenceCount(factoryCount);
            probeReferenceCount.getFreeFuture().addListener(joinBridgeUsersCount::release, directExecutor());

            if (joinType == INNER || joinType == PROBE_OUTER) {
                outerPositionsFuture = null;
            }
            else {
                // increment the user count by 1 to account for the lookup-outer factory
                joinBridgeUsersCount.retain();

                // Set the outer position future to start the outer operator:
                // 1. when all probe operators finish, and
                // 2. when lookup source is ready
                outerPositionsFuture = transformAsync(
                        probeReferenceCount.getFreeFuture(),
                        ignored -> outerPositionIteratorFutureFunction.apply(joinBridge),
                        directExecutor());
            }
        }

        public T getJoinBridge()
        {
            return joinBridge;
        }

        public ReferenceCount getProbeReferenceCount()
        {
            return probeReferenceCount;
        }

        public ReferenceCount getJoinBridgeUsersCount()
        {
            return joinBridgeUsersCount;
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
