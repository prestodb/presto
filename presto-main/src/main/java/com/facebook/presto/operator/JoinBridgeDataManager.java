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
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class JoinBridgeDataManager<T>
{
    public static JoinBridgeDataManager<LookupSourceFactory> lookup(
            PipelineExecutionStrategy probeExecutionStrategy,
            PipelineExecutionStrategy lookupSourceExecutionStrategy,
            Function<Lifespan, LookupSourceFactory> lookupSourceFactoryProvider,
            List<Type> buildOutputTypes)
    {
        return new JoinBridgeDataManager<>(
                probeExecutionStrategy,
                lookupSourceExecutionStrategy,
                lookupSourceFactoryProvider,
                buildOutputTypes,
                SharedLookupSourceFactory::new,
                LookupSourceFactory::destroy);
    }

    @VisibleForTesting
    public static JoinBridgeDataManager<LookupSourceFactory> lookupAllAtOnce(LookupSourceFactory factory)
    {
        return lookup(
                UNGROUPED_EXECUTION,
                UNGROUPED_EXECUTION,
                ignored -> factory,
                factory.getOutputTypes());
    }

    private final List<Type> buildOutputTypes;

    private final InternalLookupSourceFactoryManager<T> internalLookupSourceFactoryManager;

    private JoinBridgeDataManager(
            PipelineExecutionStrategy probeExecutionStrategy,
            PipelineExecutionStrategy lookupSourceExecutionStrategy,
            Function<Lifespan, T> lookupSourceFactoryProvider,
            List<Type> buildOutputTypes,
            BiFunction<T, Runnable, T> sharedWrapper,
            Consumer<T> destroy)
    {
        requireNonNull(probeExecutionStrategy, "probeExecutionStrategy is null");
        requireNonNull(lookupSourceExecutionStrategy, "lookupSourceExecutionStrategy is null");
        requireNonNull(lookupSourceFactoryProvider, "lookupSourceFactoryProvider is null");

        this.internalLookupSourceFactoryManager = internalLookupSourceFactoryManager(probeExecutionStrategy, lookupSourceExecutionStrategy, lookupSourceFactoryProvider, sharedWrapper, destroy);
        this.buildOutputTypes = requireNonNull(buildOutputTypes, "buildOutputTypes is null");
    }

    public List<Type> getBuildOutputTypes()
    {
        return buildOutputTypes;
    }

    public T forLifespan(Lifespan lifespan)
    {
        return internalLookupSourceFactoryManager.get(lifespan);
    }

    public void noMoreLookupSourceFactory()
    {
        internalLookupSourceFactoryManager.noMoreLookupSourceFactory();
    }

    private static <T> InternalLookupSourceFactoryManager<T> internalLookupSourceFactoryManager(
            PipelineExecutionStrategy probeExecutionStrategy,
            PipelineExecutionStrategy lookupSourceExecutionStrategy,
            Function<Lifespan, T> lookupSourceFactoryProvider,
            BiFunction<T, Runnable, T> sharedWrapper,
            Consumer<T> destroy)
    {
        switch (probeExecutionStrategy) {
            case UNGROUPED_EXECUTION:
                switch (lookupSourceExecutionStrategy) {
                    case UNGROUPED_EXECUTION:
                        return new TaskWideInternalLookupSourceFactoryManager<>(lookupSourceFactoryProvider);
                    case GROUPED_EXECUTION:
                        throw new UnsupportedOperationException("Invalid combination. Lookup source should not be grouped if probe is not going to take advantage of it.");
                    default:
                        throw new IllegalArgumentException("Unknown lookupSourceExecutionStrategy: " + lookupSourceExecutionStrategy);
                }
            case GROUPED_EXECUTION:
                switch (lookupSourceExecutionStrategy) {
                    case UNGROUPED_EXECUTION:
                        return new SharedInternalLookupSourceFactoryManager<>(lookupSourceFactoryProvider, sharedWrapper, destroy);
                    case GROUPED_EXECUTION:
                        return new OneToOneInternalLookupSourceFactoryManager<>(lookupSourceFactoryProvider);
                    default:
                        throw new IllegalArgumentException("Unknown lookupSourceExecutionStrategy: " + lookupSourceExecutionStrategy);
                }
            default:
                throw new UnsupportedOperationException();
        }
    }

    private interface InternalLookupSourceFactoryManager<T>
    {
        T get(Lifespan lifespan);

        default void noMoreLookupSourceFactory()
        {
            // do nothing
        }
    }

    // 1 probe, 1 lookup source
    private static class TaskWideInternalLookupSourceFactoryManager<T>
            implements InternalLookupSourceFactoryManager<T>
    {
        private final Supplier<T> supplier;

        public TaskWideInternalLookupSourceFactoryManager(Function<Lifespan, T> lookupSourceFactoryProvider)
        {
            supplier = Suppliers.memoize(() -> lookupSourceFactoryProvider.apply(Lifespan.taskWide()));
        }

        @Override
        public T get(Lifespan lifespan)
        {
            checkArgument(Lifespan.taskWide().equals(lifespan));
            return supplier.get();
        }
    }

    // N probe, N lookup source; one-to-one mapping, bijective
    private static class OneToOneInternalLookupSourceFactoryManager<T>
            implements InternalLookupSourceFactoryManager<T>
    {
        private final Map<Lifespan, T> map = new ConcurrentHashMap<>();
        private final Function<Lifespan, T> lookupSourceFactoryProvider;

        public OneToOneInternalLookupSourceFactoryManager(Function<Lifespan, T> lookupSourceFactoryProvider)
        {
            this.lookupSourceFactoryProvider = lookupSourceFactoryProvider;
        }

        @Override
        public T get(Lifespan lifespan)
        {
            checkArgument(!Lifespan.taskWide().equals(lifespan));
            return map.computeIfAbsent(lifespan, lookupSourceFactoryProvider);
        }
    }

    // N probe, 1 lookup source
    private static class SharedInternalLookupSourceFactoryManager<T>
            implements InternalLookupSourceFactoryManager<T>
    {
        private final T taskWideLookupSourceFactory;
        private final BiFunction<T, Runnable, T> sharedWrapper;
        private final Map<Lifespan, T> map = new ConcurrentHashMap<>();
        private final ReferenceCount referenceCount;

        public SharedInternalLookupSourceFactoryManager(Function<Lifespan, T> lookupSourceFactoryProvider, BiFunction<T, Runnable, T> sharedWrapper, Consumer<T> destroy)
        {
            this.taskWideLookupSourceFactory = lookupSourceFactoryProvider.apply(Lifespan.taskWide());
            this.referenceCount = new ReferenceCount(1);
            this.sharedWrapper = requireNonNull(sharedWrapper, "sharedWrapper is null");
            referenceCount.getFreeFuture().addListener(() -> destroy.accept(taskWideLookupSourceFactory), directExecutor());
        }

        @Override
        public T get(Lifespan lifespan)
        {
            if (Lifespan.taskWide().equals(lifespan)) {
                // build
                return taskWideLookupSourceFactory;
            }
            // probe
            return map.computeIfAbsent(lifespan, ignored -> {
                referenceCount.retain();
                return sharedWrapper.apply(taskWideLookupSourceFactory, referenceCount::release);
            });
        }

        @Override
        public void noMoreLookupSourceFactory()
        {
            referenceCount.release();
        }
    }
}
