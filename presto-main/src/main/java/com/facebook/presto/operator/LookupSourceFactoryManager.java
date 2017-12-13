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
import com.google.common.base.Suppliers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class LookupSourceFactoryManager
{
    private final List<Type> outputTypes;

    private final InternalLookupSourceFactoryManager internalLookupSourceFactoryManager;

    public LookupSourceFactoryManager(
            PipelineExecutionStrategy probeExecutionStrategy,
            PipelineExecutionStrategy lookupSourceExecutionStrategy,
            Function<Lifespan, LookupSourceFactory> lookupSourceFactoryProvider,
            List<Type> outputTypes)
    {
        requireNonNull(probeExecutionStrategy, "probeExecutionStrategy is null");
        requireNonNull(lookupSourceExecutionStrategy, "lookupSourceExecutionStrategy is null");
        requireNonNull(lookupSourceFactoryProvider, "lookupSourceFactoryProvider is null");

        this.internalLookupSourceFactoryManager = internalLookupSourceFactoryManager(probeExecutionStrategy, lookupSourceExecutionStrategy, lookupSourceFactoryProvider);
        this.outputTypes = requireNonNull(outputTypes, "outputTypes is null");
    }

    public static LookupSourceFactoryManager allAtOnce(PartitionedLookupSourceFactory factory)
    {
        return new LookupSourceFactoryManager(
                UNGROUPED_EXECUTION,
                UNGROUPED_EXECUTION,
                ignored -> factory,
                factory.getOutputTypes());
    }

    public List<Type> getBuildOutputTypes()
    {
        return outputTypes;
    }

    public LookupSourceFactory forLifespan(Lifespan lifespan)
    {
        return internalLookupSourceFactoryManager.get(lifespan);
    }

    public void noMoreLookupSourceFactory()
    {
        internalLookupSourceFactoryManager.noMoreLookupSourceFactory();
    }

    private static InternalLookupSourceFactoryManager internalLookupSourceFactoryManager(
            PipelineExecutionStrategy probeExecutionStrategy,
            PipelineExecutionStrategy lookupSourceExecutionStrategy,
            Function<Lifespan, LookupSourceFactory> lookupSourceFactoryProvider)
    {
        switch (probeExecutionStrategy) {
            case UNGROUPED_EXECUTION:
                switch (lookupSourceExecutionStrategy) {
                    case UNGROUPED_EXECUTION:
                        return new TaskWideInternalLookupSourceFactoryManager(lookupSourceFactoryProvider);
                    case GROUPED_EXECUTION:
                        throw new UnsupportedOperationException("Invalid combination. Lookup source should not be grouped if probe is not going to take advantage of it.");
                    default:
                        throw new IllegalArgumentException("Unknown lookupSourceExecutionStrategy: " + lookupSourceExecutionStrategy);
                }
            case GROUPED_EXECUTION:
                switch (lookupSourceExecutionStrategy) {
                    case UNGROUPED_EXECUTION:
                        return new SharedInternalLookupSourceFactoryManager(lookupSourceFactoryProvider);
                    case GROUPED_EXECUTION:
                        return new OneToOneInternalLookupSourceFactoryManager(lookupSourceFactoryProvider);
                    default:
                        throw new IllegalArgumentException("Unknown lookupSourceExecutionStrategy: " + lookupSourceExecutionStrategy);
                }
            default:
                throw new UnsupportedOperationException();
        }
    }

    private interface InternalLookupSourceFactoryManager
    {
        LookupSourceFactory get(Lifespan lifespan);

        default void noMoreLookupSourceFactory()
        {
            // do nothing
        }
    }

    // 1 probe, 1 lookup source
    private static class TaskWideInternalLookupSourceFactoryManager
            implements InternalLookupSourceFactoryManager
    {
        private final Supplier<LookupSourceFactory> supplier;

        public TaskWideInternalLookupSourceFactoryManager(Function<Lifespan, LookupSourceFactory> lookupSourceFactoryProvider)
        {
            supplier = Suppliers.memoize(() -> lookupSourceFactoryProvider.apply(Lifespan.taskWide()));
        }

        @Override
        public LookupSourceFactory get(Lifespan lifespan)
        {
            checkArgument(Lifespan.taskWide().equals(lifespan));
            return supplier.get();
        }
    }

    // N probe, N lookup source; one-to-one mapping, bijective
    private static class OneToOneInternalLookupSourceFactoryManager
            implements InternalLookupSourceFactoryManager
    {
        private final Map<Lifespan, LookupSourceFactory> map = new ConcurrentHashMap<>();
        private final Function<Lifespan, LookupSourceFactory> lookupSourceFactoryProvider;

        public OneToOneInternalLookupSourceFactoryManager(Function<Lifespan, LookupSourceFactory> lookupSourceFactoryProvider)
        {
            this.lookupSourceFactoryProvider = lookupSourceFactoryProvider;
        }

        @Override
        public LookupSourceFactory get(Lifespan lifespan)
        {
            checkArgument(!Lifespan.taskWide().equals(lifespan));
            return map.computeIfAbsent(lifespan, lookupSourceFactoryProvider);
        }
    }

    // N probe, 1 lookup source
    private static class SharedInternalLookupSourceFactoryManager
            implements InternalLookupSourceFactoryManager
    {
        private final LookupSourceFactory taskWideLookupSourceFactory;
        private final Map<Lifespan, LookupSourceFactory> map = new ConcurrentHashMap<>();
        private final ReferenceCount referenceCount;

        public SharedInternalLookupSourceFactoryManager(Function<Lifespan, LookupSourceFactory> lookupSourceFactoryProvider)
        {
            taskWideLookupSourceFactory = lookupSourceFactoryProvider.apply(Lifespan.taskWide());
            referenceCount = new ReferenceCount(1);
            referenceCount.getFreeFuture().addListener(taskWideLookupSourceFactory::destroy, directExecutor());
        }

        @Override
        public LookupSourceFactory get(Lifespan lifespan)
        {
            if (Lifespan.taskWide().equals(lifespan)) {
                // build
                return taskWideLookupSourceFactory;
            }
            // probe
            return map.computeIfAbsent(lifespan, ignored -> {
                referenceCount.retain();
                return new SharedLookupSourceFactory(taskWideLookupSourceFactory, referenceCount::release);
            });
        }

        @Override
        public void noMoreLookupSourceFactory()
        {
            referenceCount.release();
        }
    }
}
