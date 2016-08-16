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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.Session;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SystemPartitioningHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.operator.exchange.LocalExchangeSink.finishedLocalExchangeSink;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.isFixedBroadcastPartitioning;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.isFixedRandomPartitioning;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class LocalExchange
{
    private static final DataSize DEFAULT_MAX_BUFFERED_BYTES = new DataSize(32, MEGABYTE);

    private final List<Type> types;
    private final Supplier<Consumer<Page>> exchangerSupplier;

    private final List<LocalExchangeSource> sources;

    private final LocalExchangeMemoryManager memoryManager;

    @GuardedBy("this")
    private boolean allSourcesFinished;

    @GuardedBy("this")
    private boolean noMoreSinkFactories;

    @GuardedBy("this")
    private final Set<LocalExchangeSinkFactory> openSinkFactories = new HashSet<>();

    @GuardedBy("this")
    private final Set<LocalExchangeSink> sinks = new HashSet<>();

    public LocalExchange(
            Session session,
            NodePartitioningManager partitioningManager,
            List<Symbol> layout,
            List<Type> types,
            PartitioningScheme partitioningScheme)
    {
        this(session, partitioningManager, layout, types, partitioningScheme, DEFAULT_MAX_BUFFERED_BYTES);
    }

    public LocalExchange(
            Session session,
            NodePartitioningManager partitioningManager,
            List<Symbol> layout,
            List<Type> types,
            PartitioningScheme partitioningScheme,
            DataSize maxBufferedBytes)
    {
        requireNonNull(session, "session is null");
        requireNonNull(partitioningManager, "partitioningManager is null");
        requireNonNull(layout, "layout is null");
        requireNonNull(types, "types is null");
        requireNonNull(partitioningScheme, "partitioningScheme is null");
        requireNonNull(maxBufferedBytes, "maxBufferedBytes is null");
        checkArgument(layout.size() == types.size(), "layout and types must be the same size");

        SystemPartitioningHandle systemHandle = checkType(
                partitioningScheme.getPartitioning().getHandle().getConnectorHandle(),
                SystemPartitioningHandle.class,
                "Local exchange partitioning handle");

        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

        Partitioning partitioning = partitioningScheme.getPartitioning();

        checkArgument(systemHandle.getPartitionCount().isPresent(), "System partitioning must have a fixed partition count");
        int bufferCount = systemHandle.getPartitionCount().getAsInt();
        ImmutableList.Builder<LocalExchangeSource> sources = ImmutableList.builder();
        for (int i = 0; i < bufferCount; i++) {
            sources.add(new LocalExchangeSource(types, source -> checkAllSourcesFinished()));
        }
        this.sources = sources.build();

        List<Consumer<PageReference>> buffers = this.sources.stream()
                .map(buffer -> (Consumer<PageReference>) buffer::addPage)
                .collect(toImmutableList());

        this.memoryManager = new LocalExchangeMemoryManager(maxBufferedBytes.toBytes());
        if (bufferCount == 1) {
            // if there is only one buffer, the partitioning doesn't matter, so choose the most efficient implementation
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryManager::updateMemoryUsage);
        }
        else if (isFixedBroadcastPartitioning(partitioning)) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryManager::updateMemoryUsage);
        }
        else if (isFixedRandomPartitioning(partitioning)) {
            exchangerSupplier = () -> new RandomExchanger(buffers, memoryManager::updateMemoryUsage);
        }
        else {
            Function<Page, Page> functionArgumentExtractor = new PartitionFunctionArgumentExtractor(partitioningScheme, layout);
            Supplier<PartitionFunction> partitionFunctionFactory = partitioningManager.createLocalPartitionFunction(session, partitioningScheme);
            exchangerSupplier = () -> new PartitioningExchanger(
                    buffers,
                    memoryManager::updateMemoryUsage,
                    partitionFunctionFactory.get(),
                    functionArgumentExtractor);
        }
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public int getBufferCount()
    {
        return sources.size();
    }

    public long getBufferedBytes()
    {
        return memoryManager.getBufferedBytes();
    }

    public synchronized LocalExchangeSinkFactory createSinkFactory()
    {
        checkState(!noMoreSinkFactories, "No more sink factories already set");
        LocalExchangeSinkFactory newFactory = new LocalExchangeSinkFactory(this);
        openSinkFactories.add(newFactory);
        return newFactory;
    }

    public LocalExchangeSource getSource(int partitionIndex)
    {
        return sources.get(partitionIndex);
    }

    private void checkAllSourcesFinished()
    {
        checkNotHoldsLock(this);

        if (!sources.stream().allMatch(LocalExchangeSource::isFinished)) {
            return;
        }

        // all sources are finished, so finish the sinks
        ImmutableList<LocalExchangeSink> openSinks;
        synchronized (this) {
            allSourcesFinished = true;

            openSinks = ImmutableList.copyOf(sinks);
            sinks.clear();
        }

        // since all sources are finished there is no reason to allow new pages to be added
        // this can happen with a limit query
        openSinks.forEach(LocalExchangeSink::finish);
        checkAllSinksComplete();
    }

    private LocalExchangeSink createSink(LocalExchangeSinkFactory factory)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            checkState(openSinkFactories.contains(factory), "Factory is already closed");

            if (allSourcesFinished) {
                // all sources have completed so return a sink that is already finished
                return finishedLocalExchangeSink(types, memoryManager);
            }

            // Note: exchanger can be stateful so create a new one for each sink
            Consumer<Page> exchanger = exchangerSupplier.get();
            LocalExchangeSink sink = new LocalExchangeSink(types, exchanger, memoryManager, this::sinkFinished);
            sinks.add(sink);
            return sink;
        }
    }

    private void sinkFinished(LocalExchangeSink sink)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            sinks.remove(sink);
        }
        checkAllSinksComplete();
    }

    private void noMoreSinkFactories()
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            noMoreSinkFactories = true;
        }
        checkAllSinksComplete();
    }

    private void sinkFactoryClosed(LocalExchangeSinkFactory sinkFactory)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            openSinkFactories.remove(sinkFactory);
        }
        checkAllSinksComplete();
    }

    private void checkAllSinksComplete()
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            if (!noMoreSinkFactories || !openSinkFactories.isEmpty() || !sinks.isEmpty()) {
                return;
            }
        }

        sources.forEach(LocalExchangeSource::finish);
        memoryManager.setNoBlockOnFull();
    }

    private static void checkNotHoldsLock(Object lock)
    {
        checkState(!Thread.holdsLock(lock), "Can not execute this method while holding a lock");
    }

    // Sink factory is entirely a pass thought to LocalExchange.
    // This class only exists as a separate entity to deal with the complex lifecycle caused
    // by operator factories (e.g., duplicate and noMoreSinkFactories).
    @ThreadSafe
    public static class LocalExchangeSinkFactory
            implements Closeable
    {
        private final LocalExchange exchange;

        private LocalExchangeSinkFactory(LocalExchange exchange)
        {
            this.exchange = requireNonNull(exchange, "exchange is null");
        }

        public List<Type> getTypes()
        {
            return exchange.getTypes();
        }

        public LocalExchangeSink createSink()
        {
            return exchange.createSink(this);
        }

        public LocalExchangeSinkFactory duplicate()
        {
            return exchange.createSinkFactory();
        }

        @Override
        public void close()
        {
            exchange.sinkFactoryClosed(this);
        }

        public void noMoreSinkFactories()
        {
            exchange.noMoreSinkFactories();
        }
    }
}
