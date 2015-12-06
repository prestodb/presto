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

import com.facebook.presto.operator.Mergeable;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_RANDOM_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
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
    private final long maxBufferedBytes;

    private final AsyncWaiter writeWaiter = new AsyncWaiter();

    @GuardedBy("this")
    private boolean allSourcesFinished;

    @GuardedBy("this")
    private boolean noMoreSinkFactories;

    @GuardedBy("this")
    private final Set<LocalExchangeSinkFactory> sinkFactories = new HashSet<>();

    @GuardedBy("this")
    private final Set<LocalExchangeSink> sinks = new HashSet<>();

    private final AtomicLong bufferBytes = new AtomicLong();

    public LocalExchange(
            PartitioningHandle partitioning,
            int defaultConcurrency,
            List<? extends Type> types,
            List<Integer> partitionChannels,
            Optional<Integer> partitionHashChannel)
    {
        this(partitioning, defaultConcurrency, types, partitionChannels, partitionHashChannel, DEFAULT_MAX_BUFFERED_BYTES);
    }

    public LocalExchange(
            PartitioningHandle partitioning,
            int defaultConcurrency,
            List<? extends Type> types,
            List<Integer> partitionChannels,
            Optional<Integer> partitionHashChannel,
            DataSize maxBufferedBytes)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

        int bufferCount;
        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            bufferCount = 1;
            checkArgument(partitionChannels.isEmpty(), "Gather exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Broadcast exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_RANDOM_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Random exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION)) {
            bufferCount = defaultConcurrency;
            checkArgument(!partitionChannels.isEmpty(), "Partitioned exchange must have partition channels");
        }
        else {
            throw new IllegalArgumentException("Unsupported local exchange partitioning " + partitioning);
        }

        ImmutableList.Builder<LocalExchangeSource> sources = ImmutableList.builder();
        for (int i = 0; i < bufferCount; i++) {
            sources.add(new LocalExchangeSource(types, source -> sourceFinished()));
        }
        this.sources = sources.build();
        this.maxBufferedBytes = maxBufferedBytes.toBytes();

        LongConsumer memoryTracker = (delta) -> {
            long bytes = bufferBytes.addAndGet(delta);
            if (delta < 0 && bytes < this.maxBufferedBytes) {
                writeWaiter.notifyWaiters();
            }
        };

        List<Consumer<PageReference>> buffers = this.sources.stream()
                .map(buffer -> (Consumer<PageReference>) buffer::addPage)
                .collect(toImmutableList());

        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryTracker);
        }
        else if (partitioning.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryTracker);
        }
        else if (partitioning.equals(FIXED_RANDOM_DISTRIBUTION)) {
            exchangerSupplier = () -> new RandomExchanger(buffers, memoryTracker);
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION)) {
            exchangerSupplier = () -> new PartitioningExchanger(buffers, memoryTracker, types, partitionChannels, partitionHashChannel);
        }
        else {
            throw new IllegalArgumentException("Unsupported local exchange partitioning " + partitioning);
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

    public synchronized LocalExchangeSinkFactory createSinkFactory()
    {
        checkState(!noMoreSinkFactories, "No more sink factories already set");
        LocalExchangeSinkFactory sinkFactory = new LocalExchangeSinkFactory(this);
        sinkFactories.add(sinkFactory);
        return sinkFactory;
    }

    public LocalExchangeSource getSource(int partitionIndex)
    {
        return sources.get(partitionIndex);
    }

    private void sourceFinished()
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

        // Note: exchanger can be stateful so create a new one for each sink
        LocalExchangeSink sink = new LocalExchangeSink(this, exchangerSupplier.get());
        synchronized (this) {
            checkState(sinkFactories.contains(factory), "Factory is already closed");

            if (!allSourcesFinished) {
                sinks.add(sink);
                return sink;
            }
        }

        // all sources have already finished so return a finished sink
        // NOTE: finish must be called outside of the lock since it calls back
        sink.finish();
        return sink;
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
            sinkFactories.remove(sinkFactory);
        }
        checkAllSinksComplete();
    }

    private void checkAllSinksComplete()
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            if (!noMoreSinkFactories || !sinkFactories.isEmpty() || !sinks.isEmpty()) {
                return;
            }
        }

        sources.forEach(LocalExchangeSource::finish);
        writeWaiter.finishAndNotifyWaiters();
    }

    private ListenableFuture<?> waitForWriting()
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            if (bufferBytes.get() < maxBufferedBytes) {
                return NOT_BLOCKED;
            }
        }
        return writeWaiter.waitFor();
    }

    private static void checkNotHoldsLock(Object lock)
    {
        checkState(!Thread.holdsLock(lock), "Can not execute this method while holding a lock");
    }

    @ThreadSafe
    public static class LocalExchangeSource
    {
        private final List<Type> types;
        private final Consumer<LocalExchangeSource> onFinish;

        private final Queue<PageReference> buffer = new ConcurrentLinkedQueue<>();
        private final AtomicLong bufferedBytes = new AtomicLong();

        private final AsyncWaiter readWaiter = new AsyncWaiter();

        @GuardedBy("this")
        private boolean finishing;

        public LocalExchangeSource(List<? extends Type> types, Consumer<LocalExchangeSource> onFinish)
        {
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.onFinish = requireNonNull(onFinish, "onFinish is null");
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public LocalExchangeBufferInfo getBufferInfo()
        {
            // This must be lock free to assure task info creation is fast
            // Note: the stats my be internally inconsistent
            return new LocalExchangeBufferInfo(bufferedBytes.get(), buffer.size());
        }

        private void addPage(PageReference pageReference)
        {
            checkNotHoldsLock(this);

            boolean added = false;
            synchronized (this) {
                if (!finishing) {
                    buffer.add(pageReference);
                    added = true;
                    bufferedBytes.addAndGet(pageReference.getSizeInBytes());
                }
            }

            if (!added) {
                // dereference the page outside of lock
                pageReference.removePage();
            }

            // notify readers outside of lock
            readWaiter.notifyWaiters();
        }

        public Page removePage()
        {
            checkNotHoldsLock(this);

            PageReference pageReference = buffer.poll();
            if (pageReference == null) {
                return null;
            }

            // dereference the page outside of lock
            Page page = pageReference.removePage();
            bufferedBytes.addAndGet(-page.getSizeInBytes());

            if (isFinished()) {
                onFinish.accept(this);
            }

            return page;
        }

        public ListenableFuture<?> waitForReading()
        {
            checkNotHoldsLock(this);

            synchronized (this) {
                if (finishing || !buffer.isEmpty()) {
                    return NOT_BLOCKED;
                }
            }
            return readWaiter.waitFor();
        }

        public boolean isFinished()
        {
            checkNotHoldsLock(this);

            synchronized (this) {
                return finishing && buffer.isEmpty();
            }
        }

        public void finish()
        {
            checkNotHoldsLock(this);

            synchronized (this) {
                if (finishing) {
                    return;
                }
                finishing = true;
            }

            // notify reader outside of lock
            readWaiter.finishAndNotifyWaiters();

            // notify finish listener outside of lock
            if (isFinished()) {
                onFinish.accept(this);
            }
        }

        public void close()
        {
            checkNotHoldsLock(this);

            List<PageReference> remainingPages;
            synchronized (this) {
                finishing = true;

                remainingPages = ImmutableList.copyOf(buffer);
                buffer.clear();
                bufferedBytes.set(0);
            }

            // free the remaining pages
            remainingPages.forEach(PageReference::removePage);

            // notify reader outside of lock
            readWaiter.finishAndNotifyWaiters();

            // notify finish listener outside of lock
            onFinish.accept(this);
        }
    }

    public static class LocalExchangeBufferInfo
            implements Mergeable<LocalExchangeBufferInfo>
    {
        private final long bufferedBytes;
        private final int bufferedPages;

        @JsonCreator
        public LocalExchangeBufferInfo(
                @JsonProperty("bufferedBytes") long bufferedBytes,
                @JsonProperty("bufferedPages") int bufferedPages)
        {
            this.bufferedBytes = bufferedBytes;
            this.bufferedPages = bufferedPages;
        }

        @JsonProperty
        public long getBufferedBytes()
        {
            return bufferedBytes;
        }

        @JsonProperty
        public int getBufferedPages()
        {
            return bufferedPages;
        }

        @Override
        public LocalExchangeBufferInfo mergeWith(LocalExchangeBufferInfo other)
        {
            return new LocalExchangeBufferInfo(bufferedBytes + other.getBufferedBytes(), bufferedPages + other.getBufferedPages());
        }
    }

    @ThreadSafe
    public static class LocalExchangeSinkFactory
            implements Closeable
    {
        private final LocalExchange exchange;

        @GuardedBy("this")
        private boolean closed;

        private LocalExchangeSinkFactory(LocalExchange exchange)
        {
            this.exchange = exchange;
        }

        public List<Type> getTypes()
        {
            return exchange.getTypes();
        }

        public LocalExchangeSink createSink()
        {
            checkNotHoldsLock(this);

            synchronized (this) {
                checkState(!closed, "Factory already closed");
            }
            return exchange.createSink(this);
        }

        public LocalExchangeSinkFactory duplicate()
        {
            checkNotHoldsLock(this);

            synchronized (this) {
                checkState(!closed, "Factory already closed");
            }
            return exchange.createSinkFactory();
        }

        @Override
        public void close()
        {
            checkNotHoldsLock(this);

            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
            }
            exchange.sinkFactoryClosed(this);
        }

        public void noMoreSinkFactories()
        {
            checkNotHoldsLock(this);

            exchange.noMoreSinkFactories();
        }
    }

    @ThreadSafe
    public static class LocalExchangeSink
    {
        private final LocalExchange exchange;
        private final Consumer<Page> sink;

        @GuardedBy("this")
        private boolean finished;

        private LocalExchangeSink(LocalExchange exchange, Consumer<Page> sink)
        {
            this.exchange = exchange;
            this.sink = sink;
        }

        public List<Type> getTypes()
        {
            return exchange.getTypes();
        }

        public void finish()
        {
            checkNotHoldsLock(this);

            synchronized (this) {
                if (finished) {
                    return;
                }
                finished = true;
            }

            // Notify exchange outside of lock
            exchange.sinkFinished(this);
        }

        public boolean isFinished()
        {
            checkNotHoldsLock(this);

            synchronized (this) {
                return finished;
            }
        }

        public ListenableFuture<?> waitForWriting()
        {
            checkNotHoldsLock(this);

            if (isFinished()) {
                return NOT_BLOCKED;
            }
            return exchange.waitForWriting();
        }

        public void addPage(Page page)
        {
            checkNotHoldsLock(this);
            requireNonNull(page, "page is null");

            if (isFinished()) {
                return;
            }
            checkArgument(page.getChannelCount() == getTypes().size());
            sink.accept(page);
        }
    }
}
