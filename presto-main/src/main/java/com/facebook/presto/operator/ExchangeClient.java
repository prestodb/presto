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

import com.facebook.presto.operator.HttpPageBufferClient.ClientCallback;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.util.Threads.checkNotSameThreadExecutor;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
public class ExchangeClient
        implements Closeable
{
    private static final Page NO_MORE_PAGES = new Page(0);

    private final long maxBufferedBytes;
    private final DataSize maxResponseSize;
    private final int concurrentRequestMultiplier;
    private final AsyncHttpClient httpClient;
    private final Executor executor;

    @GuardedBy("this")
    private final Set<URI> locations = new HashSet<>();

    @GuardedBy("this")
    private boolean noMoreLocations;

    private final ConcurrentMap<URI, HttpPageBufferClient> allClients = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final Deque<HttpPageBufferClient> queuedClients = new LinkedList<>();

    private final Set<HttpPageBufferClient> completedClients = Sets.newSetFromMap(new ConcurrentHashMap<HttpPageBufferClient, Boolean>());
    private final LinkedBlockingDeque<Page> pageBuffer = new LinkedBlockingDeque<>();

    @GuardedBy("this")
    private final List<SettableFuture<?>> blockedCallers = new ArrayList<>();

    @GuardedBy("this")
    private long bufferBytes;
    @GuardedBy("this")
    private long successfulRequests;
    @GuardedBy("this")
    private long averageBytesPerRequest;

    private final AtomicBoolean closed = new AtomicBoolean();

    public ExchangeClient(DataSize maxBufferedBytes,
            DataSize maxResponseSize,
            int concurrentRequestMultiplier,
            AsyncHttpClient httpClient,
            Executor executor)
    {
        this.maxBufferedBytes = maxBufferedBytes.toBytes();
        this.maxResponseSize = maxResponseSize;
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.httpClient = httpClient;
        this.executor = checkNotSameThreadExecutor(executor, "executor");
    }

    public synchronized ExchangeClientStatus getStatus()
    {
        int bufferedPages = pageBuffer.size();
        if (bufferedPages > 0 && pageBuffer.peekLast() == NO_MORE_PAGES) {
            bufferedPages--;
        }

        ImmutableList.Builder<PageBufferClientStatus> exchangeStatus = ImmutableList.builder();
        for (HttpPageBufferClient client : allClients.values()) {
            exchangeStatus.add(client.getStatus());
        }
        return new ExchangeClientStatus(bufferBytes, averageBytesPerRequest, bufferedPages, exchangeStatus.build());
    }

    public synchronized void addLocation(URI location)
    {
        checkNotNull(location, "location is null");
        if (locations.contains(location)) {
            return;
        }
        checkState(!noMoreLocations, "No more locations already set");
        locations.add(location);
        scheduleRequestIfNecessary();
    }

    public synchronized void noMoreLocations()
    {
        noMoreLocations = true;
        scheduleRequestIfNecessary();
    }

    @Nullable
    public Page pollPage()
    {
        checkState(!Thread.holdsLock(this), "Can not get next page while holding a lock on this");

        if (closed.get()) {
            return null;
        }

        Page page = pageBuffer.poll();
        page = postProcessPage(page);
        return page;
    }

    @Nullable
    public Page getNextPage(Duration maxWaitTime)
            throws InterruptedException
    {
        checkState(!Thread.holdsLock(this), "Can not get next page while holding a lock on this");

        if (closed.get()) {
            return null;
        }

        scheduleRequestIfNecessary();

        Page page = pageBuffer.poll();
        // only wait for a page if we have remote clients
        if (page == null && maxWaitTime.toMillis() >= 1 && !allClients.isEmpty()) {
            page = pageBuffer.poll(maxWaitTime.toMillis(), TimeUnit.MILLISECONDS);
        }

        page = postProcessPage(page);
        return page;
    }

    private Page postProcessPage(Page page)
    {
        checkState(!Thread.holdsLock(this), "Can not get next page while holding a lock on this");

        if (page == NO_MORE_PAGES) {
            // mark client closed
            closed.set(true);

            // add end marker back to queue
            checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
            notifyBlockedCallers();

            // don't return end of stream marker
            page = null;
        }

        if (page != null) {
            synchronized (this) {
                bufferBytes -= page.getDataSize().toBytes();
            }
            if (!closed.get() && pageBuffer.peek() == NO_MORE_PAGES) {
                closed.set(true);
            }
            scheduleRequestIfNecessary();
        }
        return page;
    }

    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public synchronized void close()
    {
        closed.set(true);
        for (HttpPageBufferClient client : allClients.values()) {
            Closeables.closeQuietly(client);
        }
        pageBuffer.clear();
        bufferBytes = 0;
        if (pageBuffer.peekLast() != NO_MORE_PAGES) {
            checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
        }
        notifyBlockedCallers();
    }

    public synchronized void scheduleRequestIfNecessary()
    {
        if (closed.get()) {
            return;
        }

        // if finished, add the end marker
        if (noMoreLocations && completedClients.size() == locations.size()) {
            if (pageBuffer.peekLast() != NO_MORE_PAGES) {
                checkState(pageBuffer.add(NO_MORE_PAGES), "Could not add no more pages marker");
            }
            if (!closed.get() && pageBuffer.peek() == NO_MORE_PAGES) {
                closed.set(true);
            }
            notifyBlockedCallers();
            return;
        }

        // add clients for new locations
        for (URI location : locations) {
            if (!allClients.containsKey(location)) {
                HttpPageBufferClient client = new HttpPageBufferClient(httpClient, maxResponseSize, location, new ExchangeClientCallback(), executor);
                allClients.put(location, client);
                queuedClients.add(client);
            }
        }

        if (bufferBytes > maxBufferedBytes) {
            return;
        }

        long neededBytes = maxBufferedBytes - bufferBytes;
        if (neededBytes <= 0) {
            return;
        }

        int clientCount = (int) ((1.0 * neededBytes / averageBytesPerRequest) * concurrentRequestMultiplier);
        clientCount = Math.max(clientCount, 1);

        int pendingClients = allClients.size() - queuedClients.size() - completedClients.size();
        clientCount -= pendingClients;

        for (int i = 0; i < clientCount; i++) {
            HttpPageBufferClient client = queuedClients.poll();
            if (client == null) {
                // no more clients available
                return;
            }
            client.scheduleRequest();
        }
    }

    public synchronized ListenableFuture<?> isBlocked()
    {
        if (closed.get() || pageBuffer.peek() != null) {
            return Futures.immediateFuture(true);
        }
        SettableFuture<?> future = SettableFuture.create();
        blockedCallers.add(future);
        return future;
    }

    private synchronized void addPage(Page page)
    {
        if (closed.get()) {
            return;
        }

        pageBuffer.add(page);

        // notify all blocked callers
        notifyBlockedCallers();

        bufferBytes += page.getDataSize().toBytes();
        successfulRequests++;

        // AVG_n = AVG_(n-1) * (n-1)/n + VALUE_n / n
        averageBytesPerRequest = (long) (1.0 * averageBytesPerRequest * (successfulRequests - 1) / successfulRequests + page.getDataSize().toBytes() / successfulRequests);

        scheduleRequestIfNecessary();
    }

    private synchronized void notifyBlockedCallers()
    {
        List<SettableFuture<?>> callers = ImmutableList.copyOf(blockedCallers);
        blockedCallers.clear();
        for (SettableFuture<?> blockedCaller : callers) {
            blockedCaller.set(null);
        }
    }

    private synchronized void requestComplete(HttpPageBufferClient client)
    {
        if (!queuedClients.contains(client)) {
            queuedClients.add(client);
        }
        scheduleRequestIfNecessary();
    }

    private synchronized void clientFinished(HttpPageBufferClient client)
    {
        checkNotNull(client, "client is null");
        completedClients.add(client);
        scheduleRequestIfNecessary();
    }

    private class ExchangeClientCallback
            implements ClientCallback
    {
        @Override
        public void addPage(HttpPageBufferClient client, Page page)
        {
            checkNotNull(client, "client is null");
            checkNotNull(page, "page is null");
            ExchangeClient.this.addPage(page);
            scheduleRequestIfNecessary();
        }

        @Override
        public void requestComplete(HttpPageBufferClient client)
        {
            checkNotNull(client, "client is null");
            ExchangeClient.this.requestComplete(client);
        }

        @Override
        public void clientFinished(HttpPageBufferClient client)
        {
            ExchangeClient.this.clientFinished(client);
        }
    }
}
