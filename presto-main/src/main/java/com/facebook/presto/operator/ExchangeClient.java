/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.operator.HttpPageBufferClient.ClientCallback;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.net.URI;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExchangeClient
        implements Closeable
{
    private static final Page NO_MORE_PAGES = new Page(0);

    private final int maxBufferedPages;
    private final int expectedPagesPerRequest;
    private final int concurrentRequestMultiplier;
    private final AsyncHttpClient httpClient;

    @GuardedBy("this")
    private final Set<URI> locations = new HashSet<>();

    @GuardedBy("this")
    private boolean noMoreLocations;

    private final Map<URI, HttpPageBufferClient> allClients = new ConcurrentHashMap<>();
    private final Deque<HttpPageBufferClient> queuedClients = new LinkedList<>();
    private final Set<HttpPageBufferClient> completedClients = Sets.newSetFromMap(new ConcurrentHashMap<HttpPageBufferClient, Boolean>());

    private final LinkedBlockingDeque<Page> pageBuffer = new LinkedBlockingDeque<>();

    private final AtomicBoolean closed = new AtomicBoolean();

    public ExchangeClient(int maxBufferedPages,
            int expectedPagesPerRequest,
            int concurrentRequestMultiplier,
            AsyncHttpClient httpClient)
    {
        this(maxBufferedPages, expectedPagesPerRequest, concurrentRequestMultiplier, httpClient, ImmutableSet.<URI>of(), false);
    }

    public ExchangeClient(int maxBufferedPages,
            int expectedPagesPerRequest,
            int concurrentRequestMultiplier,
            AsyncHttpClient httpClient,
            Set<URI> locations,
            boolean noMoreLocations)
    {
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.maxBufferedPages = maxBufferedPages;
        this.expectedPagesPerRequest = expectedPagesPerRequest;
        this.httpClient = httpClient;
        this.locations.addAll(locations);
        this.noMoreLocations = noMoreLocations;
    }

    public List<ExchangeClientStatus> getStatus()
    {
        ImmutableList.Builder<ExchangeClientStatus> exchangeStatus = ImmutableList.builder();
        for (HttpPageBufferClient client : allClients.values()) {
            exchangeStatus.add(client.getStatus());
        }
        return exchangeStatus.build();
    }

    public synchronized void addLocation(URI location)
    {
        Preconditions.checkNotNull(location, "location is null");
        if (locations.contains(location)) {
            return;
        }
        Preconditions.checkState(!noMoreLocations, "No more locations already set");
        locations.add(location);
        scheduleRequestIfNecessary();
    }

    public synchronized void noMoreLocations()
    {
        noMoreLocations = true;
        scheduleRequestIfNecessary();
    }

    @Nullable
    public Page getNextPage(Duration maxWaitTime)
            throws InterruptedException
    {
        scheduleRequestIfNecessary();

        Page page = pageBuffer.poll();
        // only wait for a page if we have remote clients
        if (page == null && !allClients.isEmpty()) {
            page = pageBuffer.poll((long) maxWaitTime.toMillis(), TimeUnit.MILLISECONDS);
        }

        if (page == NO_MORE_PAGES) {
            // mark client closed
            closed.set(true);

            // add end marker back to queue
            pageBuffer.offer(NO_MORE_PAGES);

            // don't return end of stream marker
            page = null;
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
        if (pageBuffer.peek() != NO_MORE_PAGES) {
            pageBuffer.offer(NO_MORE_PAGES);
        }
    }

    public synchronized void scheduleRequestIfNecessary()
    {
        // if finished, add the end marker
        if (noMoreLocations && completedClients.size() == locations.size()) {
            if (pageBuffer.peek() != NO_MORE_PAGES) {
                pageBuffer.offer(NO_MORE_PAGES);
            }
            return;
        }

        // add clients for new locations
        for (URI location : locations) {
            if (!allClients.containsKey(location)) {
                HttpPageBufferClient client = new HttpPageBufferClient(httpClient, location, new ExchangeClientCallback());
                allClients.put(location, client);
                queuedClients.add(client);
            }
        }

        int bufferedPages = pageBuffer.size();
        if (bufferedPages >= maxBufferedPages) {
            return;
        }

        int neededPages = maxBufferedPages - bufferedPages;
        int clientCount = (int) ((1.0 * neededPages / expectedPagesPerRequest) * concurrentRequestMultiplier);
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

    private synchronized void addClientToQueue(HttpPageBufferClient client)
    {
        if (!queuedClients.contains(client)) {
            queuedClients.add(client);
        }
    }

    private class ExchangeClientCallback
            implements ClientCallback
    {
        @Override
        public void addPage(HttpPageBufferClient client, Page page)
        {
            Preconditions.checkNotNull(client, "client is null");
            Preconditions.checkNotNull(page, "page is null");
            pageBuffer.add(page);
            scheduleRequestIfNecessary();
        }

        @Override
        public void requestComplete(HttpPageBufferClient client)
        {
            Preconditions.checkNotNull(client, "client is null");
            addClientToQueue(client);
            scheduleRequestIfNecessary();
        }

        @Override
        public void bufferFinished(HttpPageBufferClient client)
        {
            Preconditions.checkNotNull(client, "client is null");
            completedClients.add(client);
            scheduleRequestIfNecessary();
        }
    }
}
