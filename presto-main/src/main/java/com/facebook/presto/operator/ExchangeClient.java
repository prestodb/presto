/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.operator.HttpPageBufferClient.ClientCallback;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.synchronizedSet;

public class ExchangeClient
    implements Closeable
{
    private final int maxBufferedPages;
    private final int expectedPagesPerRequest;
    private final int concurrentRequestMultiplier;
    private final AsyncHttpClient httpClient;
    private final Set<URI> locations;
    private final AtomicBoolean noMoreLocations;

    private final Map<URI, HttpPageBufferClient> allClients = new HashMap<>();
    private final LinkedBlockingDeque<HttpPageBufferClient> queuedClients = new LinkedBlockingDeque<>();
    private final Set<HttpPageBufferClient> completedClients = synchronizedSet(new HashSet<HttpPageBufferClient>());

    private final LinkedBlockingDeque<Page> pageBuffer = new LinkedBlockingDeque<>();

    private final AtomicBoolean closed = new AtomicBoolean();

    public ExchangeClient(int maxBufferedPages,
            int expectedPagesPerRequest,
            int concurrentRequestMultiplier,
            AsyncHttpClient httpClient,
            Set<URI> locations,
            AtomicBoolean noMoreLocations)
    {
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.maxBufferedPages = maxBufferedPages;
        this.expectedPagesPerRequest = expectedPagesPerRequest;
        this.httpClient = httpClient;
        this.locations = locations;
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

    @Nullable
    public Page getNextPage(Duration maxWaitTime)
            throws InterruptedException
    {
        // There is a safe race condition here. This code is checking the completed
        // clients list which can change while the code is executing, but this fine
        // since client state only moves in one direction from running to closed.
        boolean allClientsComplete = noMoreLocations.get() && completedClients.size() == locations.size();

        if (!allClientsComplete) {
            scheduleRequestIfNecessary();
        }

        Page page = pageBuffer.poll((long) maxWaitTime.toMillis(), TimeUnit.MILLISECONDS);
        if (allClientsComplete && page == null) {
            closed.set(true);
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
        for (HttpPageBufferClient client : allClients.values()) {
            Closeables.closeQuietly(client);
        }
        closed.set(true);
    }

    private synchronized void scheduleRequestIfNecessary()
    {
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
            queuedClients.add(client);
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
