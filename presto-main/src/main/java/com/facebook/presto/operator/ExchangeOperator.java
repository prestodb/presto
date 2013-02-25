/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.operator.HttpPageBufferClient.ClientCallback;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.split.Split;
import com.facebook.presto.tuple.TupleInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.synchronizedSet;

public class ExchangeOperator
        implements SourceOperator
{
    private static final Duration WAIT_TIME = new Duration(10, TimeUnit.MILLISECONDS);
    private static final int WAIT_TIME_IN_MILLIS = (int) WAIT_TIME.toMillis();

    private final AsyncHttpClient httpClient;
    private final List<TupleInfo> tupleInfos;
    private final List<URI> locations = new CopyOnWriteArrayList<>();
    private final AtomicBoolean noMoreLocations = new AtomicBoolean();

    private final int maxBufferedPages;
    private final int expectedPagesPerRequest;
    private final int concurrentRequestMultiplier;

    public ExchangeOperator(AsyncHttpClient httpClient, List<TupleInfo> tupleInfos, URI... locations)
    {
        this(httpClient, tupleInfos, 10, 3, 100);
        this.locations.addAll(ImmutableList.copyOf(locations));
        noMoreSplits();
    }

    public ExchangeOperator(AsyncHttpClient httpClient,
            Iterable<TupleInfo> tupleInfos,
            int maxBufferedPages,
            int expectedPagesPerRequest,
            int concurrentRequestMultiplier)
    {
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");

        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.maxBufferedPages = maxBufferedPages;
        this.expectedPagesPerRequest = expectedPagesPerRequest;

        this.httpClient = httpClient;
        this.tupleInfos = ImmutableList.copyOf(tupleInfos);
    }

    @Override
    public synchronized void addSplit(Split split)
    {
        Preconditions.checkNotNull(split, "split is null");
        Preconditions.checkArgument(split instanceof RemoteSplit, "split is not a %s", RemoteSplit.class.getSimpleName());
        Preconditions.checkState(!noMoreLocations.get(), "No more splits already set");
        URI location = ((RemoteSplit) split).getLocation();
        locations.add(location);
    }

    @Override
    public synchronized void noMoreSplits()
    {
        noMoreLocations.set(true);
    }

    @Override
    public int getChannelCount()
    {
        return tupleInfos.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        return new ExchangePageIterator(operatorStats, maxBufferedPages, expectedPagesPerRequest, concurrentRequestMultiplier, httpClient, tupleInfos, locations, noMoreLocations);
    }

    private static class ExchangePageIterator
            extends AbstractPageIterator
    {
        private final OperatorStats operatorStats;

        private final int maxBufferedPages;
        private final int expectedPagesPerRequest;
        private final int concurrentRequestMultiplier;
        private final AsyncHttpClient httpClient;
        private final List<URI> locations;
        private final AtomicBoolean noMoreLocations;

        private final Map<URI, HttpPageBufferClient> allClients = new HashMap<>();
        private final LinkedBlockingDeque<HttpPageBufferClient> queuedClients = new LinkedBlockingDeque<>();
        private final Set<HttpPageBufferClient> completedClients = synchronizedSet(new HashSet<HttpPageBufferClient>());

        private final LinkedBlockingDeque<Page> pageBuffer = new LinkedBlockingDeque<>();

        public ExchangePageIterator(OperatorStats operatorStats,
                int maxBufferedPages,
                int expectedPagesPerRequest,
                int concurrentRequestMultiplier,
                AsyncHttpClient httpClient,
                Iterable<TupleInfo> tupleInfos,
                List<URI> locations,
                AtomicBoolean noMoreLocations)
        {
            super(tupleInfos);

            this.operatorStats = operatorStats;

            this.concurrentRequestMultiplier = concurrentRequestMultiplier;
            this.maxBufferedPages = maxBufferedPages;
            this.expectedPagesPerRequest = expectedPagesPerRequest;
            this.httpClient = httpClient;
            this.locations = locations;
            this.noMoreLocations = noMoreLocations;
        }

        @Override
        protected Page computeNext()
        {
            while (true) {
                // There is a safe race condition here. This code is checking the completed
                // clients list which can change while the code is executing, but this fine
                // since client state only moves in one direction from running to closed.
                boolean allClientsComplete = noMoreLocations.get() && completedClients.size() == locations.size();

                if (!allClientsComplete) {
                    scheduleRequestIfNecessary();
                }

                operatorStats.setExchangeStatus(getExchangeStatus());

                long start = System.nanoTime();
                try {
                    Page page = pageBuffer.poll(WAIT_TIME_IN_MILLIS, TimeUnit.MILLISECONDS);
                    if (page != null) {
                        return page;
                    }
                    else if (allClientsComplete) {
                        // buffer is empty and all clients are complete, so we are done
                        return endOfData();
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
                finally {
                    operatorStats.addExchangeWaitTime(Duration.nanosSince(start));
                }
            }
        }

        private void scheduleRequestIfNecessary()
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

        private List<ExchangeClientStatus> getExchangeStatus()
        {
            ImmutableList.Builder<ExchangeClientStatus> exchangeStatus = ImmutableList.builder();
            for (HttpPageBufferClient client : allClients.values()) {
                exchangeStatus.add(client.getStatus());
            }
            return exchangeStatus.build();
        }

        @Override
        protected void doClose()
        {
            for (HttpPageBufferClient client : allClients.values()) {
                Closeables.closeQuietly(client);
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
            }

            @Override
            public void requestComplete(HttpPageBufferClient client)
            {
                Preconditions.checkNotNull(client, "client is null");
                queuedClients.add(client);
            }

            @Override
            public void bufferFinished(HttpPageBufferClient client)
            {
                Preconditions.checkNotNull(client, "client is null");
                completedClients.add(client);
            }
        }
    }

    public static class ExchangeClientStatus
    {
        private final URI uri;
        private final String state;
        private final DateTime lastUpdate;

        @JsonCreator
        public ExchangeClientStatus(@JsonProperty("uri") URI uri, @JsonProperty("state") String state, @JsonProperty("lastUpdate") DateTime lastUpdate)
        {
            this.uri = uri;
            this.state = state;
            this.lastUpdate = lastUpdate;
        }

        @JsonProperty
        public URI getUri()
        {
            return uri;
        }

        @JsonProperty
        public String getState()
        {
            return state;
        }

        @JsonProperty
        public DateTime getLastUpdate()
        {
            return lastUpdate;
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("uri", uri)
                    .add("state", state)
                    .add("lastUpdate", lastUpdate)
                    .toString();
        }
    }
}
