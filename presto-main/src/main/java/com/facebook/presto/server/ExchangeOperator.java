/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.AbstractPageIterator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.server.HttpPageBufferClient.ClientCallback;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.synchronizedSet;

public class ExchangeOperator
        implements Operator
{
    private static final Duration WAIT_TIME = new Duration(1, TimeUnit.SECONDS);
    private static final int WAIT_TIME_IN_MILLIS = (int) WAIT_TIME.toMillis();

    private final AsyncHttpClient httpClient;
    private final List<TupleInfo> tupleInfos;
    private final List<URI> locations;

    private final int maxBufferedPages;
    private final int expectedPagesPerRequest;
    private final int concurrentRequestMultiplier;

    public ExchangeOperator(AsyncHttpClient httpClient, List<TupleInfo> tupleInfos, URI... locations)
    {
        this(httpClient, tupleInfos, 10, 3, 100, ImmutableList.copyOf(locations));
    }

    public ExchangeOperator(AsyncHttpClient httpClient,
            Iterable<TupleInfo> tupleInfos,
            int maxBufferedPages,
            int expectedPagesPerRequest,
            int concurrentRequestMultiplier,
            Iterable<URI> locations)
    {
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.maxBufferedPages = maxBufferedPages;
        this.expectedPagesPerRequest = expectedPagesPerRequest;
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");
        Preconditions.checkNotNull(locations, "locations is null");

        this.httpClient = httpClient;
        this.tupleInfos = ImmutableList.copyOf(tupleInfos);
        this.locations = ImmutableList.copyOf(locations);
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
        return new ExchangePageIterator(operatorStats, maxBufferedPages, expectedPagesPerRequest, concurrentRequestMultiplier, httpClient, tupleInfos, locations);
    }

    private static class ExchangePageIterator
            extends AbstractPageIterator
    {
        private final OperatorStats operatorStats;

        private final int maxBufferedPages;
        private final int expectedPagesPerRequest;
        private final int concurrentRequestMultiplier;

        private final Set<HttpPageBufferClient> allClients;
        private final LinkedBlockingDeque<HttpPageBufferClient> queuedClients = new LinkedBlockingDeque<>();
        private final Set<HttpPageBufferClient> completedClients = synchronizedSet(new HashSet<HttpPageBufferClient>());

        private final LinkedBlockingDeque<Page> pageBuffer = new LinkedBlockingDeque<>();

        public ExchangePageIterator(OperatorStats operatorStats,
                int maxBufferedPages,
                int expectedPagesPerRequest,
                int concurrentRequestMultiplier,
                AsyncHttpClient httpClient,
                Iterable<TupleInfo> tupleInfos,
                List<URI> locations)
        {
            super(tupleInfos);

            this.operatorStats = operatorStats;

            this.concurrentRequestMultiplier = concurrentRequestMultiplier;
            this.maxBufferedPages = maxBufferedPages;
            this.expectedPagesPerRequest = expectedPagesPerRequest;

            ImmutableSet.Builder<HttpPageBufferClient> builder = ImmutableSet.builder();
            for (URI location : locations) {
                builder.add(new HttpPageBufferClient(httpClient, location, new ClientCallback()
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

                }));
            }
            this.allClients = builder.build();
            queuedClients.addAll(allClients);
        }

        @Override
        protected Page computeNext()
        {
            while (true) {
                // There is a safe race condition here. This code is checking the completed
                // clients list which can change while the code is executing, but this fine
                // since client state only moves in one direction from running to closed.
                boolean allClientsComplete = completedClients.size() == allClients.size();

                if (!allClientsComplete) {
                    scheduleRequestIfNecessary();
                }

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
            int bufferedPages = pageBuffer.size();
            if (bufferedPages >= maxBufferedPages) {
                return;
            }

            int neededPages = maxBufferedPages - bufferedPages;
            int clientCount = (int) ((1.0 * neededPages / expectedPagesPerRequest) * concurrentRequestMultiplier);
            clientCount = Math.min(clientCount, 1);
            for (int i = 0; i < clientCount; i++) {
                HttpPageBufferClient client = queuedClients.poll();
                if (client == null) {
                    // no more clients available
                    return;
                }
                client.scheduleRequest();
            }
        }

        @Override
        protected void doClose()
        {
            for (HttpPageBufferClient client : allClients) {
                Closeables.closeQuietly(client);
            }
        }
    }
}
