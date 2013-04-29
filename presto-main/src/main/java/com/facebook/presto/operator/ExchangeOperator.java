/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.spi.Split;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ExchangeOperator
        implements SourceOperator
{
    private static final Duration WAIT_TIME = new Duration(10, TimeUnit.MILLISECONDS);

    private final AsyncHttpClient httpClient;
    private final List<TupleInfo> tupleInfos;
    private final Set<URI> locations = new HashSet<>();
    private boolean noMoreLocations;

    private final int maxBufferedPages;
    private final int expectedPagesPerRequest;
    private final int concurrentRequestMultiplier;

    @GuardedBy("this")
    private final Set<WeakReference<ExchangeClient>> activeClients = new HashSet<>();

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
        URI location;
        synchronized (this) {
            Preconditions.checkNotNull(split, "split is null");
            Preconditions.checkArgument(split instanceof RemoteSplit, "split is not a remote split");
            Preconditions.checkState(!noMoreLocations, "No more splits already set");
            location = ((RemoteSplit) split).getLocation();
            if (!locations.add(location)) {
                return;
            }
        }

        for (WeakReference<ExchangeClient> activeClientReference : activeClients) {
            ExchangeClient activeClient = activeClientReference.get();
            if (activeClient != null) {
                activeClient.addLocation(location);
            }
        }
    }

    @Override
    public void noMoreSplits()
    {
        synchronized (this) {
            if (noMoreLocations) {
                return;
            }
            noMoreLocations = true;
        }
        for (WeakReference<ExchangeClient> activeClientReference : activeClients) {
            ExchangeClient activeClient = activeClientReference.get();
            if (activeClient != null) {
                activeClient.noMoreLocations();
            }
        }
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

    private ExchangeClient createExchangeClient()
    {
        ExchangeClient exchangeClient;
        synchronized (this) {
            exchangeClient = new ExchangeClient(maxBufferedPages, expectedPagesPerRequest, concurrentRequestMultiplier, httpClient, locations, noMoreLocations);
            activeClients.add(new WeakReference<>(exchangeClient));
        }

        exchangeClient.scheduleRequestIfNecessary();
        return exchangeClient;
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        ExchangeClient exchangeClient = createExchangeClient();
        return new ExchangePageIterator(tupleInfos, operatorStats, exchangeClient);
    }

    private static class ExchangePageIterator
            extends AbstractPageIterator
    {
        private final OperatorStats operatorStats;
        private final ExchangeClient exchangeClient;

        private ExchangePageIterator(Iterable<TupleInfo> tupleInfos, OperatorStats operatorStats, ExchangeClient exchangeClient)
        {
            super(tupleInfos);
            this.operatorStats = operatorStats;
            this.exchangeClient = exchangeClient;
        }

        @Override
        protected Page computeNext()
        {
            while (true) {
                operatorStats.setExchangeStatus(exchangeClient.getStatus());

                long start = System.nanoTime();
                try {
                    Page page = exchangeClient.getNextPage(WAIT_TIME);
                    if (page != null) {
                        operatorStats.addCompletedDataSize(page.getDataSize().toBytes());
                        operatorStats.addCompletedPositions(page.getPositionCount());
                        return page;
                    }
                    else if (exchangeClient.isClosed()) {
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

        @Override
        protected void doClose()
        {
            exchangeClient.close();
            operatorStats.setExchangeStatus(exchangeClient.getStatus());
        }
    }
}
