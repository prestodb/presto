/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.split.Split;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.newSetFromMap;

public class ExchangeOperator
        implements SourceOperator
{
    private static final Duration WAIT_TIME = new Duration(10, TimeUnit.MILLISECONDS);

    private final AsyncHttpClient httpClient;
    private final List<TupleInfo> tupleInfos;
    private final Set<URI> locations = newSetFromMap(new ConcurrentHashMap<URI, Boolean>());
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
        private final ExchangeClient exchangeClient;

        public ExchangePageIterator(OperatorStats operatorStats,
                int maxBufferedPages,
                int expectedPagesPerRequest,
                int concurrentRequestMultiplier,
                AsyncHttpClient httpClient,
                Iterable<TupleInfo> tupleInfos,
                Set<URI> locations,
                AtomicBoolean noMoreLocations)
        {
            super(tupleInfos);

            this.operatorStats = operatorStats;
            this.exchangeClient = new ExchangeClient(maxBufferedPages, expectedPagesPerRequest, concurrentRequestMultiplier, httpClient, locations, noMoreLocations);
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
