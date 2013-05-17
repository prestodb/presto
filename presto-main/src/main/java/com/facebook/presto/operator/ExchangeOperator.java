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
import com.google.inject.Provider;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ExchangeOperator
        implements SourceOperator
{
    private static final Duration WAIT_TIME = new Duration(10, TimeUnit.MILLISECONDS);

    private final List<TupleInfo> tupleInfos;
    private final Set<URI> locations = new HashSet<>();
    private final Provider<ExchangeClient> exchangeClientProvider;
    private boolean noMoreLocations;

    @GuardedBy("this")
    private ExchangeClient activeClient;

    public ExchangeOperator(Provider<ExchangeClient> exchangeClientProvider, Iterable<TupleInfo> tupleInfos)
    {
        this.exchangeClientProvider = checkNotNull(exchangeClientProvider, "exchangeClientProvider is null");


        this.tupleInfos = ImmutableList.copyOf(checkNotNull(tupleInfos, "tupleInfos is null"));
    }

    @Override
    public synchronized void addSplit(Split split)
    {
        URI location;
        synchronized (this) {
            checkNotNull(split, "split is null");
            Preconditions.checkArgument(split instanceof RemoteSplit, "split is not a remote split");
            checkState(!noMoreLocations, "No more splits already set");
            location = ((RemoteSplit) split).getLocation();
            if (!locations.add(location)) {
                return;
            }
        }

        if (activeClient != null) {
            activeClient.addLocation(location);
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
        if (activeClient != null) {
            activeClient.noMoreLocations();
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
            checkState(activeClient == null, "ExchangeOperator can only be used once");
            exchangeClient = exchangeClientProvider.get();
            for (URI location : locations) {
                exchangeClient.addLocation(location);
            }
            if (noMoreLocations) {
                exchangeClient.noMoreLocations();
            }
            activeClient = exchangeClient;
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
                    else if (operatorStats.isDone() || exchangeClient.isClosed()) {
                        // buffer is empty and all clients are complete, so we are done
                        exchangeClient.close();
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
